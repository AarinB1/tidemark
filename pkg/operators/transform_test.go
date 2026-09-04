package operators

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/state"
)

// emitContext records what an operator emits. It stands in for the runtime's
// context, which does the same thing into a set of channels.
type emitContext struct {
	emitted   []*core.Record
	watermark int64
	state     state.KeyedState
}

func (c *emitContext) Emit(rec *core.Record)   { c.emitted = append(c.emitted, rec) }
func (c *emitContext) CurrentWatermark() int64 { return c.watermark }

// Subtask stands in for the identity the runtime assigns. A test driving one
// operator by hand is subtask 0 of a vertex it does not otherwise name.
func (c *emitContext) Subtask() (string, int) { return "test", 0 }

// State hands out one Memory per context, made on first use so that a test that
// never touches state does not carry one. It stands in for the per-subtask
// state the runtime provides; a test sharing one across two operators would be
// sharing what the runtime keeps apart.
func (c *emitContext) State() state.KeyedState {
	if c.state == nil {
		c.state = state.NewMemory()
	}
	return c.state
}

func rec(key string, eventTime int64) *core.Record {
	return &core.Record{Key: []byte(key), Value: []byte(key), EventTime: eventTime}
}

func keys(recs []*core.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Key)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var errBoom = errors.New("boom")

func TestMapProcessElement(t *testing.T) {
	upper := func(r *core.Record) (*core.Record, error) {
		return &core.Record{Key: bytes.ToUpper(r.Key), Value: r.Value, EventTime: r.EventTime}, nil
	}
	dropB := func(r *core.Record) (*core.Record, error) {
		if string(r.Key) == "b" {
			return nil, nil
		}
		return r, nil
	}
	failOnB := func(r *core.Record) (*core.Record, error) {
		if string(r.Key) == "b" {
			return nil, errBoom
		}
		return r, nil
	}

	tests := []struct {
		name    string
		fn      func(*core.Record) (*core.Record, error)
		input   []*core.Record
		want    []string
		wantErr error
	}{
		{
			name:  "identity",
			fn:    func(r *core.Record) (*core.Record, error) { return r, nil },
			input: []*core.Record{rec("a", 1), rec("b", 2)},
			want:  []string{"a", "b"},
		},
		{
			name:  "transforms",
			fn:    upper,
			input: []*core.Record{rec("a", 1), rec("b", 2)},
			want:  []string{"A", "B"},
		},
		{
			name:  "nil result drops the record",
			fn:    dropB,
			input: []*core.Record{rec("a", 1), rec("b", 2), rec("c", 3)},
			want:  []string{"a", "c"},
		},
		{
			name:    "error propagates",
			fn:      failOnB,
			input:   []*core.Record{rec("a", 1), rec("b", 2)},
			want:    []string{"a"},
			wantErr: errBoom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap(tt.fn)
			ctx := &emitContext{}
			if err := m.Open(ctx); err != nil {
				t.Fatalf("Open: %v", err)
			}
			var gotErr error
			for _, r := range tt.input {
				if err := m.ProcessElement(r, ctx); err != nil {
					gotErr = err
					break
				}
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("error = %v, want %v", gotErr, tt.wantErr)
			}
			if got := keys(ctx.emitted); !equalStrings(got, tt.want) {
				t.Errorf("emitted %v, want %v", got, tt.want)
			}
			if err := m.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

func TestFilterProcessElement(t *testing.T) {
	tests := []struct {
		name  string
		pred  func(*core.Record) bool
		input []*core.Record
		want  []string
	}{
		{
			name:  "keeps everything",
			pred:  func(*core.Record) bool { return true },
			input: []*core.Record{rec("a", 1), rec("b", 2)},
			want:  []string{"a", "b"},
		},
		{
			name:  "drops everything",
			pred:  func(*core.Record) bool { return false },
			input: []*core.Record{rec("a", 1), rec("b", 2)},
			want:  nil,
		},
		{
			name:  "selects by event time",
			pred:  func(r *core.Record) bool { return r.EventTime%2 == 0 },
			input: []*core.Record{rec("a", 1), rec("b", 2), rec("c", 3), rec("d", 4)},
			want:  []string{"b", "d"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFilter(tt.pred)
			ctx := &emitContext{}
			if err := f.Open(ctx); err != nil {
				t.Fatalf("Open: %v", err)
			}
			for _, r := range tt.input {
				if err := f.ProcessElement(r, ctx); err != nil {
					t.Fatalf("ProcessElement: %v", err)
				}
			}
			if got := keys(ctx.emitted); !equalStrings(got, tt.want) {
				t.Errorf("emitted %v, want %v", got, tt.want)
			}
			if err := f.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

// TestControlMethodsEmitNothing pins the division of labour: the runtime
// broadcasts watermarks and end-of-stream to every downstream channel, so an
// operator that emitted them here would duplicate them.
func TestControlMethodsEmitNothing(t *testing.T) {
	ops := map[string]core.Operator{
		"map":    NewMap(func(r *core.Record) (*core.Record, error) { return r, nil }),
		"filter": NewFilter(func(*core.Record) bool { return true }),
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			ctx := &emitContext{watermark: 99}
			if err := op.ProcessWatermark(1234, ctx); err != nil {
				t.Fatalf("ProcessWatermark: %v", err)
			}
			if err := op.OnEndOfStream(ctx); err != nil {
				t.Fatalf("OnEndOfStream: %v", err)
			}
			if len(ctx.emitted) != 0 {
				t.Errorf("emitted %d records from control methods, want 0", len(ctx.emitted))
			}
		})
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	ops := map[string]core.Operator{
		"map":    NewMap(func(r *core.Record) (*core.Record, error) { return r, nil }),
		"filter": NewFilter(func(*core.Record) bool { return true }),
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := op.Snapshot(&buf); err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if buf.Len() != 0 {
				t.Errorf("Snapshot wrote %d bytes, want 0", buf.Len())
			}

			// Restore drains whatever it is handed, leaving the reader at its
			// end rather than part way through.
			r := bytes.NewReader([]byte("state from a later phase"))
			if err := op.Restore(r); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if n, err := r.Read(make([]byte, 1)); err != io.EOF || n != 0 {
				t.Errorf("Restore left %d bytes unread (err %v)", r.Len(), err)
			}
		})
	}
}
