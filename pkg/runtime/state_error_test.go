package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// errBackendFailed is what the fake backend below records. Tests match on it
// with errors.Is, so a subtask that reported some other failure at the same
// moment does not look like a pass.
var errBackendFailed = errors.New("state backend failed")

// failingState is a KeyedState that records an error the first time a named
// operation is called, and behaves like the memory backend otherwise.
//
// Every method is written out rather than promoted from an embedded
// KeyedState. Embedding would compile and would forward the operations this
// test does not name, so a fake that failed to override the one under test
// would silently pass through to a backend that works — the same trap
// CLAUDE.md records for source decorators, in a different package.
//
// After it has failed it returns ZERO VALUES and keeps accepting calls, which
// is the contract state.KeyedState.Err documents: an implementation that
// panicked or blocked would test a design nothing implements.
type failingState struct {
	inner *state.Memory
	// failOn names the operation that fails: "Get", "Put", "Delete", "Iterate",
	// or "" for a backend that never fails.
	failOn string
	err    error
}

func newFailingState(failOn string) *failingState {
	return &failingState{inner: state.NewMemory(), failOn: failOn}
}

// record fails the backend if op is the operation this fake is set to fail on.
// The FIRST error is kept, matching the interface.
func (f *failingState) record(op string) bool {
	if op == f.failOn && f.err == nil {
		f.err = fmt.Errorf("%s: %w", op, errBackendFailed)
	}
	return f.err != nil
}

func (f *failingState) Get(key []byte) ([]byte, bool) {
	if f.record("Get") {
		return nil, false
	}
	return f.inner.Get(key)
}

func (f *failingState) Put(key, value []byte) {
	if f.record("Put") {
		return
	}
	f.inner.Put(key, value)
}

func (f *failingState) Delete(key []byte) {
	if f.record("Delete") {
		return
	}
	f.inner.Delete(key)
}

func (f *failingState) Iterate(fn func(key, value []byte) bool) {
	if f.record("Iterate") {
		return
	}
	f.inner.Iterate(fn)
}

func (f *failingState) Err() error { return f.err }

var _ state.KeyedState = (*failingState)(nil)

// statefulOperator touches state on every call the runtime makes into it, so
// that a backend failing on any one operation reaches the runtime through the
// call that provoked it.
//
// It ignores what it reads. The point is not the computation; it is that an
// operator written the obvious way, checking nothing, still cannot run past a
// failed backend.
type statefulOperator struct {
	st state.KeyedState
}

func (o *statefulOperator) Open(ctx core.Context) error {
	o.st = ctx.State()
	return nil
}

func (o *statefulOperator) ProcessElement(rec *core.Record, ctx core.Context) error {
	key := append([]byte{state.PrefixUserState}, rec.Key...)
	v, _ := o.st.Get(key)
	o.st.Put(key, append(v, 1))
	return nil
}

func (o *statefulOperator) ProcessWatermark(wm int64, ctx core.Context) error {
	o.st.Iterate(func(k, v []byte) bool { return true })
	return nil
}

func (o *statefulOperator) OnEndOfStream(ctx core.Context) error {
	o.st.Delete([]byte{state.PrefixUserState})
	return nil
}

func (o *statefulOperator) Snapshot(io.Writer) error { return nil }
func (o *statefulOperator) Restore(io.Reader) error  { return nil }
func (o *statefulOperator) Close() error             { return nil }

var _ core.Operator = (*statefulOperator)(nil)

// runOperatorWithState drives one operator subtask over elems with st as its
// keyed state, and returns what the subtask reported.
//
// It builds the subtask's plumbing directly rather than going through Run,
// because the state a subtask gets is made by the runtime and there is no way
// to hand it a different one from outside. Everything else is the production
// path: the same gate, the same writer, the same loop.
func runOperatorWithState(t *testing.T, st state.KeyedState, elems []core.StreamElement) error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	in := transport.NewChannel(len(elems) + 1)
	out := transport.NewChannel(len(elems) + 1)
	gate := NewGate(ctx, []transport.Input{in}, faults{})
	w := transport.NewWriter([][]transport.Output{{out}})

	oc := newOpContext(ctx, w)
	oc.state = st
	op := &statefulOperator{}
	if err := op.Open(oc); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Sent before the loop runs and within the channel's capacity, so nothing
	// blocks and the test needs no second goroutine to keep it moving.
	for _, e := range elems {
		if err := in.Send(ctx, e); err != nil {
			t.Fatalf("Send(%s): %v", e.Kind, err)
		}
	}
	in.Close()

	// checkpointer{} is a subtask in a job that takes no checkpoints, which is
	// what every test here wants: the property under test is the state backend,
	// and a coordinator would put a second failure path in the way of it.
	err := runOperatorLoop(ctx, op, oc, subtaskID{vertexID: "op", index: 0}, gate, w, checkpointer{}, faults{})

	// Cancel first: a subtask that returned early leaves its forwarder blocked
	// on a send to the merged channel, and Wait is what proves no goroutine
	// outlives the test.
	cancel()
	gate.Wait()
	return err
}

// TestStateErrorFailsTheSubtask is the property step 2 buys: a backend that
// fails on a read stops the job instead of feeding zero values into an
// aggregate.
//
// The failure is silent by construction. Get returns (nil, false), which is
// indistinguishable from "this key has no state yet", so an operator that
// counted would start a fresh count and the job would finish with a plausible
// number and no error anywhere. Every row below names the runtime call the
// failure has to surface through.
func TestStateErrorFailsTheSubtask(t *testing.T) {
	record := core.NewRecordElement(&core.Record{Key: []byte("k"), Value: []byte("v"), EventTime: 1})

	tests := []struct {
		name    string
		failOn  string
		elems   []core.StreamElement
		wantErr bool
	}{
		{
			name:    "a failed Get during ProcessElement",
			failOn:  "Get",
			elems:   []core.StreamElement{record, core.NewEndOfStreamElement()},
			wantErr: true,
		},
		{
			name:    "a failed Put during ProcessElement",
			failOn:  "Put",
			elems:   []core.StreamElement{record, core.NewEndOfStreamElement()},
			wantErr: true,
		},
		{
			name:    "a failed Iterate during ProcessWatermark",
			failOn:  "Iterate",
			elems:   []core.StreamElement{core.NewWatermarkElement(10), core.NewEndOfStreamElement()},
			wantErr: true,
		},
		{
			name:    "a failed Delete during OnEndOfStream",
			failOn:  "Delete",
			elems:   []core.StreamElement{core.NewEndOfStreamElement()},
			wantErr: true,
		},
		{
			// The control. Without it every row above would pass against a
			// runtime that failed the subtask unconditionally.
			name:    "a backend that never fails",
			failOn:  "",
			elems:   []core.StreamElement{record, core.NewWatermarkElement(10), core.NewEndOfStreamElement()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runOperatorWithState(t, newFailingState(tt.failOn), tt.elems)
			switch {
			case tt.wantErr && !errors.Is(err, errBackendFailed):
				t.Fatalf("subtask returned %v, want the backend's error: a failed state read was swallowed", err)
			case !tt.wantErr && err != nil:
				t.Fatalf("subtask returned %v, want nil", err)
			}
		})
	}
}

// TestStateErrorSurfacesBeforeTheNextElement pins WHEN the runtime looks.
//
// Collecting the stash only at the end of the run would make the job fail, and
// would still be wrong: every element after the failure would be processed
// against a backend returning zero values, so an operator's state would be
// rebuilt from nothing and whatever it emitted on the way would already have
// reached the sink. The subtask must stop at the call that failed.
func TestStateErrorSurfacesBeforeTheNextElement(t *testing.T) {
	st := newFailingState("Get")
	rec := func(key string) core.StreamElement {
		return core.NewRecordElement(&core.Record{Key: []byte(key), Value: []byte("v")})
	}

	if err := runOperatorWithState(t, st, []core.StreamElement{
		rec("a"), rec("b"), rec("c"), core.NewEndOfStreamElement(),
	}); !errors.Is(err, errBackendFailed) {
		t.Fatalf("subtask returned %v, want the backend's error", err)
	}

	// The first record's Get failed, so its Put was skipped and nothing after
	// it ran at all. Anything in the backend means the loop kept going.
	entries := 0
	st.inner.Iterate(func(k, v []byte) bool { entries++; return true })
	if entries != 0 {
		t.Errorf("the backend holds %d entries after failing on the first record: the subtask processed elements past the failure", entries)
	}
}
