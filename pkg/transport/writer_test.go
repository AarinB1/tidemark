package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
)

// collectOutput records everything sent to it. It stands in for a downstream
// subtask that never blocks, so a test of routing is not also a test of
// backpressure.
type collectOutput struct {
	elements []core.StreamElement
	closes   int
	err      error
}

func (o *collectOutput) Send(ctx context.Context, e core.StreamElement) error {
	if o.err != nil {
		return o.err
	}
	o.elements = append(o.elements, e)
	return nil
}

func (o *collectOutput) Close() { o.closes++ }

func newCollectOutputs(n int) ([]Output, []*collectOutput) {
	outs := make([]Output, n)
	collected := make([]*collectOutput, n)
	for i := range collected {
		collected[i] = &collectOutput{}
		outs[i] = collected[i]
	}
	return outs, collected
}

// oneGroup wraps outs as a single group: a vertex with exactly one downstream
// vertex.
//
// Every test below that uses it is also the assertion that the single-group
// case behaves exactly as it did before outputs were grouped. Their bodies are
// unchanged from when NewWriter took a flat list, so if grouping altered
// routing, distribution, error handling, or closing for a vertex with one
// downstream vertex, they would say so.
func oneGroup(outs []Output) [][]Output { return [][]Output{outs} }

// newCollectGroups builds one group of collectors per entry in sizes, so a test
// can give a Writer downstream vertices of differing parallelism.
func newCollectGroups(sizes ...int) ([][]Output, [][]*collectOutput) {
	groups := make([][]Output, len(sizes))
	collected := make([][]*collectOutput, len(sizes))
	for i, n := range sizes {
		groups[i], collected[i] = newCollectOutputs(n)
	}
	return groups, collected
}

// key returns the fixed-width big-endian encoding the generator uses, so the
// tests here route the same byte strings the engine does.
func key(n uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	return buf[:]
}

// TestEmitRecordIsStableForTheSameKey is the property every keyed operator in
// Phase 3 depends on: a key always reaches the same subtask, so the state for
// that key lives in exactly one place.
func TestEmitRecordIsStableForTheSameKey(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 8, 16} {
		t.Run(itoa(n), func(t *testing.T) {
			outs, collected := newCollectOutputs(n)
			w := NewWriter(oneGroup(outs))

			// Where the first copy of each key landed.
			first := make(map[uint64]int)
			for k := range uint64(200) {
				for range 5 {
					if err := w.EmitRecord(context.Background(), &core.Record{Key: key(k)}); err != nil {
						t.Fatalf("EmitRecord: %v", err)
					}
				}
			}

			for i, out := range collected {
				for _, e := range out.elements {
					k := binary.BigEndian.Uint64(e.Record.Key)
					if seen, ok := first[k]; ok && seen != i {
						t.Fatalf("key %d reached output %d and output %d", k, seen, i)
					}
					first[k] = i
				}
			}
			if len(first) != 200 {
				t.Errorf("%d distinct keys arrived, want 200", len(first))
			}
		})
	}
}

// TestEmitRecordRoutesToExactlyOneOutput pins invariant 2 from the far side:
// each record is counted once across all outputs, so nothing is duplicated and
// nothing is dropped.
func TestEmitRecordRoutesToExactlyOneOutput(t *testing.T) {
	const records = 1000
	outs, collected := newCollectOutputs(4)
	w := NewWriter(oneGroup(outs))

	for k := range uint64(records) {
		if err := w.EmitRecord(context.Background(), &core.Record{Key: key(k)}); err != nil {
			t.Fatalf("EmitRecord: %v", err)
		}
	}

	total := 0
	for _, out := range collected {
		for _, e := range out.elements {
			if e.Kind != core.KindRecord {
				t.Fatalf("EmitRecord sent a %s element", e.Kind)
			}
		}
		total += len(out.elements)
	}
	if total != records {
		t.Errorf("%d records arrived across the outputs, want %d", total, records)
	}
}

// TestEmitRecordDistributesEvenly checks the hash spreads keys, not just that it
// is a function. A hash that sent every key to one output would satisfy every
// other test in this file.
func TestEmitRecordDistributesEvenly(t *testing.T) {
	const (
		keys      = 100000
		outputs   = 8
		tolerance = 0.10
	)
	outs, collected := newCollectOutputs(outputs)
	w := NewWriter(oneGroup(outs))

	for k := range uint64(keys) {
		if err := w.EmitRecord(context.Background(), &core.Record{Key: key(k)}); err != nil {
			t.Fatalf("EmitRecord: %v", err)
		}
	}

	mean := float64(keys) / outputs
	for i, out := range collected {
		got := float64(len(out.elements))
		deviation := (got - mean) / mean
		if deviation < -tolerance || deviation > tolerance {
			t.Errorf("output %d holds %d keys, want within %.0f%% of %.0f (off by %.1f%%)",
				i, len(out.elements), tolerance*100, mean, deviation*100)
		}
	}
}

func TestEmitRecordRejectsAnEmptyKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{name: "nil key", key: nil},
		{name: "zero-length key", key: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outs, collected := newCollectOutputs(4)
			w := NewWriter(oneGroup(outs))

			if err := w.EmitRecord(context.Background(), &core.Record{Key: tt.key}); !errors.Is(err, errEmptyKey) {
				t.Fatalf("EmitRecord = %v, want errEmptyKey", err)
			}
			for i, out := range collected {
				if len(out.elements) != 0 {
					t.Errorf("an unkeyed record reached output %d", i)
				}
			}
		})
	}
}

// TestEmitRecordWithNoOutputsDrops covers the sink, which has nothing
// downstream. It is the only case where a record legitimately goes nowhere, and
// an unkeyed record is fine there because no partitioning happens.
func TestEmitRecordWithNoOutputsDrops(t *testing.T) {
	w := NewWriter(nil)
	if err := w.EmitRecord(context.Background(), &core.Record{}); err != nil {
		t.Fatalf("EmitRecord with no outputs: %v", err)
	}
}

// TestBroadcastReachesEveryOutputExactlyOnce is the other half of invariant 2.
func TestBroadcastReachesEveryOutputExactlyOnce(t *testing.T) {
	elements := []core.StreamElement{
		core.NewEndOfStreamElement(),
		core.NewWatermarkElement(1700000000000),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 3}),
	}

	for _, e := range elements {
		t.Run(e.Kind.String(), func(t *testing.T) {
			outs, collected := newCollectOutputs(6)
			w := NewWriter(oneGroup(outs))

			if err := w.Broadcast(context.Background(), e); err != nil {
				t.Fatalf("Broadcast: %v", err)
			}
			for i, out := range collected {
				if len(out.elements) != 1 {
					t.Fatalf("output %d received %d elements, want exactly 1", i, len(out.elements))
				}
				if out.elements[0].Kind != e.Kind {
					t.Errorf("output %d received a %s element, want %s", i, out.elements[0].Kind, e.Kind)
				}
			}
		})
	}
}

// TestBroadcastStopsOnTheFirstError checks Broadcast reports rather than
// pressing on. Continuing past a failed Send would leave a job that is shutting
// down grinding through a Send per remaining output, each of which cannot
// succeed.
func TestBroadcastStopsOnTheFirstError(t *testing.T) {
	errSend := errors.New("send failed")
	outs, collected := newCollectOutputs(4)
	collected[1].err = errSend
	w := NewWriter(oneGroup(outs))

	if err := w.Broadcast(context.Background(), core.NewEndOfStreamElement()); !errors.Is(err, errSend) {
		t.Fatalf("Broadcast = %v, want %v", err, errSend)
	}
	if len(collected[2].elements) != 0 || len(collected[3].elements) != 0 {
		t.Error("Broadcast kept sending after an output failed")
	}
}

func TestCloseAllClosesEveryOutputOnce(t *testing.T) {
	outs, collected := newCollectOutputs(5)
	w := NewWriter(oneGroup(outs))
	w.CloseAll()

	for i, out := range collected {
		if out.closes != 1 {
			t.Errorf("output %d closed %d times, want exactly 1", i, out.closes)
		}
	}
}

// TestChannelSatisfiesTheSeam checks a Writer drives the real transport, not
// only the test double above.
func TestChannelSatisfiesTheSeam(t *testing.T) {
	chans := []*Channel{NewChannel(4), NewChannel(4)}
	outs := []Output{chans[0], chans[1]}
	w := NewWriter(oneGroup(outs))

	if err := w.EmitRecord(context.Background(), &core.Record{Key: key(1)}); err != nil {
		t.Fatalf("EmitRecord: %v", err)
	}
	if err := w.Broadcast(context.Background(), core.NewEndOfStreamElement()); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}
	w.CloseAll()

	records, ends := 0, 0
	for _, ch := range chans {
		var in Input = ch
		for {
			e, ok := in.Recv()
			if !ok {
				break
			}
			switch e.Kind {
			case core.KindRecord:
				records++
			case core.KindEndOfStream:
				ends++
			}
		}
	}
	if records != 1 {
		t.Errorf("%d records arrived, want 1", records)
	}
	if ends != 2 {
		t.Errorf("%d end-of-stream elements arrived, want one per channel", ends)
	}
}

func TestFNV1aMatchesTheReferenceVectors(t *testing.T) {
	// The published FNV-1a 64 test vectors.
	tests := []struct {
		in   string
		want uint64
	}{
		{in: "", want: 0xcbf29ce484222325},
		{in: "a", want: 0xaf63dc4c8601ec8c},
		{in: "foobar", want: 0x85944171f73967e8},
	}
	for _, tt := range tests {
		if got := fnv1a([]byte(tt.in)); got != tt.want {
			t.Errorf("fnv1a(%q) = %#x, want %#x", tt.in, got, tt.want)
		}
	}
}

// TestEmitRecordDoesNotAllocate is the assertion the benchmark below only
// reports. It runs under `go test` rather than only under `go test -bench`, so
// a change that reintroduces an allocation per record fails the build instead
// of waiting for somebody to read a benchmark line.
func TestEmitRecordDoesNotAllocate(t *testing.T) {
	outs := make([]Output, 8)
	for i := range outs {
		outs[i] = discardOutput{}
	}
	// Three groups of different sizes, so this covers the loop over groups and
	// the reuse of one hash across them, not just the single-group path.
	w := NewWriter([][]Output{outs[:3], outs[3:5], outs[5:]})
	ctx := context.Background()
	rec := &core.Record{Key: key(12345), Value: make([]byte, 16)}

	got := testing.AllocsPerRun(1000, func() {
		if err := w.EmitRecord(ctx, rec); err != nil {
			t.Fatalf("EmitRecord: %v", err)
		}
	})
	if got != 0 {
		t.Errorf("EmitRecord allocates %.1f times per record, want 0", got)
	}
}

// BenchmarkEmitRecord pins the reason fnv1a is written out here rather than
// taken from hash/fnv. hash/fnv returns an interface whose state escapes to the
// heap, which costs an allocation per record on the hottest path in the engine.
// This must report 0 allocs/op.
func BenchmarkEmitRecord(b *testing.B) {
	outs, _ := newCollectOutputs(8)
	// Replace the collectors: appending to a slice allocates, which would hide
	// the thing being measured.
	for i := range outs {
		outs[i] = discardOutput{}
	}
	w := NewWriter([][]Output{outs[:3], outs[3:5], outs[5:]})
	ctx := context.Background()
	rec := &core.Record{Key: key(12345), Value: make([]byte, 16)}

	b.ReportAllocs()
	for b.Loop() {
		if err := w.EmitRecord(ctx, rec); err != nil {
			b.Fatalf("EmitRecord: %v", err)
		}
	}
}

// TestEmitRecordReachesEveryGroup is the core of the fix: a record is
// partitioned within each group and copied across them.
//
// The groups are deliberately of different sizes. Equal sizes would let a
// single shared modulo pass, since every group would agree on an index by
// coincidence; 2 and 3 share no factor, so agreement can only come from
// computing the index per group.
func TestEmitRecordReachesEveryGroup(t *testing.T) {
	const records = 900
	groups, collected := newCollectGroups(2, 3)
	w := NewWriter(groups)

	for k := range uint64(records) {
		if err := w.EmitRecord(context.Background(), &core.Record{Key: key(k)}); err != nil {
			t.Fatalf("EmitRecord: %v", err)
		}
	}

	for gi, group := range collected {
		total := 0
		for _, out := range group {
			for _, e := range out.elements {
				if e.Kind != core.KindRecord {
					t.Fatalf("EmitRecord sent a %s element", e.Kind)
				}
			}
			total += len(out.elements)
		}
		// Exactly once within the group: every record arrived, and none of them
		// arrived twice.
		if total != records {
			t.Errorf("group %d holds %d records, want %d", gi, total, records)
		}
	}
}

// TestEmitRecordPartitionsIndependentlyPerGroup proves the modulo is taken per
// group rather than once for all of them.
//
// It finds a key that lands on index 0 of a group of 2 and on a nonzero index
// of a group of 3. A Writer that computed one index and reused it across groups
// would put that record at index 0 in both, so this is the case that
// distinguishes reusing the hash, which is correct and what the code does, from
// reusing the index, which is not.
func TestEmitRecordPartitionsIndependentlyPerGroup(t *testing.T) {
	const (
		small = 2
		large = 3
	)

	var (
		k     uint64
		found bool
	)
	for candidate := range uint64(1000) {
		h := fnv1a(key(candidate))
		if h%small == 0 && h%large != 0 {
			k, found = candidate, true
			break
		}
	}
	if !found {
		t.Fatal("no key in the first 1000 lands on index 0 of one group and a nonzero index of the other")
	}

	groups, collected := newCollectGroups(small, large)
	w := NewWriter(groups)
	if err := w.EmitRecord(context.Background(), &core.Record{Key: key(k)}); err != nil {
		t.Fatalf("EmitRecord: %v", err)
	}

	if len(collected[0][0].elements) != 1 {
		t.Errorf("key %d did not land on index 0 of the group of %d", k, small)
	}
	if n := len(collected[1][0].elements); n != 0 {
		t.Errorf("key %d landed on index 0 of the group of %d as well: the index was reused across groups", k, large)
	}
	// It still arrived somewhere in the second group.
	total := 0
	for _, out := range collected[1] {
		total += len(out.elements)
	}
	if total != 1 {
		t.Errorf("the group of %d received %d copies of the record, want exactly 1", large, total)
	}
}

// TestEmitRecordStopsOnTheFirstFailingGroup pins the contract opContext relies
// on: the first failed send returns, and no later group is attempted.
func TestEmitRecordStopsOnTheFirstFailingGroup(t *testing.T) {
	errSend := errors.New("send failed")
	groups, collected := newCollectGroups(1, 1, 1)
	collected[1][0].err = errSend
	w := NewWriter(groups)

	if err := w.EmitRecord(context.Background(), &core.Record{Key: key(7)}); !errors.Is(err, errSend) {
		t.Fatalf("EmitRecord = %v, want %v", err, errSend)
	}
	if len(collected[2][0].elements) != 0 {
		t.Error("EmitRecord kept sending after a group failed")
	}
}

// TestBroadcastReachesEveryOutputInEveryGroup is invariant 2 for control
// elements at a vertex with several downstream vertices. A barrier that reached
// every subtask of one downstream vertex but none of another deadlocks
// alignment on that second edge in Phase 3.
func TestBroadcastReachesEveryOutputInEveryGroup(t *testing.T) {
	elements := []core.StreamElement{
		core.NewEndOfStreamElement(),
		core.NewWatermarkElement(1700000000000),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 3}),
	}

	for _, e := range elements {
		t.Run(e.Kind.String(), func(t *testing.T) {
			groups, collected := newCollectGroups(1, 2, 3)
			w := NewWriter(groups)

			if err := w.Broadcast(context.Background(), e); err != nil {
				t.Fatalf("Broadcast: %v", err)
			}
			for gi, group := range collected {
				for i, out := range group {
					if len(out.elements) != 1 {
						t.Fatalf("group %d output %d received %d elements, want exactly 1",
							gi, i, len(out.elements))
					}
					if out.elements[0].Kind != e.Kind {
						t.Errorf("group %d output %d received a %s element, want %s",
							gi, i, out.elements[0].Kind, e.Kind)
					}
				}
			}
		})
	}
}

// TestCloseAllClosesEveryGroup checks no downstream vertex is left hanging.
// Recv has no context to watch, so an unclosed group blocks every subtask of
// that vertex forever.
func TestCloseAllClosesEveryGroup(t *testing.T) {
	groups, collected := newCollectGroups(1, 2, 3)
	NewWriter(groups).CloseAll()

	for gi, group := range collected {
		for i, out := range group {
			if out.closes != 1 {
				t.Errorf("group %d output %d closed %d times, want exactly 1", gi, i, out.closes)
			}
		}
	}
}

// TestNewWriterRejectsAnEmptyGroup covers the wiring bug this cannot otherwise
// report. Graph validation guarantees parallelism >= 1, so an empty group means
// the runtime built the group wrongly; accepting it would give a downstream
// vertex that silently receives nothing.
func TestNewWriterRejectsAnEmptyGroup(t *testing.T) {
	outs, _ := newCollectOutputs(2)
	tests := []struct {
		name   string
		groups [][]Output
	}{
		{name: "only group is empty", groups: [][]Output{{}}},
		{name: "later group is empty", groups: [][]Output{outs, {}}},
		{name: "nil group", groups: [][]Output{nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("NewWriter accepted a group with no outputs")
				}
			}()
			NewWriter(tt.groups)
		})
	}
}

// discardOutput accepts and forgets, so the benchmark measures routing.
type discardOutput struct{}

func (discardOutput) Send(context.Context, core.StreamElement) error { return nil }
func (discardOutput) Close()                                         {}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
