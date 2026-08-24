package runtime

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// snapshotMetadata describes a job of one source subtask, one operator subtask
// and one sink subtask.
//
// Three subtasks rather than one, so a checkpoint acknowledged by the subtask
// under test does NOT complete. That matters for the ordering test below:
// Coordinator.Acked drops to zero once a checkpoint is written, so a job whose
// only subtask is the one being watched would report zero either way.
func snapshotMetadata(sourceCount int64) checkpoint.Metadata {
	return checkpoint.Metadata{
		Seed: 1,
		Vertices: []checkpoint.VertexMeta{
			{ID: "op", Parallelism: 1},
			{ID: "out", Parallelism: 1},
			{ID: "src", Parallelism: 1, Count: sourceCount},
		},
	}
}

func newSnapshotCoordinator(t *testing.T, sourceCount int64) (*checkpoint.Coordinator, *checkpoint.Storage) {
	t.Helper()
	s := checkpoint.NewStorage(t.TempDir())
	return checkpoint.NewCoordinator(s, snapshotMetadata(sourceCount)), s
}

// drainChannel reads ch until it closes, counting what went past.
func drainChannel(ch *transport.Channel, wg *sync.WaitGroup, seen *[]core.StreamElement, mu *sync.Mutex) {
	defer wg.Done()
	for {
		e, ok := ch.Recv()
		if !ok {
			return
		}
		mu.Lock()
		*seen = append(*seen, e)
		mu.Unlock()
	}
}

// TestSourceSnapshotsThePositionAtInjection is the resume offset, checked
// against the arithmetic that produces it.
//
// Barrier k is injected after this subtask's k*interval-th element, so the
// offset that comes next is start+k*interval, and that is what must be
// recorded. One off in either direction is a recovery that replays a record or
// skips one, and at-least-once delivery hides the first while nothing catches
// the second.
func TestSourceSnapshotsThePositionAtInjection(t *testing.T) {
	const (
		count    = 400
		interval = 100
	)
	tests := []struct {
		name        string
		parallelism int
		index       int
	}{
		{name: "single subtask", parallelism: 1, index: 0},
		{name: "first of four", parallelism: 4, index: 0},
		{name: "third of four", parallelism: 4, index: 2},
		{name: "last of four", parallelism: 4, index: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testGeneratorConfig(count)
			elems := runSourceSubtaskLoop(t, cfg, newWatermarkGenerator(testWatermarkInterval, cfg.MaxLag), interval, tt.parallelism, tt.index)

			start, _ := sourceRange(count, tt.parallelism, tt.index)
			var barriers int
			for _, e := range elems {
				if !e.isBarrier() {
					continue
				}
				barriers++
				want := start + e.checkpointID*interval
				if e.position != want {
					t.Errorf("checkpoint %d recorded position %d, want %d (range starts at %d, %d elements per barrier)",
						e.checkpointID, e.position, want, start, interval)
				}
			}
			if barriers == 0 {
				t.Fatal("the subtask injected no barrier; the test asserts nothing")
			}
		})
	}
}

// TestSourceAcknowledgesThePositionItInjectedAt runs the whole source subtask
// against a real coordinator and reads back what reached the disk.
//
// The test above pins what sourceLoop hands over; this one pins that the
// subtask records THAT and not something it read afterwards.
func TestSourceAcknowledgesThePositionItInjectedAt(t *testing.T) {
	const (
		count    = 500
		interval = 100
	)
	cfg := testGeneratorConfig(count)
	co, storage := newSnapshotCoordinator(t, count)

	out := transport.NewChannel(transport.DefaultCapacity)
	var mu sync.Mutex
	var seen []core.StreamElement
	var wg sync.WaitGroup
	wg.Add(1)
	go drainChannel(out, &wg, &seen, &mu)

	v := graph.Vertex{
		ID: "src", Kind: graph.VertexSource, Parallelism: 1,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: testWatermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   interval,
	}
	if err := runSubtask(context.Background(), v, 0, nil, [][]transport.Output{{out}}, subtaskConfig{coordinator: co}); err != nil {
		t.Fatalf("runSubtask: %v", err)
	}
	wg.Wait()

	// Complete each checkpoint on the source's behalf by acknowledging the two
	// subtasks this test does not run, so the payloads can be read back through
	// the same path a restore uses.
	var barrierIDs []int64
	for _, e := range seen {
		if e.Kind == core.KindBarrier {
			barrierIDs = append(barrierIDs, e.Barrier.CheckpointID)
		}
	}
	if len(barrierIDs) == 0 {
		t.Fatal("the source injected no barrier")
	}
	for _, id := range barrierIDs {
		for _, key := range []checkpoint.SubtaskKey{{VertexID: "op"}, {VertexID: "out"}} {
			if err := co.Acknowledge(id, key, nil); err != nil {
				t.Fatalf("Acknowledge(%d, %s): %v", id, key, err)
			}
		}
	}

	for _, id := range barrierIDs {
		_, payloads, err := storage.Load(id)
		if err != nil {
			t.Fatalf("Load(%d): %v", id, err)
		}
		got, err := decodePosition(payloads[checkpoint.SubtaskKey{VertexID: "src", Index: 0}])
		if err != nil {
			t.Fatalf("decodePosition for checkpoint %d: %v", id, err)
		}
		if want := id * interval; got != want {
			t.Errorf("checkpoint %d recorded resume offset %d, want %d", id, got, want)
		}
	}
}

// watchingOutput is a transport.Output that runs a hook on the way past.
//
// The hook runs on the SUBTASK's goroutine, inside Send, which is what makes an
// ordering assertion here deterministic. Two events observed from a test
// goroutine would be a race however they were compared.
type watchingOutput struct {
	inner *transport.Channel
	onE   func(core.StreamElement)
}

func (o *watchingOutput) Send(ctx context.Context, e core.StreamElement) error {
	o.onE(e)
	return o.inner.Send(ctx, e)
}

func (o *watchingOutput) Close() { o.inner.Close() }

var _ transport.Output = (*watchingOutput)(nil)

// TestOperatorSnapshotsBeforeForwardingTheBarrier is the Chandy-Lamport
// ordering, asserted as an order rather than as an eventual presence.
//
// A test that ran the subtask and then checked that both the snapshot and the
// forward had happened would pass against either order. The assertion here is
// made AT the forward, on the subtask's own goroutine: at the moment the
// barrier goes downstream, this subtask must already have acknowledged.
//
// Forwarding first is not a cosmetic difference. It lets a downstream operator
// record a state containing the effects of elements this operator has not
// recorded, so the downstream snapshot holds records that the upstream resume
// offset will replay, and recovery delivers them twice.
func TestOperatorSnapshotsBeforeForwardingTheBarrier(t *testing.T) {
	co, _ := newSnapshotCoordinator(t, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := transport.NewChannel(16)
	// ackedAtForward[k] is how many subtasks had acknowledged checkpoint k at
	// the instant the barrier for k was handed to the transport.
	ackedAtForward := make(map[int64]int)
	out := &watchingOutput{
		inner: transport.NewChannel(16),
		onE: func(e core.StreamElement) {
			if e.Kind == core.KindBarrier {
				ackedAtForward[e.Barrier.CheckpointID] = co.Acked(e.Barrier.CheckpointID)
			}
		},
	}

	gate := NewGate(ctx, []transport.Input{in})
	w := transport.NewWriter([][]transport.Output{{out}})
	oc := newOpContext(ctx, w)
	op := &statefulOperator{}
	if err := op.Open(oc); err != nil {
		t.Fatalf("Open: %v", err)
	}

	for _, e := range []core.StreamElement{
		core.NewRecordElement(&core.Record{Key: []byte("a"), Value: []byte("1")}),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 1}),
		core.NewRecordElement(&core.Record{Key: []byte("b"), Value: []byte("2")}),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 2}),
		core.NewEndOfStreamElement(),
	} {
		if err := in.Send(ctx, e); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	in.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if _, ok := out.inner.Recv(); !ok {
				return
			}
		}
	}()

	cp := checkpointer{co: co, key: checkpoint.SubtaskKey{VertexID: "op", Index: 0}}
	if err := runOperatorLoop(ctx, op, oc, subtaskID{vertexID: "op", index: 0}, gate, w, cp); err != nil {
		t.Fatalf("runOperatorLoop: %v", err)
	}
	w.CloseAll()
	wg.Wait()
	gate.Wait()

	if len(ackedAtForward) != 2 {
		t.Fatalf("%d barriers reached the transport, want 2", len(ackedAtForward))
	}
	for _, id := range []int64{1, 2} {
		if got := ackedAtForward[id]; got != 1 {
			t.Errorf("at the moment barrier %d was forwarded, %d subtasks had acknowledged it, want 1: the barrier went downstream before the snapshot", id, got)
		}
	}
}

// TestOperatorSnapshotHoldsTheStateAtTheBarrier checks WHAT was recorded, not
// only when.
//
// The barrier separates the elements belonging to checkpoint k from those
// belonging to k+1, so the snapshot must hold the first record's effect and not
// the second's. A snapshot taken one element late looks identical in every
// ordering test and produces a state that the resume offset will replay into,
// which is a double count.
func TestOperatorSnapshotHoldsTheStateAtTheBarrier(t *testing.T) {
	co, storage := newSnapshotCoordinator(t, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := transport.NewChannel(16)
	out := transport.NewChannel(16)
	gate := NewGate(ctx, []transport.Input{in})
	w := transport.NewWriter([][]transport.Output{{out}})
	oc := newOpContext(ctx, w)
	op := &statefulOperator{}
	if err := op.Open(oc); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// One record before the barrier and one after it. statefulOperator appends
	// a byte per record under the record's key, so the two are distinguishable
	// in the snapshot.
	for _, e := range []core.StreamElement{
		core.NewRecordElement(&core.Record{Key: []byte("before"), Value: []byte("1")}),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 1}),
		core.NewRecordElement(&core.Record{Key: []byte("after"), Value: []byte("2")}),
		core.NewEndOfStreamElement(),
	} {
		if err := in.Send(ctx, e); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	in.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if _, ok := out.Recv(); !ok {
				return
			}
		}
	}()

	cp := checkpointer{co: co, key: checkpoint.SubtaskKey{VertexID: "op", Index: 0}}
	if err := runOperatorLoop(ctx, op, oc, subtaskID{vertexID: "op", index: 0}, gate, w, cp); err != nil {
		t.Fatalf("runOperatorLoop: %v", err)
	}
	w.CloseAll()
	wg.Wait()
	gate.Wait()

	// Complete checkpoint 1 on behalf of the subtasks this test does not run.
	for _, key := range []checkpoint.SubtaskKey{{VertexID: "src"}, {VertexID: "out"}} {
		if err := co.Acknowledge(1, key, nil); err != nil {
			t.Fatalf("Acknowledge(%s): %v", key, err)
		}
	}

	_, payloads, err := storage.Load(1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	restored := state.NewMemory()
	if err := state.ReadFrom(restored, bytes.NewReader(payloads[cp.key])); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	var keys []string
	restored.Iterate(func(k, v []byte) bool {
		keys = append(keys, string(k[1:]))
		return true
	})
	if want := []string{"before"}; !slices.Equal(keys, want) {
		t.Errorf("the snapshot holds keys %q, want %q: the cut is not at the barrier", keys, want)
	}
}

// TestSinkAcknowledgesWithAnEmptyPayload. A sink participates in the protocol
// and commits nothing (invariant 4). Both halves are asserted: it is counted,
// and what it recorded is empty.
func TestSinkAcknowledgesWithAnEmptyPayload(t *testing.T) {
	co, storage := newSnapshotCoordinator(t, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := transport.NewChannel(16)
	gate := NewGate(ctx, []transport.Input{in})
	collect := sinks.NewCollect()

	for _, e := range []core.StreamElement{
		core.NewRecordElement(&core.Record{Key: []byte("a"), Value: []byte("1")}),
		core.NewBarrierElement(&core.Barrier{CheckpointID: 1}),
		core.NewEndOfStreamElement(),
	} {
		if err := in.Send(ctx, e); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	in.Close()

	v := graph.Vertex{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
		NewSink: func() core.Sink { return collect }}
	if err := runSubtask(ctx, v, 0, gate, nil, subtaskConfig{coordinator: co}); err != nil {
		t.Fatalf("runSubtask: %v", err)
	}
	gate.Wait()

	for _, key := range []checkpoint.SubtaskKey{{VertexID: "src"}, {VertexID: "op"}} {
		if err := co.Acknowledge(1, key, nil); err != nil {
			t.Fatalf("Acknowledge(%s): %v", key, err)
		}
	}
	if !co.Completed(1) {
		t.Fatal("the checkpoint did not complete, so the sink did not acknowledge")
	}

	_, payloads, err := storage.Load(1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	payload, ok := payloads[checkpoint.SubtaskKey{VertexID: "out", Index: 0}]
	if !ok {
		t.Fatal("the checkpoint holds no file for the sink subtask")
	}
	if len(payload) != 0 {
		t.Errorf("the sink recorded %d bytes, want none: a sink commits on notification and stages nothing here", len(payload))
	}
	// And the record still reached it. A sink that acknowledged instead of
	// writing would pass everything above.
	if got := len(collect.Records()); got != 1 {
		t.Errorf("the sink wrote %d records, want 1", got)
	}
}
