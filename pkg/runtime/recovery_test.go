package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/test/oracle"
)

// errInjectedFailure is what the fault decorator returns. Matched with
// errors.Is so a run that failed for some other reason does not read as the
// fault having been injected.
var errInjectedFailure = errors.New("injected source failure")

// The window specification this suite runs. Tumbling, and at a lateness of
// zero, which is what the equivalence suite in test/oracle runs and what makes
// the operator's watermark load-bearing: at any positive lateness a record
// arriving against a stale watermark is still counted, and the divergence this
// suite hunts closes up on its own.
//
// The size is chosen against the generator: restoreConfig steps event time by
// 10ms per element, so a 40000-element source spans 400000ms and 5000ms windows
// give eighty of them across the range, at around eight records per
// (key, window) over 64 keys. Small enough that a window closes while the run
// is still going, which is what puts a completed window inside a checkpoint.
const (
	recoveryWindowSize     = 5000
	recoveryWindowLateness = 0
)

func recoverySpec() oracle.Spec {
	return oracle.Spec{Size: recoveryWindowSize, Slide: recoveryWindowSize}
}

// faultingGenerator fails after a fixed number of elements have been read from it.
//
// The trigger is a LOGICAL POSITION -- elements read from this subtask's range
// -- and never a wall clock. That is invariant 6, and the reason is not
// tidiness: a fault on a timer lands somewhere different on every run, so a
// test that means "kill between barrier k arriving on one input and arriving on
// another" would sometimes kill somewhere else and pass for the wrong reason.
//
// Every method core.Source declares is forwarded EXPLICITLY, and so is Count.
// Embedding core.Source would compile and would leave Count off the concrete
// type, so the runtime's splittableSource assertion would fail: the job would
// be refused above parallelism 1, and a test written around that refusal would
// quietly run a topology in which alignment cannot fail. CLAUDE.md records this
// trap for precisely this test.
type faultingGenerator struct {
	inner     *sources.Generator
	failAfter int64
	read      int64
}

func newFaultingGenerator(cfg sources.GeneratorConfig, failAfter int64) *faultingGenerator {
	return &faultingGenerator{inner: sources.NewGenerator(cfg), failAfter: failAfter}
}

func (s *faultingGenerator) Open(ctx core.Context) error { return s.inner.Open(ctx) }

func (s *faultingGenerator) Next() (*core.Record, bool, error) {
	if s.read >= s.failAfter {
		return nil, false, fmt.Errorf("after %d elements: %w", s.read, errInjectedFailure)
	}
	s.read++
	return s.inner.Next()
}

func (s *faultingGenerator) SeekTo(offset int64) error { return s.inner.SeekTo(offset) }
func (s *faultingGenerator) Position() int64           { return s.inner.Position() }
func (s *faultingGenerator) Count() int64              { return s.inner.Count() }
func (s *faultingGenerator) Close() error              { return s.inner.Close() }

var _ splittableSource = (*faultingGenerator)(nil)

// windowFactory builds one window operator per subtask and keeps a handle on
// each, so a run can be asked afterwards whether anything was dropped.
//
// The drop count is what makes the comparison against the batch oracle valid.
// The oracle has no lateness model -- it cannot have one, it has no watermark
// -- so the two agree only on a run where no record was ever late. Asserting
// zero drops turns "the oracle does not model this" into a checked precondition
// instead of a hope, and it is the assertion that would catch a recovered run
// dropping its replayed records against a watermark restored too high.
type windowFactory struct {
	mu   sync.Mutex
	made []*operators.WindowCount
}

func (f *windowFactory) newOperator() core.Operator {
	w := operators.NewTumblingCount(recoveryWindowSize, recoveryWindowLateness)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.made = append(f.made, w)
	return w
}

func (f *windowFactory) dropped() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var total int64
	for _, w := range f.made {
		total += w.Dropped()
	}
	return total
}

// windowGraph is one or more source vertices feeding a WindowCount and a sink.
//
// This is the operator the phase makes recoverable and the one Phase 6's
// Nexmark queries are built on. The suite used to run on a keyed-count operator
// chosen because its entire state was its KeyedState -- no timers, no watermark
// -- which made it the operator for which "restore the KeyedState" was the
// whole of recovery. That was the right choice while it was the only operator
// that could be recovered, and it is the wrong one now: it proved recovery for
// an operator with no timers and claimed nothing about the windowed path.
func windowGraph(t *testing.T, sink core.Sink, p int, f *windowFactory, sourceVertices ...graph.Vertex) *graph.Graph {
	t.Helper()
	vertices := append([]graph.Vertex{}, sourceVertices...)
	vertices = append(vertices,
		graph.Vertex{ID: "window", Kind: graph.VertexOperator, Parallelism: p, NewOperator: f.newOperator},
		graph.Vertex{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return sink }},
	)
	var edges [][2]string
	for _, v := range sourceVertices {
		edges = append(edges, [2]string{v.ID, "window"})
	}
	edges = append(edges, [2]string{"window", "out"})
	return buildGraph(t, vertices, edges)
}

// windowRowsOf decodes what a windowed job wrote into a sink, keeping EVERY row
// for a (key, window) rather than one.
//
// Delivery is at-least-once and there is no transactional sink in this phase --
// core.Sink has no NotifyCheckpointComplete yet -- so a recovered run leaves
// duplicates behind: every window the crashed run fired after the checkpoint is
// fired again by the resumed run. That is correct, and it is why the comparison
// here is not a straight equality against the oracle.
//
// The duplicates must AGREE, though, and that is the real assertion. A window
// re-fired from replayed state carries the same count as the one the crashed
// run emitted, because the replay is exact. Two different counts for one
// (key, window) means the replay was not exact, and it is the shape almost
// every recovery bug takes: a partial re-count, a double-count, a window
// reopened against a stale watermark.
//
// The window start is DERIVED, not read. The operator stamps a fired window
// with its end-1, so the start is EventTime-(size-1). Reading EventTime as a
// start would shift every expectation by size-1 and every row would still look
// plausible.
func windowRowsOf(t *testing.T, recs []*core.Record) map[oracle.Key][]int64 {
	t.Helper()
	out := make(map[oracle.Key][]int64)
	for _, rec := range recs {
		count, err := operators.DecodeCount(rec.Value)
		if err != nil {
			t.Fatalf("the sink holds a value that is not a count: %v", err)
		}
		k := oracle.Key{Key: string(rec.Key), WindowStart: rec.EventTime - (recoveryWindowSize - 1)}
		out[k] = append(out[k], count)
	}
	return out
}

// oracleWindowCounts is the batch answer over every source feeding the job.
//
// Each source is counted independently and the results summed, because the
// operator sees the union of them: a key that both generators produce
// accumulates into one (key, window).
func oracleWindowCounts(t *testing.T, cfgs ...sources.GeneratorConfig) map[oracle.Key]int64 {
	t.Helper()
	out := make(map[oracle.Key]int64)
	for _, cfg := range cfgs {
		counts, err := oracle.Counts(cfg, recoverySpec())
		if err != nil {
			t.Fatalf("oracle.Counts: %v", err)
		}
		for k, n := range counts {
			out[k] += n
		}
	}
	return out
}

// assertSameWindows compares the sink against the batch oracle.
//
// Contents and never emission order: delivery is at-least-once and the order
// after a recovery will differ from a clean run, so an order comparison would
// be a broken test. The rows are sorted only to report the first difference in
// a stable place.
func assertSameWindows(t *testing.T, got map[oracle.Key][]int64, want map[oracle.Key]int64, label string) {
	t.Helper()

	// Every duplicate of a (key, window) carries the same count.
	collapsed := make(map[oracle.Key]int64, len(got))
	for k, counts := range got {
		for _, c := range counts[1:] {
			if c != counts[0] {
				t.Fatalf("%s: key %x window %d was emitted with counts %v: the replay was not exact",
					label, k.Key, k.WindowStart, counts)
			}
		}
		collapsed[k] = counts[0]
	}

	if len(collapsed) != len(want) {
		t.Errorf("%s: the sink holds %d (key, window) rows, want %d", label, len(collapsed), len(want))
	}
	for _, tr := range oracle.Sorted(want) {
		k := oracle.Key{Key: tr.Key, WindowStart: tr.WindowStart}
		gotCount, ok := collapsed[k]
		if !ok {
			t.Fatalf("%s: the sink is missing key %x window %d, which the oracle counts at %d",
				label, tr.Key, tr.WindowStart, tr.Count)
		}
		if gotCount != tr.Count {
			t.Fatalf("%s: key %x window %d counted %d, want %d",
				label, tr.Key, tr.WindowStart, gotCount, tr.Count)
		}
	}
	for k := range collapsed {
		if _, ok := want[k]; !ok {
			t.Fatalf("%s: the sink holds key %x window %d, which the input does not produce", label, k.Key, k.WindowStart)
		}
	}
}

// recoveryCase is one crash-and-resume scenario: two source vertices, a window
// operator behind them, and a fault on the second source at a chosen logical
// position.
type recoveryCase struct {
	// a and b are the two source vertices' generators. Different lengths are
	// the point of the aimed case below.
	a, b sources.GeneratorConfig
	// barrierA and barrierB are the per-vertex barrier intervals. Skewing them
	// against the range lengths is what decides how far apart in the stream the
	// two sources reach barrier k.
	barrierA, barrierB int64
	// failAfterB is how many elements each subtask of source B reads before it
	// fails.
	failAfterB int64
	// parallelism of every vertex.
	parallelism int
	// backend is the keyed-state implementation the operator subtasks run on.
	backend stateBackend
}

// crash runs the case up to its injected fault and returns the checkpoint root
// it left behind, plus the sink it wrote into.
//
// ONE sink is used across the crashed run and the resumed one, deliberately. A
// sink is external and durable: it does not forget what a crashed run wrote to
// it, and handing the resumed run a fresh sink would measure recovery against a
// sink that conveniently lost the evidence of any double delivery. That matters
// more here than it did on the keyed-count operator, which emitted nothing
// until end of stream; a window operator emits as its windows close, so the
// crashed run leaves real rows behind.
func crash(t *testing.T, c recoveryCase) (root string, collect *sinks.Collect, build func(faulty bool) (*graph.Graph, *windowFactory)) {
	t.Helper()
	root = t.TempDir()
	collect = sinks.NewCollect()

	build = func(faulty bool) (*graph.Graph, *windowFactory) {
		f := &windowFactory{}
		srcA := windowSourceVertex("srcA", c.a, c.parallelism, c.barrierA)
		srcB := graph.Vertex{
			ID: "srcB", Kind: graph.VertexSource, Parallelism: c.parallelism,
			NewSource: func() core.Source {
				if faulty {
					return newFaultingGenerator(c.b, c.failAfterB)
				}
				return sources.NewGenerator(c.b)
			},
			WatermarkIntervalElements: 100,
			MaxOutOfOrderness:         c.b.MaxLag,
			BarrierIntervalElements:   c.barrierB,
		}
		return windowGraph(t, collect, c.parallelism, f, srcA, srcB), f
	}

	g, _ := build(true)
	err := RunWithOptions(context.Background(), g,
		Options{CheckpointRoot: root, Seed: c.a.Seed, NewState: c.backend.newState})
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("the crashed run returned %v, want the injected failure: the fault did not land", err)
	}
	return root, collect, build
}

// windowSourceVertex is a source vertex over cfg feeding the window operator.
func windowSourceVertex(id string, cfg sources.GeneratorConfig, p int, barrierInterval int64) graph.Vertex {
	return graph.Vertex{
		ID: id, Kind: graph.VertexSource, Parallelism: p,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrierInterval,
	}
}

// crashAndRestore runs the case to its fault, restarts from the last complete
// checkpoint, and returns the sink both runs wrote into.
//
// # Why the fault is placed thousands of elements past a barrier
//
// A checkpoint is complete only once every subtask has acknowledged, and a
// source acknowledges when it INJECTS a barrier while an operator acknowledges
// when it has PROCESSED one. Between those two lies the whole pipeline: the
// records already in the channels ahead of the barrier, the operator's snapshot
// write, and the sink's own acknowledgement behind it. A fault a few elements
// after a barrier cancels the job before any of that finishes, so no checkpoint
// completes and there is nothing to recover from. That is correct behaviour --
// it is what the crash-before-any-checkpoint test below asserts -- but a case
// meaning to test recovery has to leave the pipeline room to drain.
//
// The depth is bounded by the transport: a source runs at most one channel
// capacity ahead of its consumer, so a margin of several thousand elements is
// the pipeline depth several times over. It is a margin in ELEMENTS and not in
// time, so it does not vary with the scheduler -- but the ACKNOWLEDGEMENT
// behind it is a goroutine and an fsync, so the margin is sized generously
// rather than minimally. The window operator does strictly more work per record
// than the keyed-count operator this suite used to run on, so a margin that was
// comfortable for that one is not automatically comfortable here.
func crashAndRestore(t *testing.T, c recoveryCase) (*sinks.Collect, int) {
	t.Helper()
	root, collect, build := crash(t, c)
	crashedRows := len(collect.Records())

	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatal("the crashed run completed no checkpoint, so there is nothing to recover from and the test asserts nothing")
	}

	pending := pendingWindowsAtCheckpoint(t, storage, id, c)
	t.Logf("crashed after %d elements of srcB per subtask, having written %d rows to the sink; "+
		"resuming from checkpoint %d, which holds %d (key, window) pairs that are complete but unfired",
		c.failAfterB, crashedRows, id, pending)
	if pending == 0 {
		t.Fatal("no (key, window) in this checkpoint has all its records behind the cut and its firing watermark ahead of it, " +
			"so nothing here depends on a timer surviving the restore and this case proves nothing about recoverable timers")
	}

	g, f := build(false)
	if err := RunWithOptions(context.Background(), g,
		Options{RestoreFrom: root, Seed: c.a.Seed, NewState: c.backend.newState}); err != nil {
		t.Fatalf("the resumed run returned %v", err)
	}
	if got := f.dropped(); got != 0 {
		t.Errorf("the resumed run dropped %d assignments as late; the batch oracle has no lateness model, so the comparison below is only valid at zero", got)
	}
	return collect, pending
}

// pendingWindowsAtCheckpoint counts the (key, window) pairs that are COMPLETE
// but UNFIRED at the cut the checkpoint records.
//
// This is the number that says whether a recovery test on a window operator
// proves anything. A (key, window) whose records straddle the cut is re-armed
// by the replay, so it fires after the restore whether or not its timer
// survived; one that fired before the cut is already in the sink. Only a window
// with all of its records behind the cut and its firing watermark ahead of it
// depends on the timer being in the checkpoint -- with the timer in RAM it is
// restored as an aggregate nothing will ever fire, and it goes missing from the
// sink with no error anywhere.
//
// It is computed from the checkpoint itself rather than from a model of one:
// the timer entries under state.PrefixTimer are the windows still open at the
// cut, and the source offsets say where the replay begins, so the pairs that
// receive nothing more are the difference. That is only possible because this
// phase put the timers in the checkpoint, which is a pleasant circularity but
// not a vacuous one -- the count is over the checkpoint's contents and the
// assertions are over the sink's.
func pendingWindowsAtCheckpoint(t *testing.T, storage *checkpoint.Storage, id int64, c recoveryCase) int {
	t.Helper()
	_, payloads, err := storage.Load(id)
	if err != nil {
		t.Fatalf("Load(%d): %v", id, err)
	}

	// The windows open at the cut, from the timer partition of every operator
	// subtask's state.
	open := make(map[oracle.Key]bool)
	for index := range c.parallelism {
		payload, ok := payloads[checkpoint.SubtaskKey{VertexID: "window", Index: index}]
		if !ok {
			t.Fatalf("checkpoint %d holds no state for window subtask %d", id, index)
		}
		st := state.NewMemory()
		if err := state.ReadFrom(st, bytes.NewReader(payload)); err != nil {
			t.Fatalf("decoding window subtask %d: %v", index, err)
		}
		watermark := storedWatermark(t, st)
		st.Iterate(func(k, v []byte) bool {
			if len(k) == 0 || k[0] != state.PrefixTimer {
				return true
			}
			// The timer layout, read from outside pkg/operators on purpose: the
			// prefix and fire time are the first nine bytes, the window start is
			// the last eight, and the record key is what lies between.
			if len(k) < 1+state.OrderedInt64Bytes+8 {
				t.Fatalf("checkpoint holds a %d-byte timer key", len(k))
			}
			fireTime := state.DecodeOrderedInt64(k[1:])
			recordKey := string(k[1+state.OrderedInt64Bytes : len(k)-8])
			windowStart := int64(binary.BigEndian.Uint64(k[len(k)-8:]))
			// A timer in a checkpoint is by construction unfired: firing deletes
			// it, and firing runs to completion inside one ProcessWatermark.
			// Asserted rather than assumed, because it is also a check that the
			// watermark in the checkpoint is the one that was current at the cut.
			if fireTime <= watermark {
				t.Fatalf("checkpoint %d holds a timer due at %d against a stored watermark of %d: it should already have fired",
					id, fireTime, watermark)
			}
			open[oracle.Key{Key: recordKey, WindowStart: windowStart}] = true
			return true
		})
	}

	// The (key, window) pairs the replay will deliver more records into.
	fed := make(map[oracle.Key]bool)
	for _, src := range []struct {
		id  string
		cfg sources.GeneratorConfig
	}{{"srcA", c.a}, {"srcB", c.b}} {
		for index := range c.parallelism {
			offset, err := decodePosition(payloads[checkpoint.SubtaskKey{VertexID: src.id, Index: index}])
			if err != nil {
				t.Fatalf("decodePosition %s[%d]: %v", src.id, index, err)
			}
			_, end := sourceRange(src.cfg.Count, c.parallelism, index)
			markWindowsFrom(t, src.cfg, offset, end, fed)
		}
	}

	pending := 0
	for k := range open {
		if !fed[k] {
			pending++
		}
	}
	return pending
}

// storedWatermark reads the operator watermark out of a restored state, or
// MinInt64 if the subtask had not processed a watermark when it snapshotted.
//
// The absence is legitimate and it happens in the skewed case below. The gate's
// output watermark is the MINIMUM across its inputs, so it forwards nothing
// until EVERY input has produced one; source A injects its first barrier at
// element 50 and its first watermark at element 100, so the operator processes
// barrier 1 having seen no watermark at all. MinInt64 is then the correct
// reading rather than a missing one -- it is exactly what the operator would
// have reported at that moment -- and a checkpoint of it is a checkpoint of
// "nothing has been purged", which is true.
func storedWatermark(t *testing.T, st state.KeyedState) int64 {
	t.Helper()
	v, ok := st.Get(append([]byte{state.PrefixOperatorState}, "watermark"...))
	if !ok {
		return math.MinInt64
	}
	return state.DecodeOrderedInt64(v)
}

// markWindowsFrom marks every (key, window) that the elements in [offset, end)
// of cfg belong to. It is the replay, read straight from the generator.
func markWindowsFrom(t *testing.T, cfg sources.GeneratorConfig, offset, end int64, into map[oracle.Key]bool) {
	t.Helper()
	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = src.Close() }()
	if err := src.SeekTo(offset); err != nil {
		t.Fatalf("SeekTo(%d): %v", offset, err)
	}
	for pos := offset; pos < end; pos++ {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		start := rec.EventTime - floorModInt64(rec.EventTime, recoveryWindowSize)
		into[oracle.Key{Key: string(rec.Key), WindowStart: start}] = true
	}
}

// floorModInt64 is the oracle's assignment arithmetic, written out here because
// this file is measuring the input rather than checking the operator.
func floorModInt64(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// TestRecoveryAcrossAMultiInputTopology is the general case.
//
// Two source vertices at parallelism 2 give every operator subtask four inputs,
// a window operator holds the state, and source B dies at a chosen logical
// position between two of its own barriers. The resumed run must produce
// exactly what the batch oracle says, once the duplicates a crashed run leaves
// in an at-least-once sink are collapsed.
//
// What this case does NOT test is alignment. Both sources use the same barrier
// interval over the same range length, so they reach barrier k at nearly the
// same point, and a fault at an arbitrary position almost never lands in the
// window between one input's barrier and another's. That window is what
// alignment exists for, and hitting it is the next test's job.
func TestRecoveryAcrossAMultiInputTopology(t *testing.T) {
	forEachStateBackend(t, testRecoveryAcrossAMultiInputTopology)
}

func testRecoveryAcrossAMultiInputTopology(t *testing.T, backend stateBackend) {
	const count = 40000
	a := restoreConfig(11, count)
	b := restoreConfig(12, count)

	collect, _ := crashAndRestore(t, recoveryCase{
		a: a, b: b,
		// 20000 elements per subtask, so barriers at 5000, 10000, 15000, 20000.
		barrierA: 5000, barrierB: 5000,
		// Between source B's second barrier and its third, with 7000 elements
		// of margin behind the first for the pipeline to drain.
		failAfterB:  12000,
		parallelism: 2,
		backend:     backend,
	})

	assertSameWindows(t, windowRowsOf(t, collect.Records()), oracleWindowCounts(t, a, b), "recovered run")
}

// TestRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers is the aimed case.
//
// The two sources have deliberately SKEWED range lengths, with barrier
// intervals scaled so that both inject the same NUMBER of barriers at wildly
// different points in the stream:
//
//	srcA     400 elements, 200 per subtask, a barrier every 50 -> at 50, 100, 150, 200
//	srcB   40000 elements, 20000 per subtask, every 5000       -> at 5000, 10000, 15000, 20000
//
// Source A runs through its entire range while source B is barely started, so
// at any moment in the middle of the run the gate holds source A's barrier for
// the next checkpoint and is still waiting for source B's. That is the
// alignment window, and it is thousands of elements wide in LOGICAL terms
// rather than a race: source A's whole range is 200 elements and source B's
// barriers are 5000 apart.
//
// With alignment working, source A's elements past its barrier are held in the
// gate's buffers when the checkpoint is taken, so the operator's state is
// exactly what lies below the barrier on every input and the recorded offsets
// point at the same cut. Replay from there is exact.
//
// With alignment removed, source A -- which finishes its whole range long
// before source B reaches any barrier -- has delivered all 200 of its elements
// by the time the checkpoint is taken, while the offset it recorded is the one
// its barrier was injected at. The resumed run replays the difference into a
// state that already counted it, and every window source A touched comes out
// too high.
//
// The skew does a second thing on a window operator that it did not do on a
// keyed-count one. The operator's watermark is the MINIMUM across its inputs,
// so while source A is alive the watermark sits at source A's event times,
// which are four thousand milliseconds into a range source B spans four hundred
// thousand of. Source B's records are therefore piling up in windows far above
// the watermark, and almost every one of them is still open at the cut. That is
// what makes this case the one with the most to lose if timers do not survive.
func TestRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers(t *testing.T) {
	forEachStateBackend(t, testRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers)
}

func testRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers(t *testing.T, backend stateBackend) {
	a := restoreConfig(21, 400)
	b := restoreConfig(22, 40000)

	collect, _ := crashAndRestore(t, recoveryCase{
		a: a, b: b,
		barrierA: 50, barrierB: 5000,
		// Inside the window for source B's fourth barrier and 5000 elements
		// clear of its third.
		failAfterB:  15000,
		parallelism: 2,
		backend:     backend,
	})

	assertSameWindows(t, windowRowsOf(t, collect.Records()), oracleWindowCounts(t, a, b), "recovered run")
}

// TestCrashBeforeAnyCheckpointCompletesHasNothingToRestoreFrom is the other
// side of the margin note on crashAndRestore.
//
// Source B dies at element 1000 with its barrier interval set to 5000, so it
// never injects one at all and no checkpoint can complete however long the
// pipeline is given. Resuming has to refuse rather than start from zero: a
// caller who asked to recover and silently got a run from the beginning would
// get the right answer here, by accident, and the wrong one on a job whose sink
// already held the crashed run's output.
//
// It is fully deterministic. Nothing waits on a drain, because the barrier that
// would start a checkpoint is never emitted.
func TestCrashBeforeAnyCheckpointCompletesHasNothingToRestoreFrom(t *testing.T) {
	forEachStateBackend(t, testCrashBeforeAnyCheckpointCompletesHasNothingToRestoreFrom)
}

func testCrashBeforeAnyCheckpointCompletesHasNothingToRestoreFrom(t *testing.T, backend stateBackend) {
	a := restoreConfig(31, 40000)
	b := restoreConfig(32, 40000)

	root, _, build := crash(t, recoveryCase{
		a: a, b: b,
		barrierA: 5000, barrierB: 5000,
		failAfterB:  1000,
		parallelism: 2,
		backend:     backend,
	})

	if _, ok, err := checkpoint.NewStorage(root).Latest(); err != nil || ok {
		t.Fatalf("Latest = (ok %t, err %v), want no complete checkpoint", ok, err)
	}
	g, _ := build(false)
	err := RunWithOptions(context.Background(), g,
		Options{RestoreFrom: root, Seed: a.Seed, NewState: backend.newState})
	if !errors.Is(err, errNoRestorePoint) {
		t.Errorf("resuming from a root with nothing complete = %v, want %v", err, errNoRestorePoint)
	}
}

// TestRecoveryIsIndependentOfWhereTheFaultLands sweeps the fault across the
// skewed topology.
//
// Every position is inside the same alignment window -- source A finished its
// range long ago and source B has not reached its next barrier -- so each row
// is the aimed case at a different depth. A recovery that was right only at the
// position the test above happens to name would show up here.
//
// Each position leaves at least 4900 of source B's elements between it and the
// barrier it recovers from, which is the pipeline depth several times over; see
// the note on crashAndRestore. The last two share a barrier deliberately: the
// difference between them is where in the replay the fault fell, not which cut
// it recovers from.
func TestRecoveryIsIndependentOfWhereTheFaultLands(t *testing.T) {
	forEachStateBackend(t, testRecoveryIsIndependentOfWhereTheFaultLands)
}

func testRecoveryIsIndependentOfWhereTheFaultLands(t *testing.T, backend stateBackend) {
	a := restoreConfig(41, 400)
	b := restoreConfig(42, 40000)

	// Source B's subtasks take barriers at 5000, 10000, 15000 and 20000 of a
	// 20000-element range, and each position below sits just under the NEXT
	// one, so the checkpoint it recovers from has most of a barrier interval
	// behind it to complete in.
	for _, failAfter := range []int64{9900, 14900, 19900, 19999} {
		t.Run(fmt.Sprintf("after%d", failAfter), func(t *testing.T) {
			collect, _ := crashAndRestore(t, recoveryCase{
				a: a, b: b,
				barrierA: 50, barrierB: 5000,
				failAfterB:  failAfter,
				parallelism: 2,
				backend:     backend,
			})
			assertSameWindows(t, windowRowsOf(t, collect.Records()), oracleWindowCounts(t, a, b), "recovered run")
		})
	}
}
