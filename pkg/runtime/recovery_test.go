package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// errInjectedFailure is what the fault decorator returns. Matched with
// errors.Is so a run that failed for some other reason does not read as the
// fault having been injected.
var errInjectedFailure = errors.New("injected source failure")

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

// recoveryCase is one crash-and-resume scenario: two source vertices, a
// keyed-count operator behind them, and a fault on the second source at a
// chosen logical position.
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
}

// crash runs the case up to its injected fault and returns the checkpoint root
// it left behind, plus the sink it wrote into.
//
// ONE sink is used across the crashed run and the resumed one, deliberately. A
// sink is external and durable: it does not forget what a crashed run wrote to
// it, and handing the resumed run a fresh sink would measure recovery against a
// sink that conveniently lost the evidence of any double delivery.
func crash(t *testing.T, c recoveryCase) (root string, collect *sinks.Collect, build func(faulty bool) *graph.Graph) {
	t.Helper()
	root = t.TempDir()
	collect = sinks.NewCollect()

	build = func(faulty bool) *graph.Graph {
		srcA := countingSourceVertex("srcA", c.a, c.parallelism, c.barrierA, nil)
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
		return countingGraph(t, collect, c.parallelism, srcA, srcB)
	}

	err := RunWithOptions(context.Background(), build(true), Options{CheckpointRoot: root, Seed: c.a.Seed})
	if !errors.Is(err, errInjectedFailure) {
		t.Fatalf("the crashed run returned %v, want the injected failure: the fault did not land", err)
	}

	// The crashed run emitted nothing: keyedCount emits at end of stream and the
	// job never reached one. Asserted rather than assumed, because it is what
	// makes the comparison below a comparison of the resumed run's output
	// against a clean run rather than of a union against it.
	if got := len(collect.Records()); got != 0 {
		t.Fatalf("the crashed run wrote %d records to the sink before failing", got)
	}
	return root, collect, build
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
// time, so it does not vary with the scheduler.
func crashAndRestore(t *testing.T, c recoveryCase) *sinks.Collect {
	t.Helper()
	root, collect, build := crash(t, c)

	id, ok, err := checkpoint.NewStorage(root).Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatal("the crashed run completed no checkpoint, so there is nothing to recover from and the test asserts nothing")
	}
	t.Logf("crashed after %d elements of srcB per subtask; resuming from checkpoint %d", c.failAfterB, id)

	if err := RunWithOptions(context.Background(), build(false), Options{RestoreFrom: root, Seed: c.a.Seed}); err != nil {
		t.Fatalf("the resumed run returned %v", err)
	}
	return collect
}

// TestRecoveryAcrossAMultiInputTopology is the general case.
//
// Two source vertices at parallelism 2 give every operator subtask four inputs,
// a keyed-count operator holds the state, and source B dies at a chosen logical
// position between two of its own barriers. The resumed run must produce
// exactly what a clean run produces.
//
// It compares CONTENTS and not emission order: delivery is at-least-once and
// the order after a recovery will differ from a clean run, so an order
// comparison would be a broken test. Here the contents are the per-key counts,
// which is the whole of what a keyed-count job produces.
//
// What this case does NOT test is alignment. Both sources use the same barrier
// interval over the same range length, so they reach barrier k at nearly the
// same point, and a fault at an arbitrary position almost never lands in the
// window between one input's barrier and another's. That window is what
// alignment exists for, and hitting it is the next test's job.
func TestRecoveryAcrossAMultiInputTopology(t *testing.T) {
	const count = 40000
	a := restoreConfig(11, count)
	b := restoreConfig(12, count)

	collect := crashAndRestore(t, recoveryCase{
		a: a, b: b,
		// 20000 elements per subtask, so barriers at 5000, 10000, 15000, 20000.
		barrierA: 5000, barrierB: 5000,
		// Between source B's second barrier and its third, with 7000 elements
		// of margin behind the first for the pipeline to drain.
		failAfterB:  12000,
		parallelism: 2,
	})

	assertSameCounts(t, countsOf(t, collect.Records()), oracleCounts(t, a, b), "recovered run")
}

// TestRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers is the aimed case, and
// the one this phase turns on.
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
// The fault is at element 15000 of source B, which is inside the window for
// source B's fourth barrier and 5000 elements clear of its third.
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
// state that already counted it, and every key source A touched comes out too
// high.
func TestRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers(t *testing.T) {
	a := restoreConfig(21, 400)
	b := restoreConfig(22, 40000)

	collect := crashAndRestore(t, recoveryCase{
		a: a, b: b,
		barrierA: 50, barrierB: 5000,
		failAfterB:  15000,
		parallelism: 2,
	})

	assertSameCounts(t, countsOf(t, collect.Records()), oracleCounts(t, a, b), "recovered run")
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
	a := restoreConfig(31, 40000)
	b := restoreConfig(32, 40000)

	root, _, build := crash(t, recoveryCase{
		a: a, b: b,
		barrierA: 5000, barrierB: 5000,
		failAfterB:  1000,
		parallelism: 2,
	})

	if _, ok, err := checkpoint.NewStorage(root).Latest(); err != nil || ok {
		t.Fatalf("Latest = (ok %t, err %v), want no complete checkpoint", ok, err)
	}
	err := RunWithOptions(context.Background(), build(false), Options{RestoreFrom: root, Seed: a.Seed})
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
// Each position leaves at least 5000 of source B's elements between it and the
// barrier before it, which is the pipeline depth several times over; see the
// note on crashAndRestore.
func TestRecoveryIsIndependentOfWhereTheFaultLands(t *testing.T) {
	a := restoreConfig(41, 400)
	b := restoreConfig(42, 40000)

	// Source B's subtasks take barriers at 5000, 10000, 15000 and 20000 of a
	// 20000-element range.
	for _, failAfter := range []int64{10001, 13000, 17000, 19999} {
		t.Run(fmt.Sprintf("after%d", failAfter), func(t *testing.T) {
			collect := crashAndRestore(t, recoveryCase{
				a: a, b: b,
				barrierA: 50, barrierB: 5000,
				failAfterB:  failAfter,
				parallelism: 2,
			})
			assertSameCounts(t, countsOf(t, collect.Records()), oracleCounts(t, a, b), "recovered run")
		})
	}
}
