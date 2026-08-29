package chaos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/test/oracle"
)

// The workload every schedule runs.
//
// Two source vertices with different BaseEventTime and different range
// lengths, a WindowCount behind them, a sink behind that, every vertex at
// parallelism 2. Twenty thousand records over sixty-four keys.
//
// # Why the lengths differ
//
// srcA covers its whole range in two thousand elements per subtask while srcB
// needs eight thousand for its. Both inject four barriers, so srcA reaches
// barrier k roughly four times as early in the stream as srcB does, and the
// gate holds srcA's barrier for thousands of srcB elements waiting for the
// matching one. That gap is the alignment window, and it is measured in
// elements rather than in microseconds: without the skew it would be a race
// between two similar inputs, and TriggerDuringAlignment would be aiming at a
// position no run reliably reaches.
//
// # Why WindowCount
//
// Phase 3c made WindowCount the operator the checkpoint actually proves --
// timers and the operator watermark are in its KeyedState, so restoring the
// state is restoring the whole of it -- and it is the one Phase 6 needs. The
// keyed-count operator this suite could have run on has no timers, so a
// recovery over it says nothing about the windowed path.
//
// # Why lateness is zero
//
// The batch oracle has no lateness model, because it has no watermark. The two
// agree only on a run where no record is ever late, so lateness is zero and the
// drop count is asserted at zero: that turns "the oracle does not model this"
// into a checked precondition rather than a hope.
const (
	jobParallelism    = 2
	keyCardinality    = 64
	windowSize        = 5000
	windowLateness    = 0
	watermarkInterval = 100

	sourceACount = 4000
	sourceBCount = 16000
	// Scaled against the range lengths so both vertices inject the same NUMBER
	// of barriers: 2000/500 and 8000/2000 are both four. A vertex injecting
	// more barriers than another is not wrong, but it makes "checkpoint k" mean
	// a different depth on each input and the alignment windows stop lining up
	// with the schedule's checkpoint IDs.
	barrierIntervalA = 500
	barrierIntervalB = 2000

	// maxRecoveries bounds how many times one schedule is resumed.
	//
	// A fault fires at most once, so a schedule of at most three faults cannot
	// need more than three recoveries. The cap is above that on purpose: it is
	// not a tuning knob, it is the assertion that the at-most-once rule holds.
	// Reaching it means a fault fired twice or a recovery made no progress, and
	// either is a bug rather than a schedule that needed more attempts.
	maxRecoveries = 6
)

// sourceAConfig and sourceBConfig are the two generators. Their event-time
// ranges OVERLAP: srcB starts seven seconds into srcA's range, so some
// (key, window) pairs receive records from both vertices and the gate's
// minimum across four inputs decides when they fire. Two disjoint ranges would
// make every window the property of one source and the minimum would have
// nothing to get wrong.
func sourceAConfig() sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed: 0x5A, Count: sourceACount, KeyCardinality: keyCardinality,
		BaseEventTime: 1700000000000, EventTimeStep: 10, MaxLag: 200,
		ValueSize: 8, AmountRange: 1000,
	}
}

func sourceBConfig() sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed: 0x5B, Count: sourceBCount, KeyCardinality: keyCardinality,
		BaseEventTime: 1700000000000 + 7000, EventTimeStep: 10, MaxLag: 200,
		ValueSize: 8, AmountRange: 1000,
	}
}

func windowSpec() oracle.Spec { return oracle.Spec{Size: windowSize, Slide: windowSize} }

// windowFactory builds one window operator per subtask and keeps a handle on
// each, so a run can be asked afterwards whether anything was dropped.
type windowFactory struct {
	mu   sync.Mutex
	made []*operators.WindowCount
}

func (f *windowFactory) newOperator() core.Operator {
	w := operators.NewTumblingCount(windowSize, windowLateness)
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

// jobGraph is the workload as a graph writing into sink.
func jobGraph(sink core.Sink, f *windowFactory) *graph.Graph {
	g := graph.New()
	vertices := []graph.Vertex{
		sourceVertex("srcA", sourceAConfig(), barrierIntervalA),
		sourceVertex("srcB", sourceBConfig(), barrierIntervalB),
		{ID: "window", Kind: graph.VertexOperator, Parallelism: jobParallelism, NewOperator: f.newOperator},
		{ID: "out", Kind: graph.VertexSink, Parallelism: jobParallelism,
			NewSink: func() core.Sink { return sink }},
	}
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			panic(fmt.Sprintf("chaos: AddVertex(%s): %v", v.ID, err))
		}
	}
	for _, e := range [][2]string{{"srcA", "window"}, {"srcB", "window"}, {"window", "out"}} {
		if err := g.Connect(e[0], e[1]); err != nil {
			panic(fmt.Sprintf("chaos: Connect(%s, %s): %v", e[0], e[1], err))
		}
	}
	return g
}

func sourceVertex(id string, cfg sources.GeneratorConfig, barrierInterval int64) graph.Vertex {
	return graph.Vertex{
		ID: id, Kind: graph.VertexSource, Parallelism: jobParallelism,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: watermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrierInterval,
	}
}

// scheduleGraph is the shape ScheduleFor is asked about.
//
// Built once, from the same jobGraph every run uses, so the bounds a schedule
// is drawn against are the bounds of the job it will run on. A separately
// written description of the topology would be a second thing to keep in step,
// and the failure of it drifting is a schedule full of faults that cannot fire
// -- which the census would report as a miss and which looks exactly like the
// interesting kind of miss.
var scheduleGraphOnce = sync.OnceValue(func() *graph.Graph {
	return jobGraph(sinks.NewDiscard(), &windowFactory{})
})

// oracleCounts is the batch answer for this workload.
//
// Computed once for the whole suite rather than once per schedule. The
// workload is fixed -- every seed varies the FAULTS and nothing about the
// input -- so five hundred recomputations would be five hundred copies of one
// number. The comparison is still per schedule; only the arithmetic is shared.
var oracleCounts = sync.OnceValue(func() map[oracle.Key]int64 {
	out := make(map[oracle.Key]int64)
	for _, cfg := range []sources.GeneratorConfig{sourceAConfig(), sourceBConfig()} {
		counts, err := oracle.Counts(cfg, windowSpec())
		if err != nil {
			panic(fmt.Sprintf("chaos: oracle.Counts: %v", err))
		}
		for k, n := range counts {
			out[k] += n
		}
	}
	return out
})

// Result is what one schedule produced.
type Result struct {
	Seed   int64
	Faults []Fault
	// Recoveries is how many times the run was resumed after an abort. It is
	// zero for a schedule whose faults never fired, including the empty ones.
	Recoveries int
}

// RunSchedule runs seed's schedule and compares the result against the batch
// oracle.
//
// A clean run first, to establish that this workload produces the oracle's
// answer with nothing going wrong; then the same job under the schedule's
// faults, resumed from the last complete checkpoint after each abort, until it
// finishes. Both sinks are compared against the oracle.
//
// # Contents, never order
//
// Delivery is at-least-once. There is no transactional sink in this phase, so
// every window the aborted run fired after the checkpoint is fired again by the
// resumed one, and the order records reach the sink after a recovery differs
// from a clean run for reasons that have nothing to do with correctness. What
// is compared is the SORTED contents, with the duplicates collapsed -- and the
// duplicates having to AGREE is the real assertion, because a partial recount,
// a double count, or a window reopened against a stale watermark all show up
// as two different counts under one (key, window).
//
// # One sink across the aborted run and its recoveries
//
// A sink is external and durable: it does not forget what an aborted run wrote
// to it. Handing each attempt a fresh sink would measure recovery against a
// sink that conveniently lost the evidence of double delivery, which on a
// window operator that emits as its windows close is most of the evidence there
// is.
func RunSchedule(t *testing.T, seed int64) Result {
	t.Helper()
	faults := ScheduleFor(seed, scheduleGraphOnce())
	res := Result{Seed: seed, Faults: faults}

	// The clean run. It takes no checkpoints: it is the reference for the
	// CONTENTS of the sink, and a run that has nothing to recover from has
	// nothing to write. Barriers still flow through it, because they are part
	// of the element stream whether or not anybody records snapshots.
	cleanFactory := &windowFactory{}
	cleanSink := sinks.NewCollect()
	if err := runtime.RunWithOptions(context.Background(), jobGraph(cleanSink, cleanFactory),
		runtime.Options{Seed: uint64(seed)}); err != nil {
		t.Fatalf("seed %d: the clean run failed: %v", seed, err)
	}
	assertDroppedNothing(t, seed, cleanFactory, "clean run")
	assertCleanRunFiredEachWindowOnce(t, seed, cleanSink)
	assertMatchesOracle(t, seed, cleanSink, "clean run")

	// The fault run, and its recoveries.
	inj := newInjector(faults)
	root := t.TempDir()
	sink := sinks.NewCollect()
	restoreFrom := ""
	for attempt := 0; ; attempt++ {
		if attempt > maxRecoveries {
			t.Fatalf("seed %d: still failing after %d recoveries with faults %v: a fault fires at most once, "+
				"so this is a fault firing twice or a recovery that made no progress", seed, maxRecoveries, faults)
		}
		f := &windowFactory{}
		err := runtime.RunWithOptions(context.Background(), jobGraph(sink, f), runtime.Options{
			CheckpointRoot: root,
			RestoreFrom:    restoreFrom,
			Seed:           uint64(seed),
			FaultInjector:  inj,
		})
		if err == nil {
			assertDroppedNothing(t, seed, f, "fault run")
			break
		}
		if !errors.Is(err, runtime.ErrFaultInjected) {
			t.Fatalf("seed %d: the run failed for a reason nothing scheduled: %v", seed, err)
		}
		// A run that aborted before its first record reached a window may still
		// have dropped nothing; asserting it here would be asserting on a run
		// that did not finish, which is not what the oracle is compared to.
		res.Recoveries++
		restoreFrom = restorePoint(t, seed, root)
	}

	assertMatchesOracle(t, seed, sink, "fault run")
	return res
}

// restorePoint returns the root to resume from: the same one, if it holds a
// complete checkpoint, or the empty string, which restarts the job from zero.
//
// Restarting from zero is CORRECT and is not a fallback that papers over
// anything. A fault that fires before any checkpoint completes leaves nothing
// to recover from, and the run that follows replays the whole input into a sink
// that still holds what the aborted run wrote. The duplicates that leaves have
// to agree with the oracle exactly as a recovered run's do -- a window fires
// only once its watermark has passed, so its count is final whichever run
// produced it.
//
// Writing the next run's checkpoints into the SAME root is safe in both cases.
// Resuming continues the IDs from the one restored, so nothing is written over;
// restarting from zero reuses IDs 1..k, but the only directories there are
// incomplete ones -- if a complete one existed this function would have
// returned it -- and an incomplete directory is not a recovery point for
// anybody.
func restorePoint(t *testing.T, seed int64, root string) string {
	t.Helper()
	_, ok, err := checkpoint.NewStorage(root).Latest()
	if err != nil {
		t.Fatalf("seed %d: reading the checkpoint root: %v", seed, err)
	}
	if !ok {
		return ""
	}
	return root
}

// assertDroppedNothing is the precondition the oracle comparison rests on.
//
// The oracle has no watermark and therefore no lateness model, so it counts
// every record. The engine drops a record whose window has been purged. They
// agree only when nothing was dropped, and this is where that becomes checked
// rather than assumed. It is also the assertion that catches a recovered run
// replaying its records against a watermark restored too high.
func assertDroppedNothing(t *testing.T, seed int64, f *windowFactory, label string) {
	t.Helper()
	if got := f.dropped(); got != 0 {
		t.Errorf("seed %d: the %s dropped %d assignments as late; the batch oracle has no lateness model, "+
			"so the comparison against it is only valid at zero", seed, label, got)
	}
}

// windowRows decodes what a job wrote into a sink, keeping EVERY row for a
// (key, window) rather than one.
//
// The window start is DERIVED and not read. The operator stamps a fired window
// with its end-1, so the start is EventTime-(size-1); reading EventTime as a
// start would shift every expectation by size-1 and leave every row still
// looking plausible.
func windowRows(t *testing.T, recs []*core.Record) map[oracle.Key][]int64 {
	t.Helper()
	out := make(map[oracle.Key][]int64)
	for _, rec := range recs {
		count, err := operators.DecodeCount(rec.Value)
		if err != nil {
			t.Fatalf("the sink holds a value that is not a count: %v", err)
		}
		k := oracle.Key{Key: string(rec.Key), WindowStart: rec.EventTime - (windowSize - 1)}
		out[k] = append(out[k], count)
	}
	return out
}

// assertCleanRunFiredEachWindowOnce.
//
// A run with no fault has nothing to replay, so every (key, window) reaches the
// sink exactly once. It is asserted separately from the oracle comparison
// because the comparison collapses duplicates by design -- it has to, for the
// recovered runs -- and would therefore pass on a clean run that emitted every
// window twice.
func assertCleanRunFiredEachWindowOnce(t *testing.T, seed int64, sink *sinks.Collect) {
	t.Helper()
	for k, counts := range windowRows(t, sink.Records()) {
		if len(counts) != 1 {
			t.Fatalf("seed %d: the clean run emitted key %x window %d %d times with counts %v; nothing was replayed",
				seed, k.Key, k.WindowStart, len(counts), counts)
		}
	}
}

// assertMatchesOracle compares a sink against the batch oracle.
func assertMatchesOracle(t *testing.T, seed int64, sink *sinks.Collect, label string) {
	t.Helper()
	got := windowRows(t, sink.Records())
	want := oracleCounts()

	// Every duplicate of a (key, window) carries the same count. This is the
	// assertion the at-least-once sink makes possible rather than the one it
	// costs: a replay that was not exact shows up here as two numbers.
	collapsed := make(map[oracle.Key]int64, len(got))
	for k, counts := range got {
		for _, c := range counts[1:] {
			if c != counts[0] {
				t.Fatalf("seed %d: %s: key %x window %d was emitted with counts %v: the replay was not exact",
					seed, label, k.Key, k.WindowStart, counts)
			}
		}
		collapsed[k] = counts[0]
	}

	if len(collapsed) != len(want) {
		t.Errorf("seed %d: %s: the sink holds %d (key, window) rows, want %d",
			seed, label, len(collapsed), len(want))
	}
	for _, tr := range oracle.Sorted(want) {
		k := oracle.Key{Key: tr.Key, WindowStart: tr.WindowStart}
		gotCount, ok := collapsed[k]
		if !ok {
			t.Fatalf("seed %d: %s: the sink is missing key %x window %d, which the oracle counts at %d",
				seed, label, tr.Key, tr.WindowStart, tr.Count)
		}
		if gotCount != tr.Count {
			t.Fatalf("seed %d: %s: key %x window %d counted %d, want %d",
				seed, label, tr.Key, tr.WindowStart, gotCount, tr.Count)
		}
	}
	for k := range collapsed {
		if _, ok := want[k]; !ok {
			t.Fatalf("seed %d: %s: the sink holds key %x window %d, which the input does not produce",
				seed, label, k.Key, k.WindowStart)
		}
	}
}

// siteKey names the subtask a fault is aimed at.
type siteKey struct {
	vertexID string
	subtask  int
}

// injector fires a schedule's faults through runtime.FaultInjector.
//
// # A fault fires at most once
//
// It is one abort, not a standing rule. A recovered run starts its element
// counts again from zero and reaches the same barriers, so a fault that fired
// again would abort at the same place forever and the schedule would never
// finish: the harness would spin to its recovery cap on every seed that
// scheduled a fault near the start of a source. Firing once means at most three
// aborts per schedule, and the cap is there to assert that rather than to allow
// for it.
//
// # Locking
//
// The runtime calls this from every subtask goroutine of a job at once, and
// BeforeElement is on the record path. The indexes are built once and never
// written, so a subtask with no fault against it -- which is most of them --
// returns after one map lookup and takes no lock at all. Only a subtask a fault
// names reaches the mutex.
type injector struct {
	faults []Fault
	// Each index maps a subtask to the positions in faults aimed at it under
	// one trigger. Read-only after construction.
	byElement   map[siteKey][]int
	byBarrier   map[siteKey][]int
	byAlignment map[siteKey][]int

	mu    sync.Mutex
	fired []bool
}

var _ runtime.FaultInjector = (*injector)(nil)

func newInjector(faults []Fault) *injector {
	i := &injector{
		faults:      faults,
		byElement:   make(map[siteKey][]int),
		byBarrier:   make(map[siteKey][]int),
		byAlignment: make(map[siteKey][]int),
		fired:       make([]bool, len(faults)),
	}
	for n, f := range faults {
		key := siteKey{vertexID: f.VertexID, subtask: f.Subtask}
		switch f.Trigger {
		case TriggerAfterElements:
			i.byElement[key] = append(i.byElement[key], n)
		case TriggerAfterBarrier:
			i.byBarrier[key] = append(i.byBarrier[key], n)
		case TriggerDuringAlignment:
			i.byAlignment[key] = append(i.byAlignment[key], n)
		}
	}
	return i
}

// fire marks the first unfired fault among candidates that matches, and reports
// whether it found one.
func (i *injector) fire(candidates []int, matches func(Fault) bool) bool {
	if len(candidates) == 0 {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, n := range candidates {
		if i.fired[n] || !matches(i.faults[n]) {
			continue
		}
		i.fired[n] = true
		return true
	}
	return false
}

func (i *injector) BeforeElement(vertexID string, subtask int, n int64) bool {
	return i.fire(i.byElement[siteKey{vertexID, subtask}], func(f Fault) bool { return f.N == n })
}

func (i *injector) AfterBarrierForwarded(vertexID string, subtask int, checkpointID int64) bool {
	return i.fire(i.byBarrier[siteKey{vertexID, subtask}], func(f Fault) bool { return f.N == checkpointID })
}

func (i *injector) DuringAlignment(vertexID string, subtask int, checkpointID int64, delivered int) bool {
	return i.fire(i.byAlignment[siteKey{vertexID, subtask}], func(f Fault) bool {
		return f.N == checkpointID && f.Inputs == delivered
	})
}
