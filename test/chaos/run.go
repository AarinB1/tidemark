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
// needs eight thousand for its. Both inject eight barriers, so srcA reaches
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
	// of barriers: 2000/250 and 8000/1000 are both eight. A vertex injecting
	// more barriers than another is not wrong, but it makes "checkpoint k" mean
	// a different depth on each input and the alignment windows stop lining up
	// with the schedule's checkpoint IDs.
	//
	// # Why eight and not four
	//
	// Phase 4 ran these at 500 and 2000, four barriers a subtask, and the
	// census that came out of it said the suite was weaker than its schedule
	// count: 359 of 661 resumes restarted from ZERO because the fault fired
	// before any checkpoint had completed, and a run that restarts from zero
	// exercises replay rather than restore. Phase 5's exactly-once claim is
	// about what restore commits, so a suite that reaches restore in fewer than
	// half its resumes is validating the wrong half of the property.
	//
	// Halving both intervals halves the distance to the first complete
	// checkpoint without touching the record count, the parallelism, the keys
	// or the window size -- so the oracle's answer, the alignment skew and the
	// state the operator holds are all the workload Phase 4 measured. What
	// moves is where the cuts fall. Measured over three 500-seed runs and three
	// 25-seed ones: resumes from a real checkpoint 45.7% -> 65.3-69.3%, and
	// schedules recovering from a cut that held a complete but unfired window
	// 47.4% -> 58.6-61.4%.
	//
	// The cost is fsyncs. Twice the barriers is twice the checkpoints and twice
	// the state files, which is about twenty per cent on the 500-seed target
	// (41s -> 48-53s) and about seven per cent on the 25-seed subset `make
	// check` runs under the race detector. Quartering them instead was measured
	// too and buys more (85.8% of resumes from a checkpoint) at ninety per cent
	// more wall clock, and it moves the flush fraction out of the timing
	// baseline this workload has been held to since Phase 4 -- which would mean
	// re-committing a figure whose value is that it has not moved.
	barrierIntervalA = 250
	barrierIntervalB = 1000

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
	// tally is where the operators this factory makes report their firings. A
	// nil tally means the run is not being timed, which is what the graph built
	// only to be scheduled against uses.
	tally *firingTally

	mu   sync.Mutex
	made []*operators.WindowCount
}

func (f *windowFactory) newOperator() core.Operator {
	w := operators.NewTumblingCount(windowSize, windowLateness)
	f.mu.Lock()
	f.made = append(f.made, w)
	f.mu.Unlock()
	if f.tally == nil {
		return w
	}
	return &firingRecorder{inner: w, tally: f.tally}
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

// jobGraph is the workload as a graph whose sink subtasks come from newSink.
//
// A FACTORY and not a sink. Phase 4 handed one sinks.Collect to every subtask,
// which is right for a sink that locks and holds a slice; it is wrong for
// sinks.Transactional, which owns a file handle and an epoch counter. Several
// goroutines sharing one would write into a single staging file and commit it
// under a single name, so half the output would disappear -- and the visible
// symptom is a short write from inside bufio rather than anything about
// correctness. Transactional refuses a second Open for that reason.
func jobGraph(newSink func() core.Sink, f *windowFactory) *graph.Graph {
	g := graph.New()
	vertices := []graph.Vertex{
		sourceVertex("srcA", sourceAConfig(), barrierIntervalA),
		sourceVertex("srcB", sourceBConfig(), barrierIntervalB),
		{ID: "window", Kind: graph.VertexOperator, Parallelism: jobParallelism, NewOperator: f.newOperator},
		{ID: "out", Kind: graph.VertexSink, Parallelism: jobParallelism, NewSink: newSink},
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
	return jobGraph(func() core.Sink { return sinks.NewDiscard() }, &windowFactory{})
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
	// Outcomes says what became of each scheduled fault, in the schedule's own
	// order. It is the same length as Faults.
	Outcomes []FaultOutcome
	// Recoveries is one entry per resume, in order. It is empty for a schedule
	// whose faults never fired, including the empty ones.
	Recoveries []Recovery
	// CleanTiming and FaultTiming are what the oracle cannot see: where the
	// windows fired and how much state was held while they had not. The fault
	// side covers every attempt of the schedule together, because they are one
	// job that happened to be interrupted. See Timing.
	CleanTiming Timing
	FaultTiming Timing
}

// RunSchedule runs seed's schedule and compares the result against the batch
// oracle.
//
// A clean run first, to establish that this workload produces the oracle's
// answer with nothing going wrong; then the same job under the schedule's
// faults, resumed from the last complete checkpoint after each abort, until it
// finishes. Both sinks are compared against the oracle.
//
// # What is compared, and where it is read from
//
// COMMITTED FILES, through sinks.ReadCommitted, and never an in-memory slice.
// That is what makes the exactly-once claim mean something: the comparison runs
// against files that survived a crash rather than against a slice that was
// never at risk. A staging file is not output and is never read as one.
//
// Sorted contents, never emission order. Ordering after a recovery differs from
// a clean run for reasons that have nothing to do with correctness.
//
// Through Phase 4 the assertion was that duplicates AGREED, because delivery
// was at-least-once and a recovered run re-fired every window the aborted one
// had fired after the cut. It is now that there are NO duplicates. A window
// fired after the cut was staged into an epoch above the checkpoint, and
// restore discards those; the resumed run re-fires it into an epoch of its own
// and commits it once. That is the whole of what this phase buys, and it is a
// strictly stronger assertion than the one it replaces.
//
// # One output root across the aborted run and its recoveries
//
// A sink is external and durable: it does not forget what an aborted run
// committed. Handing each attempt a fresh directory would measure recovery
// against a sink that conveniently lost the evidence of double delivery, which
// on a window operator that emits as its windows close is most of the evidence
// there is.
func RunSchedule(t *testing.T, seed int64) Result {
	t.Helper()
	faults := ScheduleFor(seed, scheduleGraphOnce())
	res := Result{Seed: seed, Faults: faults}

	// The clean run. It takes no checkpoints: it is the reference for the
	// CONTENTS of the sink, and a run that has nothing to recover from has
	// nothing to write. Barriers still flow through it, because they are part
	// of the element stream whether or not anybody records snapshots.
	cleanTally, cleanMeter := &firingTally{}, &stateMeter{}
	cleanFactory := &windowFactory{tally: cleanTally}
	cleanOutput := t.TempDir()
	if err := runtime.RunWithOptions(context.Background(),
		jobGraph(transactionalSinks(cleanOutput), cleanFactory),
		runtime.Options{Seed: uint64(seed), NewState: cleanMeter.newState}); err != nil {
		t.Fatalf("seed %d: the clean run failed: %v", seed, err)
	}
	res.CleanTiming = cleanTally.timing()
	res.CleanTiming.PeakStateEntries = cleanMeter.peak.Load()
	assertDroppedNothing(t, seed, cleanFactory, "clean run")
	assertNothingLeftStaged(t, seed, cleanOutput)
	cleanRows := committedRows(t, seed, cleanOutput, "clean run")
	assertEachWindowCommittedOnce(t, seed, cleanRows, "clean run")
	assertMatchesOracle(t, seed, cleanRows, "clean run")

	// The fault run, and its recoveries.
	inj := newInjector(faults)
	root := t.TempDir()
	output := t.TempDir()
	restoreFrom := ""
	for attempt := 0; ; attempt++ {
		if attempt > maxRecoveries {
			t.Fatalf("seed %d: still failing after %d recoveries with faults %v: a fault fires at most once, "+
				"so this is a fault firing twice or a recovery that made no progress", seed, maxRecoveries, faults)
		}
		// One tally and one meter per ATTEMPT. The firings sum across attempts
		// because they are all firings of this schedule; the peak does not,
		// because an attempt has unwound before the next one starts and the two
		// were never held at once. Timing.add is where that distinction lives.
		tally, meter := &firingTally{}, &stateMeter{}
		f := &windowFactory{tally: tally}
		err := runtime.RunWithOptions(context.Background(), jobGraph(transactionalSinks(output), f), runtime.Options{
			CheckpointRoot: root,
			RestoreFrom:    restoreFrom,
			Seed:           uint64(seed),
			FaultInjector:  inj,
			NewState:       meter.newState,
		})
		attempt := tally.timing()
		attempt.PeakStateEntries = meter.peak.Load()
		res.FaultTiming.add(attempt)
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
		res.Recoveries = append(res.Recoveries, recoveryPoint(t, seed, root))
		if res.Recoveries[len(res.Recoveries)-1].FromCheckpoint {
			restoreFrom = root
		} else {
			restoreFrom = ""
		}
	}

	// The staging check comes FIRST, and the order is not cosmetic. It is the
	// more primitive fact -- a transaction nobody will ever commit -- and the
	// oracle comparison ends the test with a Fatalf, so a suite that checked it
	// second would report the symptom and never the cause. Seed 105 was
	// diagnosed the long way for exactly that reason.
	assertNothingLeftStaged(t, seed, output)
	faultRows := committedRows(t, seed, output, "fault run")
	assertEachWindowCommittedOnce(t, seed, faultRows, "fault run")
	assertMatchesOracle(t, seed, faultRows, "fault run")
	res.Outcomes = inj.outcomes()
	return res
}

// transactionalSinks is the factory jobGraph is handed: one sink per subtask,
// all under one output root.
func transactionalSinks(root string) func() core.Sink {
	return func() core.Sink { return sinks.NewTransactional(root) }
}

// committedRows decodes what a run COMMITTED, keeping every row for a
// (key, window) rather than one.
//
// Read from the committed directory and nowhere else. A staging file belongs to
// an epoch no checkpoint vouched for: its records are replayable, so a
// comparison that counted them would count records the recovered run produces
// again.
//
// The window start is DERIVED and not read. The operator stamps a fired window
// with its end-1, so the start is EventTime-(size-1); reading EventTime as a
// start would shift every expectation by size-1 and leave every row still
// looking plausible.
func committedRows(t *testing.T, seed int64, root, label string) map[oracle.Key][]int64 {
	t.Helper()
	recs, err := sinks.ReadCommitted(root)
	if err != nil {
		t.Fatalf("seed %d: %s: reading the committed output: %v", seed, label, err)
	}
	out := make(map[oracle.Key][]int64)
	for _, rec := range recs {
		count, err := operators.DecodeCount(rec.Value)
		if err != nil {
			t.Fatalf("seed %d: %s: the committed output holds a value that is not a count: %v",
				seed, label, err)
		}
		k := oracle.Key{Key: string(rec.Key), WindowStart: rec.EventTime - (windowSize - 1)}
		out[k] = append(out[k], count)
	}
	return out
}

// assertEachWindowCommittedOnce is the exactly-once claim.
//
// Every (key, window) appears in the committed output exactly once, on a
// recovered run as much as on a clean one. Through Phase 4 this could only be
// asserted of the clean run: delivery was at-least-once, so a recovered run
// re-fired every window the aborted one had fired after the cut and the
// comparison had to collapse duplicates. It no longer does, and this is the
// assertion that says so.
//
// A window fired after the cut was staged into an epoch above the checkpoint
// being resumed from, and restore discards those; the resumed run re-fires it
// into an epoch of its own and commits it once. Committing at snapshot time
// instead -- invariant 4 removed -- puts the aborted run's copy in committed/
// as well, and shows up here as a (key, window) with two rows.
func assertEachWindowCommittedOnce(t *testing.T, seed int64, rows map[oracle.Key][]int64, label string) {
	t.Helper()
	for k, counts := range rows {
		if len(counts) != 1 {
			t.Fatalf("seed %d: %s: key %x window %d is committed %d times with counts %v; "+
				"committed output is exactly once, so a second copy is a transaction that was "+
				"committed and then produced again", seed, label, k.Key, k.WindowStart, len(counts), counts)
		}
	}
}

// assertNothingLeftStaged.
//
// A run that finished committed every epoch: the ones a checkpoint covered, on
// its notification, and the one after the last barrier, because the job
// succeeded. A staging file surviving all of that is a transaction nobody will
// ever commit -- records that are simply absent from the output, which the
// oracle comparison catches only if no other copy of that (key, window) exists.
func assertNothingLeftStaged(t *testing.T, seed int64, root string) {
	t.Helper()
	staged, err := sinks.StagingFiles(root)
	if err != nil {
		t.Fatalf("seed %d: reading the staging directory: %v", seed, err)
	}
	if len(staged) != 0 {
		t.Errorf("seed %d: the run finished and %v are still staged: every epoch is either covered "+
			"by a checkpoint that completed or is the final one", seed, staged)
	}
}

// recoveryPoint describes where the next attempt will resume from, and counts
// the pending windows at that cut.
//
// Restarting from zero is CORRECT and is not a fallback that papers over
// anything. A fault that fires before any checkpoint completes leaves nothing
// to recover from, and the run that follows replays the whole input into a sink
// that still holds what the aborted run wrote. The duplicates that leaves have
// to agree with the oracle exactly as a recovered run's do -- a window fires
// only once its watermark has passed, so its count is final whichever run
// produced it. What a suite of only such restarts would NOT do is exercise
// restore, which is why the census counts the two apart.
//
// Writing the next run's checkpoints into the SAME root is safe in both cases.
// Resuming continues the IDs from the one restored, so nothing is written over;
// restarting from zero reuses IDs 1..k, but the only directories there are
// incomplete ones -- if a complete one existed this function would have
// returned it -- and an incomplete directory is not a recovery point for
// anybody.
func recoveryPoint(t *testing.T, seed int64, root string) Recovery {
	t.Helper()
	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil {
		t.Fatalf("seed %d: reading the checkpoint root: %v", seed, err)
	}
	if !ok {
		return Recovery{}
	}
	pending, err := pendingWindowsAt(storage, id)
	if err != nil {
		t.Fatalf("seed %d: counting the pending windows at checkpoint %d: %v", seed, id, err)
	}
	return Recovery{FromCheckpoint: true, CheckpointID: id, PendingWindows: pending}
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

// assertMatchesOracle compares committed output against the batch oracle.
func assertMatchesOracle(t *testing.T, seed int64, got map[oracle.Key][]int64, label string) {
	t.Helper()
	want := oracleCounts()

	collapsed := make(map[oracle.Key]int64, len(got))
	for k, counts := range got {
		collapsed[k] = counts[0]
	}

	if len(collapsed) != len(want) {
		t.Errorf("seed %d: %s: the committed output holds %d (key, window) rows, want %d",
			seed, label, len(collapsed), len(want))
	}
	for _, tr := range oracle.Sorted(want) {
		k := oracle.Key{Key: tr.Key, WindowStart: tr.WindowStart}
		gotCount, ok := collapsed[k]
		if !ok {
			t.Fatalf("seed %d: %s: the committed output is missing key %x window %d, which the "+
				"oracle counts at %d", seed, label, tr.Key, tr.WindowStart, tr.Count)
		}
		if gotCount != tr.Count {
			t.Fatalf("seed %d: %s: key %x window %d counted %d, want %d",
				seed, label, tr.Key, tr.WindowStart, gotCount, tr.Count)
		}
	}
	for k := range collapsed {
		if _, ok := want[k]; !ok {
			t.Fatalf("seed %d: %s: the committed output holds key %x window %d, which the input "+
				"does not produce", seed, label, k.Key, k.WindowStart)
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
	// windows records the alignment windows the gates actually opened, as the
	// greatest number of inputs seen delivered for each (subtask, checkpoint).
	//
	// It is what lets the census tell "the fault had no window to land in" from
	// "there was a window and alignment completed before the schedule's count".
	// Without it every alignment fault that did not fire would look the same,
	// and a trigger kind that had quietly stopped working would be
	// indistinguishable from one that was merely unlucky -- which is the whole
	// thing step 4 exists to rule out.
	//
	// Recorded on EVERY consultation, including the ones that fire nothing, and
	// across every attempt of a schedule. A run that aborts truncates what it
	// records, so a checkpoint absent from here may have been reached by a
	// later attempt; the classification is conservative in the direction that
	// under-reports a genuine window rather than inventing one.
	windows map[alignKey]int
}

// alignKey names one subtask's alignment of one checkpoint.
type alignKey struct {
	site         siteKey
	checkpointID int64
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
	i.windows = make(map[alignKey]int)
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
	site := siteKey{vertexID, subtask}
	i.observeWindow(alignKey{site: site, checkpointID: checkpointID}, delivered)
	return i.fire(i.byAlignment[site], func(f Fault) bool {
		return f.N == checkpointID && f.Inputs == delivered
	})
}

// observeWindow records that an alignment window was open at this many inputs.
//
// This one takes the lock unconditionally, unlike the three fire paths. It can
// afford to: the gate consults during alignment a handful of times per
// checkpoint per subtask, which is tens of calls in a run, against tens of
// thousands on the record path.
func (i *injector) observeWindow(k alignKey, delivered int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if delivered > i.windows[k] {
		i.windows[k] = delivered
	}
}

// outcomes says what became of each scheduled fault, in the schedule's order.
//
// Called after every attempt has finished, so nothing is still writing.
func (i *injector) outcomes() []FaultOutcome {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]FaultOutcome, 0, len(i.faults))
	for n, f := range i.faults {
		o := FaultOutcome{Fault: f, Fired: i.fired[n]}
		if f.Trigger == TriggerDuringAlignment {
			o.Alignment = i.classifyAlignment(f, i.fired[n])
		}
		out = append(out, o)
	}
	return out
}

// classifyAlignment says which of the three alignment outcomes f reached.
//
// A fault that fired landed inside a window, and that is a fact about the call
// site rather than an inference: the gate offers the decision only while a live
// input has still to deliver the barrier. One that did not fire is separated by
// whether that subtask ever opened an alignment for that checkpoint at all --
// if it did, alignment completed before the schedule's input count; if it did
// not, there was never a window there to aim at.
func (i *injector) classifyAlignment(f Fault, fired bool) AlignmentOutcome {
	if fired {
		return AlignmentInsideWindow
	}
	k := alignKey{site: siteKey{vertexID: f.VertexID, subtask: f.Subtask}, checkpointID: f.N}
	if _, ok := i.windows[k]; ok {
		return AlignmentCompletedFirst
	}
	return AlignmentNeverOpened
}
