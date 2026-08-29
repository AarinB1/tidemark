package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
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

// checkpointPollElements is how many of a subtask's own elements pass between
// two reads of the checkpoint root while a checkpoint-relative fault waits to
// arm.
//
// Counted in ELEMENTS and not on a timer, so the overshoot it permits is a
// logical quantity rather than a duration: the fault lands somewhere within
// fifty elements of the point where this subtask first observes a complete
// checkpoint. Fifty against a barrier interval of five thousand is a hundredth
// of an epoch, and polling on every element instead would cost a directory read
// per record to buy back a precision no assertion here uses.
const checkpointPollElements = 50

// faultPosition says where the subtasks of source B abort. Exactly one of the
// two forms is set.
//
// # Why there are two
//
// An ELEMENT position is what a case wants when the element position IS the
// thing under test. TestCrashBeforeAnyCheckpointCompletesHasNothingToRestoreFrom
// has to abort before a barrier is ever injected, and no other phrasing says
// that.
//
// A CHECKPOINT-RELATIVE position is what a case wants when its premise is that
// there is something to recover from. Barrier injection is at a fixed element
// offset, which is invariant 3, but a checkpoint COMPLETING is eight
// acknowledgements, each carrying a state serialisation and an fsync, followed
// by the _METADATA and _COMPLETE writes. That is wall-clock work. And the
// source is not held back across it: when one operator subtask finishes its
// snapshot and forwards the barrier while its sibling is still snapshotting,
// the sink's gate buffers the first one's output without bound and the whole
// source path runs free. So an element count chosen to sit "far enough" past a
// barrier is a bet on relative speed. It wins on a fast machine and loses under
// the race detector on a two-core runner, which is what it did.
//
// Naming the position relative to the thing that must be true makes the premise
// structural instead. It is the same move TriggerDuringAlignment makes in
// test/chaos: aim at the state, never at a counter that merely correlates with
// it.
type faultPosition struct {
	// atElement aborts once this subtask has read that many elements.
	atElement int64
	// afterCheckpoint aborts once the run has written a complete checkpoint
	// with at least this ID and thenElements further elements have been read
	// from this subtask. Zero selects the element form above.
	afterCheckpoint int64
	thenElements    int64
}

func atElement(n int64) faultPosition { return faultPosition{atElement: n} }

// afterCheckpointCompletes positions the fault then elements after a complete
// checkpoint with ID id or higher exists on disk.
func afterCheckpointCompletes(id, then int64) faultPosition {
	return faultPosition{afterCheckpoint: id, thenElements: then}
}

func (p faultPosition) String() string {
	if p.afterCheckpoint == 0 {
		return fmt.Sprintf("at element %d of srcB", p.atElement)
	}
	return fmt.Sprintf("%d elements after a complete checkpoint %d existed", p.thenElements, p.afterCheckpoint)
}

// faultingGenerator fails at a chosen position in its own range.
//
// The trigger is a LOGICAL POSITION -- elements read from this subtask's range,
// and a predicate over what the run has made durable -- and never a wall clock.
// That is invariant 6, and the reason is not tidiness: a fault on a timer lands
// somewhere different on every run, so a test that means "kill between barrier
// k arriving on one input and arriving on another" would sometimes kill
// somewhere else and pass for the wrong reason.
//
// Reading the checkpoint root is not a clock. It asks whether a thing has
// happened, not how long has passed, and the answer is monotone: a complete
// checkpoint is never unwritten, so a position expressed against one cannot
// come undone later in the run. That monotonicity is what lets crashAndRestore
// stop betting on relative speed.
//
// Every method core.Source declares is forwarded EXPLICITLY, and so is Count.
// Embedding core.Source would compile and would leave Count off the concrete
// type, so the runtime's splittableSource assertion would fail: the job would
// be refused above parallelism 1, and a test written around that refusal would
// quietly run a topology in which alignment cannot fail. CLAUDE.md records this
// trap for precisely this test.
type faultingGenerator struct {
	inner *sources.Generator
	at    faultPosition
	// storage is the root this run checkpoints into. A checkpoint-relative
	// position reads it; an element one never touches it.
	storage *checkpoint.Storage

	// opened is set by Open. The runtime makes one instance of a source per
	// vertex purely to ask its Count -- see buildMetadata -- and never opens
	// it, so without this the diagnostics below would report a subtask that
	// read nothing alongside the ones that did the work.
	opened bool
	read   int64
	// sincePoll counts down the elements to the next read of the root.
	sincePoll int64
	armed     bool
	armedAt   int64
	pollErr   error
}

func newFaultingGenerator(cfg sources.GeneratorConfig, at faultPosition, storage *checkpoint.Storage) *faultingGenerator {
	return &faultingGenerator{inner: sources.NewGenerator(cfg), at: at, storage: storage}
}

func (s *faultingGenerator) Open(ctx core.Context) error {
	s.opened = true
	return s.inner.Open(ctx)
}

func (s *faultingGenerator) Next() (*core.Record, bool, error) {
	if s.due() {
		return nil, false, fmt.Errorf("after %d elements: %w", s.read, errInjectedFailure)
	}
	// Surfaced rather than swallowed, and NOT as the injected failure: a root
	// that cannot be read is a broken test rather than a landed fault, and a
	// case that could not tell the two apart would report the second when it
	// meant the first.
	if s.pollErr != nil {
		return nil, false, fmt.Errorf("reading the checkpoint root to arm the fault: %w", s.pollErr)
	}
	s.read++
	return s.inner.Next()
}

// due reports whether this subtask should abort before the next element.
func (s *faultingGenerator) due() bool {
	if s.at.afterCheckpoint == 0 {
		return s.read >= s.at.atElement
	}
	if !s.armed && !s.arm() {
		return false
	}
	return s.read >= s.armedAt+s.at.thenElements
}

// arm reports whether the checkpoint this position waits for has now been
// written, latching the element count at which it was first seen.
//
// Latched, so the root is read only while the fault is still waiting. It is
// also why the position is stable once reached: what follows is a plain count
// of this subtask's own elements from a point the run has already passed.
func (s *faultingGenerator) arm() bool {
	if s.sincePoll > 0 {
		s.sincePoll--
		return false
	}
	s.sincePoll = checkpointPollElements
	id, ok, err := s.storage.Latest()
	if err != nil {
		s.pollErr = err
		return false
	}
	if !ok || id < s.at.afterCheckpoint {
		return false
	}
	s.armed, s.armedAt = true, s.read
	return true
}

func (s *faultingGenerator) SeekTo(offset int64) error { return s.inner.SeekTo(offset) }
func (s *faultingGenerator) Position() int64           { return s.inner.Position() }
func (s *faultingGenerator) Count() int64              { return s.inner.Count() }
func (s *faultingGenerator) Close() error              { return s.inner.Close() }

var _ splittableSource = (*faultingGenerator)(nil)

// faultingSources builds one faulting generator per subtask of source B and
// keeps a handle on each, the way windowFactory does for the operators.
//
// It exists for the failure message. A run that finished WITHOUT the fault
// landing is the one interesting way this harness can break -- a
// checkpoint-relative position that armed too near the end of the range would
// do it -- and "the fault did not land" on its own leaves the reader guessing
// between "never armed" and "armed too late". The handles turn that into a
// sentence naming which.
type faultingSources struct {
	cfg     sources.GeneratorConfig
	at      faultPosition
	storage *checkpoint.Storage

	mu   sync.Mutex
	made []*faultingGenerator
}

func (f *faultingSources) newSource() core.Source {
	s := newFaultingGenerator(f.cfg, f.at, f.storage)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.made = append(f.made, s)
	return s
}

// describe says how far each subtask got and whether its fault ever armed.
//
// Only the OPENED instances are listed. The runtime makes one more per source
// vertex, purely to ask its Count, and reporting that one alongside the real
// ones would read as a subtask that mysteriously did nothing.
//
// The subtasks are listed in the order the runtime happened to construct them,
// which is not their index order, and it says so rather than implying one.
//
// The mutex covers the slice. The per-generator fields it reads are written by
// the subtask goroutines, and are safe to read here only because every caller
// is downstream of a RunWithOptions that has already joined them.
func (f *faultingSources) describe() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := make([]string, 0, len(f.made))
	for _, s := range f.made {
		switch {
		case !s.opened:
			continue
		case s.at.afterCheckpoint == 0:
			parts = append(parts, fmt.Sprintf("read %d of a fault at element %d", s.read, s.at.atElement))
		case s.armed:
			parts = append(parts, fmt.Sprintf("read %d, armed at %d", s.read, s.armedAt))
		default:
			// Why it stopped is not visible from here: a subtask cancelled by
			// its sibling's fault and one that ran off the end of its range
			// both land in this branch, and only the first is normal. In the
			// message crash prints when NO fault landed they are all the
			// second, because a run that nothing aborted ended by exhausting
			// every source.
			parts = append(parts, fmt.Sprintf("read %d and stopped with no complete checkpoint %d in sight",
				s.read, s.at.afterCheckpoint))
		}
	}
	if len(parts) == 0 {
		return "no faulting source was ever opened"
	}
	return "srcB subtasks, in construction order: " + strings.Join(parts, "; ")
}

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
	// fault is where each subtask of source B aborts.
	fault faultPosition
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

	faulty := &faultingSources{cfg: c.b, at: c.fault, storage: checkpoint.NewStorage(root)}

	build = func(withFault bool) (*graph.Graph, *windowFactory) {
		f := &windowFactory{}
		srcA := windowSourceVertex("srcA", c.a, c.parallelism, c.barrierA)
		srcB := graph.Vertex{
			ID: "srcB", Kind: graph.VertexSource, Parallelism: c.parallelism,
			NewSource: func() core.Source {
				if withFault {
					return faulty.newSource()
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
		t.Fatalf("the crashed run returned %v, want the injected failure %s: the fault did not land. %s",
			err, c.fault, faulty.describe())
	}
	// Logged on the way out, not only on failure. Where a checkpoint-relative
	// fault armed is the one number in this harness that varies with the
	// machine, and a CI log that records it is what turns the next surprise
	// into a reading rather than an investigation.
	t.Logf("the fault was positioned %s; %s", c.fault, faulty.describe())
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

// recoveryOutcome is what the crashed half of a case left behind.
type recoveryOutcome struct {
	// checkpointID is the cut the resumed run recovered from.
	checkpointID int64
	// pendingWindows is how many (key, window) pairs at that cut have all their
	// records behind it and their firing watermark ahead of it.
	pendingWindows int
	// crashedRows is how many rows the crashed run had already written to the
	// sink when it stopped.
	crashedRows int
}

// crashAndRestore runs the case to its fault, restarts from the last complete
// checkpoint, and returns the sink both runs wrote into.
//
// # Two premises, and where each comes from
//
// The case asserts nothing unless the crashed run left a complete checkpoint,
// and unless that checkpoint holds a (key, window) whose timer has to survive
// the restore. Both are guarded below rather than assumed.
//
// The FIRST premise is the case's own to establish, and how it does so depends
// on the fault position it chose. A checkpoint-relative position establishes it
// by construction: the fault arms only once the run has observed a complete
// checkpoint, and a complete checkpoint is never unwritten, so one exists when
// the run stops. An element position does not establish it at all -- it names a
// point in the stream and hopes the acknowledgement round beat the source
// there, which is a bet on relative speed that the race detector on a small
// runner is entitled to lose. The guard below is what tells the two apart
// instead of letting the second quietly assert nothing.
//
// The SECOND premise is a property of the TOPOLOGY and not of any position.
// Source B's records pile into windows far above a watermark that source A's
// event times pin low, so every cut in this suite holds hundreds of them. It
// does not vary with where the fault landed, which is why the guard on it has
// never fired and why it is still worth keeping: it is the assertion that would
// catch a checkpoint whose timers stopped being written.
func crashAndRestore(t *testing.T, c recoveryCase) (*sinks.Collect, recoveryOutcome) {
	t.Helper()
	root, collect, build := crash(t, c)
	out := recoveryOutcome{crashedRows: len(collect.Records())}

	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	// The vacuity guard. It cannot fire on a checkpoint-relative position,
	// because the fault does not arm until this same call has already reported
	// a complete checkpoint to the source and nothing removes one afterwards.
	// It is NOT dead: the element positions above still reach it, and it is
	// what caught the sweep betting on relative speed rather than letting the
	// sweep pass on a fast machine and assert nothing on a slow one.
	//
	// For it to fire on a checkpoint-relative position, Latest would have to
	// report a checkpoint that is not usable -- a _COMPLETE marker written
	// before the state files it vouches for, which is invariant 8 read
	// backwards -- or something would have to remove a checkpoint directory
	// while the run was still going.
	if !ok {
		t.Fatalf("the crashed run completed no checkpoint, so there is nothing to recover from and the test asserts nothing. "+
			"The fault was positioned %s", c.fault)
	}
	out.checkpointID = id

	out.pendingWindows = pendingWindowsAtCheckpoint(t, storage, id, c)
	t.Logf("crashed with the fault positioned %s, having written %d rows to the sink; "+
		"resuming from checkpoint %d, which holds %d (key, window) pairs that are complete but unfired",
		c.fault, out.crashedRows, out.checkpointID, out.pendingWindows)
	if out.pendingWindows == 0 {
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
	return collect, out
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
		// An epoch in, which is the general case rather than an aimed one.
		// Stated relative to the checkpoint rather than as an element count, so
		// that "there is something to recover from" is a fact about the run
		// rather than a bet on the acknowledgement round beating the source to
		// element 12000.
		//
		// Behind the FIRST checkpoint and not a later one. How far a source
		// runs past a barrier before that barrier's checkpoint is durable is
		// the one quantity here that moves with the machine, and it moves by
		// thousands of elements: a subtask has been seen reaching the end of a
		// 20000-element range before checkpoint 2 appeared at all. Checkpoint 1
		// is durable with three quarters of the range still ahead, so the two
		// thousand elements of depth are bought where there is room for them.
		fault:       afterCheckpointCompletes(1, 2000),
		parallelism: 2,
		backend:     backend,
	})

	assertSameWindows(t, windowRowsOf(t, collect.Records()), oracleWindowCounts(t, a, b), "recovered run")
}

// TestRecoveryWhenTheFaultLandsBetweenTwoInputsBarriers is the aimed case.
//
// The two sources have deliberately SKEWED range lengths, with barrier
// intervals scaled so that source A injects its barriers at wildly earlier
// points in the stream, and keeps injecting them for longer:
//
//	srcA     400 elements, 200 per subtask, a barrier every 25   -> at 25, 50, ... 200
//	srcB   60000 elements, 30000 per subtask, every 5000         -> at 5000, 10000, ... 30000
//
// Source A runs through its entire range while source B is barely started, so
// at any moment in the middle of the run the gate holds source A's barrier for
// the next checkpoint and is still waiting for source B's. That is the
// alignment window, and it is thousands of elements wide in LOGICAL terms
// rather than a race: source A's whole range is 200 elements and source B's
// barriers are 5000 apart. Source A carrying MORE barriers than source B is
// what keeps that true to the end of the run: its end-of-stream is buffered
// behind its own next barrier, so it never drops out of alignment first.
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
	b := restoreConfig(22, 60000)

	collect, _ := crashAndRestore(t, recoveryCase{
		a: a, b: b,
		// Source A's 200 elements per subtask carry eight barriers and source
		// B's 30000 carry six. Source A having MORE is the half that matters:
		// its end-of-stream sits in the gate's buffers behind its own next
		// barrier, so it stays live at the gate for as long as it has barriers
		// left. If it ran out first its end-of-stream would be delivered, it
		// would drop out of alignment, and the window this case is aimed at
		// would collapse into a race between source B's two subtasks.
		barrierA: 25, barrierB: 5000,
		// Inside the alignment window for source B's FOURTH barrier. Source A
		// produced its whole range long ago and its remaining barriers sit in
		// the gate's buffers, so the moment checkpoint 3 completes the drained
		// buffer hands the gate source A's barrier 4 and alignment for it opens
		// at once, with source B five thousand elements from delivering its
		// own.
		//
		// No further into the epoch than that, and source B's range is half
		// again what the element-count phrasing needed. Both buy the same
		// thing: room. How far a source runs past a barrier before that
		// barrier's checkpoint is durable is the one quantity in this harness
		// that still moves with the machine -- it is several thousand elements
		// and it grows with the size of the snapshot -- so a position behind a
		// LATE checkpoint has to be given range to land in. This one armed at
		// element 17034 of a 20000-element range here, and at 19941 on another
		// run; at 30000 the same lag leaves ten thousand elements behind it.
		fault:       afterCheckpointCompletes(3, 0),
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
		// An ELEMENT position, and it has to be: this case's premise is that no
		// barrier is ever injected, so it cannot be phrased against a checkpoint
		// that by construction never exists.
		fault:       atElement(1000),
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
// Every position is inside an alignment window. Source A produced its whole
// range long ago and its remaining barriers sit in the gate's buffers, so the
// moment checkpoint k completes the drained buffer hands the gate source A's
// barrier k+1 and alignment for it opens at once, with source B five thousand
// elements from delivering its own. Each row is that same aimed case at a
// different depth; a recovery that was right only at the position the test
// above happens to name would show up here.
//
// # Why the positions are checkpoint-relative
//
// They used to be element counts: 9900, 14900, 19900, 19999, each chosen to sit
// just under source B's next barrier so that the checkpoint it recovers from
// had most of a barrier interval behind it to complete in. That is a bet on
// relative speed and it lost in CI. The barrier's POSITION is fixed by
// invariant 3, but the checkpoint COMPLETING is eight acknowledgements, each
// with a state serialisation and an fsync, and the source is not held back
// across them -- one operator subtask that finishes its snapshot first has its
// output buffered without bound by the sink's gate while its sibling is still
// writing, which lets the whole source path run free. On this machine
// checkpoint 1 landed before source B's element 9900; under the race detector
// on a two-core runner it did not, and the vacuity guard in crashAndRestore
// correctly reported that the case was asserting nothing.
//
// A position stated as "this many elements after a complete checkpoint exists"
// makes the premise structural. The fault does not arm until the run has
// written one, so there is always something to recover from, at any speed on
// any machine. It is the same move TriggerDuringAlignment makes in test/chaos:
// aim at the state that must hold, not at a counter that merely correlates with
// it.
//
// # What the rows cover
//
// Three cuts -- checkpoints 1, 2 and 3 -- and two depths into the epoch behind
// the first of them. The depth is how much REPLAY the resumed run has to get
// exactly right: at thenElements zero the fault lands within fifty elements of
// the cut and there is almost nothing to replay, while at two thousand the
// resumed run replays two thousand elements per subtask into a state restored
// from before them. A recovery that was right only where there was nothing to
// replay would show up in the difference.
//
// The depth is NOT a difference in how many windows have fired, and describing
// it as one would be wrong. Source A's end-of-stream is buffered behind its own
// next barrier, so source A never leaves the gate's watermark minimum while the
// run is going, and that minimum sits at source A's event times -- two thousand
// milliseconds into a range source B spans three hundred thousand of. Almost
// nothing fires before the end-of-input flush: the runs log nine to eighteen
// rows written at the fault, out of the thousands the job produces, and that
// count barely moves between the two depths.
//
// So the sweep covers no position "before any window has fired" either. The
// handful of rows that do fire are released before checkpoint 1 can complete --
// its barrier is at source B's element 5000 -- so every completed checkpoint
// here is already past them. That is not a gap to paper over: it is the same
// property that makes this topology the one with the most to lose if timers do
// not survive, since nearly every window is still open at every cut.
// TestRecoveryAcrossAMultiInputTopology is where a run has fired most of its
// windows by the time it dies, and that case is checkpoint-relative now too.
//
// The distinct cuts are ASSERTED after the loop rather than assumed. A sweep
// whose rows all recovered from the same checkpoint would be one case run four
// times, and it would look identical from the outside.
func TestRecoveryIsIndependentOfWhereTheFaultLands(t *testing.T) {
	forEachStateBackend(t, testRecoveryIsIndependentOfWhereTheFaultLands)
}

func testRecoveryIsIndependentOfWhereTheFaultLands(t *testing.T, backend stateBackend) {
	a := restoreConfig(41, 400)
	b := restoreConfig(42, 60000)

	// Source B's subtasks take a barrier every 5000 elements of a 30000-element
	// range, so the checkpoints these rows aim at sit in the first half of it.
	//
	// The range is half again what an element-count phrasing needed, and the
	// reason is the one quantity in this harness that still moves with the
	// machine: how far a source runs past a barrier before that barrier's
	// checkpoint is durable. A source is not held back across the
	// acknowledgement round -- an operator subtask that finishes its snapshot
	// first has its output buffered without bound by the sink's gate while its
	// sibling is still writing -- so the lag runs to several thousand elements
	// even on the subtask the pipeline is waiting on. At 20000 per subtask that
	// put checkpoint 3 as late as element 19941, sixty from the end, with
	// nothing left for a fault to land in. At 30000 every row below arms with a
	// third of the range still ahead of it.
	recovered := make(map[int64]bool)
	for _, tc := range []struct {
		name string
		at   faultPosition
	}{
		{"atCheckpoint1", afterCheckpointCompletes(1, 0)},
		{"deepAfterCheckpoint1", afterCheckpointCompletes(1, 2000)},
		{"atCheckpoint2", afterCheckpointCompletes(2, 0)},
		{"atCheckpoint3", afterCheckpointCompletes(3, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collect, out := crashAndRestore(t, recoveryCase{
				a: a, b: b,
				// 25 against 5000: source A's 200 elements per subtask carry
				// eight barriers and source B's 30000 carry six.
				//
				// Source A having MORE is the half that matters, and it is not
				// symmetry for its own sake. Source A's end-of-stream sits in
				// the gate's buffers behind its own next barrier, so it stays
				// live at the gate for as long as it has barriers left. If it
				// ran out first its end-of-stream would be delivered, it would
				// drop out of alignment, and every window after that would
				// collapse into a race between source B's two subtasks --
				// which is the narrow thing this topology exists not to be.
				barrierA: 25, barrierB: 5000,
				fault:       tc.at,
				parallelism: 2,
				backend:     backend,
			})
			recovered[out.checkpointID] = true
			assertSameWindows(t, windowRowsOf(t, collect.Records()), oracleWindowCounts(t, a, b), "recovered run")
		})
	}

	// The distinct cuts, asserted rather than assumed. A sweep whose rows all
	// recovered from the same checkpoint would be one case run four times under
	// four names, and from the outside it would look identical to this one.
	if len(recovered) < 2 {
		t.Errorf("every position in the sweep recovered from the same cut %v, so this is one case run four times "+
			"under four names rather than a sweep across the run", recovered)
	}
}
