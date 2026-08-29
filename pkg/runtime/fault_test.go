package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// consultation is one call the runtime made into a fault injector.
//
// Every field the runtime passes is recorded, including the ones the fire
// decision does not use. The point of these tests is that the call sites are
// keyed to logical position and nowhere else, and a recorder that kept only the
// fields it matched on could not tell a position that drifted from one that did
// not.
type consultation struct {
	site      string
	vertexID  string
	subtask   int
	n         int64
	delivered int
}

func (c consultation) String() string {
	return fmt.Sprintf("%s %s[%d] n=%d delivered=%d", c.site, c.vertexID, c.subtask, c.n, c.delivered)
}

// recordingInjector records every consultation and fires at most one of them.
//
// fire is matched against the whole consultation rather than against a
// position, so a test can say "abort the operator's third subtask before its
// 500th record" and get an abort at exactly that call and no other. A nil fire
// fires nothing, which is how the "consulted but never fires" cases run.
//
// The runtime calls this from every subtask goroutine at once, so it locks.
type recordingInjector struct {
	fire func(consultation) bool

	mu    sync.Mutex
	calls []consultation
	fired []consultation
}

var _ FaultInjector = (*recordingInjector)(nil)

func (r *recordingInjector) decide(c consultation) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
	if r.fire == nil || !r.fire(c) {
		return false
	}
	r.fired = append(r.fired, c)
	return true
}

func (r *recordingInjector) BeforeElement(vertexID string, subtask int, n int64) bool {
	return r.decide(consultation{site: "before-element", vertexID: vertexID, subtask: subtask, n: n})
}

func (r *recordingInjector) AfterBarrierForwarded(vertexID string, subtask int, checkpointID int64) bool {
	return r.decide(consultation{site: "after-barrier", vertexID: vertexID, subtask: subtask, n: checkpointID})
}

func (r *recordingInjector) DuringAlignment(vertexID string, subtask int, checkpointID int64, delivered int) bool {
	return r.decide(consultation{site: "during-alignment", vertexID: vertexID, subtask: subtask, n: checkpointID, delivered: delivered})
}

// at returns every consultation made at one site against one subtask.
func (r *recordingInjector) at(site, vertexID string, subtask int) []consultation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []consultation
	for _, c := range r.calls {
		if c.site == site && c.vertexID == vertexID && c.subtask == subtask {
			out = append(out, c)
		}
	}
	return out
}

func (r *recordingInjector) firings() []consultation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.fired)
}

// faultConfig is a generator dense enough that watermarks advance and barriers
// are injected several times over the run.
func faultConfig(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed: seed, Count: count, KeyCardinality: 64,
		BaseEventTime: 1700000000000, EventTimeStep: 10, MaxLag: 200,
		ValueSize: 8, AmountRange: 1000,
	}
}

// faultGraph is two source vertices with SKEWED range lengths feeding a window
// operator and a sink, every vertex at parallelism 2.
//
// The skew is what makes this topology able to fail the way the alignment
// assertions below name. srcA runs its whole range while srcB is barely
// started, so the gate holds srcA's barrier and waits thousands of srcB
// elements for the matching one: an alignment window wide enough to aim at.
// A single-input chain has no such window however the test is named, which is
// the working agreement's rule about topology read as a requirement.
func faultGraph(t *testing.T, sink core.Sink, injectorSource func() core.Source) *graph.Graph {
	t.Helper()
	srcA := graph.Vertex{
		ID: "srcA", Kind: graph.VertexSource, Parallelism: 2,
		NewSource:                 func() core.Source { return sources.NewGenerator(faultConfig(11, 400)) },
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         200,
		BarrierIntervalElements:   50,
	}
	srcB := graph.Vertex{
		ID: "srcB", Kind: graph.VertexSource, Parallelism: 2,
		NewSource:                 func() core.Source { return sources.NewGenerator(faultConfig(12, 8000)) },
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         200,
		BarrierIntervalElements:   1000,
	}
	if injectorSource != nil {
		srcB.NewSource = injectorSource
	}
	return buildGraph(t, []graph.Vertex{
		srcA, srcB,
		{ID: "op", Kind: graph.VertexOperator, Parallelism: 2, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 2, NewSink: func() core.Sink { return sink }},
	}, [][2]string{{"srcA", "op"}, {"srcB", "op"}, {"op", "out"}})
}

// TestBeforeElementIsConsultedOncePerRecordFromZero pins the logical position
// the element trigger is keyed to.
//
// The counts handed in must be 0, 1, 2, ... with no gap and no repeat, for
// every subtask of every vertex. A gap means a record slipped past the trigger,
// which makes a fault scheduled at element n fire at some later record; a
// repeat means two records shared a position, which makes it fire early. Either
// way a seed stops naming the position it says it names, and nothing else in
// this phase would notice.
func TestBeforeElementIsConsultedOncePerRecordFromZero(t *testing.T) {
	inj := &recordingInjector{}
	collect := sinks.NewCollect()
	if err := RunWithOptions(context.Background(), faultGraph(t, collect, nil),
		Options{FaultInjector: inj}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	total := int64(0)
	for _, vertexID := range []string{"srcA", "srcB", "op", "out"} {
		for subtask := range 2 {
			calls := inj.at("before-element", vertexID, subtask)
			for i, c := range calls {
				if c.n != int64(i) {
					t.Fatalf("%s[%d] was consulted with n=%d for its record at index %d", vertexID, subtask, c.n, i)
				}
				if c.delivered != 0 {
					t.Fatalf("%s: delivered is %d and is meaningless at this site", c, c.delivered)
				}
			}
			if len(calls) == 0 {
				t.Errorf("%s[%d] processed no records, so this subtask asserts nothing", vertexID, subtask)
			}
			if vertexID == "out" {
				total += int64(len(calls))
			}
		}
	}
	// Every record reaches the sink exactly once, so the consultations at the
	// sink count the whole input. A trigger consulted on watermarks or barriers
	// as well would overshoot this.
	if want := int64(8400); total != want {
		t.Errorf("the sink was consulted %d times, want %d: the element trigger is not counting records", total, want)
	}
	if got := int64(len(collect.Records())); got != total {
		t.Errorf("the sink wrote %d records and was consulted %d times", got, total)
	}
}

// TestBeforeElementFiresAtTheSpecifiedPositionAndNowhereElse.
//
// The subtask must process exactly n records and then stop. Asserted on the
// consultations rather than on the sink, because the sink is downstream of a
// shuffle and a partition change would move the number without the trigger
// having moved at all.
func TestBeforeElementFiresAtTheSpecifiedPositionAndNowhereElse(t *testing.T) {
	for _, tc := range []struct {
		vertexID string
		subtask  int
		n        int64
	}{
		{"srcB", 0, 1500},
		{"srcB", 1, 0},
		{"op", 1, 700},
		{"out", 0, 250},
	} {
		t.Run(fmt.Sprintf("%s%d_at%d", tc.vertexID, tc.subtask, tc.n), func(t *testing.T) {
			inj := &recordingInjector{fire: func(c consultation) bool {
				return c.site == "before-element" && c.vertexID == tc.vertexID && c.subtask == tc.subtask && c.n == tc.n
			}}
			err := RunWithOptions(context.Background(), faultGraph(t, sinks.NewCollect(), nil),
				Options{FaultInjector: inj})
			if !errors.Is(err, ErrFaultInjected) {
				t.Fatalf("Run = %v, want an injected fault", err)
			}
			fired := inj.firings()
			if len(fired) != 1 {
				t.Fatalf("the injector fired %d times: %v", len(fired), fired)
			}
			calls := inj.at("before-element", tc.vertexID, tc.subtask)
			if int64(len(calls)) != tc.n+1 {
				t.Fatalf("%s[%d] was consulted %d times, want %d: it processed records past the position the fault names",
					tc.vertexID, tc.subtask, len(calls), tc.n+1)
			}
		})
	}
}

// TestAfterBarrierForwardedFiresAfterTheBarrierIsOnTheOutputs.
//
// The barrier must already be on this subtask's outputs when the fault fires.
// That is the interesting cut: the snapshot is acknowledged, the checkpoint can
// still complete without this subtask, and a run recovering from it resumes at
// the offset that barrier recorded. A fault consulted BEFORE the broadcast
// would leave a checkpoint that never completes, so the recovery half of this
// phase would only ever exercise the restart-from-zero path while every
// assertion still passed.
//
// Driven through runSourceSubtask directly rather than through a job, and that
// is the point rather than convenience. Through a job the barrier's arrival
// downstream races the cancellation the fault triggers: the forwarders select
// on ctx.Done when they send, so an element genuinely on the channel may never
// be delivered. An assertion on the downstream side would therefore be flaky in
// the direction that passes, which is the worst direction. Here the outputs are
// read after the fact, so what was emitted is what is checked.
func TestAfterBarrierForwardedFiresAfterTheBarrierIsOnTheOutputs(t *testing.T) {
	const (
		count           = 300
		barrierInterval = 100
		fireAfter       = 2
	)
	v := graph.Vertex{
		ID: "src", Kind: graph.VertexSource, Parallelism: 1,
		NewSource:                 func() core.Source { return sources.NewGenerator(faultConfig(31, count)) },
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         200,
		BarrierIntervalElements:   barrierInterval,
	}
	// Capacity well past the whole range, so no send blocks and the subtask
	// runs to its fault without a consumer.
	ch := transport.NewChannel(4 * count)
	w := transport.NewWriter([][]transport.Output{{ch}})

	inj := &recordingInjector{fire: func(c consultation) bool {
		return c.site == "after-barrier" && c.n == fireAfter
	}}
	id := subtaskID{vertexID: "src", index: 0}
	err := runSourceSubtask(context.Background(), v, id, w, checkpointer{},
		faults{injector: inj, id: id}, subtaskConfig{injector: inj})
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("runSourceSubtask = %v, want an injected fault", err)
	}
	w.CloseAll()

	var kinds []core.ElementKind
	var records int
	var lastBarrier int64
	for {
		e, ok := ch.Recv()
		if !ok {
			break
		}
		kinds = append(kinds, e.Kind)
		switch e.Kind {
		case core.KindRecord:
			records++
		case core.KindBarrier:
			lastBarrier = e.Barrier.CheckpointID
		}
	}
	if len(kinds) == 0 {
		t.Fatal("the subtask emitted nothing")
	}
	if got := kinds[len(kinds)-1]; got != core.KindBarrier {
		t.Fatalf("the last element emitted is a %s, want the barrier: the injector was consulted before the broadcast", got)
	}
	if lastBarrier != fireAfter {
		t.Fatalf("the last barrier emitted is %d, want %d", lastBarrier, fireAfter)
	}
	// Exactly the records belonging to checkpoints 1 and 2 and not one more.
	if want := fireAfter * barrierInterval; records != want {
		t.Fatalf("the subtask emitted %d records before it stopped, want %d", records, want)
	}
}

// TestAfterBarrierForwardedIsConsultedOnceForEachBarrier is the job-level half:
// the trigger is offered every barrier the subtask forwards, in order, and
// stops at the one it fires on.
func TestAfterBarrierForwardedIsConsultedOnceForEachBarrier(t *testing.T) {
	inj := &recordingInjector{fire: func(c consultation) bool {
		return c.site == "after-barrier" && c.vertexID == "srcB" && c.subtask == 0 && c.n == 2
	}}
	err := RunWithOptions(context.Background(), faultGraph(t, sinks.NewCollect(), nil),
		Options{CheckpointRoot: t.TempDir(), FaultInjector: inj})
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Run = %v, want an injected fault", err)
	}
	var ids []int64
	for _, c := range inj.at("after-barrier", "srcB", 0) {
		ids = append(ids, c.n)
	}
	if want := []int64{1, 2}; !slices.Equal(ids, want) {
		t.Fatalf("srcB[0] was consulted for barriers %v, want %v", ids, want)
	}
}

// TestDuringAlignmentIsOfferedOnlyInsideAnAlignmentWindow.
//
// Two claims, and the second is the one worth having. The first is that the
// gate consults at all: a run of this topology has to reach a state where some
// but not all inputs have delivered a barrier, or TriggerDuringAlignment is
// scheduling faults at a position that does not exist. The second is that it
// never consults at the barrier that COMPLETES alignment: at four inputs, a
// consultation at delivered = 4 would be a fault landing after the window
// closed while the census counted it as landing inside.
func TestDuringAlignmentIsOfferedOnlyInsideAnAlignmentWindow(t *testing.T) {
	inj := &recordingInjector{}
	if err := RunWithOptions(context.Background(), faultGraph(t, sinks.NewCollect(), nil),
		Options{FaultInjector: inj}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	seen := 0
	for _, vertexID := range []string{"op", "out"} {
		inputs := 4
		if vertexID == "out" {
			inputs = 2
		}
		for subtask := range 2 {
			for _, c := range inj.at("during-alignment", vertexID, subtask) {
				seen++
				if c.delivered < 1 || c.delivered >= inputs {
					t.Fatalf("%s: %s[%d] has %d inputs, so alignment was already complete and there was no window to land in",
						c, vertexID, subtask, inputs)
				}
				if c.n < 1 {
					t.Fatalf("%s: aligned a barrier with a checkpoint ID below 1", c)
				}
			}
		}
	}
	if seen == 0 {
		t.Fatal("the gate was never consulted during an alignment, so this test asserts nothing and " +
			"TriggerDuringAlignment aims at a position no run reaches")
	}
	t.Logf("%d alignment-window consultations", seen)
}

// TestDuringAlignmentFiresAndFailsTheSubtask.
func TestDuringAlignmentFiresAndFailsTheSubtask(t *testing.T) {
	inj := &recordingInjector{fire: func(c consultation) bool {
		return c.site == "during-alignment" && c.vertexID == "op" && c.n == 2 && c.delivered == 1
	}}
	err := RunWithOptions(context.Background(), faultGraph(t, sinks.NewCollect(), nil),
		Options{FaultInjector: inj})
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Run = %v, want an injected fault", err)
	}
	fired := inj.firings()
	if len(fired) != 1 {
		t.Fatalf("the injector fired %d times: %v", len(fired), fired)
	}
	if fired[0].delivered != 1 || fired[0].n != 2 {
		t.Fatalf("fired at %s, want barrier 2 with 1 input delivered", fired[0])
	}
}

// TestANilInjectorChangesNothing is the assertion that this step is inert on
// every job that is not a chaos run.
//
// Compared against a run with an injector that is consulted everywhere and
// fires nowhere, as well as against no injector at all. The first comparison is
// what says the call sites do not themselves move anything -- an injector
// consulted between a snapshot and a broadcast, say, would reorder nothing but
// an injector that took a lock the runtime then waited on would.
func TestANilInjectorChangesNothing(t *testing.T) {
	sorted := func(sink *sinks.Collect) []string {
		var out []string
		for _, rec := range sink.Records() {
			out = append(out, fmt.Sprintf("%x@%d", rec.Key, rec.EventTime))
		}
		slices.Sort(out)
		return out
	}

	bare := sinks.NewCollect()
	if err := RunWithOptions(context.Background(), faultGraph(t, bare, nil), Options{}); err != nil {
		t.Fatalf("the run with no injector: %v", err)
	}
	quiet := sinks.NewCollect()
	inj := &recordingInjector{}
	if err := RunWithOptions(context.Background(), faultGraph(t, quiet, nil),
		Options{FaultInjector: inj}); err != nil {
		t.Fatalf("the run with a silent injector: %v", err)
	}

	if !slices.Equal(sorted(bare), sorted(quiet)) {
		t.Errorf("a silent injector changed the sink contents: %d records against %d",
			len(bare.Records()), len(quiet.Records()))
	}
	if len(inj.firings()) != 0 {
		t.Errorf("a silent injector fired %v", inj.firings())
	}
	if len(inj.at("before-element", "srcA", 0)) == 0 {
		t.Error("the silent injector was never consulted, so this test compared a run against a copy of itself")
	}
}

// closeCountingSink counts its own Close calls.
type closeCountingSink struct {
	core.Sink
	closes *atomic.Int64
}

func (s *closeCountingSink) Close() error {
	s.closes.Add(1)
	return s.Sink.Close()
}

// TestAnInjectedAbortTakesTheOrdinaryFailurePath.
//
// The abort must be indistinguishable from any other subtask failure: the
// failing subtask reports, everything downstream exits quietly rather than
// reporting a second error, and Close runs exactly once on every operator and
// sink instance. A fault that took a different path would be testing a code
// path that no real failure reaches, and five hundred schedules of it would say
// nothing about recovery from a real one.
func TestAnInjectedAbortTakesTheOrdinaryFailurePath(t *testing.T) {
	var opCloses, sinkCloses atomic.Int64
	collect := sinks.NewCollect()

	srcA := graph.Vertex{
		ID: "srcA", Kind: graph.VertexSource, Parallelism: 2,
		NewSource:                 func() core.Source { return sources.NewGenerator(faultConfig(21, 400)) },
		WatermarkIntervalElements: 100, MaxOutOfOrderness: 200, BarrierIntervalElements: 50,
	}
	srcB := graph.Vertex{
		ID: "srcB", Kind: graph.VertexSource, Parallelism: 2,
		NewSource:                 func() core.Source { return sources.NewGenerator(faultConfig(22, 8000)) },
		WatermarkIntervalElements: 100, MaxOutOfOrderness: 200, BarrierIntervalElements: 1000,
	}
	g := buildGraph(t, []graph.Vertex{
		srcA, srcB,
		{ID: "op", Kind: graph.VertexOperator, Parallelism: 2,
			NewOperator: func() core.Operator {
				return &closeRecordingOperator{Operator: identity(), closes: &opCloses}
			}},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 2,
			NewSink: func() core.Sink { return &closeCountingSink{Sink: collect, closes: &sinkCloses} }},
	}, [][2]string{{"srcA", "op"}, {"srcB", "op"}, {"op", "out"}})

	inj := &recordingInjector{fire: func(c consultation) bool {
		return c.site == "before-element" && c.vertexID == "srcB" && c.subtask == 0 && c.n == 500
	}}
	err := RunWithOptions(context.Background(), g, Options{FaultInjector: inj})
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Run = %v, want an injected fault", err)
	}
	// The error names the subtask that was aborted, not a downstream one that
	// merely noticed its inputs close.
	if want := "srcB[0]"; !strings.Contains(err.Error(), want) {
		t.Errorf("Run = %q, want the error to name %s", err, want)
	}
	if got := opCloses.Load(); got != 2 {
		t.Errorf("the operator was closed %d times, want once per subtask (2)", got)
	}
	if got := sinkCloses.Load(); got != 2 {
		t.Errorf("the sink was closed %d times, want once per subtask (2)", got)
	}
}
