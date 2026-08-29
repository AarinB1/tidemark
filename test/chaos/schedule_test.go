package chaos

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// scheduleGraph is one topology to schedule against, together with the bounds
// its shape imposes -- written out by hand rather than recomputed from
// sitesOf, so the range assertions below check the implementation instead of
// agreeing with it.
type scheduleGraph struct {
	name string
	g    *graph.Graph
	// bounds is keyed by vertex ID. A vertex absent from it does not exist,
	// which is what makes "a schedule never names a vertex that does not
	// exist" checkable.
	bounds map[string]vertexBounds
	// maxCheckpoint is the highest barrier ID any source subtask injects.
	maxCheckpoint int64
}

type vertexBounds struct {
	parallelism int
	inputs      int
	maxElements int64
}

func genCfg(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed: seed, Count: count, KeyCardinality: 64,
		BaseEventTime: 1700000000000, EventTimeStep: 10, MaxLag: 200,
		ValueSize: 8, AmountRange: 1000,
	}
}

func srcVertex(id string, cfg sources.GeneratorConfig, p int, barrier int64) graph.Vertex {
	return graph.Vertex{
		ID: id, Kind: graph.VertexSource, Parallelism: p,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: 100,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrier,
	}
}

func opVertex(id string, p int) graph.Vertex {
	return graph.Vertex{ID: id, Kind: graph.VertexOperator, Parallelism: p,
		NewOperator: func() core.Operator { return operators.NewTumblingCount(5000, 0) }}
}

func sinkVertex(id string, p int) graph.Vertex {
	return graph.Vertex{ID: id, Kind: graph.VertexSink, Parallelism: p,
		NewSink: func() core.Sink { return sinks.NewCollect() }}
}

func mustGraph(t *testing.T, vertices []graph.Vertex, edges [][2]string) *graph.Graph {
	t.Helper()
	g := graph.New()
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex(%s): %v", v.ID, err)
		}
	}
	for _, e := range edges {
		if err := g.Connect(e[0], e[1]); err != nil {
			t.Fatalf("Connect(%s, %s): %v", e[0], e[1], err)
		}
	}
	return g
}

// scheduleGraphs is the table every schedule property below runs against.
//
// The shapes differ in the things a schedule has to respect: a vertex with one
// input cannot host an alignment fault, a fan-out vertex sees the whole stream
// on each of its outgoing edges rather than a share of it, and a source's
// element bound is its own range rather than the job's.
func scheduleGraphs(t *testing.T) []scheduleGraph {
	t.Helper()
	return []scheduleGraph{
		{
			name: "chain",
			g: mustGraph(t, []graph.Vertex{
				srcVertex("src", genCfg(1, 20000), 2, 5000),
				opVertex("op", 2),
				sinkVertex("out", 2),
			}, [][2]string{{"src", "op"}, {"op", "out"}}),
			bounds: map[string]vertexBounds{
				"src": {parallelism: 2, inputs: 0, maxElements: 10000},
				"op":  {parallelism: 2, inputs: 2, maxElements: 10000},
				"out": {parallelism: 2, inputs: 2, maxElements: 10000},
			},
			maxCheckpoint: 2,
		},
		{
			name: "two-sources",
			g: mustGraph(t, []graph.Vertex{
				srcVertex("srcA", genCfg(2, 4000), 2, 500),
				srcVertex("srcB", genCfg(3, 16000), 2, 2000),
				opVertex("window", 2),
				sinkVertex("out", 2),
			}, [][2]string{{"srcA", "window"}, {"srcB", "window"}, {"window", "out"}}),
			bounds: map[string]vertexBounds{
				"srcA":   {parallelism: 2, inputs: 0, maxElements: 2000},
				"srcB":   {parallelism: 2, inputs: 0, maxElements: 8000},
				"window": {parallelism: 2, inputs: 4, maxElements: 10000},
				"out":    {parallelism: 2, inputs: 2, maxElements: 10000},
			},
			maxCheckpoint: 4,
		},
		{
			name: "fan-out",
			g: mustGraph(t, []graph.Vertex{
				srcVertex("src", genCfg(4, 8000), 2, 1000),
				opVertex("a", 1),
				opVertex("b", 3),
				sinkVertex("outA", 1),
				sinkVertex("outB", 2),
			}, [][2]string{{"src", "a"}, {"src", "b"}, {"a", "outA"}, {"b", "outB"}}),
			bounds: map[string]vertexBounds{
				// Each downstream vertex receives the FULL stream, so both a
				// and b are bounded by 8000 rather than by half of it.
				"src":  {parallelism: 2, inputs: 0, maxElements: 4000},
				"a":    {parallelism: 1, inputs: 2, maxElements: 8000},
				"b":    {parallelism: 3, inputs: 2, maxElements: 2666},
				"outA": {parallelism: 1, inputs: 1, maxElements: 8000},
				"outB": {parallelism: 2, inputs: 3, maxElements: 4000},
			},
			maxCheckpoint: 4,
		},
		{
			name: "single-input",
			g: mustGraph(t, []graph.Vertex{
				srcVertex("src", genCfg(5, 1000), 1, 250),
				opVertex("op", 1),
				sinkVertex("out", 1),
			}, [][2]string{{"src", "op"}, {"op", "out"}}),
			bounds: map[string]vertexBounds{
				"src": {parallelism: 1, inputs: 0, maxElements: 1000},
				"op":  {parallelism: 1, inputs: 1, maxElements: 1000},
				"out": {parallelism: 1, inputs: 1, maxElements: 1000},
			},
			maxCheckpoint: 4,
		},
	}
}

// encodeSchedule renders a schedule as one line per fault.
//
// It exists so that "byte-identical" is something a test can actually compare,
// in this process and in another one. Every field is printed: a rendering that
// dropped one would let that field vary between processes without any test
// noticing.
func encodeSchedule(faults []Fault) string {
	var b strings.Builder
	fmt.Fprintf(&b, "n=%d\n", len(faults))
	for _, f := range faults {
		fmt.Fprintf(&b, "%s|%d|%d|%d|%d\n", f.VertexID, f.Subtask, uint8(f.Trigger), f.N, f.Inputs)
	}
	return b.String()
}

// TestScheduleFieldsAreInRangeForTheGraph is the assertion that a seed cannot
// aim a fault at something the job does not have.
//
// A vertex the graph does not hold, a subtask index at or above the vertex's
// parallelism, an element count past what a subtask can see, a checkpoint ID
// above the last barrier any source injects, or an alignment fault at every
// input rather than at some of them: each of those is a fault that can never
// fire, and a schedule full of them would run five hundred seeds and test
// nothing while reporting no error.
func TestScheduleFieldsAreInRangeForTheGraph(t *testing.T) {
	for _, tc := range scheduleGraphs(t) {
		t.Run(tc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 500; seed++ {
				for _, f := range ScheduleFor(seed, tc.g) {
					b, ok := tc.bounds[f.VertexID]
					if !ok {
						t.Fatalf("seed %d scheduled %s against a vertex the graph does not have", seed, f)
					}
					if f.Subtask < 0 || f.Subtask >= b.parallelism {
						t.Fatalf("seed %d scheduled %s, but %s has %d subtasks", seed, f, f.VertexID, b.parallelism)
					}
					switch f.Trigger {
					case TriggerAfterElements:
						if f.N < 0 || f.N >= b.maxElements {
							t.Fatalf("seed %d scheduled %s, but a subtask of %s sees at most %d elements", seed, f, f.VertexID, b.maxElements)
						}
						if f.Inputs != 0 {
							t.Fatalf("seed %d scheduled %s with Inputs %d, which is meaningless for this trigger", seed, f, f.Inputs)
						}
					case TriggerAfterBarrier:
						if f.N < 1 || f.N > tc.maxCheckpoint {
							t.Fatalf("seed %d scheduled %s, but the job injects barriers 1..%d", seed, f, tc.maxCheckpoint)
						}
						if f.Inputs != 0 {
							t.Fatalf("seed %d scheduled %s with Inputs %d, which is meaningless for this trigger", seed, f, f.Inputs)
						}
					case TriggerDuringAlignment:
						if f.N < 1 || f.N > tc.maxCheckpoint {
							t.Fatalf("seed %d scheduled %s, but the job injects barriers 1..%d", seed, f, tc.maxCheckpoint)
						}
						if b.inputs < 2 {
							t.Fatalf("seed %d scheduled %s, but %s has %d inputs and so has no alignment window at all", seed, f, f.VertexID, b.inputs)
						}
						if f.Inputs < 1 || f.Inputs >= b.inputs {
							t.Fatalf("seed %d scheduled %s, but %s has %d inputs: an alignment fault at all of them is the moment alignment completes", seed, f, f.VertexID, b.inputs)
						}
					default:
						t.Fatalf("seed %d scheduled an unknown trigger %d", seed, uint8(f.Trigger))
					}
				}
			}
		})
	}
}

// TestScheduleHoldsAtMostThreeFaults pins the size of the draw, including the
// empty schedules that are this suite's control group.
func TestScheduleHoldsAtMostThreeFaults(t *testing.T) {
	g := scheduleGraphs(t)[1].g
	counts := make(map[int]int)
	for seed := int64(1); seed <= 500; seed++ {
		n := len(ScheduleFor(seed, g))
		if n > MaxFaultsPerSchedule {
			t.Fatalf("seed %d scheduled %d faults, at most %d are allowed", seed, n, MaxFaultsPerSchedule)
		}
		counts[n]++
	}
	for n := 0; n <= MaxFaultsPerSchedule; n++ {
		if counts[n] == 0 {
			t.Errorf("no seed in 1..500 scheduled %d faults; the draw does not cover its own range", n)
		}
	}
	t.Logf("fault counts over seeds 1..500: %v", counts)
}

// TestScheduleReachesEveryTriggerKind is the assertion that
// TriggerDuringAlignment is not dead code.
//
// It exists for the reason the standing rule of this phase names: the alignment
// trigger is the whole justification for the third kind, and a derivation that
// silently never drew it would leave five hundred schedules sampling only the
// easy part of the space while every other assertion still passed.
func TestScheduleReachesEveryTriggerKind(t *testing.T) {
	g := scheduleGraphs(t)[1].g
	seen := make(map[TriggerKind]int)
	for seed := int64(1); seed <= 500; seed++ {
		for _, f := range ScheduleFor(seed, g) {
			seen[f.Trigger]++
		}
	}
	for _, k := range []TriggerKind{TriggerAfterElements, TriggerAfterBarrier, TriggerDuringAlignment} {
		if seen[k] == 0 {
			t.Errorf("no seed in 1..500 drew %s", k)
		}
	}
	t.Logf("trigger kinds over seeds 1..500: after-elements %d, after-barrier %d, during-alignment %d",
		seen[TriggerAfterElements], seen[TriggerAfterBarrier], seen[TriggerDuringAlignment])
}

// TestSingleInputGraphNeverSchedulesAlignment is the negative half of the test
// above. A vertex with one input has no window between "some inputs have
// delivered the barrier" and "all of them have", so aiming at one would spend a
// fault that can never fire.
func TestSingleInputGraphNeverSchedulesAlignment(t *testing.T) {
	g := scheduleGraphs(t)[3].g
	for seed := int64(1); seed <= 500; seed++ {
		for _, f := range ScheduleFor(seed, g) {
			if f.Trigger == TriggerDuringAlignment {
				t.Fatalf("seed %d scheduled %s on a graph where every vertex has at most one input", seed, f)
			}
		}
	}
}

// TestDifferentSeedsProduceDifferentSchedules.
//
// Distinctness rather than pairwise inequality: with zero-to-three faults over
// a handful of sites, collisions are expected and are not a bug. What would be
// a bug is a derivation where the seed reached only some of the fields -- only
// the fault count, say -- and that shows up as a distinct count far below the
// number of seeds rather than as any single collision.
func TestDifferentSeedsProduceDifferentSchedules(t *testing.T) {
	for _, tc := range scheduleGraphs(t) {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(map[string]bool)
			for seed := int64(1); seed <= 500; seed++ {
				seen[encodeSchedule(ScheduleFor(seed, tc.g))] = true
			}
			if len(seen) < 300 {
				t.Errorf("seeds 1..500 produced only %d distinct schedules; the seed is not reaching every field", len(seen))
			}
			t.Logf("%d distinct schedules over 500 seeds", len(seen))
		})
	}
}

// TestScheduleIsStableAcrossCalls is the cheap half of reproducibility: the
// same seed, twice in one process.
func TestScheduleIsStableAcrossCalls(t *testing.T) {
	for _, tc := range scheduleGraphs(t) {
		t.Run(tc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 100; seed++ {
				first := encodeSchedule(ScheduleFor(seed, tc.g))
				for range 5 {
					if got := encodeSchedule(ScheduleFor(seed, tc.g)); got != first {
						t.Fatalf("seed %d produced\n%s\nand then\n%s", seed, first, got)
					}
				}
			}
		})
	}
}

// goldenSchedules is what seeds 1 to 8 produce against the two-sources graph.
//
// A written-down constant rather than a computed one. It is the only assertion
// here that survives a change to the derivation: the stability and subprocess
// tests both compare this code against itself, so a rewrite that made every
// schedule the same would pass them both. This is what says the schedules are
// the ones a person reading a failing seed from an old CI log will get back.
const goldenSchedules = `seed 1
n=1
out|1|0|8761|0
seed 2
n=2
window|0|0|6649|0
window|1|2|1|3
seed 3
n=1
srcB|1|1|3|0
seed 4
n=2
srcA|0|1|2|0
window|1|2|2|3
seed 5
n=2
srcA|1|1|2|0
srcB|0|1|4|0
seed 6
n=0
seed 7
n=3
srcA|1|0|1674|0
window|1|0|4425|0
srcA|0|0|1190|0
seed 8
n=2
srcB|0|1|3|0
out|1|0|8380|0
`

func TestScheduleMatchesTheGolden(t *testing.T) {
	g := scheduleGraphs(t)[1].g
	var b strings.Builder
	for seed := int64(1); seed <= 8; seed++ {
		fmt.Fprintf(&b, "seed %d\n%s", seed, encodeSchedule(ScheduleFor(seed, g)))
	}
	if got := b.String(); got != goldenSchedules {
		t.Errorf("the derivation has changed.\ngot:\n%s\nwant:\n%s", got, goldenSchedules)
	}
}

// helperEnv makes a run of this test binary print schedules instead of
// checking them.
const helperEnv = "TIDEMARK_CHAOS_SCHEDULE_HELPER"

// helperPrefix marks the lines the helper process writes, so they can be picked
// out of the test framework's own output.
const helperPrefix = "SCHEDULE: "

// TestScheduleIsIdenticalInAnotherProcess is the reproducibility claim taken
// literally.
//
// A schedule derived from a map iteration, from an address, or from anything
// else the runtime randomises per process would still be stable within one
// process and would still differ between seeds. Only a second process can tell
// the difference, and "print the seed to reproduce the run" is a claim about
// exactly that: somebody else's machine, tomorrow.
//
// The test re-executes its own binary with an environment variable set, which
// is the standard way to get a second process out of a test without building
// one. The child runs this same function, takes the branch that prints, and
// exits through the framework normally.
func TestScheduleIsIdenticalInAnotherProcess(t *testing.T) {
	g := scheduleGraphs(t)[1].g

	var want strings.Builder
	for seed := int64(1); seed <= 50; seed++ {
		fmt.Fprintf(&want, "%sseed %d %s\n", helperPrefix, seed,
			strings.ReplaceAll(strings.TrimSpace(encodeSchedule(ScheduleFor(seed, g))), "\n", " "))
	}

	if os.Getenv(helperEnv) == "1" {
		fmt.Print(want.String())
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestScheduleIsIdenticalInAnotherProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the helper process failed: %v\n%s", err, out)
	}

	var got strings.Builder
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, helperPrefix) {
			got.WriteString(line)
			got.WriteString("\n")
		}
	}
	if got.String() != want.String() {
		t.Errorf("the schedules differ between processes.\nthis process:\n%s\nthe other:\n%s", want.String(), got.String())
	}
	if got.Len() == 0 {
		t.Fatal("the helper process printed no schedules at all, so this test compared nothing")
	}
}
