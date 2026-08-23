package oracle

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// Every equivalence run uses allowed lateness 0.
//
// The generator's out-of-orderness is bounded by MaxLag and the source's
// watermark is maxSeen-MaxLag-1, so no record is ever late: when record n
// reaches an operator, the last watermark that source emitted was derived from
// records before it and is therefore at most Base+(n-1)*Step-MaxLag-1, which is
// below Base+n*Step-MaxLag, the floor on record n's own event time. The gate's
// minimum is at most that, so it is below it too, whatever the parallelism or
// the topology. Nothing is late, so the oracle does not need to model lateness,
// and an oracle complicated enough to model lateness is not an oracle. The drop
// path is checked against a hand-written input in pkg/operators instead.
//
// Every run asserts the drop count is zero, so the premise is checked rather
// than assumed.
const allowedLateness = 0

// watermarkInterval is how many records a source subtask emits between
// watermarks. Small enough that windows fire progressively through the run
// rather than all at once on the end-of-input flush, which is what
// TestWindowsFireOnWatermarksAndNotOnlyAtEndOfInput below pins.
const watermarkInterval = 100

// depthRecords is the per-source record count for the depth matrix. Three
// topologies at three parallelisms under -race is slow, and 10k is enough to
// give every key a hundred windows.
const depthRecords = 10000

// specs are the two window types every equivalence assertion runs.
var specs = []struct {
	name string
	spec Spec
}{
	{name: "tumbling", spec: Spec{Size: 1000, Slide: 1000}},
	{name: "sliding", spec: Spec{Size: 1000, Slide: 250}},
}

// keyCardinality is the generator's distinct key count, named because the
// firing-distribution bound below is expressed in terms of it.
const keyCardinality = 8

func equivalenceConfig(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed:           seed,
		Count:          count,
		KeyCardinality: keyCardinality,
		BaseEventTime:  1700000000000,
		EventTimeStep:  10,
		MaxLag:         200,
		ValueSize:      8,
	}
}

// sourceVertex returns a source vertex configured for event time. The
// out-of-orderness the watermark allows for is exactly the generator's, which
// is what makes lateness impossible.
func sourceVertex(id string, cfg sources.GeneratorConfig, p int) graph.Vertex {
	return graph.Vertex{
		ID:                        id,
		Kind:                      graph.VertexSource,
		Parallelism:               p,
		NewSource:                 func() core.Source { return sources.NewGenerator(cfg) },
		WatermarkIntervalElements: watermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
	}
}

// windowFactory builds one window operator per subtask and keeps a handle on
// each, so a run can be asked afterwards whether anything was dropped.
type windowFactory struct {
	spec Spec
	mu   sync.Mutex
	made []*operators.WindowCount
}

func newWindowFactory(spec Spec) *windowFactory { return &windowFactory{spec: spec} }

func (f *windowFactory) newOperator() core.Operator {
	w := operators.NewSlidingCount(f.spec.Size, f.spec.Slide, allowedLateness)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.made = append(f.made, w)
	return w
}

// dropped totals the drops across every subtask. Run returns only after all of
// them have unwound, so the number is final rather than sampled.
func (f *windowFactory) dropped() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var total int64
	for _, w := range f.made {
		total += w.Dropped()
	}
	return total
}

// recorder collects the records one branch of a fan-out produced. All p
// subtasks of that branch share one, so it locks.
type recorder struct {
	mu      sync.Mutex
	records []*core.Record
}

func (r *recorder) see(rec *core.Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
}

func (r *recorder) seen() []*core.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

// tap returns a pass-through operator that records everything crossing it.
func tap(into *recorder) func() core.Operator {
	return func() core.Operator {
		return operators.NewMap(func(rec *core.Record) (*core.Record, error) {
			into.see(rec)
			return rec, nil
		})
	}
}

func buildGraph(t *testing.T, vertices []graph.Vertex, edges [][2]string) *graph.Graph {
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

func run(t *testing.T, g *graph.Graph) {
	t.Helper()
	if err := runtime.Run(context.Background(), g); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// triplesOf decodes fired windows into the comparison form.
//
// A record's event time carries the window start and its value the count, so
// this is a decode rather than a computation. Sorted, because the sink contents
// are a set: the subtasks of a window vertex finish their windows concurrently
// and comparing emission order would be a broken test.
func triplesOf(t *testing.T, recs []*core.Record) []Triple {
	t.Helper()
	out := make([]Triple, 0, len(recs))
	for _, r := range recs {
		count, err := operators.DecodeCount(r.Value)
		if err != nil {
			t.Fatalf("DecodeCount: %v", err)
		}
		out = append(out, Triple{Key: string(r.Key), WindowStart: r.EventTime, Count: count})
	}
	slices.SortFunc(out, CompareTriples)
	return out
}

// assertSameTriples compares two result sets and reports the first row they
// differ on, rather than dumping thousands of rows nobody will read.
func assertSameTriples(t *testing.T, got, want []Triple, format string, args ...any) {
	t.Helper()
	label := fmt.Sprintf(format, args...)
	if slices.Equal(got, want) {
		return
	}

	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d of %d: engine has %+v, oracle has %+v (engine produced %d rows, oracle %d)",
				label, i, len(want), got[i], want[i], len(got), len(want))
		}
	}
	// One is a prefix of the other, so the first extra row is the difference.
	if len(got) > len(want) {
		t.Fatalf("%s: the engine produced %d rows to the oracle's %d; the first extra is %+v",
			label, len(got), len(want), got[len(want)])
	}
	t.Fatalf("%s: the engine produced %d rows to the oracle's %d; the first missing is %+v",
		label, len(got), len(want), want[len(got)])
}

// mergeCounts adds b into a copy of a. Two sources feeding one window operator
// share its key space, so a key present in both contributes to one aggregate.
func mergeCounts(a, b map[Key]int64) map[Key]int64 {
	out := make(map[Key]int64, len(a)+len(b))
	for k, n := range a {
		out[k] += n
	}
	for k, n := range b {
		out[k] += n
	}
	return out
}

// TestEngineMatchesOracleAcrossSeeds is the breadth half of the exit criterion:
// a hundred seeds, both window types, on the simplest topology.
//
// Breadth is what catches an off-by-one at a boundary that one seed's data
// happens not to land on. It is deliberately at parallelism 1 and on a chain,
// because at a hundred seeds the run has to be cheap, and depth is the other
// test's job.
func TestEngineMatchesOracleAcrossSeeds(t *testing.T) {
	const (
		seeds = 100
		count = 4000
	)

	for _, s := range specs {
		t.Run(s.name, func(t *testing.T) {
			for seed := uint64(1); seed <= seeds; seed++ {
				cfg := equivalenceConfig(seed, count)

				factory := newWindowFactory(s.spec)
				collect := sinks.NewCollect()
				run(t, buildGraph(t, []graph.Vertex{
					sourceVertex("src", cfg, 1),
					{ID: "win", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: factory.newOperator},
					{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
						NewSink: func() core.Sink { return collect }},
				}, [][2]string{{"src", "win"}, {"win", "out"}}))

				want, err := Counts(cfg, s.spec)
				if err != nil {
					t.Fatalf("Counts: %v", err)
				}
				assertSameTriples(t, triplesOf(t, collect.Records()), Sorted(want), "seed %d", seed)
				if dropped := factory.dropped(); dropped != 0 {
					t.Fatalf("seed %d: %d records were dropped as late, but the watermark allows for the generator's full lag",
						seed, dropped)
				}
			}
		})
	}
}

// TestEngineMatchesOracleAcrossTopologiesAndParallelism is the depth half, and
// the one that pins invariant 1.
//
// Three topologies at three parallelisms. The chain cannot distinguish a gate
// that takes the minimum from one that takes the maximum, because a window
// operator in a chain has one input per upstream subtask of one vertex and, at
// parallelism 1, exactly one input; with one input the minimum and the maximum
// are the same number. That is the whole reason the multi-input row exists.
//
// The two sources in the multi-input row cover DIFFERENT event-time ranges,
// five million milliseconds apart, which is far more than either source spans.
// Identical ranges would keep the minimum and the maximum within a lag of each
// other and the row would pass under a broken gate. Disjoint, a maximum gate
// races the watermark to the later source's range while the earlier source is
// still sending, so the earlier source's windows are purged out from under it
// and its records are dropped.
//
// The fan-out row gives the two branches DIFFERENT window sizes and records
// each branch separately. Sink totals would not do: the two branches emit into
// one sink, their window starts collide wherever one size divides the other,
// and a row lost from one branch could be masked by a row of the same shape
// from the other.
func TestEngineMatchesOracleAcrossTopologiesAndParallelism(t *testing.T) {
	const seeds = 10
	parallelisms := []int{1, 2, 4}

	for _, s := range specs {
		for _, p := range parallelisms {
			t.Run(fmt.Sprintf("%s/p%d", s.name, p), func(t *testing.T) {
				for seed := uint64(1); seed <= seeds; seed++ {
					t.Run(fmt.Sprintf("chain/seed%d", seed), func(t *testing.T) {
						checkChain(t, seed, s.spec, p)
					})
					t.Run(fmt.Sprintf("multiinput/seed%d", seed), func(t *testing.T) {
						checkMultiInput(t, seed, s.spec, p)
					})
					t.Run(fmt.Sprintf("fanout/seed%d", seed), func(t *testing.T) {
						checkFanOut(t, seed, s.spec, p)
					})
				}
			})
		}
	}
}

// checkChain is source -> window -> sink, every vertex at p.
func checkChain(t *testing.T, seed uint64, spec Spec, p int) {
	t.Helper()
	cfg := equivalenceConfig(seed, depthRecords)

	factory := newWindowFactory(spec)
	collect := sinks.NewCollect()
	run(t, buildGraph(t, []graph.Vertex{
		sourceVertex("src", cfg, p),
		{ID: "win", Kind: graph.VertexOperator, Parallelism: p, NewOperator: factory.newOperator},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return collect }},
	}, [][2]string{{"src", "win"}, {"win", "out"}}))

	want, err := Counts(cfg, spec)
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	assertSameTriples(t, triplesOf(t, collect.Records()), Sorted(want), "chain at parallelism %d", p)
	assertNothingDropped(t, factory, "chain at parallelism %d", p)
}

// checkMultiInput is two sources over disjoint event-time ranges feeding one
// window operator.
func checkMultiInput(t *testing.T, seed uint64, spec Spec, p int) {
	t.Helper()
	a := equivalenceConfig(seed, depthRecords)
	b := equivalenceConfig(seed+1000, depthRecords)
	// Source b's whole range sits five million milliseconds above source a's,
	// which spans a hundred thousand. See the comment on the test.
	b.BaseEventTime = a.BaseEventTime + 5_000_000

	factory := newWindowFactory(spec)
	collect := sinks.NewCollect()
	run(t, buildGraph(t, []graph.Vertex{
		sourceVertex("srcA", a, p),
		sourceVertex("srcB", b, p),
		{ID: "win", Kind: graph.VertexOperator, Parallelism: p, NewOperator: factory.newOperator},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return collect }},
	}, [][2]string{{"srcA", "win"}, {"srcB", "win"}, {"win", "out"}}))

	countsA, err := Counts(a, spec)
	if err != nil {
		t.Fatalf("Counts(a): %v", err)
	}
	countsB, err := Counts(b, spec)
	if err != nil {
		t.Fatalf("Counts(b): %v", err)
	}
	assertSameTriples(t, triplesOf(t, collect.Records()), Sorted(mergeCounts(countsA, countsB)),
		"multi-input at parallelism %d", p)
	assertNothingDropped(t, factory, "multi-input at parallelism %d", p)
}

// checkFanOut is one source feeding two window operators of different sizes,
// each recorded separately on its way to a shared sink.
func checkFanOut(t *testing.T, seed uint64, spec Spec, p int) {
	t.Helper()
	cfg := equivalenceConfig(seed, depthRecords)

	// The right branch is the same window type at twice the size, so the two
	// branches genuinely disagree about where every boundary is.
	leftSpec := spec
	rightSpec := Spec{Size: spec.Size * 2, Slide: spec.Slide * 2}

	leftFactory, rightFactory := newWindowFactory(leftSpec), newWindowFactory(rightSpec)
	var leftSeen, rightSeen recorder

	run(t, buildGraph(t, []graph.Vertex{
		sourceVertex("src", cfg, p),
		{ID: "left", Kind: graph.VertexOperator, Parallelism: p, NewOperator: leftFactory.newOperator},
		{ID: "right", Kind: graph.VertexOperator, Parallelism: p, NewOperator: rightFactory.newOperator},
		{ID: "tapLeft", Kind: graph.VertexOperator, Parallelism: p, NewOperator: tap(&leftSeen)},
		{ID: "tapRight", Kind: graph.VertexOperator, Parallelism: p, NewOperator: tap(&rightSeen)},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return sinks.NewDiscard() }},
	}, [][2]string{
		{"src", "left"}, {"src", "right"},
		{"left", "tapLeft"}, {"right", "tapRight"},
		{"tapLeft", "out"}, {"tapRight", "out"},
	}))

	for _, branch := range []struct {
		name    string
		spec    Spec
		seen    *recorder
		factory *windowFactory
	}{
		{"left", leftSpec, &leftSeen, leftFactory},
		{"right", rightSpec, &rightSeen, rightFactory},
	} {
		want, err := Counts(cfg, branch.spec)
		if err != nil {
			t.Fatalf("Counts(%s): %v", branch.name, err)
		}
		assertSameTriples(t, triplesOf(t, branch.seen.seen()), Sorted(want),
			"the %s branch (size %d slide %d) at parallelism %d",
			branch.name, branch.spec.Size, branch.spec.Slide, p)
		assertNothingDropped(t, branch.factory, "the %s branch at parallelism %d", branch.name, p)
	}

	// Each branch computed the whole stream, so neither was handed a share of
	// it. Records partition WITHIN an edge and copy ACROSS edges; the counts
	// above already say so, and this says it once more in the form the Phase 1
	// fan-out bug would have broken.
	if len(leftSeen.seen()) == 0 || len(rightSeen.seen()) == 0 {
		t.Fatal("a fan-out branch produced nothing at all")
	}
}

func assertNothingDropped(t *testing.T, f *windowFactory, format string, args ...any) {
	t.Helper()
	if dropped := f.dropped(); dropped != 0 {
		t.Fatalf("%s: %d records were dropped as late, but the watermark allows for the generator's full lag",
			fmt.Sprintf(format, args...), dropped)
	}
}

// firingProbe wraps a window operator and counts how many windows it emitted
// before the end-of-input flush and how many at it.
type firingProbe struct {
	core.Operator
	mu     sync.Mutex
	before int
	atEnd  int
}

// countingContext counts the emissions of one operator call.
type countingContext struct {
	core.Context
	n int
}

func (c *countingContext) Emit(rec *core.Record) {
	c.n++
	c.Context.Emit(rec)
}

func (p *firingProbe) ProcessWatermark(wm int64, ctx core.Context) error {
	counting := &countingContext{Context: ctx}
	if err := p.Operator.ProcessWatermark(wm, counting); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if wm == math.MaxInt64 {
		p.atEnd += counting.n
	} else {
		p.before += counting.n
	}
	return nil
}

func (p *firingProbe) totals() (before, atEnd int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.before, p.atEnd
}

// probeFactory builds one firingProbe per subtask and totals them afterwards.
type probeFactory struct {
	spec   Spec
	mu     sync.Mutex
	probes []*firingProbe
}

func newProbeFactory(spec Spec) *probeFactory { return &probeFactory{spec: spec} }

func (f *probeFactory) newOperator() core.Operator {
	p := &firingProbe{Operator: operators.NewSlidingCount(f.spec.Size, f.spec.Slide, allowedLateness)}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probes = append(f.probes, p)
	return p
}

func (f *probeFactory) totals() (before, atEnd int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.probes {
		b, e := p.totals()
		before += b
		atEnd += e
	}
	return before, atEnd
}

// TestWindowsFireOnWatermarksAndNotOnlyAtEndOfInput is the guard against every
// other test in this file being vacuous, and the only equivalence-level test
// that can see either watermark mutation.
//
// The gate emits MaxInt64 when its inputs run out, and that alone flushes every
// window still open. So the FINAL SINK CONTENTS are the same whether event time
// advanced during the run or not at all: a build with watermark generation
// switched off, or one that froze exhausted inputs into the minimum instead of
// excluding them, produces exactly the right answer by flushing everything at
// the end. Every comparison against the oracle passes under both. What changes
// is WHEN windows fire, and therefore how much state is held at the peak, which
// is what this measures.
//
// The chain row says watermarks fire windows at all. The multi-input row says
// an exhausted input is excluded from the minimum: its two sources cover
// event-time ranges five million milliseconds apart, so once the earlier source
// finishes, a gate that froze it at its last watermark would pin the minimum
// there for the rest of the run and every window of the later source would wait
// for the flush.
func TestWindowsFireOnWatermarksAndNotOnlyAtEndOfInput(t *testing.T) {
	const count = 10000
	spec := Spec{Size: 1000, Slide: 1000}

	tests := []struct {
		name string
		// sources is how many source vertices feed the window operator, which
		// bounds how many windows can still be open at end of input.
		sources int64
		// build returns the graph and the oracle's answer for it.
		build func(t *testing.T, factory *probeFactory, sink core.Sink) (*graph.Graph, []Triple)
	}{
		{
			name:    "chain at parallelism 1",
			sources: 1,
			build: func(t *testing.T, factory *probeFactory, sink core.Sink) (*graph.Graph, []Triple) {
				cfg := equivalenceConfig(1, count)
				g := buildGraph(t, []graph.Vertex{
					sourceVertex("src", cfg, 1),
					{ID: "win", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: factory.newOperator},
					{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
						NewSink: func() core.Sink { return sink }},
				}, [][2]string{{"src", "win"}, {"win", "out"}})
				want, err := Counts(cfg, spec)
				if err != nil {
					t.Fatalf("Counts: %v", err)
				}
				return g, Sorted(want)
			},
		},
		{
			// Source a is a tenth of source b, so a exhausts early with most of
			// b still to come, and the minimum has to move off it for b's
			// windows to fire at all.
			//
			// Two things this row must NOT do, both of which reintroduce the
			// staircase documented on watermarkGenerator and drown the signal.
			// Equal-length sources: a would pin the minimum for the whole run,
			// so b's windows would legitimately reach the flush. Parallelism
			// above 1: b's subtasks take contiguous offset ranges, so b's
			// subtask 0 pins the minimum until IT exhausts, and the whole of
			// subtask 1's share legitimately reaches the flush. Both were
			// measured; both leave around half the windows firing at the end
			// under correct behaviour, and both are scheduling-dependent.
			//
			// The exclusion rule is a property of a gate with several inputs,
			// and two source vertices give it two inputs at parallelism 1. It
			// does not need parallelism as well.
			name:    "a short source exhausting beside a long one",
			sources: 2,
			build: func(t *testing.T, factory *probeFactory, sink core.Sink) (*graph.Graph, []Triple) {
				a := equivalenceConfig(1, count/10)
				b := equivalenceConfig(1001, count)
				b.BaseEventTime = a.BaseEventTime + 5_000_000

				g := buildGraph(t, []graph.Vertex{
					sourceVertex("srcA", a, 1),
					sourceVertex("srcB", b, 1),
					{ID: "win", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: factory.newOperator},
					{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
						NewSink: func() core.Sink { return sink }},
				}, [][2]string{{"srcA", "win"}, {"srcB", "win"}, {"win", "out"}})

				countsA, err := Counts(a, spec)
				if err != nil {
					t.Fatalf("Counts(a): %v", err)
				}
				countsB, err := Counts(b, spec)
				if err != nil {
					t.Fatalf("Counts(b): %v", err)
				}
				return g, Sorted(mergeCounts(countsA, countsB))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := newProbeFactory(spec)
			collect := sinks.NewCollect()
			g, want := tt.build(t, factory, collect)
			run(t, g)

			// The answer still has to be right, or the counts below measure
			// nothing.
			assertSameTriples(t, triplesOf(t, collect.Records()), want, "the probed topology")

			before, atEnd := factory.totals()
			if before == 0 {
				t.Fatal("every window fired on the end-of-input flush; no watermark generated during the run fired anything")
			}
			if atEnd == 0 {
				t.Error("nothing fired on the end-of-input flush, so the flush path is not exercised here at all")
			}
			// Only the windows still open when the input ends belong to the
			// flush, and how many that is depends on watermark granularity
			// rather than on the length of the run: a couple per key per
			// source, against a hundred windows per key produced. The bound is
			// absolute for that reason, and it is an order of magnitude below
			// what a frozen exhausted input produces, which is every window of
			// the source that outlived the frozen one.
			if limit := 4 * keyCardinality * tt.sources; int64(atEnd) > limit {
				t.Errorf("%d windows fired on watermarks and %d on the end-of-input flush, which is above the %d still-open windows this topology can account for: an exhausted input is being frozen into the minimum rather than excluded from it",
					before, atEnd, limit)
			}
		})
	}
}

// arrivalProbe records, for every record reaching it, the watermark its subtask
// had already been given.
type arrivalProbe struct {
	core.Operator
	mu       sync.Mutex
	arrivals []arrival
}

type arrival struct {
	windowStart int64
	watermark   int64
}

func (p *arrivalProbe) ProcessElement(rec *core.Record, ctx core.Context) error {
	p.mu.Lock()
	p.arrivals = append(p.arrivals, arrival{windowStart: rec.EventTime, watermark: ctx.CurrentWatermark()})
	p.mu.Unlock()
	return p.Operator.ProcessElement(rec, ctx)
}

func (p *arrivalProbe) seen() []arrival {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.arrivals)
}

// TestWindowEmissionsReachDownstreamBeforeTheirWatermark pins the emission
// ordering the window operator depends on.
//
// When a watermark arrives the subtask must call ProcessWatermark first, let
// the operator emit the windows it completes, and broadcast the watermark only
// afterwards. Broadcasting first would put the watermark ahead of the records
// it is meant to bound, so a downstream operator would see event time pass a
// window before the records closing that window arrived. Everything is in-band,
// which is invariant 5, but in-band only guarantees that the order is
// PRESERVED; it does not make the order right in the first place.
//
// A chain of window -> operator is the shortest topology where the difference
// is visible. The window at [start, end) fires on the first watermark at or
// above end-1, so under the correct ordering the downstream subtask still holds
// the previous watermark when the record lands, and that one is below end-1 or
// the window would have fired earlier. Under the wrong ordering it holds the
// firing watermark itself, which is at or above end-1. The assertion is that
// gap, on every record.
func TestWindowEmissionsReachDownstreamBeforeTheirWatermark(t *testing.T) {
	const size = 1000
	cfg := equivalenceConfig(7, 5000)

	probe := &arrivalProbe{Operator: operators.NewMap(
		func(rec *core.Record) (*core.Record, error) { return rec, nil })}

	run(t, buildGraph(t, []graph.Vertex{
		sourceVertex("src", cfg, 1),
		{ID: "win", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: func() core.Operator {
			return operators.NewTumblingCount(size, allowedLateness)
		}},
		{ID: "probe", Kind: graph.VertexOperator, Parallelism: 1,
			NewOperator: func() core.Operator { return probe }},
		{ID: "out", Kind: graph.VertexSink, Parallelism: 1,
			NewSink: func() core.Sink { return sinks.NewDiscard() }},
	}, [][2]string{{"src", "win"}, {"win", "probe"}, {"probe", "out"}}))

	got := probe.seen()
	if len(got) == 0 {
		t.Fatal("no window reached the downstream operator; the test asserts nothing")
	}

	checked := 0
	for _, a := range got {
		// The end-of-input flush is not evidence either way: nothing follows
		// MaxInt64, so there is no ordering left to get wrong.
		if a.watermark == math.MaxInt64 {
			continue
		}
		checked++
		if fireAt := a.windowStart + size - 1; a.watermark >= fireAt {
			t.Fatalf("the window at %d reached the downstream operator with the watermark already at %d, but that window fires at %d: the watermark was broadcast ahead of the records it bounds",
				a.windowStart, a.watermark, fireAt)
		}
	}
	if checked == 0 {
		t.Error("every window arrived at or after the end-of-input flush, so the ordering was never actually tested")
	}
}
