package oracle

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// The Nexmark equivalence suite: five queries against five oracles, across
// seeds, at parallelism 1, 2 and 4.
//
// Everything here compares SORTED CONTENTS. The sink holds a set of records in
// whatever order its subtasks wrote them, and ordering after a recovery differs
// from a clean run for reasons that have nothing to do with correctness;
// comparing emission order would be a broken test.
//
// Every run uses allowed lateness 0 and asserts that nothing was dropped. The
// generator's out-of-orderness is bounded by MaxLag and a source's watermark is
// maxSeen-MaxLag-1, so no record can be late -- but the batch oracles have no
// lateness model at all, so the premise is checked on every run rather than
// assumed. For q5 the assertion has a second half: its stage 2 also counts what
// it ACCEPTED, because a stage that saw everything as late would drop
// everything, emit nothing, and report no error.
//
// # Why the multi-input rows exist
//
// A window operator fed by one source vertex at parallelism 1 has exactly one
// input, and over one input the minimum and the maximum are the same number. So
// a chain cannot distinguish an input gate that takes the minimum from one that
// takes the maximum, and every seed would pass under a broken gate. Two source
// vertices give the gate two inputs, and their event-time ranges are five
// million milliseconds apart -- far more than either spans -- so a maximum gate
// races event time to the later source's range while the earlier one is still
// sending, purging its windows out from under it.
//
// They are on q5 and q7, the two queries with windows. q0, q1 and q2 hold no
// state and have no window to purge, so a gate taking the maximum would change
// nothing about their answers; a multi-input row on them would be decoration.

// nexmarkCount is the per-source event count for the parallelism matrix. Enough
// that every auction takes bids in many windows, and small enough that five
// queries at three parallelisms under the race detector stay inside a test
// budget.
const nexmarkCount = 4000

// nexmarkWindowSize and nexmarkSlide are the windowing q5 and q7 run.
//
// The size is a whole multiple of the slide, which NewSlidingCount requires.
// The slide is a quarter of the size, so each bid joins four windows and the
// sliding assignment is genuinely exercised rather than degenerating to
// tumbling.
const (
	nexmarkWindowSize = 5000
	nexmarkSlide      = 1250
)

// nexmarkSeparation is how far apart the two sources of a multi-input row sit
// in event time. Far more than either source spans, so their windows are
// disjoint and the union of the two oracles is the answer for the pair -- which
// the assertions check rather than assume.
const nexmarkSeparation = 5_000_000

func nexmarkEquivalenceConfig(seed uint64, count int64) sources.NexmarkConfig {
	return sources.NexmarkConfig{
		Seed:  seed,
		Count: count,
		// Small enough that a five-second window holds many bids per auction,
		// which is what makes a maximum and a hot item mean anything: an id
		// space larger than the bid count gives every auction one bid and every
		// window a tie between all of them.
		AuctionCardinality: 32,
		PersonCardinality:  16,
		PriceRange:         1000,
		CategoryCount:      4,
		AuctionDuration:    5000,
		BaseEventTime:      1700000000000,
		EventTimeStep:      10,
		MaxLag:             200,
	}
}

// nexmarkSourceVertex returns a source vertex configured for event time. The
// out-of-orderness the watermark allows for is exactly the source's, which is
// what makes lateness impossible.
func nexmarkSourceVertex(id string, cfg sources.NexmarkConfig, p int) graph.Vertex {
	return graph.Vertex{
		ID:                        id,
		Kind:                      graph.VertexSource,
		Parallelism:               p,
		NewSource:                 func() core.Source { return sources.NewNexmark(cfg) },
		WatermarkIntervalElements: watermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrierInterval,
	}
}

// maxBidFactory builds one q7 operator per subtask and keeps a handle on each,
// so a run can be asked afterwards what it dropped and what it accepted.
type maxBidFactory struct {
	mu   sync.Mutex
	made []*operators.MaxBid
}

func (f *maxBidFactory) newOperator() core.Operator {
	op := operators.NewQ7(nexmarkWindowSize, allowedLateness)
	f.mu.Lock()
	f.made = append(f.made, op)
	f.mu.Unlock()
	return op
}

func (f *maxBidFactory) totals() (dropped, onTime int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.made {
		dropped += op.Dropped()
		onTime += op.OnTime()
	}
	return dropped, onTime
}

// hotItemsFactory does the same for q5's second stage.
type hotItemsFactory struct {
	mu   sync.Mutex
	made []*operators.HotItems
}

func (f *hotItemsFactory) newOperator() core.Operator {
	op := operators.NewQ5HotItems(nexmarkWindowSize, allowedLateness)
	f.mu.Lock()
	f.made = append(f.made, op)
	f.mu.Unlock()
	return op
}

func (f *hotItemsFactory) totals() (dropped, onTime int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.made {
		dropped += op.Dropped()
		onTime += op.OnTime()
	}
	return dropped, onTime
}

// q5Factories holds the two stateful stages of q5 so a run can be asked about
// both: stage 1's drops and stage 2's drops and acceptances.
type q5Factories struct {
	stage1 *windowFactory
	stage2 *hotItemsFactory
}

func newQ5Factories() *q5Factories {
	return &q5Factories{
		stage1: newWindowFactory(Spec{Size: nexmarkWindowSize, Slide: nexmarkSlide}),
		stage2: &hotItemsFactory{},
	}
}

// nexmarkVertices returns the operator vertices of one query, in order, with
// the edges implied by that order. Every query in this file is a straight line
// from its sources to its sink, so the chain is enough to describe it.
func nexmarkVertices(query string, p int, q5 *q5Factories, q7 *maxBidFactory) []graph.Vertex {
	operatorVertex := func(id string, newOperator func() core.Operator) graph.Vertex {
		return graph.Vertex{ID: id, Kind: graph.VertexOperator, Parallelism: p, NewOperator: newOperator}
	}
	switch query {
	case "q0":
		return []graph.Vertex{operatorVertex("q0", func() core.Operator { return operators.NewQ0() })}
	case "q1":
		return []graph.Vertex{operatorVertex("q1", func() core.Operator { return operators.NewQ1(operators.Q1Factor) })}
	case "q2":
		return []graph.Vertex{operatorVertex("q2", func() core.Operator { return operators.NewQ2(operators.Q2Divisor) })}
	case "q7":
		return []graph.Vertex{operatorVertex("q7", q7.newOperator)}
	case "q5":
		// Four vertices. The filter is separate because stage 1 counts records
		// without looking at them; the re-key is separate because a record's
		// key is what the writer BEHIND its vertex partitions on, so the move
		// onto the window has to happen before the edge into stage 2.
		return []graph.Vertex{
			operatorVertex("bids", func() core.Operator { return operators.NewBidsOnly() }),
			operatorVertex("count", q5.stage1.newOperator),
			operatorVertex("rekey", func() core.Operator { return operators.NewQ5Rekey(nexmarkWindowSize) }),
			operatorVertex("hot", q5.stage2.newOperator),
		}
	}
	panic("unknown query " + query)
}

// buildNexmarkGraph wires sourceIDs into the query's chain and the chain into
// one sink.
func buildNexmarkGraph(t *testing.T, query string, p int, sourceVertices []graph.Vertex,
	sink core.Sink, q5 *q5Factories, q7 *maxBidFactory) *graph.Graph {
	t.Helper()

	chain := nexmarkVertices(query, p, q5, q7)
	vertices := append(slices.Clone(sourceVertices), chain...)
	vertices = append(vertices, graph.Vertex{
		ID: "out", Kind: graph.VertexSink, Parallelism: p,
		NewSink: func() core.Sink { return sink },
	})

	var edges [][2]string
	for _, s := range sourceVertices {
		edges = append(edges, [2]string{s.ID, chain[0].ID})
	}
	for i := 1; i < len(chain); i++ {
		edges = append(edges, [2]string{chain[i-1].ID, chain[i].ID})
	}
	edges = append(edges, [2]string{chain[len(chain)-1].ID, "out"})
	return buildGraph(t, vertices, edges)
}

// rowsFromSink flattens sink records into the stateless comparison form.
func rowsFromSink(recs []*core.Record) map[NexmarkRow]int64 {
	out := make(map[NexmarkRow]int64, len(recs))
	for _, rec := range recs {
		out[NexmarkRow{Key: string(rec.Key), Value: string(rec.Value), EventTime: rec.EventTime}]++
	}
	return out
}

// hotItemsFromSink decodes q5's output.
//
// The window start is DERIVED from the emitted event time and not read off it:
// a fired window carries its end-1, so the start is EventTime+1-size. Reading
// the event time as a start would shift every row by size-1 against the oracle.
func hotItemsFromSink(t *testing.T, recs []*core.Record) []HotItem {
	t.Helper()
	out := make([]HotItem, 0, len(recs))
	for _, rec := range recs {
		auction, err := sources.NexmarkKeyID(rec.Key)
		if err != nil {
			t.Fatalf("q5 output key: %v", err)
		}
		count, err := operators.DecodeCount(rec.Value)
		if err != nil {
			t.Fatalf("q5 output value: %v", err)
		}
		out = append(out, HotItem{
			WindowStart: rec.EventTime + 1 - nexmarkWindowSize, Auction: auction, Count: count,
		})
	}
	slices.SortFunc(out, CompareHotItems)
	return out
}

// maxBidsFromSink decodes q7's output, deriving the window start the same way.
func maxBidsFromSink(t *testing.T, recs []*core.Record) []MaxBidRow {
	t.Helper()
	out := make([]MaxBidRow, 0, len(recs))
	for _, rec := range recs {
		auction, err := sources.NexmarkKeyID(rec.Key)
		if err != nil {
			t.Fatalf("q7 output key: %v", err)
		}
		w, err := operators.DecodeWinningBid(rec.Value)
		if err != nil {
			t.Fatalf("q7 output value: %v", err)
		}
		if w.Auction != auction {
			t.Fatalf("q7 emitted a record keyed on auction %d whose value names auction %d", auction, w.Auction)
		}
		out = append(out, MaxBidRow{
			WindowStart: rec.EventTime + 1 - nexmarkWindowSize,
			Auction:     auction, Price: w.Price, Bidder: w.Bidder,
		})
	}
	slices.SortFunc(out, CompareMaxBids)
	return out
}

// nexmarkOracleFor computes the expected answer for one query over the given
// source configurations.
//
// Several configurations are the multi-input rows, and their answers are
// UNIONED. That is only correct because the sources cover disjoint event-time
// ranges, so no (auction, window) is fed by both -- which is asserted here
// rather than assumed, because a config change that made the ranges overlap
// would silently make every windowed expectation wrong.
func nexmarkOracleFor(t *testing.T, query string, cfgs []sources.NexmarkConfig) any {
	t.Helper()
	spec := Spec{Size: nexmarkWindowSize, Slide: nexmarkSlide}

	switch query {
	case "q0", "q1", "q2":
		merged := make(map[NexmarkRow]int64)
		for _, cfg := range cfgs {
			var part map[NexmarkRow]int64
			var err error
			switch query {
			case "q0":
				part, err = NexmarkQ0(cfg)
			case "q1":
				part, err = NexmarkQ1(cfg, operators.Q1Factor)
			case "q2":
				part, err = NexmarkQ2(cfg, operators.Q2Divisor)
			}
			if err != nil {
				t.Fatalf("%s oracle: %v", query, err)
			}
			for row, n := range part {
				merged[row] += n
			}
		}
		return SortedNexmarkRows(merged)

	case "q5":
		merged := make(map[HotItemKey]int64)
		for _, cfg := range cfgs {
			part, err := NexmarkQ5(cfg, spec)
			if err != nil {
				t.Fatalf("q5 oracle: %v", err)
			}
			for k, n := range part {
				if _, clash := merged[k]; clash {
					t.Fatalf("two sources both produce q5 rows for window %d auction %d: their event-time ranges overlap, so the union of their oracles is not the answer for the pair",
						k.WindowStart, k.Auction)
				}
				merged[k] = n
			}
		}
		return SortedHotItems(merged)

	case "q7":
		merged := make(map[MaxBidKey]MaxBid)
		for _, cfg := range cfgs {
			part, err := NexmarkQ7(cfg, nexmarkWindowSize)
			if err != nil {
				t.Fatalf("q7 oracle: %v", err)
			}
			for k, v := range part {
				if _, clash := merged[k]; clash {
					t.Fatalf("two sources both produce q7 rows for window %d auction %d: their event-time ranges overlap",
						k.WindowStart, k.Auction)
				}
				merged[k] = v
			}
		}
		return SortedMaxBids(merged)
	}
	panic("unknown query " + query)
}

// assertNexmarkMatches compares one run's sink against one oracle's answer,
// reporting the first row they differ on rather than dumping thousands nobody
// will read.
func assertNexmarkMatches(t *testing.T, query string, recs []*core.Record, want any, label string) {
	t.Helper()
	switch want := want.(type) {
	case []NexmarkRow:
		got := SortedNexmarkRows(rowsFromSink(recs))
		assertRowsEqual(t, got, want, label)
	case []HotItem:
		got := hotItemsFromSink(t, recs)
		if slices.Equal(got, want) {
			return
		}
		reportFirstDifference(t, len(got), len(want), label, func(i int) (any, any) {
			return indexOr(got, i), indexOr(want, i)
		})
	case []MaxBidRow:
		got := maxBidsFromSink(t, recs)
		if slices.Equal(got, want) {
			return
		}
		reportFirstDifference(t, len(got), len(want), label, func(i int) (any, any) {
			return indexOr(got, i), indexOr(want, i)
		})
	default:
		t.Fatalf("%s: no comparison for %T", query, want)
	}
}

func assertRowsEqual(t *testing.T, got, want []NexmarkRow, label string) {
	t.Helper()
	if slices.Equal(got, want) {
		return
	}
	reportFirstDifference(t, len(got), len(want), label, func(i int) (any, any) {
		return indexOr(got, i), indexOr(want, i)
	})
}

func indexOr[T any](s []T, i int) any {
	if i < len(s) {
		return s[i]
	}
	return nil
}

// reportFirstDifference fails with the first row two answers disagree on, and
// with the lengths when one is a prefix of the other.
func reportFirstDifference(t *testing.T, gotLen, wantLen int, label string, at func(int) (any, any)) {
	t.Helper()
	for i := range min(gotLen, wantLen) {
		g, w := at(i)
		if fmt.Sprint(g) != fmt.Sprint(w) {
			t.Fatalf("%s: row %d of %d: the engine has %v, the oracle has %v (engine produced %d rows, oracle %d)",
				label, i, wantLen, g, w, gotLen, wantLen)
		}
	}
	if gotLen > wantLen {
		g, _ := at(wantLen)
		t.Fatalf("%s: the engine produced %d rows to the oracle's %d; the first extra is %v", label, gotLen, wantLen, g)
	}
	_, w := at(gotLen)
	t.Fatalf("%s: the engine produced %d rows to the oracle's %d; the first missing is %v", label, gotLen, wantLen, w)
}

// runNexmarkQuery runs one query over the given source configurations and
// compares its sink against the oracle. It returns nothing: everything it
// establishes it asserts.
func runNexmarkQuery(t *testing.T, query string, p int, cfgs []sources.NexmarkConfig, label string) {
	t.Helper()

	sourceVertices := make([]graph.Vertex, len(cfgs))
	for i, cfg := range cfgs {
		sourceVertices[i] = nexmarkSourceVertex(fmt.Sprintf("src%d", i), cfg, p)
	}

	q5, q7 := newQ5Factories(), &maxBidFactory{}
	collect := sinks.NewCollect()
	run(t, buildNexmarkGraph(t, query, p, sourceVertices, collect, q5, q7))

	assertNexmarkMatches(t, query, collect.Records(), nexmarkOracleFor(t, query, cfgs), label)

	// The precondition every oracle comparison rests on: nothing was late.
	// The oracles have no watermark and therefore no lateness model, so they
	// count every event; the engine drops a record whose window has been
	// purged. They agree only at zero.
	switch query {
	case "q7":
		dropped, onTime := q7.totals()
		if dropped != 0 {
			t.Errorf("%s: q7 dropped %d bids as late; the watermark allows for the source's full lag", label, dropped)
		}
		if onTime == 0 {
			t.Errorf("%s: q7 accepted no bids at all, so the drop count above says nothing", label)
		}
	case "q5":
		if dropped := q5.stage1.dropped(); dropped != 0 {
			t.Errorf("%s: q5 stage 1 dropped %d assignments as late", label, dropped)
		}
		// Stage 2 is the half that matters. Stage 1 emits a fired window at
		// its END-1 precisely so that a second event-time stage does not see
		// the whole upstream output as late; a stage 2 that saw it that way
		// would drop everything, emit nothing, and report no error, which
		// looks like a windowing bug. So the acceptances are asserted, not
		// only the drops.
		dropped, onTime := q5.stage2.totals()
		if dropped != 0 {
			t.Errorf("%s: q5 stage 2 dropped %d of stage 1's records as late; stage 1 emits at end-1, so none of them can be",
				label, dropped)
		}
		if onTime == 0 {
			t.Fatalf("%s: q5 stage 2 accepted NO records on time. Its whole input was late, which produces empty output and no error",
				label)
		}
	}
}

// nexmarkQueries is the five, named once so that every table below runs all of
// them and a sixth cannot be added to one and forgotten in another.
var nexmarkQueries = []string{"q0", "q1", "q2", "q5", "q7"}

// TestNexmarkMatchesOracleAcrossSeeds is the breadth half: many seeds, every
// query, at parallelism 1.
//
// Breadth is what catches an off-by-one at a boundary that one seed's data
// happens not to land on. It stays at parallelism 1 because at this many seeds
// the run has to be cheap, and parallelism is the depth matrix's job.
func TestNexmarkMatchesOracleAcrossSeeds(t *testing.T) {
	const (
		seeds = 20
		count = 2000
	)
	for _, query := range nexmarkQueries {
		t.Run(query, func(t *testing.T) {
			for seed := uint64(1); seed <= seeds; seed++ {
				cfg := nexmarkEquivalenceConfig(seed, count)
				runNexmarkQuery(t, query, 1, []sources.NexmarkConfig{cfg}, fmt.Sprintf("%s seed %d", query, seed))
			}
		})
	}
}

// TestNexmarkMatchesOracleAcrossParallelism is the depth half.
//
// Every query at parallelism 1, 2 and 4, on a chain; and q5 and q7 also on a
// multi-input topology, which is the row that can distinguish an input gate
// taking the minimum from one taking the maximum. See the note at the top of
// this file for why the stateless queries do not get that row.
func TestNexmarkMatchesOracleAcrossParallelism(t *testing.T) {
	const seeds = 3
	parallelisms := []int{1, 2, 4}

	for _, query := range nexmarkQueries {
		for _, p := range parallelisms {
			t.Run(fmt.Sprintf("%s/p%d", query, p), func(t *testing.T) {
				for seed := uint64(1); seed <= seeds; seed++ {
					cfg := nexmarkEquivalenceConfig(seed, nexmarkCount)
					runNexmarkQuery(t, query, p, []sources.NexmarkConfig{cfg},
						fmt.Sprintf("%s chain at parallelism %d, seed %d", query, p, seed))

					if query != "q5" && query != "q7" {
						continue
					}
					a := nexmarkEquivalenceConfig(seed, nexmarkCount)
					b := nexmarkEquivalenceConfig(seed+1000, nexmarkCount)
					b.BaseEventTime = a.BaseEventTime + nexmarkSeparation
					runNexmarkQuery(t, query, p, []sources.NexmarkConfig{a, b},
						fmt.Sprintf("%s multi-input at parallelism %d, seed %d", query, p, seed))
				}
			})
		}
	}
}

// TestNexmarkQueriesProduceDifferentAnswers is the guard against the whole file
// being vacuous.
//
// Five queries compared against five oracles proves nothing if the five are the
// same computation under different names: a bug that made every query a
// passthrough would pass every comparison above, because the oracle it is
// compared against would have the same bug only if the oracles were shared, and
// they are not -- but a MIS-WIRED suite that ran q0's graph five times and
// compared each against q0's oracle would.
//
// So the five answers are taken over one input and asserted to be five
// different things, with the relationships between them stated: q0 carries the
// whole stream, q1 and q2 carry strictly fewer records, and the two windowed
// queries produce rows the stream itself does not contain.
func TestNexmarkQueriesProduceDifferentAnswers(t *testing.T) {
	cfg := nexmarkEquivalenceConfig(7, nexmarkCount)

	counts := map[string]int{}
	for _, query := range nexmarkQueries {
		q5, q7 := newQ5Factories(), &maxBidFactory{}
		collect := sinks.NewCollect()
		run(t, buildNexmarkGraph(t, query, 1,
			[]graph.Vertex{nexmarkSourceVertex("src0", cfg, 1)}, collect, q5, q7))
		counts[query] = len(collect.Records())
		if counts[query] == 0 {
			t.Fatalf("%s produced nothing", query)
		}
	}
	t.Logf("over %d events: q0 %d rows, q1 %d, q2 %d, q5 %d, q7 %d",
		cfg.Count, counts["q0"], counts["q1"], counts["q2"], counts["q5"], counts["q7"])

	if int64(counts["q0"]) != cfg.Count {
		t.Errorf("q0 produced %d rows for %d events; a passthrough produces one per event", counts["q0"], cfg.Count)
	}
	if counts["q1"] >= counts["q0"] {
		t.Errorf("q1 produced %d rows and q0 %d; q1 drops the persons and the auctions", counts["q1"], counts["q0"])
	}
	if counts["q2"] >= counts["q1"] {
		t.Errorf("q2 produced %d rows and q1 %d; q2 keeps a share of the bids q1 keeps all of", counts["q2"], counts["q1"])
	}
	// The windowed queries produce a row per window rather than per event, so
	// they are an order of magnitude smaller. Stated as "fewer than the bids"
	// rather than as a figure, because the exact count depends on the seed.
	for _, query := range []string{"q5", "q7"} {
		if counts[query] >= counts["q1"] {
			t.Errorf("%s produced %d rows and q1 %d; a windowed query emits per window, not per bid",
				query, counts[query], counts["q1"])
		}
	}
}
