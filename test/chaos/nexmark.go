package chaos

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/test/oracle"
)

// The Nexmark chaos suite: the same seeded fault schedules, run against the
// five queries instead of against the keyed-count workload.
//
// It sits BESIDE the Phase 5 suite rather than replacing it. That suite is the
// committed evidence for exactly-once output and its census floors were
// measured on it; changing its workload would invalidate both. What is shared
// is the machinery -- the schedules, the injector, the census, the recovery
// bookkeeping -- and what is new is the workload and the comparison.
//
// # The workload
//
// Two source vertices at parallelism 2, as in the Phase 5 suite, with the same
// asymmetry: srcA covers its range in a quarter of the elements srcB needs, and
// the barrier intervals are scaled so both inject the same NUMBER of barriers.
// srcA therefore reaches barrier k four times as early in the stream, and the
// gate holds its barrier for thousands of srcB elements waiting for the match.
// That gap is the alignment window, measured in elements rather than in
// microseconds, and it is what TriggerDuringAlignment aims at.
//
// # Why the two ranges are DISJOINT here and overlapping there
//
// The Phase 5 workload overlaps its sources so that some (key, window) pairs
// are fed by both and the gate's minimum across four inputs decides when they
// fire. This one separates them, and the reason is the oracle rather than the
// engine.
//
// Four of these five queries merge across sources trivially -- a multiset union
// for q0, q1 and q2, and a fold under oracle.BetterBid for q7 -- but q5 does
// not. Its answer is a per-window MAXIMUM over auctions, and two per-source
// maxima cannot be combined into the maximum over the union unless no window is
// fed by both: the counts they were selected from are gone. Exposing those
// counts would mean widening test/oracle/nexmark.go, which is not this step's
// to edit, so the sources are separated instead and the union is exact.
//
// Nothing is lost that is not covered elsewhere. The gate's minimum is what the
// Phase 5 suite exercises with overlapping ranges, and what the equivalence
// suite exercises with separated ones -- separated is the STRONGER shape for
// detecting a maximum gate, because a maximum races event time to the later
// source's range and purges the earlier one's windows. This suite is about what
// recovery commits, and a window fed by one source rather than two does not
// change that.
//
// # Lateness
//
// Zero, and the drop counts are asserted at zero, because the batch oracles
// have no lateness model. That turns "the oracle does not model this" into a
// checked precondition. q5's second stage also asserts what it ACCEPTED: a
// stage that saw its whole input as late would produce empty output and no
// error at all.
const (
	nexParallelism = 2
	// The auction id space. Small against the bid count so that a five-second
	// window holds many bids per auction, which is what makes q5's hot item and
	// q7's maximum mean anything.
	nexAuctionCardinality = 32
	nexPersonCardinality  = 16
	nexWindowSize         = 5000
	// A quarter of the size, so each bid joins four windows and q5's sliding
	// assignment is exercised rather than degenerating to tumbling.
	nexSlide             = 1250
	nexLateness          = 0
	nexWatermarkInterval = 100

	nexSourceACount = 4000
	nexSourceBCount = 16000
	// Scaled against the range lengths so both vertices inject eight barriers
	// per subtask, as in the Phase 5 suite: 2000/250 and 8000/1000.
	nexBarrierIntervalA = 250
	nexBarrierIntervalB = 1000

	// nexSeparation puts srcB's whole event-time range above srcA's. srcA spans
	// 40 seconds and srcB 160; five million milliseconds apart, no window of
	// five seconds can contain records from both.
	nexSeparation = 5_000_000

	// nexMaxRecoveries bounds how many times one schedule is resumed. A fault
	// fires at most once, so a schedule of at most three faults cannot need
	// more than three; the cap is above that on purpose, so reaching it means a
	// fault fired twice or a recovery made no progress.
	nexMaxRecoveries = 6
)

// nexQueries is the five, in the order the suite reports them.
var nexQueries = []string{"q0", "q1", "q2", "q5", "q7"}

func nexSourceAConfig() sources.NexmarkConfig {
	return sources.NexmarkConfig{
		Seed: 0x6A, Count: nexSourceACount,
		AuctionCardinality: nexAuctionCardinality, PersonCardinality: nexPersonCardinality,
		PriceRange: 1000, CategoryCount: 4, AuctionDuration: 5000,
		BaseEventTime: 1700000000000, EventTimeStep: 10, MaxLag: 200,
	}
}

func nexSourceBConfig() sources.NexmarkConfig {
	cfg := nexSourceAConfig()
	cfg.Seed = 0x6B
	cfg.Count = nexSourceBCount
	cfg.BaseEventTime += nexSeparation
	return cfg
}

func nexSourceConfigs() []sources.NexmarkConfig {
	return []sources.NexmarkConfig{nexSourceAConfig(), nexSourceBConfig()}
}

// nexmarkFactories keeps a handle on every stateful operator a run built, so
// the run can be asked afterwards what it dropped and what it accepted.
type nexmarkFactories struct {
	mu     sync.Mutex
	counts []*operators.WindowCount
	hot    []*operators.HotItems
	maxBid []*operators.MaxBid
}

func (f *nexmarkFactories) newCount() core.Operator {
	op := operators.NewSlidingCount(nexWindowSize, nexSlide, nexLateness)
	f.mu.Lock()
	f.counts = append(f.counts, op)
	f.mu.Unlock()
	return op
}

func (f *nexmarkFactories) newHot() core.Operator {
	op := operators.NewQ5HotItems(nexWindowSize, nexLateness)
	f.mu.Lock()
	f.hot = append(f.hot, op)
	f.mu.Unlock()
	return op
}

func (f *nexmarkFactories) newMaxBid() core.Operator {
	op := operators.NewQ7(nexWindowSize, nexLateness)
	f.mu.Lock()
	f.maxBid = append(f.maxBid, op)
	f.mu.Unlock()
	return op
}

// totals sums the drop and acceptance counters across every subtask a run made.
func (f *nexmarkFactories) totals() (dropped, onTime int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, op := range f.counts {
		dropped += op.Dropped()
	}
	for _, op := range f.hot {
		dropped += op.Dropped()
		onTime += op.OnTime()
	}
	for _, op := range f.maxBid {
		dropped += op.Dropped()
		onTime += op.OnTime()
	}
	return dropped, onTime
}

// nexmarkChain returns the operator vertices of one query, in order. The edges
// are the order itself: every query here is a straight line.
func nexmarkChain(query string, f *nexmarkFactories) []graph.Vertex {
	operatorVertex := func(id string, newOperator func() core.Operator) graph.Vertex {
		return graph.Vertex{ID: id, Kind: graph.VertexOperator, Parallelism: nexParallelism, NewOperator: newOperator}
	}
	switch query {
	case "q0":
		return []graph.Vertex{operatorVertex("q0", func() core.Operator { return operators.NewQ0() })}
	case "q1":
		return []graph.Vertex{operatorVertex("q1", func() core.Operator { return operators.NewQ1(operators.Q1Factor) })}
	case "q2":
		return []graph.Vertex{operatorVertex("q2", func() core.Operator { return operators.NewQ2(operators.Q2Divisor) })}
	case "q7":
		return []graph.Vertex{operatorVertex("q7", f.newMaxBid)}
	case "q5":
		return []graph.Vertex{
			operatorVertex("bids", func() core.Operator { return operators.NewBidsOnly() }),
			operatorVertex("count", f.newCount),
			operatorVertex("rekey", func() core.Operator { return operators.NewQ5Rekey(nexWindowSize) }),
			operatorVertex("hot", f.newHot),
		}
	}
	panic("chaos: unknown nexmark query " + query)
}

// nexTimerVertices names the vertices of a query that arm event-time timers.
// They are what the census reads out of a checkpoint; a query with none reports
// no pending windows, which is a fact about the query rather than a gap.
func nexTimerVertices(query string) []string {
	switch query {
	case "q5":
		return []string{"count", "hot"}
	case "q7":
		return []string{"q7"}
	default:
		return nil
	}
}

// nexmarkJobGraph is one query as a graph whose sink subtasks come from
// newSink.
//
// A FACTORY and not a sink: sinks.Transactional owns a file handle and an epoch
// counter, and several subtasks sharing one would write into a single staging
// file and commit it under a single name.
func nexmarkJobGraph(query string, newSink func() core.Sink, f *nexmarkFactories) *graph.Graph {
	g := graph.New()
	chain := nexmarkChain(query, f)

	vertices := []graph.Vertex{
		nexSourceVertex("srcA", nexSourceAConfig(), nexBarrierIntervalA),
		nexSourceVertex("srcB", nexSourceBConfig(), nexBarrierIntervalB),
	}
	vertices = append(vertices, chain...)
	vertices = append(vertices, graph.Vertex{
		ID: "out", Kind: graph.VertexSink, Parallelism: nexParallelism, NewSink: newSink,
	})
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			panic(fmt.Sprintf("chaos: AddVertex(%s): %v", v.ID, err))
		}
	}

	edges := [][2]string{{"srcA", chain[0].ID}, {"srcB", chain[0].ID}}
	for i := 1; i < len(chain); i++ {
		edges = append(edges, [2]string{chain[i-1].ID, chain[i].ID})
	}
	edges = append(edges, [2]string{chain[len(chain)-1].ID, "out"})
	for _, e := range edges {
		if err := g.Connect(e[0], e[1]); err != nil {
			panic(fmt.Sprintf("chaos: Connect(%s, %s): %v", e[0], e[1], err))
		}
	}
	return g
}

func nexSourceVertex(id string, cfg sources.NexmarkConfig, barrierInterval int64) graph.Vertex {
	return graph.Vertex{
		ID: id, Kind: graph.VertexSource, Parallelism: nexParallelism,
		NewSource:                 func() core.Source { return sources.NewNexmark(cfg) },
		WatermarkIntervalElements: nexWatermarkInterval,
		MaxOutOfOrderness:         cfg.MaxLag,
		BarrierIntervalElements:   barrierInterval,
	}
}

// nexScheduleGraph is the shape ScheduleFor is asked about, one per query.
//
// Built from the same nexmarkJobGraph every run uses, so the bounds a schedule
// is drawn against are the bounds of the job it will run on. A separately
// written description of the topology would be a second thing to keep in step,
// and the failure of it drifting is a schedule full of faults that cannot fire
// -- which the census reports as a miss and which looks exactly like the
// interesting kind of miss.
//
// The five queries have different shapes, so a seed draws a different schedule
// for each. That is correct rather than unfortunate: a schedule is a set of
// positions in a specific topology.
var nexScheduleGraph = memoise(func(query string) *graph.Graph {
	return nexmarkJobGraph(query, func() core.Sink { return sinks.NewDiscard() }, &nexmarkFactories{})
})

// nexOracle is the batch answer for one query over this workload.
//
// Computed once per query for the whole suite rather than once per schedule.
// Every seed varies the FAULTS and nothing about the input, so five hundred
// recomputations would be five hundred copies of one answer. The comparison is
// still per schedule; only the arithmetic is shared.
var nexOracle = memoise(func(query string) any { return computeNexOracle(query) })

// memoise returns a function computing f once per distinct argument.
//
// A tiny helper rather than five sync.OnceValue variables, because there are
// two of these and five queries and the alternative is ten package-level
// singletons that have to be kept in step with nexQueries.
func memoise[K comparable, V any](f func(K) V) func(K) V {
	var mu sync.Mutex
	cache := map[K]V{}
	return func(k K) V {
		mu.Lock()
		defer mu.Unlock()
		if v, ok := cache[k]; ok {
			return v
		}
		v := f(k)
		cache[k] = v
		return v
	}
}

// computeNexOracle merges the per-source answers for one query.
//
// The merge is per query because the answers are different shapes, and it is
// exact in every case only because the two sources cover disjoint event-time
// ranges. That is asserted for the windowed queries rather than assumed: a
// config change that let the ranges overlap would make every windowed
// expectation quietly wrong.
func computeNexOracle(query string) any {
	spec := oracle.Spec{Size: nexWindowSize, Slide: nexSlide}
	switch query {
	case "q0", "q1", "q2":
		merged := make(map[oracle.NexmarkRow]int64)
		for _, cfg := range nexSourceConfigs() {
			var part map[oracle.NexmarkRow]int64
			var err error
			switch query {
			case "q0":
				part, err = oracle.NexmarkQ0(cfg)
			case "q1":
				part, err = oracle.NexmarkQ1(cfg, operators.Q1Factor)
			case "q2":
				part, err = oracle.NexmarkQ2(cfg, operators.Q2Divisor)
			}
			if err != nil {
				panic(fmt.Sprintf("chaos: %s oracle: %v", query, err))
			}
			for row, n := range part {
				merged[row] += n
			}
		}
		return merged

	case "q5":
		merged := make(map[oracle.HotItemKey]int64)
		for _, cfg := range nexSourceConfigs() {
			part, err := oracle.NexmarkQ5(cfg, spec)
			if err != nil {
				panic(fmt.Sprintf("chaos: q5 oracle: %v", err))
			}
			for k, n := range part {
				if _, clash := merged[k]; clash {
					panic(fmt.Sprintf("chaos: two sources both produce q5 rows for window %d auction %d: "+
						"their event-time ranges overlap, so the union of their oracles is not the answer for the pair",
						k.WindowStart, k.Auction))
				}
				merged[k] = n
			}
		}
		return merged

	case "q7":
		merged := make(map[oracle.MaxBidKey]oracle.MaxBid)
		for _, cfg := range nexSourceConfigs() {
			part, err := oracle.NexmarkQ7(cfg, nexWindowSize)
			if err != nil {
				panic(fmt.Sprintf("chaos: q7 oracle: %v", err))
			}
			for k, v := range part {
				if _, clash := merged[k]; clash {
					panic(fmt.Sprintf("chaos: two sources both produce q7 rows for window %d auction %d: "+
						"their event-time ranges overlap", k.WindowStart, k.Auction))
				}
				merged[k] = v
			}
		}
		return merged
	}
	panic("chaos: unknown nexmark query " + query)
}

// RunNexmarkSchedule runs seed's schedule against one query and compares the
// result against the batch oracle.
//
// A clean run first, to establish that this workload produces the oracle's
// answer with nothing going wrong; then the same job under the schedule's
// faults, resumed from the last complete checkpoint after each abort, until it
// finishes. Both are compared.
//
// What is compared is the COMMITTED FILES, through sinks.ReadCommitted, and
// never an in-memory slice. That is what makes the exactly-once claim mean
// something: the comparison runs against files that survived a crash rather
// than against a slice that was never at risk. A staging file is not output.
//
// Sorted contents, never emission order. Ordering after a recovery differs from
// a clean run for reasons that have nothing to do with correctness.
//
// One output root across the aborted run and its recoveries. A sink is external
// and durable: it does not forget what an aborted run committed, and handing
// each attempt a fresh directory would measure recovery against a sink that
// conveniently lost the evidence of double delivery.
func RunNexmarkSchedule(t *testing.T, seed int64, query string) Result {
	t.Helper()
	faults := ScheduleFor(seed, nexScheduleGraph(query))
	res := Result{Seed: seed, Faults: faults}

	// The clean run. It takes no checkpoints: it is the reference for the
	// CONTENTS of the sink, and a run with nothing to recover from has nothing
	// to write. Barriers still flow, because they are part of the element
	// stream whether or not anybody records snapshots.
	cleanFactories := &nexmarkFactories{}
	cleanOutput := t.TempDir()
	if err := runtime.RunWithOptions(context.Background(),
		nexmarkJobGraph(query, transactionalSinks(cleanOutput), cleanFactories),
		runtime.Options{Seed: uint64(seed)}); err != nil {
		t.Fatalf("seed %d, %s: the clean run failed: %v", seed, query, err)
	}
	cleanDropped, cleanOnTime := cleanFactories.totals()
	assertNoLateDrops(t, seed, query, cleanDropped, "clean run")
	assertAcceptedSomething(t, seed, query, cleanOnTime, "clean run")
	assertNothingLeftStaged(t, seed, cleanOutput)
	assertNexmarkMatchesOracle(t, seed, query, cleanOutput, "clean run")

	// The fault run, and its recoveries.
	inj := newInjector(faults)
	root := t.TempDir()
	output := t.TempDir()
	restoreFrom := ""
	// Summed across ATTEMPTS. A schedule whose fault fires at the last barrier
	// leaves a resumed run with a handful of records to replay and possibly no
	// bid among them, so the final attempt legitimately accepts nothing into a
	// window. What has to be non-zero is the schedule as a whole; asserting it
	// per attempt was asserting on a run that had nothing to do.
	var faultOnTime int64
	for attempt := 0; ; attempt++ {
		if attempt > nexMaxRecoveries {
			t.Fatalf("seed %d, %s: still failing after %d recoveries with faults %v: a fault fires at most once, "+
				"so this is a fault firing twice or a recovery that made no progress",
				seed, query, nexMaxRecoveries, faults)
		}
		f := &nexmarkFactories{}
		err := runtime.RunWithOptions(context.Background(),
			nexmarkJobGraph(query, transactionalSinks(output), f),
			runtime.Options{
				CheckpointRoot: root,
				RestoreFrom:    restoreFrom,
				Seed:           uint64(seed),
				FaultInjector:  inj,
			})
		dropped, onTime := f.totals()
		faultOnTime += onTime
		if err == nil {
			assertNoLateDrops(t, seed, query, dropped, "fault run")
			break
		}
		if !errors.Is(err, runtime.ErrFaultInjected) {
			t.Fatalf("seed %d, %s: the run failed for a reason nothing scheduled: %v", seed, query, err)
		}
		// A run that aborted before its first record reached a window may still
		// have dropped nothing; asserting the counters here would be asserting
		// on a run that did not finish.
		res.Recoveries = append(res.Recoveries, nexRecoveryPoint(t, seed, query, root))
		if res.Recoveries[len(res.Recoveries)-1].FromCheckpoint {
			restoreFrom = root
		} else {
			restoreFrom = ""
		}
	}

	// The staging check comes FIRST, and the order is not cosmetic. It is the
	// more primitive fact -- a transaction nobody will ever commit -- and the
	// oracle comparison ends the test with a Fatalf, so a suite that checked it
	// second would report the symptom and never the cause.
	assertAcceptedSomething(t, seed, query, faultOnTime, "fault run")
	assertNothingLeftStaged(t, seed, output)
	assertNexmarkMatchesOracle(t, seed, query, output, "fault run")
	res.Outcomes = inj.outcomes()
	return res
}

// assertNoLateDrops is the precondition every oracle comparison rests on.
//
// The oracles have no watermark and therefore no lateness model, so they count
// every event; the engine drops a record whose window has been purged. They
// agree only at zero.
//
// It is checked on runs that FINISHED. An attempt that aborted before its first
// record reached a window may also have dropped nothing, and asserting there
// would be asserting on a run that did not happen.
func assertNoLateDrops(t *testing.T, seed int64, query string, dropped int64, label string) {
	t.Helper()
	if dropped != 0 {
		t.Errorf("seed %d, %s: the %s dropped %d assignments as late; the batch oracle has no lateness model, "+
			"so the comparison against it is only valid at zero", seed, query, label, dropped)
	}
}

// assertAcceptedSomething stops "dropped nothing" from being satisfied by a run
// that received nothing.
//
// It is q5's second stage this exists for. A stage that saw its whole input as
// late would drop everything, emit nothing and report no error, which reads as
// a windowing bug -- and a suite that only counted drops would not distinguish
// it from a stage with no input at all. q7 is included because it has the same
// counter and the same failure is available to it.
//
// Queries with no windows have no such counter and are skipped, which is a fact
// about them rather than a gap: they hold no state for a watermark to purge.
func assertAcceptedSomething(t *testing.T, seed int64, query string, onTime int64, label string) {
	t.Helper()
	if len(nexTimerVertices(query)) == 0 {
		return
	}
	if onTime == 0 {
		t.Errorf("seed %d, %s: the %s accepted no records into any window, so the drop count says nothing",
			seed, query, label)
	}
}

// assertNexmarkMatchesOracle compares COMMITTED output against the batch
// oracle, and asserts exactly-once while it is there.
//
// Exactly-once is the multiplicity: every row the oracle names appears in the
// committed output the number of times the oracle names and no more. For the
// stateless queries that number can legitimately exceed one -- two events can
// encode to the same bytes at the same millisecond -- so it is a MULTISET
// comparison rather than a set one, and a lost copy is caught as surely as a
// duplicated one.
func assertNexmarkMatchesOracle(t *testing.T, seed int64, query, root, label string) {
	t.Helper()
	recs, err := sinks.ReadCommitted(root)
	if err != nil {
		t.Fatalf("seed %d, %s: %s: reading the committed output: %v", seed, query, label, err)
	}

	switch want := nexOracle(query).(type) {
	case map[oracle.NexmarkRow]int64:
		got := make(map[oracle.NexmarkRow]int64, len(recs))
		for _, rec := range recs {
			got[oracle.NexmarkRow{
				Key: string(rec.Key), Value: string(rec.Value), EventTime: rec.EventTime,
			}]++
		}
		assertSameMultiset(t, seed, query, label, got, want)

	case map[oracle.HotItemKey]int64:
		got := make(map[oracle.HotItemKey][]int64, len(recs))
		for _, rec := range recs {
			auction, err := sources.NexmarkKeyID(rec.Key)
			if err != nil {
				t.Fatalf("seed %d, %s: %s: committed output key: %v", seed, query, label, err)
			}
			count, err := operators.DecodeCount(rec.Value)
			if err != nil {
				t.Fatalf("seed %d, %s: %s: committed output value: %v", seed, query, label, err)
			}
			k := oracle.HotItemKey{WindowStart: rec.EventTime + 1 - nexWindowSize, Auction: auction}
			got[k] = append(got[k], count)
		}
		for k, counts := range got {
			if len(counts) != 1 {
				t.Fatalf("seed %d, %s: %s: window %d auction %d is committed %d times with counts %v; "+
					"committed output is exactly once, so a second copy is a transaction that was committed "+
					"and then produced again", seed, query, label, k.WindowStart, k.Auction, len(counts), counts)
			}
		}
		if len(got) != len(want) {
			t.Errorf("seed %d, %s: %s: the committed output holds %d rows, want %d",
				seed, query, label, len(got), len(want))
		}
		for k, n := range want {
			counts, ok := got[k]
			if !ok {
				t.Fatalf("seed %d, %s: %s: the committed output is missing window %d auction %d, count %d",
					seed, query, label, k.WindowStart, k.Auction, n)
			}
			if counts[0] != n {
				t.Fatalf("seed %d, %s: %s: window %d auction %d counted %d, want %d",
					seed, query, label, k.WindowStart, k.Auction, counts[0], n)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Fatalf("seed %d, %s: %s: the committed output holds window %d auction %d, which the input "+
					"does not produce", seed, query, label, k.WindowStart, k.Auction)
			}
		}

	case map[oracle.MaxBidKey]oracle.MaxBid:
		got := make(map[oracle.MaxBidKey][]oracle.MaxBid, len(recs))
		for _, rec := range recs {
			auction, err := sources.NexmarkKeyID(rec.Key)
			if err != nil {
				t.Fatalf("seed %d, %s: %s: committed output key: %v", seed, query, label, err)
			}
			w, err := operators.DecodeWinningBid(rec.Value)
			if err != nil {
				t.Fatalf("seed %d, %s: %s: committed output value: %v", seed, query, label, err)
			}
			k := oracle.MaxBidKey{WindowStart: rec.EventTime + 1 - nexWindowSize, Auction: auction}
			got[k] = append(got[k], oracle.MaxBid{Price: w.Price, Bidder: w.Bidder})
		}
		for k, winners := range got {
			if len(winners) != 1 {
				t.Fatalf("seed %d, %s: %s: window %d auction %d is committed %d times with %v; "+
					"committed output is exactly once", seed, query, label,
					k.WindowStart, k.Auction, len(winners), winners)
			}
		}
		if len(got) != len(want) {
			t.Errorf("seed %d, %s: %s: the committed output holds %d rows, want %d",
				seed, query, label, len(got), len(want))
		}
		for k, v := range want {
			winners, ok := got[k]
			if !ok {
				t.Fatalf("seed %d, %s: %s: the committed output is missing window %d auction %d, want %+v",
					seed, query, label, k.WindowStart, k.Auction, v)
			}
			if winners[0] != v {
				t.Fatalf("seed %d, %s: %s: window %d auction %d won by %+v, want %+v",
					seed, query, label, k.WindowStart, k.Auction, winners[0], v)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Fatalf("seed %d, %s: %s: the committed output holds window %d auction %d, which the input "+
					"does not produce", seed, query, label, k.WindowStart, k.Auction)
			}
		}
	}
}

// assertSameMultiset compares two multisets of rows, reporting the first row
// they differ on in a stable order.
func assertSameMultiset(t *testing.T, seed int64, query, label string,
	got, want map[oracle.NexmarkRow]int64) {
	t.Helper()
	rows := make([]oracle.NexmarkRow, 0, len(want))
	for row := range want {
		rows = append(rows, row)
	}
	slices.SortFunc(rows, oracle.CompareNexmarkRows)

	for _, row := range rows {
		if got[row] != want[row] {
			t.Fatalf("seed %d, %s: %s: the committed output holds the row {key %x time %d} %d times, want %d; "+
				"committed output is exactly once per copy the input produces",
				seed, query, label, row.Key, row.EventTime, got[row], want[row])
		}
	}
	if len(got) != len(want) {
		for row, n := range got {
			if _, ok := want[row]; !ok {
				t.Fatalf("seed %d, %s: %s: the committed output holds {key %x time %d} %d times, "+
					"and the input does not produce it at all", seed, query, label, row.Key, row.EventTime, n)
			}
		}
	}
}

// nexRecoveryPoint describes where the next attempt resumes from, and counts
// the pending windows at that cut.
func nexRecoveryPoint(t *testing.T, seed int64, query, root string) Recovery {
	t.Helper()
	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil {
		t.Fatalf("seed %d, %s: reading the checkpoint root: %v", seed, query, err)
	}
	if !ok {
		return Recovery{}
	}
	pending, err := nexPendingWindowsAt(storage, id, query)
	if err != nil {
		t.Fatalf("seed %d, %s: counting the pending windows at checkpoint %d: %v", seed, query, id, err)
	}
	return Recovery{FromCheckpoint: true, CheckpointID: id, PendingWindows: pending}
}

// nexPendingWindowsAt counts the windows that are COMPLETE but UNFIRED at the
// cut checkpoint id records.
//
// It is the number that says whether a recovery proved anything. A window whose
// records straddle the cut is re-armed by the replay and fires again whether or
// not its timer survived; one that fired before the cut is already in the sink.
// Only a window that will receive nothing more depends on the timer being IN
// the checkpoint: with timers in RAM it comes back as an aggregate nothing will
// ever fire, and it goes missing from the sink with no error anywhere.
//
// # How completeness is decided
//
// By REPLAYING the input from each source subtask's resume offset and marking
// what it would feed, which is what the Phase 5 census does and for the same
// reason: a bound computed from event times alone is far weaker. A window of
// five seconds holds hundreds of records but only a handful for any one
// auction, so a (key, window) pair routinely receives nothing more long before
// its window is globally complete. Measured on this workload, the weaker bound
// reported zero pending windows on every cut; this one reports what is there.
//
// Two sets come out of the replay, because the two kinds of vertex here are fed
// differently:
//
//   - fedPairs, the (auction key, window start) pairs a source-fed window
//     operator will still receive records for. That is q7 and q5's stage 1.
//   - fedWindows, the window starts anything will still be produced for. q5's
//     stage 2 is fed by stage 1, not by a source, so no source offset maps to
//     one of ITS keys -- but stage 1 emits into a window only if a record falls
//     in it, so a window absent from this set is one stage 2 will hear nothing
//     more about.
//
// Only BIDS are replayed. Persons and auctions never reach a window operator in
// any of these queries: q7 drops them itself and q5 has a vertex in front that
// does. Counting them would mark windows as still-fed that nothing will feed.
func nexPendingWindowsAt(storage *checkpoint.Storage, id int64, query string) (int, error) {
	timerVertices := nexTimerVertices(query)
	if len(timerVertices) == 0 {
		return 0, nil
	}
	_, payloads, err := storage.Load(id)
	if err != nil {
		return 0, fmt.Errorf("loading checkpoint %d: %w", id, err)
	}

	fedPairs := make(map[nexWindowKey]bool)
	fedWindows := make(map[int64]bool)
	for _, src := range []struct {
		id  string
		cfg sources.NexmarkConfig
	}{{"srcA", nexSourceAConfig()}, {"srcB", nexSourceBConfig()}} {
		for index := range nexParallelism {
			payload, ok := payloads[checkpoint.SubtaskKey{VertexID: src.id, Index: index}]
			if !ok {
				return 0, fmt.Errorf("checkpoint %d holds no state for %s subtask %d", id, src.id, index)
			}
			offset, err := decodePosition(payload)
			if err != nil {
				return 0, fmt.Errorf("checkpoint %d, %s subtask %d: %w", id, src.id, index, err)
			}
			_, end := sourceRange(src.cfg.Count, nexParallelism, index)
			if err := nexMarkFedFrom(src.cfg, offset, end, query, fedPairs, fedWindows); err != nil {
				return 0, err
			}
		}
	}

	open := make(map[nexOpenWindow]bool)
	for _, vertexID := range timerVertices {
		for index := range nexParallelism {
			payload, ok := payloads[checkpoint.SubtaskKey{VertexID: vertexID, Index: index}]
			if !ok {
				return 0, fmt.Errorf("checkpoint %d holds no state for %s subtask %d", id, vertexID, index)
			}
			st := state.NewMemory()
			if err := state.ReadFrom(st, bytes.NewReader(payload)); err != nil {
				return 0, fmt.Errorf("decoding %s subtask %d of checkpoint %d: %w", vertexID, index, id, err)
			}
			watermark := storedWatermark(st)
			var iterErr error
			st.Iterate(func(k, v []byte) bool {
				if len(k) == 0 || k[0] != state.PrefixTimer {
					return true
				}
				if len(k) < 1+state.OrderedInt64Bytes+windowStartBytes {
					iterErr = fmt.Errorf("checkpoint %d holds a %d-byte timer key", id, len(k))
					return false
				}
				// A timer in a checkpoint is unfired by construction: firing
				// deletes it, and firing runs to completion inside one
				// ProcessWatermark. Asserted rather than assumed, because it is
				// also the check that the watermark in the checkpoint is the
				// one that was current at the cut -- a stale one would make
				// every count here wrong in the direction that flatters the
				// suite.
				if fireTime := state.DecodeOrderedInt64(k[1:]); fireTime <= watermark {
					iterErr = fmt.Errorf("checkpoint %d, %s subtask %d: a timer due at %d against a stored "+
						"watermark of %d should already have fired", id, vertexID, index, fireTime, watermark)
					return false
				}
				open[nexOpenWindow{
					vertex: vertexID,
					pair: nexWindowKey{
						key:         string(k[1+state.OrderedInt64Bytes : len(k)-windowStartBytes]),
						windowStart: int64(binary.BigEndian.Uint64(k[len(k)-windowStartBytes:])),
					},
				}] = true
				return true
			})
			if iterErr != nil {
				return 0, iterErr
			}
		}
	}

	pending := 0
	for w := range open {
		// Stage 2 is keyed on the window rather than on an auction, so the
		// pair's key is the window key and only the window start is meaningful
		// against the replay.
		if w.vertex == "hot" {
			if !fedWindows[w.pair.windowStart] {
				pending++
			}
			continue
		}
		if !fedPairs[w.pair] {
			pending++
		}
	}
	return pending, nil
}

// nexWindowKey is one (record key, window start) pair.
type nexWindowKey struct {
	key         string
	windowStart int64
}

// nexOpenWindow is one armed timer, named by the vertex that armed it. The
// vertex is part of the identity because q5 arms timers in two vertices for
// overlapping window starts, and collapsing them would undercount.
type nexOpenWindow struct {
	vertex string
	pair   nexWindowKey
}

// nexMarkFedFrom marks every (key, window) and window the bids in [offset, end)
// of cfg belong to. It is the replay, read straight from the source.
//
// The window assignment matches the query: q5 counts over a sliding
// specification, so a bid joins size/slide windows, and q7 over a tumbling one,
// so it joins exactly one.
func nexMarkFedFrom(cfg sources.NexmarkConfig, offset, end int64, query string,
	pairs map[nexWindowKey]bool, windows map[int64]bool) error {
	slide := int64(nexWindowSize)
	if query == "q5" {
		slide = nexSlide
	}

	src := sources.NewNexmark(cfg)
	if err := src.Open(nil); err != nil {
		return fmt.Errorf("opening the source to replay [%d, %d): %w", offset, end, err)
	}
	defer func() { _ = src.Close() }()
	if err := src.SeekTo(offset); err != nil {
		return fmt.Errorf("seeking to %d: %w", offset, err)
	}
	for pos := offset; pos < end; pos++ {
		rec, ok, err := src.Next()
		if err != nil {
			return fmt.Errorf("reading element %d: %w", pos, err)
		}
		if !ok {
			break
		}
		typ, err := sources.NexmarkTypeOf(rec.Value)
		if err != nil {
			return fmt.Errorf("element %d: %w", pos, err)
		}
		if typ != sources.EventBid {
			continue
		}
		start := rec.EventTime - floorMod(rec.EventTime, slide)
		for n := int64(nexWindowSize) / slide; n > 0; n-- {
			pairs[nexWindowKey{key: string(rec.Key), windowStart: start}] = true
			windows[start] = true
			start -= slide
		}
	}
	return nil
}
