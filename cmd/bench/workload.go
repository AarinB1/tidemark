package main

import (
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// The workloads a run can measure.
//
// identity is the Phase 6a job -- generator, a Map that returns its argument,
// Discard -- and it stays the default so that `make bench` keeps measuring what
// the committed baseline measured.
//
// The rest are the Nexmark queries this engine implements. q3, q4 and q6 are
// absent because no operator implements them: a selector that accepted them and
// silently ran something else would put a number in the record under a query
// name that did not produce it.
const (
	queryIdentity = "identity"
	queryQ0       = "q0"
	queryQ1       = "q1"
	queryQ2       = "q2"
	queryQ5       = "q5"
	queryQ7       = "q7"
)

// queries is every selector value, in the order the sweep runs them.
var queries = []string{queryIdentity, queryQ0, queryQ1, queryQ2, queryQ5, queryQ7}

// windowedQueries are the ones whose state size the window flags move. The
// others hold no state at all, so a window size means nothing to them.
var windowedQueries = []string{queryQ5, queryQ7}

// Config is one measured configuration: everything that decides what ran, and
// nothing about how it went.
//
// It is a type rather than a parameter list because it is the identity of a
// result. A baseline comparison has to know that a q7 number at parallelism 4
// with ten thousand auctions is not comparable to a q7 number at parallelism 4
// with a hundred; matching on parallelism alone, which is what Phase 6a did
// when parallelism was the only dial, would compare them.
//
// It is embedded in Result, so its fields sit at the top level of a result's
// JSON exactly where Phase 6a's baseline put them.
type Config struct {
	Query       string `json:"query"`
	Parallelism int    `json:"parallelism"`
	Records     int64  `json:"records"`
	Seed        uint64 `json:"seed"`
	// Keys is the identity workload's key space. It is meaningless to the
	// Nexmark queries, which key on an auction id.
	Keys int64 `json:"keys"`
	// AuctionCardinality is the auction id space of the Nexmark source, and it
	// is the state-size dial: a windowed query holds one aggregate and one
	// timer per (auction, window) pair it has seen and not yet purged.
	//
	// It saturates, and the saturation is the thing to watch rather than a
	// detail. A window holds a bounded number of bids, so once the id space is
	// large against that number nearly every bid opens its own pair and raising
	// it further buys almost nothing -- while making every window hold one bid,
	// which is a state-size dial that has stopped being an aggregation.
	AuctionCardinality int64 `json:"auction_cardinality"`
	// WindowMillis and SlideMillis are the window specification. The Nexmark
	// source steps event time by one millisecond per element, so a window of n
	// milliseconds holds n elements and the number reads directly as a window
	// occupancy.
	//
	// Slide is ignored by the tumbling queries. It is still recorded, because a
	// zero there and a zero anywhere else in this struct mean different things
	// and the file is read by people.
	WindowMillis int64 `json:"window_millis"`
	SlideMillis  int64 `json:"slide_millis"`
}

// label names a configuration in one line, for the console and for a regression
// message.
func (c Config) label() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s p=%d records=%d", c.Query, c.Parallelism, c.Records)
	if c.Query == queryIdentity {
		fmt.Fprintf(&b, " keys=%d", c.Keys)
		return b.String()
	}
	fmt.Fprintf(&b, " auctions=%d", c.AuctionCardinality)
	if slices.Contains(windowedQueries, c.Query) {
		fmt.Fprintf(&b, " window=%d", c.WindowMillis)
		if c.Query == queryQ5 {
			fmt.Fprintf(&b, " slide=%d", c.SlideMillis)
		}
	}
	return b.String()
}

// oversubscribed reports whether this configuration asked for more parallelism
// than the machine has cores.
//
// It is recorded on every result rather than refused, because the sweep has to
// produce the 8 and 16 rows for the machine that can run them. What it must not
// do is let a 16-way number from a 4-core box enter the record looking like a
// measurement of 16-way scaling.
//
// The threshold is deliberately generous. A job runs one goroutine per SUBTASK
// and this graph has three vertices, so parallelism p is 3p runnable goroutines
// and the real ceiling arrives at p = cores/3. A flag on p > cores understates
// the problem; it does not overstate it.
func (c Config) oversubscribed() bool { return c.Parallelism > runtime.NumCPU() }

// check reports a configuration that cannot be measured, before anything runs.
func (c Config) check() error {
	if !slices.Contains(queries, c.Query) {
		return fmt.Errorf("query %q: this engine implements %s; Nexmark q3, q4 and q6 have no operator here",
			c.Query, strings.Join(queries, ", "))
	}
	switch {
	case c.Parallelism < 1:
		return fmt.Errorf("%s: parallelism %d: must be >= 1", c.label(), c.Parallelism)
	case c.Records < 1:
		return fmt.Errorf("%s: records %d: must be >= 1", c.label(), c.Records)
	case c.Query == queryIdentity && c.Keys < 1:
		return fmt.Errorf("%s: keys %d: must be >= 1", c.label(), c.Keys)
	case c.Query != queryIdentity && c.AuctionCardinality < 1:
		return fmt.Errorf("%s: auctions %d: must be >= 1", c.label(), c.AuctionCardinality)
	case slices.Contains(windowedQueries, c.Query) && c.WindowMillis < 1:
		return fmt.Errorf("%s: window %d: must be >= 1", c.label(), c.WindowMillis)
	case c.Query == queryQ5 && (c.SlideMillis < 1 || c.SlideMillis > c.WindowMillis):
		return fmt.Errorf("%s: slide %d: must be in [1, window=%d]", c.label(), c.SlideMillis, c.WindowMillis)
	}
	return nil
}

// Fixed properties of the benchmark's Nexmark stream.
//
// EventTimeStep is one so that a window of n milliseconds holds n elements: the
// window flag then reads as an occupancy, which is the quantity that decides
// whether a windowed query is aggregating anything. It is not a flag for the
// same reason: two dials that both scale window occupancy would let a sweep
// state the same configuration two ways.
const (
	benchEventTimeStep = 1
	benchMaxLag        = 500
	// Bids key on their auction, and q7 breaks ties on the bidder. A person
	// space of one would send every tie through to the auction id and leave
	// that rule taking no part in the work.
	benchPersonCardinality = 1000
	benchPriceRange        = 1000
	benchCategoryCount     = 4
	benchAuctionDuration   = 5000
	// Coarse against a multi-million record run: a hundred broadcasts per
	// subtask. This measures the record path, and a tight interval here would
	// move the number without that path having changed.
	benchWatermarkInterval = 10000
)

// benchBaseEventTime is the same instant every run starts at, so that two runs
// with the same seed produce the same event times.
func benchBaseEventTime() int64 {
	return time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
}

// nexmarkConfig is the source configuration for one benchmark run.
func nexmarkConfig(c Config) sources.NexmarkConfig {
	return sources.NexmarkConfig{
		Seed:               c.Seed,
		Count:              c.Records,
		AuctionCardinality: c.AuctionCardinality,
		PersonCardinality:  benchPersonCardinality,
		PriceRange:         benchPriceRange,
		CategoryCount:      benchCategoryCount,
		AuctionDuration:    benchAuctionDuration,
		BaseEventTime:      benchBaseEventTime(),
		EventTimeStep:      benchEventTimeStep,
		MaxLag:             benchMaxLag,
	}
}

// buildGraph returns the job for one configuration.
//
// barrierInterval is how many elements a source subtask puts between barriers,
// and it must be at least one: invariant 3 says a source injects barriers at a
// fixed element interval regardless of data volume, and graph.Validate enforces
// it. A throughput run passes benchThroughputBarrierInterval, which is coarse
// on purpose; the state-size and recovery harnesses pass something tighter,
// because they need checkpoints to exist.
//
// Barriers still flow through a throughput run even though nothing records a
// snapshot. That is what Phase 6a measured and what the committed baseline
// holds, and it is also the truth about the engine: the element stream carries
// barriers whether or not a coordinator is listening.
//
// Every vertex runs at the configuration's parallelism, sources included. A
// source pinned at 1 feeding p operator subtasks would measure the source
// rather than the shuffle, and the curve would go flat for a reason that has
// nothing to do with the engine.
//
// The sink is Discard, never Collect: Collect appends every record to a slice,
// so a throughput run against it measures slice growth and the collector.
func buildGraph(c Config, barrierInterval int64) (*graph.Graph, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	// Refused here rather than left to graph.Validate. The runtime rejects it
	// either way, but the caller that meant "no barriers" needs to be told that
	// is not a setting rather than shown a validation error several frames
	// down.
	if barrierInterval < 1 {
		return nil, fmt.Errorf("%s: barrier interval %d: a source injects barriers at a fixed element "+
			"interval regardless of data volume, so there is no setting for none", c.label(), barrierInterval)
	}

	g := graph.New()
	vertices := []graph.Vertex{sourceVertex(c, barrierInterval)}
	chain := operatorChain(c)
	vertices = append(vertices, chain...)
	vertices = append(vertices, graph.Vertex{
		ID: "sink", Kind: graph.VertexSink, Parallelism: c.Parallelism,
		NewSink: func() core.Sink { return sinks.NewDiscard() },
	})
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			return nil, err
		}
	}
	for i := 1; i < len(vertices); i++ {
		if err := g.Connect(vertices[i-1].ID, vertices[i].ID); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// sourceVertex is the head of the graph: the generator for identity, the
// Nexmark source for everything else.
func sourceVertex(c Config, barrierInterval int64) graph.Vertex {
	v := graph.Vertex{
		ID: "source", Kind: graph.VertexSource, Parallelism: c.Parallelism,
		WatermarkIntervalElements: benchWatermarkInterval,
		BarrierIntervalElements:   barrierInterval,
	}
	if c.Query == queryIdentity {
		cfg := sources.GeneratorConfig{
			Seed:           c.Seed,
			Count:          c.Records,
			KeyCardinality: c.Keys,
			BaseEventTime:  benchBaseEventTime(),
			EventTimeStep:  1,
			MaxLag:         500,
			ValueSize:      16,
			AmountRange:    1000,
		}
		v.NewSource = func() core.Source { return sources.NewGenerator(cfg) }
		v.MaxOutOfOrderness = cfg.MaxLag
		return v
	}
	cfg := nexmarkConfig(c)
	v.NewSource = func() core.Source { return sources.NewNexmark(cfg) }
	v.MaxOutOfOrderness = cfg.MaxLag
	return v
}

// operatorChain is the query's operators, in order. Every workload here is a
// straight line, so the edges are the order itself.
//
// q5 is four vertices and the others are one. That is the query rather than an
// accident of this file: hot items is a two-stage event-time computation, and
// the re-key between the stages is a shuffle whose cost belongs in q5's number.
func operatorChain(c Config) []graph.Vertex {
	vertex := func(id string, newOperator func() core.Operator) graph.Vertex {
		return graph.Vertex{ID: id, Kind: graph.VertexOperator, Parallelism: c.Parallelism, NewOperator: newOperator}
	}
	switch c.Query {
	case queryIdentity:
		return []graph.Vertex{vertex("identity", func() core.Operator {
			return operators.NewMap(func(rec *core.Record) (*core.Record, error) { return rec, nil })
		})}
	case queryQ0:
		return []graph.Vertex{vertex("q0", func() core.Operator { return operators.NewQ0() })}
	case queryQ1:
		return []graph.Vertex{vertex("q1", func() core.Operator { return operators.NewQ1(operators.Q1Factor) })}
	case queryQ2:
		return []graph.Vertex{vertex("q2", func() core.Operator { return operators.NewQ2(operators.Q2Divisor) })}
	case queryQ7:
		return []graph.Vertex{vertex("q7", func() core.Operator {
			return operators.NewQ7(c.WindowMillis, benchLateness)
		})}
	case queryQ5:
		return []graph.Vertex{
			vertex("bids", func() core.Operator { return operators.NewBidsOnly() }),
			vertex("count", func() core.Operator {
				return operators.NewSlidingCount(c.WindowMillis, c.SlideMillis, benchLateness)
			}),
			vertex("rekey", func() core.Operator { return operators.NewQ5Rekey(c.WindowMillis) }),
			vertex("hot", func() core.Operator { return operators.NewQ5HotItems(c.WindowMillis, benchLateness) }),
		}
	}
	// check has already refused an unknown query, and buildGraph calls it
	// first. Reaching here means the two lists have come apart.
	panic("bench: no operator chain for query " + c.Query)
}

// benchLateness is zero: a benchmark measures the window path, and allowed
// lateness only holds fired windows open longer, which inflates state without
// changing the work.
const benchLateness = 0

// benchThroughputBarrierInterval is the barrier spacing of a run that records
// no snapshot.
//
// Deliberately coarse: twenty broadcasts per subtask against a two-million
// record run. A throughput job measures the record path, and a tight interval
// here would move the number without that path having changed. It cannot be
// zero -- invariant 3 -- so "as few as the job can be given" is the honest
// setting rather than "none".
const benchThroughputBarrierInterval = 100000

// timerVertices names the vertices of a query that hold keyed state.
//
// It is what the state-size harness reads a checkpoint by: a source subtask's
// payload is an offset, not a serialised KeyedState, so a sweep that summed
// every file in a checkpoint directory would be summing two different things.
func timerVertices(query string) []string {
	switch query {
	case queryQ5:
		return []string{"count", "hot"}
	case queryQ7:
		return []string{"q7"}
	default:
		return nil
	}
}

// parseLevels reads the comma-separated parallelism sweep.
func parseLevels(s string) ([]int, error) {
	var levels []int
	for _, field := range strings.Split(s, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		p, err := strconv.Atoi(field)
		if err != nil {
			return nil, fmt.Errorf("parallelism %q: %w", field, err)
		}
		if p < 1 {
			return nil, fmt.Errorf("parallelism %d: must be >= 1", p)
		}
		levels = append(levels, p)
	}
	if len(levels) == 0 {
		return nil, fmt.Errorf("no parallelism levels given")
	}
	return levels, nil
}

// parseInt64s reads a comma-separated list of int64s, for the sweep flags that
// take several values.
func parseInt64s(s, what string) ([]int64, error) {
	var out []int64
	for _, field := range strings.Split(s, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		n, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", what, field, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s given", what)
	}
	return out, nil
}

// parseQueries reads the comma-separated query selector, expanding "all".
func parseQueries(s string) ([]string, error) {
	if strings.TrimSpace(s) == "all" {
		return slices.Clone(queries), nil
	}
	var out []string
	for _, field := range strings.Split(s, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if !slices.Contains(queries, field) {
			return nil, fmt.Errorf("query %q: this engine implements %s; Nexmark q3, q4 and q6 have no operator here",
				field, strings.Join(queries, ", "))
		}
		out = append(out, field)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no query given")
	}
	return out, nil
}
