// Command worker runs a job in a single process.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

func main() {
	job := flag.String("job", "identity", `job to run; "identity" is the only one that exists`)
	records := flag.Int64("records", 100000, "number of records to generate")
	seed := flag.Uint64("seed", 1, "generator seed; the same seed always produces the same records")
	keys := flag.Int64("keys", 1000, "number of distinct keys")
	flag.Parse()

	if err := run(*job, *records, *seed, *keys); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run(job string, records int64, seed uint64, keys int64) error {
	if job != "identity" {
		return fmt.Errorf("unknown job %q; only \"identity\" exists", job)
	}

	// Collect rather than Discard so the line printed below reports what
	// actually reached the sink. That holds every record in memory, which is
	// the right trade at demo sizes and the wrong one at benchmark sizes;
	// cmd/bench is where a throughput run belongs.
	collect := sinks.NewCollect()

	g, err := identityGraph(sources.GeneratorConfig{
		Seed:           seed,
		Count:          records,
		KeyCardinality: keys,
		BaseEventTime:  time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		EventTimeStep:  1,
		MaxLag:         500,
		ValueSize:      16,
		AmountRange:    1000,
	}, collect)
	if err != nil {
		return err
	}

	start := time.Now()
	if err := runtime.Run(context.Background(), g); err != nil {
		return err
	}
	elapsed := time.Since(start)

	written := int64(len(collect.Records()))
	fmt.Printf("job=%s records=%d written=%d elapsed=%s\n", job, records, written, elapsed.Round(time.Millisecond))
	if written != records {
		return fmt.Errorf("sink holds %d records, want %d", written, records)
	}
	return nil
}

// identityGraph is source -> identity map -> sink.
func identityGraph(cfg sources.GeneratorConfig, sink core.Sink) (*graph.Graph, error) {
	g := graph.New()
	vertices := []graph.Vertex{
		{
			ID:          "source",
			Kind:        graph.VertexSource,
			Parallelism: 1,
			NewSource:   func() core.Source { return sources.NewGenerator(cfg) },
			// Both counted in elements, never on a clock. The identity job does
			// no event-time work, but a source vertex has to state a
			// granularity for each rather than fall into one.
			WatermarkIntervalElements: 10000,
			MaxOutOfOrderness:         cfg.MaxLag,
			BarrierIntervalElements:   10000,
		},
		{
			ID:          "identity",
			Kind:        graph.VertexOperator,
			Parallelism: 1,
			NewOperator: func() core.Operator {
				return operators.NewMap(func(rec *core.Record) (*core.Record, error) { return rec, nil })
			},
		},
		{
			ID:          "sink",
			Kind:        graph.VertexSink,
			Parallelism: 1,
			NewSink:     func() core.Sink { return sink },
		},
	}
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			return nil, err
		}
	}
	for _, e := range [][2]string{{"source", "identity"}, {"identity", "sink"}} {
		if err := g.Connect(e[0], e[1]); err != nil {
			return nil, err
		}
	}
	return g, nil
}
