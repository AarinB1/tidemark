// Command bench measures throughput at a range of parallelism levels and
// reports the scaling curve.
//
// It runs a fixed number of records rather than for a fixed duration, so two
// runs do the same work and the only thing that varies is how long it takes.
// The timer covers Run alone: graph construction and teardown are excluded,
// because they do not scale with the record count and would flatten the curve
// at small sizes.
//
// Never run this under -race. The race detector costs 5 to 20x and the number
// it produces means nothing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	tmruntime "github.com/AarinB1/tidemark/pkg/runtime"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// Report is what a benchmark run writes and what a baseline holds.
//
// The machine fields sit on the report rather than on each result because they
// describe the run, not the level. Without them a laptop number and a CI number
// get compared silently, and the comparison is meaningless in a way nothing in
// the output would show.
//
// encoding/json is fine here. The scope rule bars reflection-based
// serialization from the data path; this is a benchmark's result file, which
// runs once per invocation and never touches a record.
type Report struct {
	GoVersion  string   `json:"go_version"`
	GOMAXPROCS int      `json:"gomaxprocs"`
	NumCPU     int      `json:"num_cpu"`
	Results    []Result `json:"results"`
}

// Result is one parallelism level.
type Result struct {
	Parallelism   int     `json:"parallelism"`
	Records       int64   `json:"records"`
	Seed          uint64  `json:"seed"`
	Keys          int64   `json:"keys"`
	ElapsedMillis int64   `json:"elapsed_millis"`
	RecordsPerSec float64 `json:"records_per_sec"`
}

func main() {
	records := flag.Int64("records", 2000000, "records to push through the pipeline at each level")
	parallelism := flag.String("parallelism", "1,2,4,8", "comma-separated parallelism levels to measure")
	seed := flag.Uint64("seed", 1, "generator seed; the same seed always produces the same records")
	keys := flag.Int64("keys", 10000, "number of distinct keys")
	jsonPath := flag.String("json", "", "write the report to this file as JSON; empty writes nothing")
	baseline := flag.String("baseline", "", "compare against this baseline report and fail on a regression")
	threshold := flag.Float64("threshold", 15, "percent slower than the baseline that counts as a regression")
	flag.Parse()

	if err := run(*records, *parallelism, *seed, *keys, *jsonPath, *baseline, *threshold); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(records int64, parallelism string, seed uint64, keys int64, jsonPath, baseline string, threshold float64) error {
	levels, err := parseLevels(parallelism)
	if err != nil {
		return err
	}

	report := Report{
		GoVersion:  runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		NumCPU:     runtime.NumCPU(),
	}

	for _, p := range levels {
		res, err := measure(records, p, seed, keys)
		if err != nil {
			return err
		}
		report.Results = append(report.Results, res)
		fmt.Printf("parallelism=%d records=%d elapsed=%dms rate=%.0f rec/s\n",
			res.Parallelism, res.Records, res.ElapsedMillis, res.RecordsPerSec)
	}

	printScaling(report)

	if jsonPath != "" {
		if err := writeReport(jsonPath, report); err != nil {
			return err
		}
	}
	if baseline != "" {
		return compare(baseline, report, threshold)
	}
	return nil
}

// measure times one parallelism level.
func measure(records int64, p int, seed uint64, keys int64) (Result, error) {
	g, err := benchGraph(records, p, seed, keys)
	if err != nil {
		return Result{}, err
	}

	// The timer starts after construction and stops when Run returns, so what
	// is measured is the pipeline and nothing around it.
	start := time.Now()
	if err := tmruntime.Run(context.Background(), g); err != nil {
		return Result{}, fmt.Errorf("parallelism %d: %w", p, err)
	}
	elapsed := time.Since(start)

	return Result{
		Parallelism:   p,
		Records:       records,
		Seed:          seed,
		Keys:          keys,
		ElapsedMillis: elapsed.Milliseconds(),
		RecordsPerSec: float64(records) / elapsed.Seconds(),
	}, nil
}

// benchGraph is source -> identity map -> discard sink, every vertex at p.
//
// Every vertex scales together. A source pinned at 1 feeding p map subtasks
// would measure the source rather than the shuffle, and the curve would go
// flat for a reason that has nothing to do with the engine.
//
// The sink is Discard, never Collect. Collect appends every record to a slice,
// so a throughput run against it measures slice growth and the garbage
// collector.
func benchGraph(records int64, p int, seed uint64, keys int64) (*graph.Graph, error) {
	cfg := sources.GeneratorConfig{
		Seed:           seed,
		Count:          records,
		KeyCardinality: keys,
		BaseEventTime:  time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		EventTimeStep:  1,
		MaxLag:         500,
		ValueSize:      16,
		AmountRange:    1000,
	}

	g := graph.New()
	vertices := []graph.Vertex{
		{ID: "source", Kind: graph.VertexSource, Parallelism: p,
			NewSource: func() core.Source { return sources.NewGenerator(cfg) }},
		{ID: "identity", Kind: graph.VertexOperator, Parallelism: p,
			NewOperator: func() core.Operator {
				return operators.NewMap(func(rec *core.Record) (*core.Record, error) { return rec, nil })
			}},
		{ID: "sink", Kind: graph.VertexSink, Parallelism: p,
			NewSink: func() core.Sink { return sinks.NewDiscard() }},
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

// printScaling reports each level against parallelism 1, which is the number
// the exit criterion is stated in.
func printScaling(r Report) {
	base, ok := resultAt(r, 1)
	if !ok {
		return
	}
	fmt.Printf("scaling (gomaxprocs=%d cpus=%d %s):\n", r.GOMAXPROCS, r.NumCPU, r.GoVersion)
	for _, res := range r.Results {
		fmt.Printf("  parallelism %d: %.0f rec/s, %.2fx\n",
			res.Parallelism, res.RecordsPerSec, res.RecordsPerSec/base.RecordsPerSec)
	}
}

func resultAt(r Report, p int) (Result, bool) {
	for _, res := range r.Results {
		if res.Parallelism == p {
			return res, true
		}
	}
	return Result{}, false
}

// compare fails when any level is more than threshold percent slower than the
// baseline. Levels the baseline does not cover are skipped rather than treated
// as a regression, so adding a level does not fail the check that the baseline
// predates.
func compare(path string, got Report, threshold float64) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	var want Report
	if err := json.Unmarshal(data, &want); err != nil {
		return fmt.Errorf("baseline %s: %w", path, err)
	}

	if want.GOMAXPROCS != got.GOMAXPROCS || want.NumCPU != got.NumCPU {
		fmt.Printf("note: baseline ran on %d cpus at gomaxprocs %d, this run on %d at %d\n",
			want.NumCPU, want.GOMAXPROCS, got.NumCPU, got.GOMAXPROCS)
	}

	var regressions []string
	for _, res := range got.Results {
		base, ok := resultAt(want, res.Parallelism)
		if !ok {
			continue
		}
		change := (res.RecordsPerSec - base.RecordsPerSec) / base.RecordsPerSec * 100
		fmt.Printf("parallelism %d: %.0f rec/s against baseline %.0f (%+.1f%%)\n",
			res.Parallelism, res.RecordsPerSec, base.RecordsPerSec, change)
		if change < -threshold {
			regressions = append(regressions, fmt.Sprintf("parallelism %d is %.1f%% slower", res.Parallelism, -change))
		}
	}
	if len(regressions) > 0 {
		return fmt.Errorf("regression worse than %.0f%%: %s", threshold, strings.Join(regressions, "; "))
	}
	return nil
}

func writeReport(path string, r Report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

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
