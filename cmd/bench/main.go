// Command bench measures throughput across a sweep of configurations and
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
//
// One invocation produces the whole sweep, and that is deliberate: the numbers
// that get published come from one machine in one sitting, and a sweep
// assembled from several invocations is a sweep whose rows were measured under
// conditions nobody wrote down. See docs/BENCHMARKS.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AarinB1/tidemark/pkg/graph"
	tmruntime "github.com/AarinB1/tidemark/pkg/runtime"
)

// Report is what a benchmark run writes and what a baseline holds.
//
// The machine sits on the report rather than on each result because it
// describes the run, not the configuration. Without it a laptop number and a CI
// number get compared silently, and the comparison is meaningless in a way
// nothing in the output would show. See Fingerprint, and compareBaseline for
// what a mismatch does.
//
// encoding/json is fine here. The scope rule bars reflection-based
// serialization from the data path; this is a benchmark's result file, which
// runs once per invocation and never touches a record.
type Report struct {
	Fingerprint Fingerprint `json:"fingerprint"`
	// Note is free text a COMMITTED baseline carries and a fresh run does not.
	// It is where a baseline says what machine it came from and how much its
	// numbers are worth, since JSON has nowhere else to put a caveat and a
	// caveat that lives only in a document travels separately from the file it
	// is about. Nothing in the comparison reads it.
	Note    string   `json:"note,omitempty"`
	Results []Result `json:"results"`
}

// Result is one measured configuration.
type Result struct {
	Config
	ElapsedMillis int64   `json:"elapsed_millis"`
	RecordsPerSec float64 `json:"records_per_sec"`
	// Oversubscribed marks a configuration whose parallelism exceeded the
	// machine's core count. It is carried into the JSON so that a 16-way number
	// from a 4-core box cannot enter the record looking like a measurement of
	// 16-way scaling.
	Oversubscribed bool `json:"oversubscribed"`
}

// baselineFor finds the baseline result that measured the same configuration.
//
// The whole Config has to agree, not just the parallelism. A q7 run at
// parallelism 4 over ten thousand auctions and one over a hundred are different
// jobs, and a comparison that matched on parallelism alone would call one a
// regression of the other.
//
// A configuration the baseline does not cover is SKIPPED by the caller rather
// than treated as a regression, so adding a row does not fail the check against
// a baseline that predates it.
func baselineFor(want Report, got Result) (Result, bool) {
	for _, res := range want.Results {
		if res.Config == got.Config {
			return res, true
		}
	}
	return Result{}, false
}

func main() {
	records := flag.Int64("records", 2000000, "records to push through the pipeline in each configuration")
	parallelism := flag.String("parallelism", "1,2,4,8,16", "comma-separated parallelism levels to measure")
	query := flag.String("query", queryIdentity,
		"comma-separated workloads to measure, or \"all\": identity, q0, q1, q2, q5, q7")
	seed := flag.Uint64("seed", 1, "generator seed; the same seed always produces the same records")
	keys := flag.Int64("keys", 10000, "number of distinct keys, for the identity workload")
	auctions := flag.String("auctions", "10000",
		"comma-separated auction id spaces to measure, for the Nexmark workloads; the state-size dial")
	window := flag.Int64("window", 5000, "window size in millis; the source steps event time 1ms per element")
	slide := flag.Int64("slide", 1250, "window slide in millis, for q5")
	jsonPath := flag.String("json", "", "write the report to this file as JSON; empty writes nothing")
	baseline := flag.String("baseline", "", "compare against this baseline report and fail on a regression")
	threshold := flag.Float64("threshold", 15, "percent slower than the baseline that counts as a regression")
	mode := flag.String("mode", modeThroughput,
		"what to measure: throughput, state, recovery, or all")
	checkpoints := flag.Int("checkpoints", 8, "checkpoints each source subtask takes, in the state and recovery modes")
	stateDir := flag.String("state-dir", "",
		"where the state and recovery modes write checkpoints; empty is the system temp directory")
	flag.Parse()

	opts := sweepOptions{
		mode:        *mode,
		records:     *records,
		parallelism: *parallelism,
		query:       *query,
		seed:        *seed,
		keys:        *keys,
		auctions:    *auctions,
		window:      *window,
		slide:       *slide,
		checkpoints: *checkpoints,
		stateDir:    *stateDir,
		jsonPath:    *jsonPath,
		baseline:    *baseline,
		threshold:   *threshold,
	}
	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

// sweepOptions is the flag set, gathered so that run has one argument rather
// than eleven positional ones that can be transposed silently.
type sweepOptions struct {
	mode        string
	records     int64
	parallelism string
	query       string
	seed        uint64
	keys        int64
	auctions    string
	window      int64
	slide       int64
	checkpoints int
	stateDir    string
	jsonPath    string
	baseline    string
	threshold   float64
}

// The measurement modes.
//
// Separate modes rather than one run producing everything, because they want
// different jobs: throughput takes no snapshot and wants the coarsest barriers
// the engine allows, while the state and recovery harnesses exist to write
// checkpoints. A single run doing both would report a throughput number for a
// job that spent its time serialising state.
const (
	modeThroughput = "throughput"
	modeState      = "state"
	modeRecovery   = "recovery"
	modeAll        = "all"
)

func run(opts sweepOptions) error {
	switch opts.mode {
	case modeThroughput:
		return runThroughput(opts)
	case modeState:
		return runState(opts)
	case modeAll:
		if err := runThroughput(opts); err != nil {
			return err
		}
		return runState(opts)
	}
	return fmt.Errorf("mode %q: want %s, %s, %s or %s",
		opts.mode, modeThroughput, modeState, modeRecovery, modeAll)
}

// runState measures how much state each configuration holds and what its
// checkpoints weigh.
//
// The JSON goes beside the throughput report rather than into it: the two hold
// different rows measured on different jobs, and one document with both would
// invite a reader to compare a rate against a byte count as if they came from
// the same run.
func runState(opts sweepOptions) error {
	report, err := runStateSweep(opts)
	if err != nil {
		return err
	}
	printStateTable(report)
	if opts.jsonPath != "" {
		return writeJSON(stateJSONPath(opts.jsonPath), report)
	}
	return nil
}

// stateJSONPath puts the state report beside the throughput one, so that
// `-mode all -json bench.json` writes bench.json and bench.state.json rather
// than one overwriting the other.
func stateJSONPath(path string) string {
	ext := filepath.Ext(path)
	return strings.TrimSuffix(path, ext) + ".state" + ext
}

func runThroughput(opts sweepOptions) error {
	configs, err := sweep(opts)
	if err != nil {
		return err
	}

	// The working directory, because that is the filesystem checkpoints land
	// on and the one whose rotational flag belongs in the fingerprint.
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("working directory: %w", err)
	}
	report := Report{Fingerprint: machineFingerprint(dir)}

	for _, cfg := range configs {
		res, err := measure(cfg)
		if err != nil {
			return err
		}
		report.Results = append(report.Results, res)
		fmt.Printf("%s elapsed=%dms rate=%.0f rec/s%s\n",
			res.label(), res.ElapsedMillis, res.RecordsPerSec, oversubscribedNote(res))
	}

	printScaling(report)

	if opts.jsonPath != "" {
		if err := writeJSON(opts.jsonPath, report); err != nil {
			return err
		}
	}
	if opts.baseline != "" {
		return compareBaseline(opts.baseline, report, opts.threshold)
	}
	return nil
}

// sweep expands the flags into the configurations to measure: query outer,
// then auction cardinality, then parallelism.
//
// Query outer so that a sweep reads down a query's scaling curve rather than
// across unrelated jobs, and so a run interrupted part way has whole curves
// rather than a fragment of each. Parallelism innermost for the same reason one
// level down: it is the axis a row is read against.
func sweep(opts sweepOptions) ([]Config, error) {
	names, err := parseQueries(opts.query)
	if err != nil {
		return nil, err
	}
	levels, err := parseLevels(opts.parallelism)
	if err != nil {
		return nil, err
	}
	auctions, err := parseInt64s(opts.auctions, "auctions")
	if err != nil {
		return nil, err
	}

	var configs []Config
	for _, name := range names {
		for _, a := range auctions {
			for _, p := range levels {
				cfg := Config{
					Query:              name,
					Parallelism:        p,
					Records:            opts.records,
					Seed:               opts.seed,
					Keys:               opts.keys,
					AuctionCardinality: a,
					WindowMillis:       opts.window,
					SlideMillis:        opts.slide,
				}
				// Refused here rather than at the point of running, so a sweep
				// with a bad flag fails before it spends ten minutes on the
				// rows that were fine.
				if err := cfg.check(); err != nil {
					return nil, err
				}
				configs = append(configs, cfg)
			}
		}
	}
	return configs, nil
}

// measure times one configuration.
func measure(cfg Config) (Result, error) {
	// Coarse barriers, and no coordinator listening: a throughput run records
	// no snapshot, and invariant 3 does not let a source have no barriers at
	// all. See benchThroughputBarrierInterval.
	g, err := buildGraph(cfg, benchThroughputBarrierInterval)
	if err != nil {
		return Result{}, err
	}
	elapsed, err := timeRun(g)
	if err != nil {
		return Result{}, fmt.Errorf("%s: %w", cfg.label(), err)
	}
	return Result{
		Config:         cfg,
		ElapsedMillis:  elapsed.Milliseconds(),
		RecordsPerSec:  float64(cfg.Records) / elapsed.Seconds(),
		Oversubscribed: cfg.oversubscribed(),
	}, nil
}

// timeRun is the measured interval, and it is one function so that every
// harness in this command measures the same thing.
//
// The clock starts after the graph is built and stops when Run returns. Graph
// construction allocates the vertices and the channels and does not scale with
// the record count; teardown is inside Run, because Run does not return until
// every subtask has unwound and every gate forwarder has finished.
func timeRun(g *graph.Graph) (time.Duration, error) {
	start := time.Now()
	err := tmruntime.Run(context.Background(), g)
	return time.Since(start), err
}

func oversubscribedNote(res Result) string {
	if !res.Oversubscribed {
		return ""
	}
	return " OVERSUBSCRIBED"
}

// printScaling reports each level against parallelism 1 of the same query,
// which is the number the exit criterion is stated in.
//
// Per query, because the ratio is only meaningful within one workload: q5 at
// parallelism 4 against identity at parallelism 1 is two jobs and a division.
func printScaling(r Report) {
	fmt.Printf("scaling (%s):\n", r.Fingerprint)
	for _, name := range queries {
		base, ok := baseOf(r, name)
		if !ok {
			continue
		}
		for _, res := range r.Results {
			if res.Query != name {
				continue
			}
			fmt.Printf("  %s: %.0f rec/s, %.2fx%s\n",
				res.label(), res.RecordsPerSec, res.RecordsPerSec/base.RecordsPerSec, oversubscribedNote(res))
		}
	}
}

// baseOf returns the parallelism-1 result of one query, which every other level
// of that query is reported against.
func baseOf(r Report, query string) (Result, bool) {
	for _, res := range r.Results {
		if res.Query == query && res.Parallelism == 1 {
			return res, true
		}
	}
	return Result{}, false
}

// writeJSON writes any of the reports this command produces.
func writeJSON(path string, r any) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
