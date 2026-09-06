package main

import (
	"runtime"
	"slices"
	"strings"
	"testing"
)

// benchConfig is a valid configuration for query, small enough to run inside a
// test.
func benchConfig(query string, p int) Config {
	return Config{
		Query:              query,
		Parallelism:        p,
		Records:            20000,
		Seed:               1,
		Keys:               1000,
		AuctionCardinality: 200,
		WindowMillis:       2000,
		SlideMillis:        500,
	}
}

func TestConfigCheck(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(Config) Config
		wantErr string
	}{
		{
			name:   "valid",
			mutate: func(c Config) Config { return c },
		},
		{
			// The queries this engine does not implement have to be refused by
			// name. A selector that took q4 and ran something else would put a
			// number in the record under a query that did not produce it.
			name:    "an unimplemented nexmark query",
			mutate:  func(c Config) Config { c.Query = "q4"; return c },
			wantErr: "q3, q4 and q6 have no operator here",
		},
		{
			name:    "an unknown workload",
			mutate:  func(c Config) Config { c.Query = "tpch"; return c },
			wantErr: "q3, q4 and q6 have no operator here",
		},
		{
			name:    "parallelism below one",
			mutate:  func(c Config) Config { c.Parallelism = 0; return c },
			wantErr: "parallelism 0",
		},
		{
			name:    "no records",
			mutate:  func(c Config) Config { c.Records = 0; return c },
			wantErr: "records 0",
		},
		{
			name:    "no auctions for a nexmark query",
			mutate:  func(c Config) Config { c.Query = queryQ7; c.AuctionCardinality = 0; return c },
			wantErr: "auctions 0",
		},
		{
			// The identity workload keys on GeneratorConfig.KeyCardinality and
			// never reads the auction space, so a zero there is fine.
			name:   "no auctions for the identity workload",
			mutate: func(c Config) Config { c.AuctionCardinality = 0; return c },
		},
		{
			name:    "no window for a windowed query",
			mutate:  func(c Config) Config { c.Query = queryQ7; c.WindowMillis = 0; return c },
			wantErr: "window 0",
		},
		{
			// q0, q1 and q2 hold no state, so a window means nothing to them
			// and a zero must not stop them being measured.
			name:   "no window for a stateless query",
			mutate: func(c Config) Config { c.Query = queryQ1; c.WindowMillis = 0; return c },
		},
		{
			name:    "a slide wider than its window",
			mutate:  func(c Config) Config { c.Query = queryQ5; c.SlideMillis = c.WindowMillis + 1; return c },
			wantErr: "slide 2001",
		},
		{
			name:   "a slide equal to its window is tumbling and allowed",
			mutate: func(c Config) Config { c.Query = queryQ5; c.SlideMillis = c.WindowMillis; return c },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.mutate(benchConfig(queryIdentity, 1)).check()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("check = %v, want nil", err)
			case tc.wantErr == "":
				return
			case err == nil:
				t.Fatalf("check = nil, want an error mentioning %q", tc.wantErr)
			case !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("check = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseQueries(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{name: "all", in: "all", want: queries},
		{name: "one", in: "q7", want: []string{queryQ7}},
		{name: "several with spaces", in: "q0, q5 ,q7", want: []string{queryQ0, queryQ5, queryQ7}},
		{name: "unimplemented", in: "q6", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseQueries(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseQueries(%q) error = %v, want error %v", tc.in, err, tc.wantErr)
			}
			if err == nil && !slices.Equal(got, tc.want) {
				t.Errorf("parseQueries(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseLevels(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []int
		wantErr bool
	}{
		{name: "the default sweep", in: "1,2,4,8,16", want: []int{1, 2, 4, 8, 16}},
		{name: "one level", in: "4", want: []int{4}},
		{name: "spaces and a trailing comma", in: "1, 2,", want: []int{1, 2}},
		{name: "zero", in: "0", wantErr: true},
		{name: "negative", in: "-1", wantErr: true},
		{name: "not a number", in: "four", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLevels(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseLevels(%q) error = %v, want error %v", tc.in, err, tc.wantErr)
			}
			if err == nil && !slices.Equal(got, tc.want) {
				t.Errorf("parseLevels(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseInt64s(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []int64
		wantErr bool
	}{
		{name: "two orders of magnitude", in: "100,1000,10000,100000", want: []int64{100, 1000, 10000, 100000}},
		{name: "one", in: "100", want: []int64{100}},
		{name: "not a number", in: "many", wantErr: true},
		{name: "empty", in: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInt64s(tc.in, "auctions")
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseInt64s(%q) error = %v, want error %v", tc.in, err, tc.wantErr)
			}
			if err == nil && !slices.Equal(got, tc.want) {
				t.Errorf("parseInt64s(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestBuildGraphShape pins the two properties of the job that decide whether a
// throughput number means anything.
//
// Every vertex runs at the configuration's parallelism, sources included: a
// source pinned at 1 feeding p operator subtasks measures the source, and the
// curve goes flat for a reason that has nothing to do with the engine. And the
// barrier interval is whatever the caller asked for, because a throughput run
// asks for the coarsest one it can and the state-size harness asks for a tight
// one.
func TestBuildGraphShape(t *testing.T) {
	tests := []struct {
		query    string
		vertices []string
	}{
		{query: queryIdentity, vertices: []string{"source", "identity", "sink"}},
		{query: queryQ0, vertices: []string{"source", "q0", "sink"}},
		{query: queryQ1, vertices: []string{"source", "q1", "sink"}},
		{query: queryQ2, vertices: []string{"source", "q2", "sink"}},
		{query: queryQ5, vertices: []string{"source", "bids", "count", "rekey", "hot", "sink"}},
		{query: queryQ7, vertices: []string{"source", "q7", "sink"}},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			const parallelism = 3
			const barrierInterval = 500
			g, err := buildGraph(benchConfig(tc.query, parallelism), barrierInterval)
			if err != nil {
				t.Fatalf("buildGraph: %v", err)
			}
			order, err := g.TopoOrder()
			if err != nil {
				t.Fatalf("TopoOrder: %v", err)
			}

			var ids []string
			for _, v := range order {
				ids = append(ids, v.ID)
				if v.Parallelism != parallelism {
					t.Errorf("vertex %s runs at parallelism %d, want %d: a vertex that does not scale "+
						"with the sweep flattens the curve for a reason that is not the engine",
						v.ID, v.Parallelism, parallelism)
				}
				if v.ID == "source" && v.BarrierIntervalElements != barrierInterval {
					t.Errorf("source injects barriers every %d elements, want %d",
						v.BarrierIntervalElements, barrierInterval)
				}
			}
			if !slices.Equal(ids, tc.vertices) {
				t.Errorf("vertices = %v, want %v", ids, tc.vertices)
			}
		})
	}
}

// TestBuildGraphRefusesASourceWithNoBarriers pins invariant 3 at the harness
// boundary.
//
// A throughput run records no snapshot, which makes "no barriers at all" look
// like the setting to want. It is not available: a source injects barriers at a
// fixed element interval regardless of data volume, and a benchmark is not
// where that stops being true. The harness therefore asks for the coarsest
// interval it can rather than for none, and a caller that asks for none is
// refused here rather than producing a graph the runtime will not accept.
func TestBuildGraphRefusesASourceWithNoBarriers(t *testing.T) {
	if _, err := buildGraph(benchConfig(queryQ7, 1), 0); err == nil {
		t.Fatal("buildGraph accepted a source with no barrier interval")
	}
}

// TestThroughputRunUsesTheCoarsestBarrierInterval states what the measured job
// actually carries, since it is not "nothing".
func TestThroughputRunUsesTheCoarsestBarrierInterval(t *testing.T) {
	g, err := buildGraph(benchConfig(queryQ7, 1), benchThroughputBarrierInterval)
	if err != nil {
		t.Fatalf("buildGraph: %v", err)
	}
	order, err := g.TopoOrder()
	if err != nil {
		t.Fatalf("TopoOrder: %v", err)
	}
	for _, v := range order {
		if v.ID == "source" && v.BarrierIntervalElements != benchThroughputBarrierInterval {
			t.Errorf("source injects barriers every %d elements, want %d",
				v.BarrierIntervalElements, benchThroughputBarrierInterval)
		}
	}
}

// TestEveryQueryRuns is the check that the selector is not a list of names.
//
// Each query is measured at parallelism 1 and 2 over a small record count. A
// query whose graph does not validate, whose operator chain is mis-wired, or
// whose window specification the operator refuses fails here rather than ten
// minutes into a sweep on a rented machine.
//
// Parallelism 2 is in the table rather than 1 alone because it is where the
// shuffle and the input gate exist at all: a chain at parallelism 1 has one
// channel per edge and cannot exercise partitioning.
func TestEveryQueryRuns(t *testing.T) {
	for _, query := range queries {
		for _, p := range []int{1, 2} {
			t.Run(query, func(t *testing.T) {
				res, err := measure(benchConfig(query, p))
				if err != nil {
					t.Fatalf("measure: %v", err)
				}
				if res.RecordsPerSec <= 0 {
					t.Errorf("%s: rate is %f", res.label(), res.RecordsPerSec)
				}
				if res.Query != query || res.Parallelism != p {
					t.Errorf("result is %s, want %s at parallelism %d", res.label(), query, p)
				}
			})
		}
	}
}

// TestOversubscribedIsFlagged covers both sides, because a flag that is always
// set and a flag that is never set both leave the JSON saying nothing.
func TestOversubscribedIsFlagged(t *testing.T) {
	cores := runtime.NumCPU()
	tests := []struct {
		name        string
		parallelism int
		want        bool
	}{
		{name: "one", parallelism: 1, want: cores < 1},
		{name: "exactly the core count", parallelism: cores, want: false},
		{name: "one past the core count", parallelism: cores + 1, want: true},
		{name: "far past the core count", parallelism: cores * 4, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := benchConfig(queryQ7, tc.parallelism)
			if got := cfg.oversubscribed(); got != tc.want {
				t.Errorf("parallelism %d on %d cores: oversubscribed = %v, want %v",
					tc.parallelism, cores, got, tc.want)
			}
		})
	}
}

// TestLabelNamesTheDialsThatMatter guards the one-line description a result is
// read by. A label that omitted the auction cardinality would put two different
// state-size configurations in the record under the same name.
func TestLabelNamesTheDialsThatMatter(t *testing.T) {
	tests := []struct {
		query    string
		contains []string
		omits    []string
	}{
		{
			query:    queryIdentity,
			contains: []string{"identity", "p=1", "records=20000", "keys=1000"},
			omits:    []string{"auctions", "window", "slide"},
		},
		{
			query:    queryQ1,
			contains: []string{"q1", "auctions=200"},
			omits:    []string{"keys", "window", "slide"},
		},
		{
			query:    queryQ7,
			contains: []string{"q7", "auctions=200", "window=2000"},
			omits:    []string{"keys", "slide"},
		},
		{
			query:    queryQ5,
			contains: []string{"q5", "auctions=200", "window=2000", "slide=500"},
			omits:    []string{"keys"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			label := benchConfig(tc.query, 1).label()
			for _, want := range tc.contains {
				if !strings.Contains(label, want) {
					t.Errorf("label %q does not mention %q", label, want)
				}
			}
			for _, unwanted := range tc.omits {
				if strings.Contains(label, unwanted) {
					t.Errorf("label %q mentions %q, which that query does not use", label, unwanted)
				}
			}
		})
	}
}

// TestSweepExpandsQueryOuterParallelismInner pins the order the sweep runs in:
// a run interrupted part way leaves whole scaling curves rather than a fragment
// of each.
func TestSweepExpandsQueryOuterParallelismInner(t *testing.T) {
	opts := sweepOptions{
		records: 1000, parallelism: "1,2", query: "q7,q5",
		seed: 1, keys: 10, auctions: "100", window: 2000, slide: 500,
	}
	configs, err := sweep(opts)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	var got []string
	for _, cfg := range configs {
		got = append(got, cfg.label())
	}
	want := []string{
		"q7 p=1 records=1000 auctions=100 window=2000",
		"q7 p=2 records=1000 auctions=100 window=2000",
		"q5 p=1 records=1000 auctions=100 window=2000 slide=500",
		"q5 p=2 records=1000 auctions=100 window=2000 slide=500",
	}
	if !slices.Equal(got, want) {
		t.Errorf("sweep = %v, want %v", got, want)
	}
}

// TestSweepRefusesABadConfigurationBeforeRunningAnything is why check is called
// during expansion: a sweep whose last row is invalid must not spend ten
// minutes on the rows that were fine before saying so.
func TestSweepRefusesABadConfigurationBeforeRunningAnything(t *testing.T) {
	opts := sweepOptions{
		records: 1000, parallelism: "1,2", query: "q5",
		seed: 1, keys: 10, auctions: "100", window: 2000, slide: 9999,
	}
	if _, err := sweep(opts); err == nil {
		t.Fatal("sweep accepted a slide wider than its window")
	}
}
