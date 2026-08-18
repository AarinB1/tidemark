package runtime

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/operators"
	"github.com/AarinB1/tidemark/pkg/sinks"
	"github.com/AarinB1/tidemark/pkg/sources"
)

// TestEndToEndMatchesTheSourceRead runs source -> operator -> sink and compares
// the sink's contents against the generator read directly.
//
// Both sides are sorted first. Identical output means the final contents of the
// sink and never the emission order of the stream: delivery is at-least-once
// and order after a recovery will differ from a clean run, so a test that
// compared emission order would start failing in Phase 3 for a reason that is
// not a bug.
func TestEndToEndMatchesTheSourceRead(t *testing.T) {
	tests := []struct {
		name string
		cfg  sources.GeneratorConfig
		// keep decides which records the operator forwards. nil is identity.
		keep func(*core.Record) bool
	}{
		{
			name: "identity, one record",
			cfg:  e2eConfig(1, 1),
		},
		{
			name: "identity",
			cfg:  e2eConfig(2, 10000),
		},
		{
			name: "identity, single key",
			cfg:  singleKey(e2eConfig(3, 5000)),
		},
		{
			name: "filtered",
			cfg:  e2eConfig(4, 10000),
			keep: func(r *core.Record) bool { return r.EventTime%3 == 0 },
		},
		{
			name: "filter drops everything",
			cfg:  e2eConfig(5, 1000),
			keep: func(*core.Record) bool { return false },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collect := sinks.NewCollect()
			g := e2eGraph(t, tt.cfg, tt.keep, collect)

			if err := Run(context.Background(), g); err != nil {
				t.Fatalf("Run: %v", err)
			}

			got := collect.Records()
			want := readSource(t, tt.cfg, tt.keep)
			sortRecords(got)
			sortRecords(want)

			if len(got) != len(want) {
				t.Fatalf("sink holds %d records, want %d", len(got), len(want))
			}
			for i := range want {
				if compareRecords(got[i], want[i]) != 0 {
					t.Fatalf("record %d: got %+v, want %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// TestEndToEndIsReproducible runs the same job twice and compares the two
// sinks. A seed that reached the records through anything but (seed, n) would
// show up here.
func TestEndToEndIsReproducible(t *testing.T) {
	cfg := e2eConfig(11, 5000)

	first, second := sinks.NewCollect(), sinks.NewCollect()
	for _, sink := range []*sinks.Collect{first, second} {
		if err := Run(context.Background(), e2eGraph(t, cfg, nil, sink)); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}

	a, b := first.Records(), second.Records()
	sortRecords(a)
	sortRecords(b)
	if len(a) != len(b) {
		t.Fatalf("runs produced %d and %d records", len(a), len(b))
	}
	for i := range a {
		if compareRecords(a[i], b[i]) != 0 {
			t.Fatalf("record %d differs between runs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func e2eConfig(seed uint64, count int64) sources.GeneratorConfig {
	return sources.GeneratorConfig{
		Seed:           seed,
		Count:          count,
		KeyCardinality: 32,
		BaseEventTime:  1704067200000,
		EventTimeStep:  5,
		MaxLag:         100,
		ValueSize:      16,
	}
}

func singleKey(cfg sources.GeneratorConfig) sources.GeneratorConfig {
	cfg.KeyCardinality = 1
	return cfg
}

// e2eGraph builds source -> operator -> sink. keep nil means an identity map.
func e2eGraph(t *testing.T, cfg sources.GeneratorConfig, keep func(*core.Record) bool, sink core.Sink) *graph.Graph {
	t.Helper()

	newOperator := func() core.Operator {
		if keep == nil {
			return operators.NewMap(func(r *core.Record) (*core.Record, error) { return r, nil })
		}
		return operators.NewFilter(keep)
	}

	g := graph.New()
	vertices := []graph.Vertex{
		{ID: "source", Kind: graph.VertexSource, Parallelism: 1,
			NewSource: func() core.Source { return sources.NewGenerator(cfg) }},
		{ID: "operator", Kind: graph.VertexOperator, Parallelism: 1, NewOperator: newOperator},
		{ID: "sink", Kind: graph.VertexSink, Parallelism: 1,
			NewSink: func() core.Sink { return sink }},
	}
	for _, v := range vertices {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex(%s): %v", v.ID, err)
		}
	}
	for _, e := range [][2]string{{"source", "operator"}, {"operator", "sink"}} {
		if err := g.Connect(e[0], e[1]); err != nil {
			t.Fatalf("Connect(%s, %s): %v", e[0], e[1], err)
		}
	}
	return g
}

// readSource is the oracle: the generator drained directly, with the same
// predicate applied, and nothing from the runtime involved.
func readSource(t *testing.T, cfg sources.GeneratorConfig, keep func(*core.Record) bool) []*core.Record {
	t.Helper()
	src := sources.NewGenerator(cfg)
	if err := src.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := src.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	var out []*core.Record
	for {
		rec, ok, err := src.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		if keep == nil || keep(rec) {
			out = append(out, rec)
		}
	}
}

func sortRecords(recs []*core.Record) {
	slices.SortFunc(recs, compareRecords)
}

func compareRecords(a, b *core.Record) int {
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c
	}
	if c := cmp.Compare(a.EventTime, b.EventTime); c != 0 {
		return c
	}
	return bytes.Compare(a.Value, b.Value)
}
