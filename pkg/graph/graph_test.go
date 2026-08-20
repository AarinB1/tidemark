package graph

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
)

// Minimal implementations so the factory fields can be non-nil. The graph never
// calls them; it only checks which fields are set.
type nopSource struct{}

func (nopSource) Open(core.Context) error           { return nil }
func (nopSource) Next() (*core.Record, bool, error) { return nil, false, nil }
func (nopSource) SeekTo(int64) error                { return nil }
func (nopSource) Position() int64                   { return 0 }
func (nopSource) Close() error                      { return nil }

type nopOperator struct{}

func (nopOperator) Open(core.Context) error                         { return nil }
func (nopOperator) ProcessElement(*core.Record, core.Context) error { return nil }
func (nopOperator) ProcessWatermark(int64, core.Context) error      { return nil }
func (nopOperator) OnEndOfStream(core.Context) error                { return nil }
func (nopOperator) Snapshot(io.Writer) error                        { return nil }
func (nopOperator) Restore(io.Reader) error                         { return nil }
func (nopOperator) Close() error                                    { return nil }

type nopSink struct{}

func (nopSink) Open(core.Context) error  { return nil }
func (nopSink) Write(*core.Record) error { return nil }
func (nopSink) Close() error             { return nil }

func source(id string) Vertex {
	return Vertex{ID: id, Kind: VertexSource, Parallelism: 1, NewSource: func() core.Source { return nopSource{} }}
}

func operator(id string) Vertex {
	return Vertex{ID: id, Kind: VertexOperator, Parallelism: 1, NewOperator: func() core.Operator { return nopOperator{} }}
}

func sink(id string) Vertex {
	return Vertex{ID: id, Kind: VertexSink, Parallelism: 1, NewSink: func() core.Sink { return nopSink{} }}
}

type edge struct{ from, to string }

// build adds every vertex, then every edge, then validates, returning the first
// error from any stage. Rejections are asserted the same way regardless of
// which call detects them.
func build(vs []Vertex, es []edge) (*Graph, error) {
	g := New()
	for _, v := range vs {
		if err := g.AddVertex(v); err != nil {
			return g, err
		}
	}
	for _, e := range es {
		if err := g.Connect(e.from, e.to); err != nil {
			return g, err
		}
	}
	return g, g.Validate()
}

// chain is source "s" -> operator "m" -> sink "k".
func chain() ([]Vertex, []edge) {
	return []Vertex{source("s"), operator("m"), sink("k")},
		[]edge{{"s", "m"}, {"m", "k"}}
}

// diamond fans out of "s" into two operators and back into sink "k". The
// operators are added in reverse lexicographic order so that a topological
// order which followed insertion order rather than ID order would show up.
func diamond() ([]Vertex, []edge) {
	return []Vertex{source("s"), operator("b"), operator("a"), sink("k")},
		[]edge{{"s", "a"}, {"s", "b"}, {"a", "k"}, {"b", "k"}}
}

func TestValidate(t *testing.T) {
	chainVs, chainEs := chain()
	diamondVs, diamondEs := diamond()

	badKind := source("s")
	badKind.Kind = VertexKind(99)

	missingFactory := source("s")
	missingFactory.NewSource = nil

	extraFactory := source("s")
	extraFactory.NewSink = func() core.Sink { return nopSink{} }

	zeroParallelism := source("s")
	zeroParallelism.Parallelism = 0

	overTheCap := source("s")
	overTheCap.Parallelism = maxParallelism + 1

	tests := []struct {
		name     string
		vertices []Vertex
		edges    []edge
		wantErr  error
	}{
		{
			name:     "valid chain",
			vertices: chainVs,
			edges:    chainEs,
		},
		{
			name:     "valid diamond",
			vertices: diamondVs,
			edges:    diamondEs,
		},
		{
			name:     "empty ID",
			vertices: []Vertex{source("")},
			wantErr:  errEmptyID,
		},
		{
			name:     "duplicate vertex ID",
			vertices: []Vertex{source("s"), operator("s")},
			wantErr:  errDuplicateID,
		},
		{
			name:     "edge from unknown vertex",
			vertices: []Vertex{source("s"), sink("k")},
			edges:    []edge{{"nope", "k"}},
			wantErr:  errUnknownVertex,
		},
		{
			name:     "edge to unknown vertex",
			vertices: []Vertex{source("s"), sink("k")},
			edges:    []edge{{"s", "nope"}},
			wantErr:  errUnknownVertex,
		},
		{
			name:     "duplicate edge",
			vertices: []Vertex{source("s"), sink("k")},
			edges:    []edge{{"s", "k"}, {"s", "k"}},
			wantErr:  errDuplicateEdge,
		},
		{
			name:     "unknown kind",
			vertices: []Vertex{badKind, sink("k")},
			edges:    []edge{{"s", "k"}},
			wantErr:  errUnknownKind,
		},
		{
			name:     "matching factory nil",
			vertices: []Vertex{missingFactory, sink("k")},
			edges:    []edge{{"s", "k"}},
			wantErr:  errMissingFactory,
		},
		{
			name:     "factory set that does not match kind",
			vertices: []Vertex{extraFactory, sink("k")},
			edges:    []edge{{"s", "k"}},
			wantErr:  errExtraFactory,
		},
		{
			name:     "parallelism below one",
			vertices: []Vertex{zeroParallelism, sink("k")},
			edges:    []edge{{"s", "k"}},
			wantErr:  errParallelismTooLow,
		},
		{
			// Parallelism above one is no longer a failure: it is what Phase 1
			// executes. Only the typo guard remains.
			name:     "parallelism above the cap",
			vertices: []Vertex{overTheCap, sink("k")},
			edges:    []edge{{"s", "k"}},
			wantErr:  errParallelismTooHigh,
		},
		{
			name:     "source with inbound edge",
			vertices: []Vertex{source("s"), operator("m"), sink("k"), source("s2")},
			edges:    []edge{{"s", "m"}, {"m", "k"}, {"m", "s2"}},
			wantErr:  errSourceHasInbound,
		},
		{
			name:     "sink with outbound edge",
			vertices: []Vertex{source("s"), sink("k"), sink("z")},
			edges:    []edge{{"s", "k"}, {"k", "z"}},
			wantErr:  errSinkHasOutbound,
		},
		{
			name:     "operator with no inputs",
			vertices: []Vertex{source("s"), operator("m"), sink("k")},
			edges:    []edge{{"s", "k"}, {"m", "k"}},
			wantErr:  errOperatorNoInput,
		},
		{
			name:     "operator with no outputs",
			vertices: []Vertex{source("s"), operator("m"), sink("k")},
			edges:    []edge{{"s", "k"}, {"s", "m"}},
			wantErr:  errOperatorNoOutput,
		},
		{
			name:     "disconnected vertex",
			vertices: append(append([]Vertex{}, chainVs...), source("z")),
			edges:    chainEs,
			wantErr:  errDisconnected,
		},
		{
			name:     "no sources",
			vertices: []Vertex{operator("a"), operator("b")},
			edges:    []edge{{"a", "b"}, {"b", "a"}},
			wantErr:  errNoSource,
		},
		{
			name:     "no sinks",
			vertices: []Vertex{source("s"), operator("a"), operator("b")},
			edges:    []edge{{"s", "a"}, {"a", "b"}, {"b", "a"}},
			wantErr:  errNoSink,
		},
		{
			name:     "cycle",
			vertices: []Vertex{source("s"), operator("a"), operator("b"), sink("k")},
			edges:    []edge{{"s", "a"}, {"a", "b"}, {"b", "a"}, {"b", "k"}},
			wantErr:  errCycle,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := build(tt.vertices, tt.edges)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("build: unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("build: got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateCycleReportsLeftoverVertices(t *testing.T) {
	_, err := build(
		[]Vertex{source("s"), operator("a"), operator("b"), sink("k")},
		[]edge{{"s", "a"}, {"a", "b"}, {"b", "a"}, {"b", "k"}},
	)
	if !errors.Is(err, errCycle) {
		t.Fatalf("got %v, want a cycle error", err)
	}
	if want := "[a b k] remain"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not name the leftover vertices %q", err, want)
	}
}

// TestValidateAcceptsParallelism covers the range the runtime executes. The
// rejection this replaces was the Phase 0 boundary; Phase 1 removes it.
func TestValidateAcceptsParallelism(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8, 64, maxParallelism} {
		vs, es := chain()
		for i := range vs {
			vs[i].Parallelism = p
		}
		if _, err := build(vs, es); err != nil {
			t.Errorf("parallelism %d: %v", p, err)
		}
	}
}

// TestValidateParallelismCapMessage checks the cap names its number. The cap
// exists so a mistyped parallelism fails at validation rather than spawning
// tens of thousands of goroutines, and an error that did not say the limit
// would leave the reader guessing what to change it to.
func TestValidateParallelismCapMessage(t *testing.T) {
	v := source("s")
	v.Parallelism = maxParallelism + 1
	_, err := build([]Vertex{v, sink("k")}, []edge{{"s", "k"}})
	if err == nil {
		t.Fatal("expected an error")
	}
	if want := "parallelism > 256"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err, want)
	}
}

func TestTopoOrder(t *testing.T) {
	chainVs, chainEs := chain()
	diamondVs, diamondEs := diamond()

	tests := []struct {
		name     string
		vertices []Vertex
		edges    []edge
		want     []string
	}{
		{name: "chain", vertices: chainVs, edges: chainEs, want: []string{"s", "m", "k"}},
		{name: "diamond", vertices: diamondVs, edges: diamondEs, want: []string{"s", "a", "b", "k"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := build(tt.vertices, tt.edges)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			// Repeated because a lost sort would surface as an order that
			// varies with Go's randomized map iteration, not as a wrong order
			// on the first call.
			for i := 0; i < 100; i++ {
				order, err := g.TopoOrder()
				if err != nil {
					t.Fatalf("TopoOrder: %v", err)
				}
				got := ids(order)
				if !slices.Equal(got, tt.want) {
					t.Fatalf("iteration %d: got %v, want %v", i, got, tt.want)
				}
			}
		})
	}
}

func TestTopoOrderRejectsInvalidGraph(t *testing.T) {
	g := New()
	if err := g.AddVertex(source("s")); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	if err := g.AddVertex(sink("k")); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	// No edges: both vertices are disconnected.
	if _, err := g.TopoOrder(); !errors.Is(err, errDisconnected) {
		t.Fatalf("got %v, want %v", err, errDisconnected)
	}
}

func TestDownstreamIsSorted(t *testing.T) {
	g := New()
	for _, v := range []Vertex{source("s"), sink("c"), sink("a"), sink("b")} {
		if err := g.AddVertex(v); err != nil {
			t.Fatalf("AddVertex: %v", err)
		}
	}
	for _, to := range []string{"c", "a", "b"} {
		if err := g.Connect("s", to); err != nil {
			t.Fatalf("Connect: %v", err)
		}
	}
	if got, want := g.Downstream("s"), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Errorf("Downstream(s) = %v, want %v", got, want)
	}
	if got := g.Downstream("a"); len(got) != 0 {
		t.Errorf("Downstream(a) = %v, want empty", got)
	}
}

func TestVertexKindString(t *testing.T) {
	tests := []struct {
		kind VertexKind
		want string
	}{
		{VertexSource, "source"},
		{VertexOperator, "operator"},
		{VertexSink, "sink"},
		{VertexKind(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("VertexKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
		}
	}
}

func ids(vs []Vertex) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.ID
	}
	return out
}
