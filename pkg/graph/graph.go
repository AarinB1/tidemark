// Package graph describes a job as vertices and the edges between them. It
// holds no running state: the runtime turns a validated graph into subtasks.
package graph

import (
	"errors"
	"fmt"
	"slices"

	"github.com/AarinB1/tidemark/pkg/core"
)

// maxParallelism bounds the subtasks one vertex may have.
//
// The runtime gives every subtask a goroutine and every edge P*Q channels, so a
// mistyped parallelism is not a slow job but tens of thousands of goroutines and
// a machine that stops responding. The cap is a typo guard rather than a claim
// about what the engine can schedule; a job that genuinely wants more subtasks
// than this wants more than one process, which is Phase 7.
const maxParallelism = 256

// VertexKind is the role a vertex plays in the pipeline.
type VertexKind uint8

const (
	VertexSource VertexKind = iota
	VertexOperator
	VertexSink
)

// String renders the kind for error messages.
func (k VertexKind) String() string {
	switch k {
	case VertexSource:
		return "source"
	case VertexOperator:
		return "operator"
	case VertexSink:
		return "sink"
	default:
		return "unknown"
	}
}

// Vertex is one logical stage of a job.
//
// The three factory fields hold constructors rather than instances because a
// vertex with parallelism N becomes N subtasks, each of which must own its
// operator. Sharing one instance across subtasks is a data race that no test at
// parallelism 1 can catch, so the shape that makes it impossible is chosen
// before parallelism exists. Exactly the field matching Kind is set.
type Vertex struct {
	ID          string
	Kind        VertexKind
	Parallelism int
	NewSource   func() core.Source
	NewOperator func() core.Operator
	NewSink     func() core.Sink

	// WatermarkIntervalElements is how many records one subtask of a source
	// vertex emits between two watermarks. Zero disables watermark generation,
	// which is what a job doing no event-time work wants.
	//
	// It lives here, on the vertex, rather than on core.Source, because
	// generation is the source RUNNER's job: every source needs a watermark and
	// the policy is uniform, so a method on the interface would be polymorphism
	// with one behaviour behind it. It is per-vertex rather than per-job
	// because two source vertices feeding one operator can legitimately carry
	// different out-of-orderness.
	//
	// Counted in ELEMENTS, never on a wall clock. See watermarkGenerator in
	// pkg/runtime and invariant 6.
	WatermarkIntervalElements int64
	// MaxOutOfOrderness is how far behind the maximum observed event time one
	// subtask of a source vertex holds its watermark, in millis.
	MaxOutOfOrderness int64
}

// Validation failures. Each condition has its own error so that callers and
// tests can tell them apart with errors.Is.
var (
	errEmptyID            = errors.New("vertex ID is empty")
	errDuplicateID        = errors.New("duplicate vertex ID")
	errUnknownVertex      = errors.New("unknown vertex")
	errDuplicateEdge      = errors.New("duplicate edge")
	errUnknownKind        = errors.New("unknown vertex kind")
	errMissingFactory     = errors.New("factory for kind is nil")
	errExtraFactory       = errors.New("factory set that does not match kind")
	errParallelismTooLow  = errors.New("parallelism < 1")
	errParallelismTooHigh = errors.New("parallelism > 256")
	errSourceHasInbound   = errors.New("source has inbound edges")
	errSinkHasOutbound    = errors.New("sink has outbound edges")
	errOperatorNoInput    = errors.New("operator has no inputs")
	errOperatorNoOutput   = errors.New("operator has no outputs")
	errDisconnected       = errors.New("vertex has no edges")
	errNoSource           = errors.New("graph has no source")
	errNoSink             = errors.New("graph has no sink")
	errCycle              = errors.New("graph has a cycle")

	errNegativeOutOfOrderness = errors.New("MaxOutOfOrderness < 0")
	errWatermarkOnNonSource   = errors.New("watermark configuration on a vertex that is not a source")
)

// Graph is a job under construction. The zero value is not usable; call New.
type Graph struct {
	byID map[string]Vertex
	out  map[string][]string
	in   map[string][]string
}

// New returns an empty graph.
func New() *Graph {
	return &Graph{
		byID: make(map[string]Vertex),
		out:  make(map[string][]string),
		in:   make(map[string][]string),
	}
}

// AddVertex adds v. IDs must be unique; the duplicate is rejected here rather
// than in Validate because the second vertex would otherwise be dropped
// silently.
func (g *Graph) AddVertex(v Vertex) error {
	if v.ID == "" {
		return errEmptyID
	}
	if _, ok := g.byID[v.ID]; ok {
		return fmt.Errorf("vertex %q: %w", v.ID, errDuplicateID)
	}
	g.byID[v.ID] = v
	return nil
}

// Connect adds an edge from fromID to toID. Both vertices must already exist,
// and an edge may not be added twice: a repeated edge would give the pair two
// channels, and a record partitioned to "one" downstream channel would arrive
// at that vertex twice.
func (g *Graph) Connect(fromID, toID string) error {
	if _, ok := g.byID[fromID]; !ok {
		return fmt.Errorf("edge %q -> %q: from %q: %w", fromID, toID, fromID, errUnknownVertex)
	}
	if _, ok := g.byID[toID]; !ok {
		return fmt.Errorf("edge %q -> %q: to %q: %w", fromID, toID, toID, errUnknownVertex)
	}
	if slices.Contains(g.out[fromID], toID) {
		return fmt.Errorf("edge %q -> %q: %w", fromID, toID, errDuplicateEdge)
	}
	g.out[fromID] = append(g.out[fromID], toID)
	g.in[toID] = append(g.in[toID], fromID)
	return nil
}

// Downstream returns the IDs the edges out of id point at, in lexicographic
// order. The runtime wires one channel per entry, and the order is fixed so
// that a given downstream vertex keeps the same channel index across runs.
func (g *Graph) Downstream(id string) []string {
	ids := slices.Clone(g.out[id])
	slices.Sort(ids)
	return ids
}

// Validate reports the first structural problem with the graph. Vertices are
// examined in lexicographic ID order so that a graph with several problems
// always reports the same one.
func (g *Graph) Validate() error {
	sources, sinks := 0, 0
	for _, id := range g.sortedIDs() {
		v := g.byID[id]
		if v.Parallelism < 1 {
			return fmt.Errorf("vertex %q: parallelism %d: %w", id, v.Parallelism, errParallelismTooLow)
		}
		if v.Parallelism > maxParallelism {
			return fmt.Errorf("vertex %q: parallelism %d: %w", id, v.Parallelism, errParallelismTooHigh)
		}
		if err := checkFactories(v); err != nil {
			return fmt.Errorf("vertex %q: %w", id, err)
		}
		if err := checkWatermarkConfig(v); err != nil {
			return fmt.Errorf("vertex %q: %w", id, err)
		}
		switch v.Kind {
		case VertexSource:
			sources++
			if len(g.in[id]) > 0 {
				return fmt.Errorf("vertex %q: %w", id, errSourceHasInbound)
			}
		case VertexSink:
			sinks++
			if len(g.out[id]) > 0 {
				return fmt.Errorf("vertex %q: %w", id, errSinkHasOutbound)
			}
		case VertexOperator:
			if len(g.in[id]) == 0 {
				return fmt.Errorf("vertex %q: %w", id, errOperatorNoInput)
			}
			if len(g.out[id]) == 0 {
				return fmt.Errorf("vertex %q: %w", id, errOperatorNoOutput)
			}
		}
		if len(g.in[id]) == 0 && len(g.out[id]) == 0 {
			return fmt.Errorf("vertex %q: %w", id, errDisconnected)
		}
	}
	if sources == 0 {
		return errNoSource
	}
	if sinks == 0 {
		return errNoSink
	}
	_, err := g.kahn()
	return err
}

// TopoOrder returns the vertices in topological order. It validates first, so
// a caller that gets an order back has a graph the runtime can execute.
func (g *Graph) TopoOrder() ([]Vertex, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g.kahn()
}

// kahn runs Kahn's algorithm. When several vertices have in-degree zero it
// takes the lexicographically smallest, which makes the order a function of the
// graph alone. A topological order that varied between runs would make every
// downstream test flaky in a way that looks like a concurrency bug.
func (g *Graph) kahn() ([]Vertex, error) {
	indeg := make(map[string]int, len(g.byID))
	var ready []string
	for _, id := range g.sortedIDs() {
		indeg[id] = len(g.in[id])
		if indeg[id] == 0 {
			ready = append(ready, id)
		}
	}
	order := make([]Vertex, 0, len(g.byID))
	for len(ready) > 0 {
		slices.Sort(ready)
		id := ready[0]
		ready = ready[1:]
		order = append(order, g.byID[id])
		for _, to := range g.Downstream(id) {
			indeg[to]--
			if indeg[to] == 0 {
				ready = append(ready, to)
			}
		}
	}
	if len(order) != len(g.byID) {
		var left []string
		for id, d := range indeg {
			if d > 0 {
				left = append(left, id)
			}
		}
		slices.Sort(left)
		return nil, fmt.Errorf("%w: %v remain", errCycle, left)
	}
	return order, nil
}

// sortedIDs returns every vertex ID in lexicographic order. Map iteration order
// is randomized, so nothing that affects a result may iterate g.byID directly.
func (g *Graph) sortedIDs() []string {
	ids := make([]string, 0, len(g.byID))
	for id := range g.byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// checkWatermarkConfig rejects watermark settings that would silently do the
// wrong thing.
//
// A negative MaxOutOfOrderness pushes the watermark AHEAD of the maximum event
// time observed, so a window fires before its last element has arrived. That is
// invariant 1's failure mode reached by another route, and like that one it
// produces slightly wrong counts rather than an error.
//
// Settings on an operator or a sink are rejected rather than ignored. Only a
// source generates watermarks; every other vertex forwards what its gate gives
// it. Ignoring the fields would let a job be configured for event time at a
// vertex that cannot honour it and then run to completion looking fine. The two
// fields are reported separately so the error names the one that was set: a
// message that only says "watermark configuration" leaves the reader looking at
// both.
func checkWatermarkConfig(v Vertex) error {
	if v.MaxOutOfOrderness < 0 {
		return fmt.Errorf("MaxOutOfOrderness %d: %w", v.MaxOutOfOrderness, errNegativeOutOfOrderness)
	}
	if v.Kind != VertexSource {
		if v.WatermarkIntervalElements != 0 {
			return fmt.Errorf("%s: WatermarkIntervalElements %d: %w", v.Kind, v.WatermarkIntervalElements, errWatermarkOnNonSource)
		}
		if v.MaxOutOfOrderness != 0 {
			return fmt.Errorf("%s: MaxOutOfOrderness %d: %w", v.Kind, v.MaxOutOfOrderness, errWatermarkOnNonSource)
		}
	}
	return nil
}

// checkFactories requires exactly the factory matching v.Kind to be set.
func checkFactories(v Vertex) error {
	var matching bool
	switch v.Kind {
	case VertexSource:
		matching = v.NewSource != nil
	case VertexOperator:
		matching = v.NewOperator != nil
	case VertexSink:
		matching = v.NewSink != nil
	default:
		return fmt.Errorf("kind %d: %w", uint8(v.Kind), errUnknownKind)
	}
	if !matching {
		return fmt.Errorf("%s: %w", v.Kind, errMissingFactory)
	}
	set := 0
	if v.NewSource != nil {
		set++
	}
	if v.NewOperator != nil {
		set++
	}
	if v.NewSink != nil {
		set++
	}
	if set != 1 {
		return fmt.Errorf("%s: %w", v.Kind, errExtraFactory)
	}
	return nil
}
