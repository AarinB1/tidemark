// Package chaos runs a job under a fault schedule derived from a seed.
//
// The whole phase rests on one property: printing the seed is enough to
// reproduce the run. Everything about a schedule -- how many faults it holds,
// which subtask each one kills, and the logical position it kills at -- is a
// pure function of that one integer and the graph. Nothing here reads a clock,
// iterates a map, or looks at the environment. That is invariant 6, and it is
// what makes a failing seed something a person can rerun rather than something
// they have to reproduce by luck.
//
// A run is still CONCURRENT, and this package does not pretend otherwise. The
// order two inputs of a gate deliver their barriers, and therefore whether a
// fault aimed between them lands, is decided by the Go scheduler. The schedule
// being deterministic is what makes the aim reproducible; the census in
// census.go is what measures whether the shot connected.
package chaos

import (
	"fmt"

	"github.com/AarinB1/tidemark/pkg/graph"
)

// TriggerKind is the logical position a fault fires at.
type TriggerKind uint8

const (
	// TriggerAfterElements fires when the subtask has processed N records.
	TriggerAfterElements TriggerKind = iota
	// TriggerAfterBarrier fires just after the subtask forwards barrier N.
	TriggerAfterBarrier
	// TriggerDuringAlignment fires when barrier N has been delivered on
	// exactly Inputs of the subtask's inputs and at least one live input has
	// not delivered it yet.
	//
	// This kind exists because Phase 3b established that a fault at an
	// arbitrary element count almost never lands between barrier k arriving on
	// one input and arriving on another. That gap is what alignment exists for,
	// so without a trigger aimed at it the widest class of interesting failure
	// is unreachable from any seed, and five hundred schedules would sample
	// only the easy part of the space.
	TriggerDuringAlignment
)

// String renders the kind for the census table and for error messages.
func (k TriggerKind) String() string {
	switch k {
	case TriggerAfterElements:
		return "after-elements"
	case TriggerAfterBarrier:
		return "after-barrier"
	case TriggerDuringAlignment:
		return "during-alignment"
	default:
		return "unknown"
	}
}

// Fault is one scheduled abort of one subtask.
type Fault struct {
	VertexID string
	Subtask  int
	Trigger  TriggerKind
	// N is an element count for TriggerAfterElements and a checkpoint ID for
	// the other two.
	N int64
	// Inputs is how many of the subtask's inputs have delivered the barrier,
	// and is meaningful only for TriggerDuringAlignment. It is zero otherwise.
	Inputs int
}

// String renders a fault as one line of a census table.
func (f Fault) String() string {
	if f.Trigger == TriggerDuringAlignment {
		return fmt.Sprintf("%s[%d] %s checkpoint %d at %d inputs", f.VertexID, f.Subtask, f.Trigger, f.N, f.Inputs)
	}
	return fmt.Sprintf("%s[%d] %s %d", f.VertexID, f.Subtask, f.Trigger, f.N)
}

// MaxFaultsPerSchedule bounds how many faults one seed produces.
//
// Zero is drawn as often as any other count, and a schedule with no fault at
// all is the control group: it is the run that says the harness produces the
// right answer when nothing goes wrong, checked by the same comparison as every
// other schedule rather than by a separate test that could drift from it.
const MaxFaultsPerSchedule = 3

// ScheduleFor returns the faults seed schedules against g.
//
// The graph is what bounds every field: a fault names a vertex the graph has,
// a subtask index below that vertex's parallelism, an element count no larger
// than the elements one of its subtasks can see, and a checkpoint ID no larger
// than the number of barriers the job's sources inject. A schedule that named
// anything else would spend a seed on a fault that could not fire, which the
// census would report as a miss and which would look exactly like the
// interesting kind of miss.
//
// It PANICS on a graph the runtime would refuse. There is no error return
// because there is nothing a caller could do with one: a chaos suite handed an
// invalid graph has no run to make, and the panic names the validation failure.
// operators.NewSlidingCount refuses a bad specification the same way and for
// the same reason.
func ScheduleFor(seed int64, g *graph.Graph) []Fault {
	sites := sitesOf(g)
	if len(sites) == 0 {
		return nil
	}

	st := newStream(uint64(seed))
	n := int(st.below(MaxFaultsPerSchedule + 1))
	faults := make([]Fault, 0, n)
	for range n {
		f, ok := drawFault(st, sites)
		if !ok {
			continue
		}
		// An exact duplicate is dropped rather than kept. Two identical faults
		// describe one abort at one position -- a fault fires at most once, so
		// the second could only ever be recorded as a miss -- and a miss that
		// is an artefact of the draw would be indistinguishable in the census
		// from a fault that genuinely never reached its position.
		if !containsFault(faults, f) {
			faults = append(faults, f)
		}
	}
	return faults
}

// drawFault draws one fault from the sites available.
//
// ok is false when the drawn site cannot host the drawn trigger with a
// non-empty range -- a source vertex whose subtask range is empty, say. The
// draw is SPENT either way: consuming the same number of values from the stream
// whatever the outcome is what keeps the schedule a function of the seed alone
// rather than of which branch was taken.
func drawFault(st *stream, sites []site) (Fault, bool) {
	s := sites[st.below(int64(len(sites)))]
	kinds := s.triggers()
	kind := kinds[st.below(int64(len(kinds)))]
	subtask := int(st.below(int64(s.parallelism)))
	n := st.next()
	inputs := st.next()

	f := Fault{VertexID: s.vertexID, Subtask: subtask, Trigger: kind}
	switch kind {
	case TriggerAfterElements:
		if s.maxElements <= 0 {
			return Fault{}, false
		}
		f.N = int64(n % uint64(s.maxElements))
	case TriggerAfterBarrier:
		f.N = 1 + int64(n%uint64(s.maxCheckpoint))
	case TriggerDuringAlignment:
		f.N = 1 + int64(n%uint64(s.maxCheckpoint))
		// At least one and at most one fewer than the subtask's inputs: a fault
		// at every input is the moment alignment COMPLETES, which is not an
		// alignment window at all.
		f.Inputs = 1 + int(inputs%uint64(s.inputs-1))
	}
	return f, true
}

// containsFault reports whether f is already in faults.
func containsFault(faults []Fault, f Fault) bool {
	for _, g := range faults {
		if g == f {
			return true
		}
	}
	return false
}

// site is one vertex a fault may be scheduled against, with the bounds its
// shape imposes on the fields of a fault.
type site struct {
	vertexID    string
	parallelism int
	// inputs is how many input channels ONE subtask of this vertex has: the
	// sum of the parallelisms of the vertices upstream of it. A source has
	// none.
	inputs int
	// maxElements bounds the records one subtask processes. For a source it is
	// the floor of its range length, so a fault inside it always lands. For
	// anything downstream it is the average share, which key skew puts the
	// actual count either side of -- so a fault near the top of the range
	// sometimes misses, and the census is what says how often.
	maxElements int64
	// maxCheckpoint is the highest barrier ID any source subtask injects.
	maxCheckpoint int64
}

// triggers returns the trigger kinds this site can host, in a fixed order.
//
// Every vertex processes elements. A vertex can only be killed after barrier k
// if some source injects a barrier k at all. Alignment needs two inputs: with
// one, the first barrier completes alignment on arrival and there is no window
// between "some inputs have delivered" and "all of them have".
func (s site) triggers() []TriggerKind {
	kinds := []TriggerKind{TriggerAfterElements}
	if s.maxCheckpoint > 0 {
		kinds = append(kinds, TriggerAfterBarrier)
		if s.inputs > 1 {
			kinds = append(kinds, TriggerDuringAlignment)
		}
	}
	return kinds
}

// sitesOf describes every vertex of g as a fault site, in topological order.
//
// Topological order with a lexicographic tie-break, taken from the graph rather
// than from a map, so the list is a function of the graph alone. A map
// iteration here would make the schedule depend on Go's hash seed, and the
// symptom would be a seed that reproduces a failure on one process and not on
// the next -- which is the one thing this package exists to rule out.
func sitesOf(g *graph.Graph) []site {
	order, err := g.TopoOrder()
	if err != nil {
		panic(fmt.Sprintf("chaos: ScheduleFor: %v", err))
	}

	inputs := make(map[string]int, len(order))
	// elements[v] is how many records reach vertex v across all its subtasks.
	// Propagated forward in topological order: a record partitions to exactly
	// one subtask WITHIN an edge and every downstream vertex receives the full
	// stream, so a vertex sees the sum of what its upstream vertices produce.
	elements := make(map[string]int64, len(order))
	maxCheckpoint := int64(0)
	for _, v := range order {
		if v.Kind == graph.VertexSource {
			count := sourceCount(v)
			elements[v.ID] = count
			maxCheckpoint = max(maxCheckpoint, barriersPerSubtask(count, v.Parallelism, v.BarrierIntervalElements))
		}
		for _, to := range g.Downstream(v.ID) {
			inputs[to] += v.Parallelism
			elements[to] += elements[v.ID]
		}
	}

	sites := make([]site, 0, len(order))
	for _, v := range order {
		sites = append(sites, site{
			vertexID:      v.ID,
			parallelism:   v.Parallelism,
			inputs:        inputs[v.ID],
			maxElements:   elements[v.ID] / int64(v.Parallelism),
			maxCheckpoint: maxCheckpoint,
		})
	}
	return sites
}

// countableSource is a core.Source whose offset space is finite and known,
// which is what lets this package bound an element count against the input.
//
// Asserted for rather than required, and asserted on an instance made and
// discarded, exactly as runtime.buildMetadata does. A source that does not
// report one contributes no elements to the bound, which makes a fault against
// it fall in an empty range and be dropped from the schedule rather than
// scheduled somewhere meaningless.
type countableSource interface {
	Count() int64
}

// sourceCount returns how many records v produces, or zero if it will not say.
func sourceCount(v graph.Vertex) int64 {
	if v.NewSource == nil {
		return 0
	}
	s, ok := v.NewSource().(countableSource)
	if !ok {
		return 0
	}
	return s.Count()
}

// barriersPerSubtask is how many barriers every subtask of a source vertex
// injects.
//
// This is runtime.maxBarriers written out again, over the floor of
// count/parallelism, because that is the budget every subtask shares however
// the remainder of the split falls. A checkpoint ID above it names a barrier no
// subtask ever injects.
func barriersPerSubtask(count int64, parallelism int, intervalElements int64) int64 {
	if intervalElements <= 0 || parallelism <= 0 {
		return 0
	}
	return (count / int64(parallelism)) / intervalElements
}

// stream is a counter-indexed splitmix64, the same derivation
// sources.Generator draws its records from.
//
// Counter-indexed rather than iterated: value i is a pure function of
// (seed, i), so nothing about a draw depends on how many draws came before it
// in any sense other than its index. A held *rand.Rand would make the schedule
// depend on the order the fields happened to be read in, which is the shape of
// bug that survives every test until somebody reorders two lines.
type stream struct {
	seed uint64
	i    uint64
}

func newStream(seed uint64) *stream { return &stream{seed: seed} }

// next returns the next value in the stream.
func (s *stream) next() uint64 {
	v := mix(s.seed, s.i)
	s.i++
	return v
}

// below returns the next value reduced to [0, n). n must be positive.
//
// Modulo, with the bias that implies. The ranges here are tens or hundreds
// against a 64-bit draw, so the bias is on the order of 2^-58 and irrelevant;
// rejection sampling would make the number of draws depend on the values drawn,
// which is a worse property for a schedule that has to be reproducible field by
// field.
func (s *stream) below(n int64) int64 {
	return int64(s.next() % uint64(n))
}

// mix is splitmix64 applied to (seed, n), written out here rather than shared
// with pkg/sources.
//
// It is five lines and it is the definition of what a seed means in this
// repository. Exporting it from pkg/sources to share it would put a random
// number generator in the public API of the package that holds the input
// generator, for one caller in a test tree.
func mix(seed, n uint64) uint64 {
	z := seed + (n+1)*0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
