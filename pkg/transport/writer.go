package transport

import (
	"context"
	"errors"
	"fmt"

	"github.com/AarinB1/tidemark/pkg/core"
)

// FNV-1a 64-bit constants, inlined below.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// errEmptyKey is returned by EmitRecord for a record with no key.
var errEmptyKey = errors.New("record has no key: an unkeyed record has no partition")

// fnv1a returns the FNV-1a hash of b.
//
// Written out rather than taken from hash/fnv because fnv.New64a returns a
// hash.Hash64 interface: the state escapes to the heap and every record costs
// an allocation on the hottest path in the engine. This version is a loop over
// bytes with no allocation at all, which the benchmark in writer_test.go pins
// at 0 allocs/op.
//
// Not hash/maphash either. maphash randomises its seed per process, so the same
// key would route to a different subtask on every run. No single-process test
// detects that, and it would quietly make Phase 4's reproducibility claim false.
func fnv1a(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// Writer routes elements from one producing subtask to its downstream outputs.
//
// It is the enforcement point for the partitioning invariant, and the shape of
// the type is what makes that invariant expressible. Outputs are grouped by
// downstream vertex: one group per outgoing edge, holding that edge's channel
// to each subtask of the vertex on the far end.
//
// A record partitions WITHIN a group and broadcasts ACROSS groups. Those are
// different questions and conflating them is the bug this type exists to
// prevent. "Which subtask of B handles this key" is partitioning: exactly one,
// chosen by hash, because keyed state for a key must live in one place. "Does B
// see this record at all" is not a routing question: B and C are different
// computations over the same stream, so each must receive all of it. A flat
// list of outputs cannot tell the two apart, and a Writer holding one sends
// each record to B or to C, leaving both computing on a fraction of their input
// with no error to show for it.
//
// Watermarks, barriers, and end-of-stream broadcast everywhere: to every output
// in every group.
//
// A Writer is used by exactly one goroutine, the one that owns its outputs, and
// needs no locking.
type Writer struct {
	groups [][]Output
	// outputs is the total across groups, kept for error messages so the
	// unkeyed-record path does not walk the groups to count them.
	outputs int
}

// NewWriter returns a Writer over groups, one group per downstream vertex.
//
// Within a group the order of outputs is the partition order: output i of a
// group receives the records whose key hashes to i, so a caller must supply
// them in a stable order or the same key lands on a different subtask between
// runs. The order of the groups themselves does not affect where any record
// goes, since every group receives every record; it fixes only which send is
// attempted first, and therefore which failure surfaces when a job is coming
// apart.
//
// No groups is valid: a sink has nothing downstream. An empty group is not, and
// panics rather than being tolerated. Graph validation rejects parallelism < 1
// before the runtime wires anything, so a group with no outputs cannot arise
// from a valid graph; it means the wiring built the group wrongly. Tolerating
// it would mean a downstream vertex that silently receives nothing, which is
// the same class of silent wrong answer this type exists to prevent, arriving
// by a different route.
func NewWriter(groups [][]Output) *Writer {
	outputs := 0
	for i, group := range groups {
		if len(group) == 0 {
			panic(fmt.Sprintf("transport: NewWriter: group %d has no outputs", i))
		}
		outputs += len(group)
	}
	return &Writer{groups: groups, outputs: outputs}
}

// EmitRecord sends r to exactly one output in every group: partitioned within
// each downstream vertex, copied across them.
//
// The hash is computed once and reused for every group rather than recomputed
// per group. Not for the cycles: the modulo differs per group because the
// groups differ in length, so the same hash still lands on an independently
// chosen subtask in each. Hashing once makes it structurally impossible for two
// groups to disagree about a key.
//
// A nil or empty key is an error rather than a route to output 0. Sending every
// unkeyed record to one subtask is a skew bug that presents as a performance
// problem: the job produces correct results, slowly, and nothing points at the
// cause.
//
// The first failing send returns immediately, leaving the later groups
// untouched. This matches opContext, which keeps the first error and stops: a
// send fails because the job is being torn down, and the remaining sends cannot
// succeed either.
//
// A Writer with no groups drops the record. That is the sink case, where there
// is nothing downstream to route to, and it is the only case where a record
// legitimately goes nowhere.
func (w *Writer) EmitRecord(ctx context.Context, r *core.Record) error {
	if len(w.groups) == 0 {
		return nil
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("cannot partition across %d outputs: %w", w.outputs, errEmptyKey)
	}
	h := fnv1a(r.Key)
	e := core.NewRecordElement(r)
	for _, group := range w.groups {
		if err := group[h%uint64(len(group))].Send(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Broadcast sends e to every output in every group.
//
// End-of-stream uses it now; watermarks in Phase 2 and barriers in Phase 3 use
// it for the same reason. A control element that reached only one downstream
// channel would leave the others with no signal at all: the watermark case
// stalls their event time silently, and the barrier case deadlocks alignment.
// Reaching every channel on one edge but not the others is the same failure one
// level up, and is what grouping makes it possible to state.
func (w *Writer) Broadcast(ctx context.Context, e core.StreamElement) error {
	for _, group := range w.groups {
		for _, out := range group {
			if err := out.Send(ctx, e); err != nil {
				return err
			}
		}
	}
	return nil
}

// CloseAll closes every output in every group. The producing subtask calls it on
// every exit path, including a failure: a downstream Recv has no context to
// watch, and closure is the only signal that unblocks it. A group left unclosed
// would hang every subtask of that downstream vertex.
func (w *Writer) CloseAll() {
	for _, group := range w.groups {
		for _, out := range group {
			out.Close()
		}
	}
}
