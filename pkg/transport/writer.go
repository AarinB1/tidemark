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
// It is the enforcement point for the partitioning invariant: a record goes to
// exactly one output, while watermarks, barriers, and end-of-stream go to all
// of them. Keeping both in one type is what makes the distinction hard to get
// wrong at a call site.
//
// A Writer is used by exactly one goroutine, the one that owns its outputs, and
// needs no locking.
type Writer struct {
	outputs []Output
}

// NewWriter returns a Writer over outputs. The order of outputs is the
// partition order: output i receives the records whose key hashes to i, so a
// caller must supply them in a stable order or the same key lands on a
// different subtask between runs.
func NewWriter(outputs []Output) *Writer {
	return &Writer{outputs: outputs}
}

// EmitRecord sends r to exactly one output, chosen by the hash of its key.
//
// A nil or empty key is an error rather than a route to output 0. Sending every
// unkeyed record to one subtask is a skew bug that presents as a performance
// problem: the job produces correct results, slowly, and nothing points at the
// cause.
//
// A Writer with no outputs drops the record. That is the sink case, where there
// is nothing downstream to route to, and it is the only case where a record
// legitimately goes nowhere.
func (w *Writer) EmitRecord(ctx context.Context, r *core.Record) error {
	if len(w.outputs) == 0 {
		return nil
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("cannot partition across %d outputs: %w", len(w.outputs), errEmptyKey)
	}
	i := fnv1a(r.Key) % uint64(len(w.outputs))
	return w.outputs[i].Send(ctx, core.NewRecordElement(r))
}

// Broadcast sends e to every output.
//
// End-of-stream uses it now; watermarks in Phase 2 and barriers in Phase 3 use
// it for the same reason. A control element that reached only one downstream
// channel would leave the others with no signal at all: the watermark case
// stalls their event time silently, and the barrier case deadlocks alignment.
func (w *Writer) Broadcast(ctx context.Context, e core.StreamElement) error {
	for _, out := range w.outputs {
		if err := out.Send(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// CloseAll closes every output. The producing subtask calls it on every exit
// path, including a failure: a downstream Recv has no context to watch, and
// closure is the only signal that unblocks it.
func (w *Writer) CloseAll() {
	for _, out := range w.outputs {
		out.Close()
	}
}
