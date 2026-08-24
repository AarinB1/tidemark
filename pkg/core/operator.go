package core

import (
	"io"

	"github.com/AarinB1/tidemark/pkg/state"
)

// Context is the operator's handle on the runtime.
//
// It is deliberately smaller than it will eventually be. Nothing is stubbed
// here in advance: State() is here because the state backend is here, and
// RegisterEventTimeTimer() is still absent because event-time timers are held
// by the operator that fires them rather than by the runtime.
type Context interface {
	// Emit forwards rec to the downstream channels.
	Emit(rec *Record)
	// CurrentWatermark returns the watermark most recently delivered to the
	// operator, or the initial minimum if none has arrived.
	CurrentWatermark() int64
	// State returns the keyed state of this subtask.
	//
	// One state per subtask, handed out rather than created by the operator, so
	// that the runtime decides which backend a job runs on: Memory now, Pebble
	// in Phase 3b. An operator that made its own map could not be checkpointed
	// without every operator being taught about the backend separately.
	//
	// This is a method on an interface that already exists, not a new
	// interface, and the import runs one way: pkg/core depends on pkg/state and
	// pkg/state depends on nothing in this repository.
	//
	// An operator does NOT check state.KeyedState.Err. Get, Put, Delete and
	// Iterate cannot fail in their signatures, so a backend that failed stashes
	// the error and hands back zero values, and the runtime collects that stash
	// after every call it makes into this operator and fails the subtask. An
	// operator that checked it per record would be duplicating a check the
	// runtime already makes at exactly the granularity that matters, and one
	// that checked it in some methods and not others would swallow the error in
	// the rest.
	State() state.KeyedState
}

// Operator is the user-defined computation run by a subtask. The runtime calls
// its methods from a single goroutine, so implementations need no locking.
type Operator interface {
	// Open is called once before any element is processed.
	Open(ctx Context) error
	// ProcessElement handles one data record.
	ProcessElement(rec *Record, ctx Context) error
	// ProcessWatermark handles an advance of event time. wm is monotonically
	// non-decreasing across calls.
	ProcessWatermark(wm int64, ctx Context) error
	// OnEndOfStream is called after the last record, giving aggregating
	// operators a place to flush.
	OnEndOfStream(ctx Context) error
	// Snapshot writes the operator's state for a checkpoint.
	Snapshot(w io.Writer) error
	// Restore reads state previously written by Snapshot. It is called after
	// Open and before any element is processed.
	Restore(r io.Reader) error
	// Close releases resources. It is called exactly once, whether or not the
	// subtask completed successfully.
	Close() error
}
