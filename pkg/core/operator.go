package core

import "io"

// Context is the operator's handle on the runtime.
//
// It is deliberately smaller than it will eventually be: State() arrives with
// the state backend and RegisterEventTimeTimer() with event-time processing.
// Nothing is stubbed here in advance.
type Context interface {
	// Emit forwards rec to the downstream channels.
	Emit(rec *Record)
	// CurrentWatermark returns the watermark most recently delivered to the
	// operator, or the initial minimum if none has arrived.
	CurrentWatermark() int64
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
