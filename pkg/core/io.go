package core

import "io"

// Source produces the records at the head of a pipeline.
//
// SeekTo and Position are part of the contract from the start, even though
// nothing calls them in Phase 0. Recovery restarts a source from the offset
// recorded in a checkpoint, and a source written as a stateful loop cannot be
// retrofitted with that behaviour cheaply. Requiring it now forces every
// implementation to derive element n from (seed, n) rather than from
// accumulated state.
//
// Implementations are used by exactly one subtask goroutine and need no
// locking.
type Source interface {
	// Open is called once before the first Next.
	Open(ctx Context) error
	// Next returns the record at the current position and advances it. ok is
	// false when the input is exhausted, in which case the record is nil.
	Next() (rec *Record, ok bool, err error)
	// SeekTo positions the source so that the next Next returns the element at
	// offset. Seeking to n and reading must produce the same sequence as
	// reading from 0 and discarding the first n elements.
	SeekTo(offset int64) error
	// Position returns the offset of the element the next Next will return.
	Position() int64
	// Close releases resources. It is called exactly once.
	Close() error
}

// Sink consumes the records at the tail of a pipeline.
//
// # The two-phase commit
//
// Snapshot and NotifyCheckpointComplete are the sink's half of exactly-once
// output, and the gap between them is the whole of it. Snapshot stages: it puts
// whatever this sink has written since the last barrier somewhere durable and
// records, in the payload, which staged thing belongs to this checkpoint.
// NotifyCheckpointComplete commits it. Nothing becomes output in between.
//
// Phase 0 left both of these out and said why: a sink that could see the
// notification but had no staging area to commit would just invite writing at
// snapshot time. They arrive here together, with sinks.Transactional behind
// them, for that reason.
//
// # Concurrency
//
// Implementations are used by exactly one subtask goroutine and need no
// locking, and that INCLUDES these two. The runtime does not hand a sink to the
// coordinator; the sink's own subtask asks which checkpoints have completed and
// calls NotifyCheckpointComplete itself. See runSinkSubtask for why: a
// checkpoint completes inside the last sink subtask's acknowledgement, so a
// coordinator that called the other sinks directly would reach them mid-Write.
type Sink interface {
	// Open is called once before the first Write.
	Open(ctx Context) error
	// Write consumes one record.
	Write(rec *Record) error
	// Snapshot writes the sink's state for a checkpoint: which staged
	// transaction belongs to it.
	//
	// It STAGES and does not commit. Committing here would commit data
	// belonging to a checkpoint that may never complete, and the run that
	// recovered from the previous one would write those records again --
	// duplicates, from a sink whose whole purpose is not to produce them. That
	// is invariant 4.
	Snapshot(w io.Writer) error
	// NotifyCheckpointComplete tells the sink that checkpointID's _COMPLETE
	// marker is durable, which is the sink's licence to commit the transaction
	// it staged at that checkpoint.
	//
	// It may arrive for a checkpoint this sink staged nothing for, and it may
	// arrive twice for one that it did. Neither is an error: the commit is
	// idempotent by construction in the implementation this interface was
	// designed around, and a notification that is refused is a notification
	// that turns a completed checkpoint into a failed job.
	//
	// It may also never arrive, for the last checkpoint before a crash. A sink
	// therefore cannot treat the notification as the only path to a commit; see
	// Restore on sinks.Transactional for the other one.
	NotifyCheckpointComplete(checkpointID int64) error
	// Close releases resources. It is called exactly once.
	Close() error
}
