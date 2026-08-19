package core

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
// There is deliberately no NotifyCheckpointComplete here. It arrives with the
// transactional sink in Phase 5, together with the two-phase commit that gives
// it meaning; a sink that could see the notification but had no staging area to
// commit would just invite writing at snapshot time, which duplicates records
// on recovery.
//
// Implementations are used by exactly one subtask goroutine and need no
// locking.
type Sink interface {
	// Open is called once before the first Write.
	Open(ctx Context) error
	// Write consumes one record.
	Write(rec *Record) error
	// Close releases resources. It is called exactly once.
	Close() error
}
