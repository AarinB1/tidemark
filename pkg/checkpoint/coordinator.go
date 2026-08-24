package checkpoint

import (
	"errors"
	"fmt"
	"sync"
)

// Protocol failures. A coordinator that tolerated these quietly would hide the
// bugs this phase exists to make loud.
var (
	errUnknownSubtask = errors.New("acknowledgement from a subtask the job does not have")
	errDuplicateAck   = errors.New("subtask acknowledged the same checkpoint twice")
)

// Coordinator decides when a checkpoint is complete and writes it.
//
// One process. There is no gRPC here and no transport of any kind: subtasks are
// goroutines in this process and they call Acknowledge directly. The
// coordinator/worker split is Phase 7, and the seam it needs is a transport
// under this type rather than an interface over it, so nothing here is shaped
// to be replaced.
//
// # What it is counting
//
// Every subtask acknowledges every checkpoint, INCLUDING sinks, which
// acknowledge with an empty payload in this phase. A sink has nothing to
// snapshot until the transactional sink in Phase 5 gives it a staging area, and
// the tempting shortcut is to leave sinks out of the count. Uniformity is worth
// more: when Phase 5 gives a sink real content to record, it changes what a
// sink puts in its payload and nothing about who is expected to speak. A
// coordinator that had to learn about a new participant would be a protocol
// change, and a protocol change is where the "which subtasks are expected"
// question gets answered differently in two places.
//
// The expected set comes from the metadata, which is also what a restore reads,
// so the job's shape is stated once.
//
// # Concurrency
//
// Acknowledge is called from every subtask goroutine. One mutex guards
// everything, and it is HELD ACROSS THE DISK WRITE. That serialises the
// snapshot writes of subtasks that acknowledge at the same moment, which is a
// real cost and the obvious implementation. Releasing the lock to write would
// mean two acknowledgements for the same checkpoint could both observe
// themselves as the last one, and both would call Complete; the alternative is
// a per-checkpoint lock, which is a second locking scheme to keep in step with
// the first for a saving that does not matter at a checkpoint interval measured
// in thousands of elements.
//
// # Concurrent checkpoints
//
// Checkpoints overlap. A source subtask injects barrier k+1 as soon as its
// interval comes round, whether or not k has completed anywhere, and with
// sources of different lengths one input can be several barriers ahead of
// another. So acknowledgement state is per checkpoint ID in a map, never a
// single counter: a counter would fold two checkpoints' acknowledgements
// together and complete one of them on the other's evidence.
type Coordinator struct {
	storage *Storage
	meta    Metadata
	// expected is every subtask of the job, held as a set so an acknowledgement
	// from a subtask the job does not have is rejected rather than counted.
	expected map[SubtaskKey]bool

	mu    sync.Mutex
	state map[int64]*checkpointState
}

// checkpointState is one checkpoint's progress.
//
// acked is dropped once the checkpoint reaches a terminal state, so a long run
// keeps one small record per checkpoint rather than one record per subtask per
// checkpoint. The record itself is kept: a later acknowledgement for a
// checkpoint that has already finished has to be told apart from the first one.
type checkpointState struct {
	acked     map[SubtaskKey]bool
	complete  bool
	abandoned bool
}

func (s *checkpointState) terminal() bool { return s.complete || s.abandoned }

// NewCoordinator returns a coordinator that writes checkpoints of the job
// described by meta into storage.
func NewCoordinator(storage *Storage, meta Metadata) *Coordinator {
	expected := make(map[SubtaskKey]bool)
	for _, key := range meta.Subtasks() {
		expected[key] = true
	}
	return &Coordinator{
		storage:  storage,
		meta:     meta,
		expected: expected,
		state:    make(map[int64]*checkpointState),
	}
}

// Expected returns the number of acknowledgements a checkpoint needs: the sum
// of parallelism over every vertex.
func (c *Coordinator) Expected() int { return len(c.expected) }

// Acknowledge records that key has snapshotted its state for checkpoint id, and
// completes the checkpoint if it was the last one outstanding.
//
// The payload is written to disk before the acknowledgement is counted, so the
// count is a count of state that is durable rather than of state that was
// promised. If the write fails the checkpoint is ABANDONED here rather than
// left outstanding: the subtask that failed is about to return an error and
// will not try again, so a checkpoint waiting for it would sit incomplete
// forever and a later one would complete on top of it.
//
// An error returned here is the calling subtask's failure. A checkpoint that
// cannot be written is not a checkpoint that is skipped: the job that continued
// would be running with no usable recovery point and nothing would say so.
func (c *Coordinator) Acknowledge(id int64, key SubtaskKey, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.expected[key] {
		return fmt.Errorf("checkpoint %d: %w: %s", id, errUnknownSubtask, key)
	}
	st := c.stateFor(id)
	if st.abandoned {
		// Nothing to record and nothing wrong. This subtask reached its barrier
		// after some other subtask had already failed at the same checkpoint;
		// the directory is left as it was at that moment, which is what makes
		// it evidence.
		return nil
	}
	if st.complete || st.acked[key] {
		return fmt.Errorf("checkpoint %d: %w: %s", id, errDuplicateAck, key)
	}

	if err := c.storage.WriteSubtaskState(id, key, payload); err != nil {
		st.abandoned = true
		st.acked = nil
		return err
	}
	st.acked[key] = true
	if len(st.acked) < len(c.expected) {
		return nil
	}

	if err := c.storage.Complete(id, c.meta); err != nil {
		st.abandoned = true
		st.acked = nil
		return err
	}
	st.complete = true
	st.acked = nil
	return nil
}

// Fail abandons checkpoint id because key could not snapshot.
//
// The partial directory is LEFT IN PLACE. _COMPLETE is never written, so Latest
// will not select it and Load refuses it, which is all that correctness needs;
// deleting it would additionally throw away the only record of how far the
// checkpoint got. Phase 4 wants to look at that.
//
// Abandoning k says nothing about k+1. Checkpoints are independent cuts, and a
// source that keeps producing keeps injecting barriers, so the next one can
// complete normally and become the recovery point.
func (c *Coordinator) Fail(id int64, key SubtaskKey, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.stateFor(id)
	if st.complete {
		// Every subtask acknowledged and the checkpoint is on disk. Whatever
		// this subtask failed at afterwards is not this checkpoint's problem,
		// and unmarking a completed checkpoint would be worse than useless: it
		// is already durable and already selectable.
		return
	}
	st.abandoned = true
	st.acked = nil
}

// Completed reports whether checkpoint id has been written in full.
func (c *Coordinator) Completed(id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[id]
	return ok && st.complete
}

// Abandoned reports whether checkpoint id was given up on.
func (c *Coordinator) Abandoned(id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[id]
	return ok && st.abandoned
}

// Acked returns how many subtasks have acknowledged checkpoint id and not yet
// completed it.
//
// It reads zero for a checkpoint that has finished, because the per-subtask
// record is dropped at that point. Tests use it to observe the ORDER of a
// subtask's snapshot against what it does next, which is the only way to assert
// that the snapshot happened first rather than that both eventually happened.
func (c *Coordinator) Acked(id int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[id]
	if !ok {
		return 0
	}
	return len(st.acked)
}

// stateFor returns the record for checkpoint id, creating it on first mention.
// The caller holds the mutex.
func (c *Coordinator) stateFor(id int64) *checkpointState {
	st, ok := c.state[id]
	if !ok {
		st = &checkpointState{acked: make(map[SubtaskKey]bool)}
		c.state[id] = st
	}
	return st
}
