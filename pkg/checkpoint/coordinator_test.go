package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// coordinatorMetadata is a job with four subtasks across three vertices: two
// source subtasks, one operator subtask and one sink subtask.
//
// The sink is in the count on purpose. It acknowledges with an empty payload
// and has nothing to record until Phase 5, and leaving it out of the expected
// set is the shortcut this protocol is written against.
func coordinatorMetadata() Metadata {
	return Metadata{
		Seed: 3,
		Vertices: []VertexMeta{
			{ID: "op", Parallelism: 1},
			{ID: "out", Parallelism: 1},
			{ID: "src", Parallelism: 2, Count: 100},
		},
	}
}

func newTestCoordinator(t *testing.T) (*Coordinator, *Storage) {
	t.Helper()
	s := NewStorage(t.TempDir())
	return NewCoordinator(s, coordinatorMetadata()), s
}

// ackAll acknowledges checkpoint id from every subtask but the ones in skip.
func ackAll(t *testing.T, c *Coordinator, id int64, skip ...SubtaskKey) {
	t.Helper()
	for _, key := range coordinatorMetadata().Subtasks() {
		if containsKey(skip, key) {
			continue
		}
		if err := c.Acknowledge(id, key, []byte(key.String())); err != nil {
			t.Fatalf("Acknowledge(%d, %s): %v", id, key, err)
		}
	}
}

func containsKey(keys []SubtaskKey, want SubtaskKey) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

// TestPartialAcknowledgementWritesNothingUsable is the first half of invariant
// 8: a checkpoint that not every subtask reached is not a recovery point.
//
// The state files ARE on disk, and that is correct. What is absent is
// _COMPLETE, so Latest does not select the checkpoint and Load refuses it. A
// coordinator that wrote the marker as it went would pass every other test in
// this file and fail this one.
func TestPartialAcknowledgementWritesNothingUsable(t *testing.T) {
	c, s := newTestCoordinator(t)
	missing := SubtaskKey{VertexID: "src", Index: 1}

	ackAll(t, c, 1, missing)

	if c.Completed(1) {
		t.Fatal("the coordinator completed a checkpoint one subtask had not acknowledged")
	}
	if got, want := c.Acked(1), c.Expected()-1; got != want {
		t.Errorf("%d subtasks acknowledged, want %d", got, want)
	}
	if _, ok, err := s.Latest(); err != nil || ok {
		t.Fatalf("Latest = (ok %t, err %v), want no complete checkpoint", ok, err)
	}
	if _, _, err := s.Load(1); !errors.Is(err, errNoCheckpoint) {
		t.Errorf("Load of a partial checkpoint = %v, want %v", err, errNoCheckpoint)
	}

	// The last acknowledgement is what makes it usable, and nothing else is.
	if err := c.Acknowledge(1, missing, []byte("late")); err != nil {
		t.Fatalf("Acknowledge from the last subtask: %v", err)
	}
	if !c.Completed(1) {
		t.Fatal("the checkpoint is not complete after every subtask acknowledged")
	}
	id, ok, err := s.Latest()
	if err != nil || !ok || id != 1 {
		t.Fatalf("Latest = (%d, ok %t, err %v), want checkpoint 1", id, ok, err)
	}
}

// TestFullAcknowledgementWritesEveryPayload checks that what the subtasks
// handed over is what a restore will read back, per subtask.
func TestFullAcknowledgementWritesEveryPayload(t *testing.T) {
	c, s := newTestCoordinator(t)
	meta := coordinatorMetadata()

	ackAll(t, c, 1)

	gotMeta, payloads, err := s.Load(1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if gotMeta.Seed != meta.Seed {
		t.Errorf("the checkpoint records seed %d, want %d", gotMeta.Seed, meta.Seed)
	}
	for _, key := range meta.Subtasks() {
		if got, ok := payloads[key]; !ok {
			t.Errorf("no payload for %s", key)
		} else if string(got) != key.String() {
			t.Errorf("payload for %s is %q, want %q", key, got, key.String())
		}
	}
}

// TestFailedSnapshotAbandonsOneCheckpointAndNotTheNext is the case a job
// actually hits: one subtask cannot write its state, and the job carries on
// long enough to take another checkpoint.
//
// Two properties, and both matter. The abandoned checkpoint never becomes a
// recovery point, so a restore does not resume from a cut that half the job
// never agreed to. And the abandonment does not poison k+1: checkpoints are
// independent cuts, so a coordinator that latched a failure would leave a job
// running with no recovery point at all and nothing saying so.
func TestFailedSnapshotAbandonsOneCheckpointAndNotTheNext(t *testing.T) {
	c, s := newTestCoordinator(t)
	broken := SubtaskKey{VertexID: "op", Index: 0}

	// Some subtasks got as far as writing state before the failure.
	if err := c.Acknowledge(1, SubtaskKey{VertexID: "src", Index: 0}, []byte("src0")); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	c.Fail(1, broken, errors.New("state backend failed"))

	if !c.Abandoned(1) {
		t.Fatal("the coordinator did not abandon a checkpoint a subtask failed")
	}
	// A straggler acknowledging an abandoned checkpoint is not an error. It
	// reached its barrier after somebody else had already given up.
	if err := c.Acknowledge(1, SubtaskKey{VertexID: "out", Index: 0}, nil); err != nil {
		t.Errorf("Acknowledge on an abandoned checkpoint = %v, want nil", err)
	}
	if c.Completed(1) {
		t.Fatal("an abandoned checkpoint completed")
	}

	// The partial directory is left where it is. Phase 4 reads it to see how
	// far the checkpoint got before the job came apart.
	dir := s.dir(1)
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the abandoned checkpoint directory was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, completeName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the abandoned checkpoint has a %s marker", completeName)
	}
	if _, err := os.Stat(filepath.Join(dir, SubtaskKey{VertexID: "src", Index: 0}.fileName())); err != nil {
		t.Errorf("the state written before the failure is gone: %v", err)
	}

	// k+1 completes normally and is what a restore would select.
	ackAll(t, c, 2)
	if !c.Completed(2) {
		t.Fatal("the checkpoint after an abandoned one did not complete")
	}
	id, ok, err := s.Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (ok %t, err %v), want a checkpoint", ok, err)
	}
	if id != 2 {
		t.Errorf("Latest selected checkpoint %d, want 2", id)
	}
}

// TestFailAfterCompletionDoesNotUnmakeTheCheckpoint. A subtask can fail at the
// element AFTER the one that closed a checkpoint. The checkpoint is durable and
// selectable by then, and marking it abandoned would throw away the recovery
// point the job is about to need.
func TestFailAfterCompletionDoesNotUnmakeTheCheckpoint(t *testing.T) {
	c, s := newTestCoordinator(t)
	ackAll(t, c, 1)

	c.Fail(1, SubtaskKey{VertexID: "op", Index: 0}, errors.New("failed later"))

	if c.Abandoned(1) {
		t.Error("a completed checkpoint was abandoned by a later failure")
	}
	if !c.Completed(1) {
		t.Error("a completed checkpoint stopped being complete")
	}
	if _, _, err := s.Load(1); err != nil {
		t.Errorf("Load of the completed checkpoint: %v", err)
	}
}

// TestCheckpointsCompleteIndependentlyWhileInFlight is what the per-ID map
// buys.
//
// Three checkpoints are open at once, which is what two sources of different
// lengths produce: one input runs several barriers ahead of the other. The
// acknowledgements are interleaved, and a coordinator holding one counter would
// complete the first checkpoint to reach four acknowledgements from whichever
// subtasks happened to have spoken.
func TestCheckpointsCompleteIndependentlyWhileInFlight(t *testing.T) {
	c, s := newTestCoordinator(t)
	subtasks := coordinatorMetadata().Subtasks()

	// Every subtask acknowledges 1, 2 and 3 in that order, but the subtasks
	// interleave: subtask 0 gets through all three before subtask 1 starts.
	for _, key := range subtasks[:len(subtasks)-1] {
		for id := int64(1); id <= 3; id++ {
			if err := c.Acknowledge(id, key, []byte(key.String())); err != nil {
				t.Fatalf("Acknowledge(%d, %s): %v", id, key, err)
			}
		}
	}
	for id := int64(1); id <= 3; id++ {
		if c.Completed(id) {
			t.Fatalf("checkpoint %d completed with one subtask still outstanding", id)
		}
	}

	// The last subtask completes 1 and 3 but not 2, so a coordinator that let
	// the counts run together would complete 2 as well.
	last := subtasks[len(subtasks)-1]
	for _, id := range []int64{1, 3} {
		if err := c.Acknowledge(id, last, []byte(last.String())); err != nil {
			t.Fatalf("Acknowledge(%d, %s): %v", id, last, err)
		}
	}

	for _, tt := range []struct {
		id   int64
		want bool
	}{{id: 1, want: true}, {id: 2, want: false}, {id: 3, want: true}} {
		if got := c.Completed(tt.id); got != tt.want {
			t.Errorf("Completed(%d) = %t, want %t", tt.id, got, tt.want)
		}
	}

	// And the highest COMPLETE one is what a restore takes, over the incomplete
	// 2 sitting between them.
	id, ok, err := s.Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (ok %t, err %v), want a checkpoint", ok, err)
	}
	if id != 3 {
		t.Errorf("Latest selected %d, want 3", id)
	}
}

// TestConcurrentAcknowledgement runs the real shape: every subtask on its own
// goroutine, acknowledging every checkpoint. Under -race this is what says the
// mutex covers the state it needs to.
func TestConcurrentAcknowledgement(t *testing.T) {
	const checkpoints = 20

	// A wider job than the others here, so there are more goroutines than a
	// scheduler will run in lockstep.
	meta := Metadata{
		Seed: 1,
		Vertices: []VertexMeta{
			{ID: "op", Parallelism: 8},
			{ID: "out", Parallelism: 4},
			{ID: "src", Parallelism: 8, Count: 10000},
		},
	}
	s := NewStorage(t.TempDir())
	c := NewCoordinator(s, meta)

	var wg sync.WaitGroup
	errs := make(chan error, len(meta.Subtasks()))
	for _, key := range meta.Subtasks() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := int64(1); id <= checkpoints; id++ {
				if err := c.Acknowledge(id, key, fmt.Appendf(nil, "%s at %d", key, id)); err != nil {
					errs <- fmt.Errorf("Acknowledge(%d, %s): %w", id, key, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for id := int64(1); id <= checkpoints; id++ {
		if !c.Completed(id) {
			t.Fatalf("checkpoint %d did not complete", id)
		}
	}
	id, ok, err := s.Latest()
	if err != nil || !ok || id != checkpoints {
		t.Fatalf("Latest = (%d, ok %t, err %v), want %d", id, ok, err, checkpoints)
	}
	// Every payload landed in the file belonging to the subtask that wrote it,
	// which is what a mutex covering the count but not the write would break.
	_, payloads, err := s.Load(checkpoints)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, key := range meta.Subtasks() {
		want := fmt.Sprintf("%s at %d", key, checkpoints)
		if got := string(payloads[key]); got != want {
			t.Errorf("payload for %s is %q, want %q", key, got, want)
		}
	}
}

// TestAcknowledgementProtocolFailures covers the things that cannot happen
// in a correct run.
//
// They are rejected rather than absorbed. A subtask the job does not have means
// the coordinator and the graph disagree about the job's shape, a duplicate
// acknowledgement means one subtask saw the same checkpoint ID twice, and a
// duplicate finish means a source reported end-of-stream twice. None of those
// would stop a checkpoint completing if they were tolerated -- the count is
// over distinct live subtasks -- so tolerating them would leave the symptom
// invisible.
func TestAcknowledgementProtocolFailures(t *testing.T) {
	c, _ := newTestCoordinator(t)
	known := SubtaskKey{VertexID: "op", Index: 0}

	if err := c.Acknowledge(1, SubtaskKey{VertexID: "nope", Index: 0}, nil); !errors.Is(err, errUnknownSubtask) {
		t.Errorf("Acknowledge from an unknown vertex = %v, want %v", err, errUnknownSubtask)
	}
	if err := c.Acknowledge(1, SubtaskKey{VertexID: "op", Index: 9}, nil); !errors.Is(err, errUnknownSubtask) {
		t.Errorf("Acknowledge from an out-of-range index = %v, want %v", err, errUnknownSubtask)
	}

	if err := c.Acknowledge(1, known, nil); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := c.Acknowledge(1, known, nil); !errors.Is(err, errDuplicateAck) {
		t.Errorf("a second Acknowledge from the same subtask = %v, want %v", err, errDuplicateAck)
	}

	// And on a checkpoint that has already completed.
	ackAll(t, c, 2)
	if err := c.Acknowledge(2, known, nil); !errors.Is(err, errDuplicateAck) {
		t.Errorf("Acknowledge on a completed checkpoint = %v, want %v", err, errDuplicateAck)
	}

	if err := c.Finished(SubtaskKey{VertexID: "nope", Index: 0}, nil); !errors.Is(err, errUnknownSubtask) {
		t.Errorf("Finished from an unknown vertex = %v, want %v", err, errUnknownSubtask)
	}
	if err := c.Finished(known, nil); err != nil {
		t.Fatalf("Finished: %v", err)
	}
	if err := c.Finished(known, nil); !errors.Is(err, errDuplicateFinish) {
		t.Errorf("a second Finished from the same subtask = %v, want %v", err, errDuplicateFinish)
	}
}

// TestFinishedSourceDoesNotBlockLaterCheckpoints is the coordinator's half of
// the gate's exhausted-input rule.
//
// Two source vertices, different barrier budgets. srcA acknowledges checkpoint
// 1 and then finishes; srcB keeps going. A coordinator that still expected
// srcA on checkpoint 2 would never write _COMPLETE: srcA will not speak again,
// and a job that kept running would have no recovery point past srcA's last
// barrier and nothing saying so.
//
// The payload written for srcA on that later checkpoint is the one Finished
// handed over -- the end of its range -- and not the offset it recorded at
// its last barrier. Reusing the last barrier's offset would replay the tail
// into a state that already counted it.
func TestFinishedSourceDoesNotBlockLaterCheckpoints(t *testing.T) {
	meta := Metadata{
		Seed: 3,
		Vertices: []VertexMeta{
			{ID: "op", Parallelism: 1},
			{ID: "out", Parallelism: 1},
			{ID: "srcA", Parallelism: 1, Count: 250},
			{ID: "srcB", Parallelism: 1, Count: 1000},
		},
	}
	s := NewStorage(t.TempDir())
	c := NewCoordinator(s, meta)

	srcA := SubtaskKey{VertexID: "srcA", Index: 0}
	srcB := SubtaskKey{VertexID: "srcB", Index: 0}
	op := SubtaskKey{VertexID: "op", Index: 0}
	out := SubtaskKey{VertexID: "out", Index: 0}

	ack := func(id int64, key SubtaskKey, payload string) {
		t.Helper()
		if err := c.Acknowledge(id, key, []byte(payload)); err != nil {
			t.Fatalf("Acknowledge(%d, %s): %v", id, key, err)
		}
	}

	// Checkpoint 1 is the last one srcA participates in.
	ack(1, srcA, "srcA@100")
	ack(1, srcB, "srcB@100")
	ack(1, op, "op@1")
	ack(1, out, "out@1")
	if !c.Completed(1) {
		t.Fatal("checkpoint 1 did not complete with every subtask still live")
	}

	// Checkpoint 2 is already waiting when srcA stops: everyone but srcA has
	// acknowledged, which is the shape the job actually hits -- srcB injects
	// the next barrier, the operator and sink align, and only then does srcA's
	// end-of-stream reach the coordinator.
	ack(2, srcB, "srcB@200")
	ack(2, op, "op@2")
	ack(2, out, "out@2")
	if c.Completed(2) {
		t.Fatal("checkpoint 2 completed without srcA and without srcA having finished")
	}
	if err := c.Finished(srcA, []byte("srcA@250")); err != nil {
		t.Fatalf("Finished: %v", err)
	}
	if !c.Completed(2) {
		t.Fatal("checkpoint 2 did not complete after the shorter source finished")
	}

	_, payloads, err := s.Load(1)
	if err != nil {
		t.Fatalf("Load(1): %v", err)
	}
	if got := string(payloads[srcA]); got != "srcA@100" {
		t.Errorf("checkpoint 1 recorded %q for srcA, want the acknowledgement, not the final payload", got)
	}

	_, payloads, err = s.Load(2)
	if err != nil {
		t.Fatalf("Load(2): %v", err)
	}
	if got := string(payloads[srcA]); got != "srcA@250" {
		t.Errorf("checkpoint 2 recorded %q for srcA, want the final payload", got)
	}
	if got := string(payloads[srcB]); got != "srcB@200" {
		t.Errorf("checkpoint 2 recorded %q for srcB, want its acknowledgement", got)
	}

	// A finished source does not stand in for a live one.
	ack(3, op, "op@3")
	ack(3, out, "out@3")
	if c.Completed(3) {
		t.Fatal("checkpoint 3 completed with srcB still live and silent")
	}
	ack(3, srcB, "srcB@300")
	if !c.Completed(3) {
		t.Fatal("checkpoint 3 did not complete after the remaining live source acknowledged")
	}
}

// TestUnwritableStorageAbandonsTheCheckpoint. A snapshot that cannot be
// written must fail the subtask AND give up on the checkpoint: the subtask is
// about to stop and will not try again, so a checkpoint still waiting for it
// would sit outstanding forever.
func TestUnwritableStorageAbandonsTheCheckpoint(t *testing.T) {
	// A file where the root directory should be, so MkdirAll cannot succeed.
	root := filepath.Join(t.TempDir(), "root")
	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	c := NewCoordinator(NewStorage(root), coordinatorMetadata())
	if err := c.Acknowledge(1, SubtaskKey{VertexID: "op", Index: 0}, nil); err == nil {
		t.Fatal("Acknowledge over an unwritable root returned nil")
	}
	if !c.Abandoned(1) {
		t.Error("a checkpoint whose write failed was left outstanding rather than abandoned")
	}
}

// The completion queries a sink subtask commits on.
//
// These are the coordinator's half of invariant 4. A sink stages at a barrier
// and commits when it learns the checkpoint completed, and "learns" means one
// of the two calls below -- never a push, because a checkpoint completes inside
// the last sink subtask's Acknowledge and a push would reach every OTHER sink
// subtask on a foreign goroutine, mid-Write.

// TestCompletedAmongReportsOnlyTheCompletedOnes.
//
// The interesting rows are the two that are not complete. An outstanding
// checkpoint must not be reported, because a sink that committed on it would be
// committing data belonging to a checkpoint that may never complete -- which is
// exactly the duplicate invariant 4 exists to prevent. An ABANDONED one must
// not be reported either, and for a different reason: it will never complete,
// so a sink that treated "terminal" as "committable" would commit a cut that no
// recovery will ever reproduce.
func TestCompletedAmongReportsOnlyTheCompletedOnes(t *testing.T) {
	c, _ := newTestCoordinator(t)

	// 1 completes, 2 is abandoned, 3 is left outstanding, 4 is never mentioned.
	ackAll(t, c, 1)
	c.Fail(2, SubtaskKey{VertexID: "op"}, errors.New("no disk"))
	ackAll(t, c, 3, SubtaskKey{VertexID: "out"})

	for _, tt := range []struct {
		name string
		ids  []int64
		want []int64
	}{
		{"the completed one", []int64{1}, []int64{1}},
		{"an abandoned one", []int64{2}, nil},
		{"an outstanding one", []int64{3}, nil},
		{"one nothing has mentioned", []int64{4}, nil},
		{"all of them, in order", []int64{1, 2, 3, 4}, []int64{1}},
		{"none", nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := c.CompletedAmong(tt.ids)
			if len(got) != len(tt.want) {
				t.Fatalf("CompletedAmong(%v) = %v, want %v", tt.ids, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("CompletedAmong(%v) = %v, want %v", tt.ids, got, tt.want)
				}
			}
		})
	}
}

// TestAwaitCompletedAmongWaitsForTheOutstandingOne.
//
// This is the end-of-stream case, and the race it closes is silent data loss:
// sink subtask 0 reaches end of stream and returns before sink subtask 1
// acknowledges checkpoint k, so nothing commits subtask 0's transaction for k
// and a clean run has no recovery to fix it up.
//
// The test asserts the WAIT and not merely the answer. A CompletedAmong that
// happened to be called late would return the same slice, so the assertion is
// that the call had not returned while the checkpoint was outstanding.
func TestAwaitCompletedAmongWaitsForTheOutstandingOne(t *testing.T) {
	c, _ := newTestCoordinator(t)
	ackAll(t, c, 1, SubtaskKey{VertexID: "out"})

	returned := make(chan []int64)
	go func() { returned <- c.AwaitCompletedAmong(context.Background(), []int64{1}) }()

	// Nothing has completed, so the call must still be blocked. Read with a
	// default rather than a timer: a sleep here would make the assertion "it
	// had not returned within some duration", which is a statement about the
	// scheduler. The goroutine may not have reached the call yet, and that is
	// fine -- this direction can only fail if it returned EARLY, which is the
	// failure being ruled out.
	select {
	case got := <-returned:
		t.Fatalf("AwaitCompletedAmong returned %v while checkpoint 1 was outstanding", got)
	default:
	}

	if err := c.Acknowledge(1, SubtaskKey{VertexID: "out"}, nil); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	got := <-returned
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("AwaitCompletedAmong = %v, want [1] once the last subtask acknowledged", got)
	}
}

// TestAwaitCompletedAmongReturnsOnAnAbandonedCheckpoint.
//
// Abandoned is terminal, so the wait ends -- and the checkpoint is not reported
// as committable. Without this the sink at end of stream would block forever on
// a checkpoint some subtask's failure had already given up on, and the job
// would hang in the runtime's wait for its goroutines rather than fail.
func TestAwaitCompletedAmongReturnsOnAnAbandonedCheckpoint(t *testing.T) {
	c, _ := newTestCoordinator(t)
	ackAll(t, c, 1, SubtaskKey{VertexID: "out"})
	ackAll(t, c, 2)

	go func() {
		c.Fail(1, SubtaskKey{VertexID: "out"}, errors.New("the sink died before its barrier"))
	}()

	got := c.AwaitCompletedAmong(context.Background(), []int64{1, 2})
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("AwaitCompletedAmong([1 2]) = %v, want [2]: 1 was abandoned and is not committable", got)
	}
}

// TestAwaitCompletedAmongGivesUpWhenTheJobIsCancelled.
//
// The remaining way a checkpoint never settles is a subtask that fails without
// telling the coordinator -- it returns an error, the executor cancels, and
// nobody abandons the checkpoint it had acknowledged. A wait with no way out
// would hold the whole job in its goroutine wait, turning one subtask's failure
// into a hang with no error anywhere.
//
// What comes back is what completed, which for a cancelled job is nothing worth
// committing: a cancelled job is a failed job, and the transaction it staged is
// the recovered run's to commit.
func TestAwaitCompletedAmongGivesUpWhenTheJobIsCancelled(t *testing.T) {
	c, _ := newTestCoordinator(t)
	ackAll(t, c, 1, SubtaskKey{VertexID: "out"})

	ctx, cancel := context.WithCancel(context.Background())
	go cancel()

	if got := c.AwaitCompletedAmong(ctx, []int64{1}); len(got) != 0 {
		t.Fatalf("AwaitCompletedAmong = %v, want nothing: checkpoint 1 never completed", got)
	}
}

// TestAwaitCompletedAmongDoesNotMissACompletionItRacedWith.
//
// The wake-up is a channel read outside the mutex, so the question is whether a
// completion landing between the state check and the select can be lost. It
// cannot: announce closes the channel the waiter already read under the same
// lock that guards the state it just examined, so a transition after the check
// closes the channel the waiter is about to select on.
//
// Run over many attempts because it is a race, and a race that reproduces once
// in fifty attempts is one a single attempt reports as passing.
func TestAwaitCompletedAmongDoesNotMissACompletionItRacedWith(t *testing.T) {
	for attempt := range 200 {
		c, _ := newTestCoordinator(t)
		ackAll(t, c, 1, SubtaskKey{VertexID: "out"})

		returned := make(chan []int64, 1)
		go func() { returned <- c.AwaitCompletedAmong(context.Background(), []int64{1}) }()
		go func() { _ = c.Acknowledge(1, SubtaskKey{VertexID: "out"}, nil) }()

		got := <-returned
		if len(got) != 1 || got[0] != 1 {
			t.Fatalf("attempt %d: AwaitCompletedAmong = %v, want [1]: a completion was lost between "+
				"the state check and the wait", attempt, got)
		}
	}
}
