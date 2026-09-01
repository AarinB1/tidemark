package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/sinks"
)

// The sink half of the checkpoint protocol.
//
// core.Sink gained Snapshot and NotifyCheckpointComplete in Phase 5, and the
// gap between them is exactly-once output. What these tests pin is the ORDER
// the runtime calls them in and the state of the disk when it does, because
// every way of getting that wrong is silent: committing early duplicates on
// recovery, and never committing loses.

// sinkCall is one thing the runtime asked of a recording sink.
type sinkCall struct {
	// What was called: "open", "write", "snapshot", "notify" or "close".
	Kind string
	// CheckpointID is meaningful for "snapshot" and "notify".
	CheckpointID int64
	// CompleteOnDisk is whether <root>/chk-<id>/_COMPLETE existed at the moment
	// of a "notify" call. It is the whole of invariant 4 as an observation: a
	// notification that arrived before the marker was durable would be a
	// licence to commit a checkpoint that may never exist.
	CompleteOnDisk bool
}

func (c sinkCall) String() string {
	if c.Kind == "snapshot" || c.Kind == "notify" {
		return c.Kind + " " + strconv.FormatInt(c.CheckpointID, 10)
	}
	return c.Kind
}

// recordingSink records what the runtime asks of it, in order, and can be told
// to fail its notifications.
//
// It is shared by every sink subtask of a job, which is why it locks. That is a
// property of the TEST rather than a claim about core.Sink: the runtime gives
// one sink to one subtask goroutine, and this one is handed to all of them so
// that the calls can be read back in a single sequence.
type recordingSink struct {
	// root is the checkpoint root, so a notification can look for the marker.
	root string
	// failNotify, when true, makes every NotifyCheckpointComplete fail.
	failNotify bool

	mu       sync.Mutex
	calls    []sinkCall
	subtasks []string
	// nextSnapshot is what Snapshot writes, so the payload the coordinator
	// stores can be told apart from the empty one a sink used to record.
	snapshots int64
}

var _ core.Sink = (*recordingSink)(nil)

func (s *recordingSink) Open(ctx core.Context) error {
	vertexID, index := ctx.Subtask()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, sinkCall{Kind: "open"})
	s.subtasks = append(s.subtasks, fmt.Sprintf("%s[%d]", vertexID, index))
	return nil
}

func (s *recordingSink) Write(rec *core.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, sinkCall{Kind: "write"})
	return nil
}

// Snapshot writes a payload that is not empty, so that a runtime which stored
// nothing is distinguishable from one that stored what the sink wrote.
func (s *recordingSink) Snapshot(w io.Writer) error {
	s.mu.Lock()
	s.snapshots++
	n := s.snapshots
	s.calls = append(s.calls, sinkCall{Kind: "snapshot"})
	s.mu.Unlock()
	if _, err := fmt.Fprintf(w, "staged-%d", n); err != nil {
		return err
	}
	return nil
}

func (s *recordingSink) NotifyCheckpointComplete(checkpointID int64) error {
	_, err := os.Stat(filepath.Join(s.root, "chk-"+strconv.FormatInt(checkpointID, 10), "_COMPLETE"))
	s.mu.Lock()
	s.calls = append(s.calls, sinkCall{Kind: "notify", CheckpointID: checkpointID, CompleteOnDisk: err == nil})
	fail := s.failNotify
	s.mu.Unlock()
	if fail {
		return errors.New("the sink cannot commit right now")
	}
	return nil
}

func (s *recordingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, sinkCall{Kind: "close"})
	return nil
}

func (s *recordingSink) recorded() []sinkCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *recordingSink) identities() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(s.subtasks)
	slices.Sort(out)
	return out
}

// sinkContractGraph is one generator source feeding a keyed count and a sink,
// every vertex at parallelism p.
//
// It reuses the recovery suite's helpers on purpose: what these tests assert is
// about the protocol the runtime runs, so the workload under it should be one
// the rest of the package already trusts rather than a second one written here.
//
// The sink instance is SHARED across subtasks, which is what countingGraph
// does. That is right for the recording sinks here and for sinks.Collect, both
// of which lock; it is wrong for a sink that owns a file handle, which is what
// sinkFactoryGraph below exists for.
func sinkContractGraph(t *testing.T, sink core.Sink, p int, count int64, barrierInterval int64) *graph.Graph {
	t.Helper()
	return countingGraph(t, sink, p,
		countingSourceVertex("src", restoreConfig(11, count), p, barrierInterval, nil))
}

// sinkFactoryGraph is a source, an identity map and a sink, with the sink built
// PER SUBTASK.
//
// Two things separate it from sinkContractGraph, and both are load-bearing.
//
// The sink is built per subtask because a subtask is the unit of state, and a
// sink that owns a file handle and an epoch counter is per-subtask state.
// Sharing one across subtasks gives several goroutines one buffered writer:
// they write into one staging file and commit it under one name, so half the
// output disappears. sinks.Transactional refuses to be opened twice for exactly
// that reason, and this is the shape a job wiring one has to use.
//
// The operator is an identity MAP rather than the keyed count, because a keyed
// count emits only at end of stream. Every epoch before the last barrier would
// then be empty, there would be no transaction to commit during the run, and a
// test asserting on committed files would be asserting on an empty directory --
// which is what it does if the operator is chosen for familiarity rather than
// for whether the property under test can happen.
func sinkFactoryGraph(t *testing.T, newSink func() core.Sink, p int, count int64, barrierInterval int64) *graph.Graph {
	t.Helper()
	return buildGraph(t, []graph.Vertex{
		countingSourceVertex("src", restoreConfig(11, count), p, barrierInterval, nil),
		{ID: "id", Kind: graph.VertexOperator, Parallelism: p, NewOperator: identity},
		{ID: "out", Kind: graph.VertexSink, Parallelism: p, NewSink: newSink},
	}, [][2]string{{"src", "id"}, {"id", "out"}})
}

// TestASinkIsNotifiedOnlyAfterCompleteIsDurable is invariant 4 as an
// observation of the disk.
//
// The sink stats _COMPLETE at the moment it is notified. A runtime that
// notified during the snapshot, or anywhere before Storage.Complete returned,
// would hand a sink a licence to commit a checkpoint that may never exist --
// and the run that recovered from the previous checkpoint would replay those
// records, so the sink would be the thing producing duplicates.
//
// Two sink subtasks, because that is where the ordering can go wrong: a
// checkpoint completes inside the LAST sink subtask's acknowledgement, so a
// design that pushed the notification would reach the other subtask from a
// foreign goroutine and could reach it before the marker landed.
func TestASinkIsNotifiedOnlyAfterCompleteIsDurable(t *testing.T) {
	root := t.TempDir()
	sink := &recordingSink{root: root}
	g := sinkContractGraph(t, sink, 2, 4000, 500)

	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	notifications := 0
	for _, c := range sink.recorded() {
		if c.Kind != "notify" {
			continue
		}
		notifications++
		if !c.CompleteOnDisk {
			t.Errorf("the sink was notified for checkpoint %d while %s was not on disk: "+
				"a sink that committed there would commit a checkpoint that may never complete",
				c.CheckpointID, filepath.Join(root, fmt.Sprintf("chk-%d", c.CheckpointID), "_COMPLETE"))
		}
	}
	if notifications == 0 {
		t.Fatal("the sink was never notified, so the assertion above compared nothing: " +
			"either no checkpoint completed or the runtime does not notify at all")
	}
	t.Logf("%d notifications, every one of them after _COMPLETE was durable", notifications)
}

// TestASinkIsNotifiedForEveryCheckpointItStaged is the other direction, and it
// is the one that catches LOSS rather than duplication.
//
// Every checkpoint this sink acknowledged and that completed must reach it as a
// notification before the job ends. The window is real: sink subtask 0 can
// reach end of stream and return before sink subtask 1 acknowledges checkpoint
// k, and nothing would then commit subtask 0's transaction for k. On a clean
// run there is no recovery to fix that up, so the records are simply absent.
//
// Two sink subtasks, because with one there is nobody to be waited for.
//
// Repeated, because the window is a race and one attempt is a weak detector.
// Removing the wait was measured at four failures in five attempts of a single
// run: whether subtask 0 reaches end of stream before subtask 1 acknowledges is
// the Go scheduler's to decide, and on the attempt where it does not, a runtime
// with no wait at all looks correct. Over these attempts a miss needs every one
// of them to go the same way.
func TestASinkIsNotifiedForEveryCheckpointItStaged(t *testing.T) {
	for attempt := range sinkNotificationAttempts {
		t.Run(fmt.Sprintf("attempt%d", attempt), func(t *testing.T) {
			assertEveryCompletedCheckpointWasNotified(t)
		})
	}
}

// sinkNotificationAttempts is how many times the run above is repeated. Eight
// takes about a second and puts a miss, at the measured one-in-five per
// attempt, below one run in three thousand.
const sinkNotificationAttempts = 8

func assertEveryCompletedCheckpointWasNotified(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	sink := &recordingSink{root: root}
	g := sinkContractGraph(t, sink, 2, 4000, 250)

	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	// What completed, read from the disk rather than from the coordinator: the
	// marker is what a recovery selects on, so it is what a notification owes
	// its existence to.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", root, err)
	}
	var complete []int64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id, convErr := strconv.ParseInt(e.Name()[len("chk-"):], 10, 64)
		if convErr != nil {
			continue
		}
		if _, statErr := os.Stat(filepath.Join(root, e.Name(), "_COMPLETE")); statErr == nil {
			complete = append(complete, id)
		}
	}
	slices.Sort(complete)
	if len(complete) == 0 {
		t.Fatal("no checkpoint completed, so this test asserts nothing")
	}

	// One notification per completed checkpoint per sink subtask.
	notified := make(map[int64]int)
	for _, c := range sink.recorded() {
		if c.Kind == "notify" {
			notified[c.CheckpointID]++
		}
	}
	for _, id := range complete {
		if got := notified[id]; got != 2 {
			t.Errorf("checkpoint %d completed and %d of the 2 sink subtasks were notified: "+
				"a subtask that is not told cannot commit, and on a clean run nothing else will",
				id, got)
		}
	}
	t.Logf("%d complete checkpoints, each notified to both sink subtasks", len(complete))
}

// TestASinkSnapshotIsWhatTheCoordinatorStores.
//
// A sink acknowledged with an empty payload for three phases. It now writes
// which staged transaction belongs to the checkpoint, and this is the assertion
// that the runtime stores what the sink wrote rather than the nil it used to
// pass. Without it, sinks.Transactional would stage correctly, record
// correctly, and restore from an empty payload.
func TestASinkSnapshotIsWhatTheCoordinatorStores(t *testing.T) {
	root := t.TempDir()
	sink := &recordingSink{root: root}
	g := sinkContractGraph(t, sink, 1, 2000, 500)

	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the run failed: %v", err)
	}

	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (%d, %t, %v), want a complete checkpoint", id, ok, err)
	}
	_, payloads, err := storage.Load(id)
	if err != nil {
		t.Fatalf("Load(%d): %v", id, err)
	}
	payload, ok := payloads[subtaskID{vertexID: "out", index: 0}.checkpointKey()]
	if !ok {
		t.Fatalf("checkpoint %d holds no state for the sink", id)
	}
	if len(payload) == 0 {
		t.Fatalf("checkpoint %d stored an empty payload for the sink: what core.Sink.Snapshot "+
			"wrote was thrown away, and a transactional sink would restore from nothing", id)
	}
	if got := string(payload); got[:len("staged-")] != "staged-" {
		t.Errorf("checkpoint %d stored %q for the sink, which is not what its Snapshot wrote", id, got)
	}
}

// TestANotificationFailureDoesNotFailTheJob.
//
// The checkpoint IS complete when the notification goes out: _COMPLETE is on
// the disk and it is a valid recovery point. A job that failed here would be a
// job destroyed by the success of its own checkpoint, and the sink that could
// not commit now commits on restore instead -- which is a path that has to
// exist anyway, because a crash between _COMPLETE and the notification means no
// notification ever arrives.
func TestANotificationFailureDoesNotFailTheJob(t *testing.T) {
	root := t.TempDir()
	sink := &recordingSink{root: root, failNotify: true}
	g := sinkContractGraph(t, sink, 2, 4000, 250)

	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the run failed because a notification did: %v", err)
	}

	notifications := 0
	for _, c := range sink.recorded() {
		if c.Kind == "notify" {
			notifications++
		}
	}
	if notifications == 0 {
		t.Fatal("no notification was attempted, so nothing failed and this test asserts nothing")
	}
	if _, ok, err := checkpoint.NewStorage(root).Latest(); err != nil || !ok {
		t.Fatalf("Latest = (%t, %v), want a complete checkpoint: the checkpoints are valid "+
			"recovery points whether or not the sink could act on them", ok, err)
	}
}

// TestASinkLearnsItsOwnSubtaskIdentity.
//
// core.Context.Subtask is what lets a sink name what it writes outside the
// runtime, and the transactional sink's deduplication IS that naming. Two
// subtasks that reported the same identity would commit to one path and one
// half of the output would disappear.
//
// Parallelism 2, because at 1 every wrong answer is also the right one.
func TestASinkLearnsItsOwnSubtaskIdentity(t *testing.T) {
	sink := &recordingSink{root: t.TempDir()}
	g := sinkContractGraph(t, sink, 2, 2000, 500)

	if err := Run(context.Background(), g); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	want := []string{"out[0]", "out[1]"}
	if got := sink.identities(); !slices.Equal(got, want) {
		t.Errorf("the sink subtasks reported %v, want %v: two subtasks sharing an identity would "+
			"share a committed file name", got, want)
	}
}

// TestASinkStagesEvenWithoutCheckpointing.
//
// A job with no checkpoint root still calls Snapshot at every barrier. Barriers
// flow whether or not anybody records snapshots, and a sink that staged only
// when checkpointing was on would be running a different code path in the two
// cases -- with the untested one being the one every existing test uses.
func TestASinkStagesEvenWithoutCheckpointing(t *testing.T) {
	sink := &recordingSink{root: t.TempDir()}
	g := sinkContractGraph(t, sink, 1, 2000, 500)

	if err := Run(context.Background(), g); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	snapshots, notifications := 0, 0
	for _, c := range sink.recorded() {
		switch c.Kind {
		case "snapshot":
			snapshots++
		case "notify":
			notifications++
		}
	}
	if snapshots == 0 {
		t.Error("a job that takes no checkpoints never asked the sink to stage")
	}
	if notifications != 0 {
		t.Errorf("a job that takes no checkpoints notified the sink %d times: nothing completed, "+
			"so nothing licensed a commit", notifications)
	}
}

// The restore half.
//
// A sink's restore is the only thing that will ever commit the transaction
// staged at the checkpoint a run resumes from: the crash may have landed after
// _COMPLETE and before the notification, and nothing repeats a notification.
// These assert that the runtime actually calls it, because a sink whose Restore
// is perfect and never invoked fails in exactly the same silent way.

// restorableRecordingSink is a recordingSink that also remembers what it was
// handed to restore from.
//
// Restore is declared here rather than on recordingSink so that the two cases
// below -- a sink that can restore and one that cannot -- are two types rather
// than one type with a flag. The runtime distinguishes them with an interface
// assertion, and a flag would test the flag.
type restorableRecordingSink struct {
	recordingSink

	restoreMu sync.Mutex
	restored  [][]byte
}

func (s *restorableRecordingSink) Restore(r io.Reader) error {
	payload, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()
	s.restored = append(s.restored, payload)
	return nil
}

func (s *restorableRecordingSink) restorePayloads() [][]byte {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()
	return slices.Clone(s.restored)
}

// TestARestoredSinkIsHandedTheCheckpointsPayload.
//
// One run writes checkpoints, a second resumes from the newest. Each sink
// subtask must be handed the bytes ITS OWN Snapshot wrote at that checkpoint --
// not nil, not another subtask's. The payload names the staged transaction, so
// a subtask given the wrong one commits the wrong file, and both files exist:
// the failure is wrong output rather than a missing file.
//
// Two sink subtasks, because at parallelism 1 every payload is the right one.
func TestARestoredSinkIsHandedTheCheckpointsPayload(t *testing.T) {
	root := t.TempDir()
	first := &restorableRecordingSink{recordingSink: recordingSink{root: root}}
	g := sinkContractGraph(t, first, 2, 4000, 500)
	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the first run failed: %v", err)
	}

	storage := checkpoint.NewStorage(root)
	id, ok, err := storage.Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (%d, %t, %v), want a complete checkpoint", id, ok, err)
	}
	_, payloads, err := storage.Load(id)
	if err != nil {
		t.Fatalf("Load(%d): %v", id, err)
	}

	second := &restorableRecordingSink{recordingSink: recordingSink{root: root}}
	g2 := sinkContractGraph(t, second, 2, 4000, 500)
	if err := RunWithOptions(context.Background(), g2, Options{
		CheckpointRoot: root, RestoreFrom: root, Seed: 5,
	}); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}

	got := second.restorePayloads()
	if len(got) != 2 {
		t.Fatalf("Restore was called %d times, want once per sink subtask (2): a subtask that is "+
			"not restored never commits the transaction the checkpoint vouched for", len(got))
	}
	var want [][]byte
	for index := range 2 {
		want = append(want, payloads[subtaskID{vertexID: "out", index: index}.checkpointKey()])
	}
	for _, payload := range got {
		if len(payload) == 0 {
			t.Errorf("a sink subtask was restored from an empty payload; checkpoint %d holds %q and %q",
				id, want[0], want[1])
			continue
		}
		if !slices.ContainsFunc(want, func(w []byte) bool { return string(w) == string(payload) }) {
			t.Errorf("a sink subtask was restored from %q, which checkpoint %d does not hold for "+
				"either subtask (%q, %q)", payload, id, want[0], want[1])
		}
	}
}

// TestASinkThatRecordedStateAndCannotRestoreItIsRefused.
//
// The runtime finds a sink's Restore by an interface assertion, and the way an
// assertion fails is silently. A sink that staged a transaction, recorded it,
// and has no way to read it back would come up on restore having forgotten
// everything -- the transaction stays staged forever and its records are simply
// absent from the output.
//
// So it is refused with an error naming the type. This is also the guard on
// decorators: embedding core.Sink satisfies core.Sink and not restorableSink,
// so a wrapper that forgot to forward Restore fails here rather than quietly
// skipping it.
func TestASinkThatRecordedStateAndCannotRestoreItIsRefused(t *testing.T) {
	root := t.TempDir()
	// recordingSink writes a non-empty payload and has no Restore.
	sink := &recordingSink{root: root}
	g := sinkContractGraph(t, sink, 1, 2000, 500)
	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the first run failed: %v", err)
	}

	g2 := sinkContractGraph(t, &recordingSink{root: root}, 1, 2000, 500)
	err := RunWithOptions(context.Background(), g2, Options{
		CheckpointRoot: root, RestoreFrom: root, Seed: 5,
	})
	if err == nil {
		t.Fatal("the resumed run succeeded with a sink that cannot read the state the checkpoint " +
			"holds for it: the staged transaction is never committed and its records are absent")
	}
	if !strings.Contains(err.Error(), "has no Restore") {
		t.Errorf("the run failed with %v, which does not name the missing Restore", err)
	}
}

// TestASinkWithNoRecordedStateRestoresWithoutOne.
//
// The other side of the assertion. sinks.Collect and sinks.Discard record an
// empty payload and have nothing to bring back, and a runtime that demanded a
// Restore from every sink would make every fake in every test implement a
// no-op. An empty payload and no Restore is the ordinary case, not an error.
func TestASinkWithNoRecordedStateRestoresWithoutOne(t *testing.T) {
	root := t.TempDir()
	g := sinkContractGraph(t, sinks.NewCollect(), 2, 4000, 500)
	if err := RunWithOptions(context.Background(), g, Options{CheckpointRoot: root, Seed: 5}); err != nil {
		t.Fatalf("the first run failed: %v", err)
	}
	g2 := sinkContractGraph(t, sinks.NewCollect(), 2, 4000, 500)
	if err := RunWithOptions(context.Background(), g2, Options{
		CheckpointRoot: root, RestoreFrom: root, Seed: 5,
	}); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}
}

// TestTheTransactionalSinkCommitsAcrossARealRecovery.
//
// End to end through the runtime: a run aborted by a fault part way through,
// then a run resumed from the newest complete checkpoint. What is asserted is
// the COMMITTED FILE SET, because that is what says which transactions became
// output -- and because the records inside them are replayable, so a contents
// comparison alone cannot tell a committed transaction from a re-derived one.
//
// The fault is what makes this a recovery rather than a re-run. Without it the
// first run reaches its last checkpoint at the end of the input, the resumed
// run has nothing left to replay, and every assertion below is about a job that
// recovered from nothing -- which is the shape this test had before, and it
// passed.
//
// The tail of each run is NOT committed here. Records after the last barrier
// belong to no checkpoint and nothing will ever notify for them; that is the
// final epoch, and it arrives in the next step.
func TestTheTransactionalSinkCommitsAcrossARealRecovery(t *testing.T) {
	checkpoints := t.TempDir()
	output := t.TempDir()
	newTransactional := func() core.Sink { return sinks.NewTransactional(output) }

	// Abort one source subtask three quarters of the way through its range, so
	// several checkpoints are behind the fault and a quarter of the input is in
	// front of it. Both halves matter: without checkpoints behind it there is
	// nothing committed to carry across, and without input in front the resumed
	// run replays nothing and recovers from nothing.
	abort := &recordingInjector{fire: func(c consultation) bool {
		return c.site == "before-element" && c.vertexID == "src" && c.subtask == 0 && c.n == 3000
	}}
	g := sinkFactoryGraph(t, newTransactional, 2, 8000, 500)
	err := RunWithOptions(context.Background(), g, Options{
		CheckpointRoot: checkpoints, Seed: 5, FaultInjector: abort,
	})
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("the first run = %v, want the injected fault", err)
	}

	afterFirst, err := sinks.CommittedFiles(output)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	stagedAfterFirst, err := sinks.StagingFiles(output)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if len(afterFirst) == 0 {
		t.Fatal("the aborted run committed nothing, so this test asserts nothing about recovery")
	}
	if len(stagedAfterFirst) == 0 {
		t.Fatal("the aborted run left nothing staged, so the discard-above-the-cut path is untouched")
	}

	restored, ok, err := checkpoint.NewStorage(checkpoints).Latest()
	if err != nil || !ok {
		t.Fatalf("Latest = (%d, %t, %v), want the checkpoint the resume will use", restored, ok, err)
	}

	g2 := sinkFactoryGraph(t, newTransactional, 2, 8000, 500)
	if err := RunWithOptions(context.Background(), g2, Options{
		CheckpointRoot: checkpoints, RestoreFrom: checkpoints, Seed: 5,
	}); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}

	afterSecond, err := sinks.CommittedFiles(output)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	// A commit is a rename into committed/ and nothing renames back, so the
	// resumed run can only add.
	for _, name := range afterFirst {
		if !slices.Contains(afterSecond, name) {
			t.Errorf("%s was committed by the aborted run and is gone after the resumed one", name)
		}
	}
	if len(afterSecond) <= len(afterFirst) {
		t.Errorf("the aborted run committed %d files and the resumed one committed %d in total: "+
			"the resumed run reached checkpoints of its own and committed none of them",
			len(afterFirst), len(afterSecond))
	}
	// Every committed transaction the ABORTED run left staged above the cut is
	// gone: those epochs never completed and their records are replayed.
	staged, err := sinks.StagingFiles(output)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	for _, name := range staged {
		if slices.Contains(stagedAfterFirst, name) {
			t.Errorf("%s was staged by the aborted run and is still staged after the resumed one; "+
				"the resume was at checkpoint %d and everything above it should have been discarded",
				name, restored)
		}
	}
	t.Logf("aborted run: %d committed, %d staged; resumed run: %d committed in total, restored at "+
		"checkpoint %d", len(afterFirst), len(stagedAfterFirst), len(afterSecond), restored)
}
