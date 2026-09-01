package sinks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/state"
)

// sinkContext is the identity a sink learns from the runtime, standing in for
// the runtime's own opContext.
//
// Every method is written out rather than promoted from an embedded
// core.Context: an embedded one would compile and would panic on any method the
// interface grew, which is the trap CLAUDE.md records against decorators.
type sinkContext struct {
	vertexID string
	index    int
}

var _ core.Context = (*sinkContext)(nil)

func (c *sinkContext) Emit(rec *core.Record)   {}
func (c *sinkContext) CurrentWatermark() int64 { return 0 }
func (c *sinkContext) State() state.KeyedState { return state.NewMemory() }
func (c *sinkContext) Subtask() (string, int)  { return c.vertexID, c.index }

// openSink returns an opened Transactional under a fresh root, with the given
// identity.
func openSink(t *testing.T, root, vertexID string, index int) *Transactional {
	t.Helper()
	s := NewTransactional(root)
	if err := s.Open(&sinkContext{vertexID: vertexID, index: index}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// write puts one record with the given key into s.
func write(t *testing.T, s *Transactional, key string) {
	t.Helper()
	if err := s.Write(&core.Record{Key: []byte(key), Value: []byte("v-" + key), EventTime: 1}); err != nil {
		t.Fatalf("Write(%s): %v", key, err)
	}
}

// snapshot ends the current epoch and returns the payload the sink recorded.
func snapshot(t *testing.T, s *Transactional) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := s.Snapshot(&buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return buf.Bytes()
}

// committedKeys is the sorted keys of every record under committed/.
func committedKeys(t *testing.T, root string) []string {
	t.Helper()
	recs, err := ReadCommitted(root)
	if err != nil {
		t.Fatalf("ReadCommitted: %v", err)
	}
	out := make([]string, 0, len(recs))
	for _, rec := range recs {
		out = append(out, string(rec.Key))
	}
	slices.Sort(out)
	return out
}

// TestARecordStagedAndNotNotifiedIsNotOutput is invariant 4 as a statement
// about the disk.
//
// The record is written, the epoch is snapshotted, and the checkpoint never
// completes. Nothing is committed, so nothing is output. A sink that committed
// at snapshot time would pass every other test in this file and would produce a
// duplicate on the recovery that replays the record.
func TestARecordStagedAndNotNotifiedIsNotOutput(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "a")
	snapshot(t, s)

	if got := committedKeys(t, root); len(got) != 0 {
		t.Errorf("committed output holds %v after a snapshot with no notification: "+
			"the record belongs to a checkpoint that may never complete", got)
	}
	// It IS staged. The other failure would be a sink that lost the record
	// entirely, which is indistinguishable from correctness by the assertion
	// above alone.
	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if want := []string{"out-0-1" + stagingSuffix}; !slices.Equal(staged, want) {
		t.Errorf("staging holds %v, want %v: the transaction was not staged at all", staged, want)
	}
}

// TestTheSameRecordAppearsOnceNotified is the other half: the commit happens,
// and it happens on the notification.
func TestTheSameRecordAppearsOnceNotified(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "a")
	snapshot(t, s)
	if err := s.NotifyCheckpointComplete(1); err != nil {
		t.Fatalf("NotifyCheckpointComplete(1): %v", err)
	}

	if got, want := committedKeys(t, root), []string{"a"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v", got, want)
	}
	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if len(staged) != 0 {
		t.Errorf("staging still holds %v after the commit: the rename IS the commit, so the "+
			"staged file must be gone", staged)
	}
}

// TestNotifyingTwiceProducesOneCopy is the deduplication, and it is structural.
//
// There is no dedup table to consult. The committed name carries the checkpoint
// ID and the subtask, so the second commit is the same rename to the same path
// -- and since the staging file is gone by then, it is a no-op that reports
// success. A sink that returned an error on the second notification would fail
// a job whose only mistake was to be told twice.
func TestNotifyingTwiceProducesOneCopy(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "a")
	snapshot(t, s)
	for attempt := range 3 {
		if err := s.NotifyCheckpointComplete(1); err != nil {
			t.Fatalf("NotifyCheckpointComplete(1), attempt %d: %v", attempt, err)
		}
	}

	if got, want := committedKeys(t, root), []string{"a"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v after three notifications, want %v", got, want)
	}
	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if want := []string{"out-0-1" + commitSuffix}; !slices.Equal(files, want) {
		t.Errorf("committed holds %v, want %v: three commits produced more than one file", files, want)
	}
}

// TestNotifyingForACheckpointThatWasNeverStagedIsNotAnError.
//
// It happens for real. An epoch that received no records leaves no staging
// file, and a checkpoint can complete after this sink's last barrier. Refusing
// either would fail a job over a checkpoint that succeeded.
func TestNotifyingForACheckpointThatWasNeverStagedIsNotAnError(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	for _, tt := range []struct {
		name string
		id   int64
	}{
		{"an epoch that received no records", 1},
		{"a checkpoint above every epoch this sink reached", 99},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.NotifyCheckpointComplete(tt.id); err != nil {
				t.Fatalf("NotifyCheckpointComplete(%d) = %v, want no error", tt.id, err)
			}
		})
	}
	// Epoch 1 held no records and must therefore have produced no file. An
	// empty committed file is not wrong on its face, but it is a file a reader
	// has to know to skip, and it would be produced by a sink that opened its
	// staging file eagerly rather than on the first record.
	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("committing an epoch with no records produced %v, want nothing", files)
	}
}

// TestEachEpochCommitsIndependently.
//
// Three epochs, and only the middle one is notified. This is the case the
// per-checkpoint naming exists for: without the checkpoint ID in the name, all
// three would be one path and the commit of any of them would replace the
// others.
func TestEachEpochCommitsIndependently(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	for _, key := range []string{"a", "b", "c"} {
		write(t, s, key)
		snapshot(t, s)
	}
	if err := s.NotifyCheckpointComplete(2); err != nil {
		t.Fatalf("NotifyCheckpointComplete(2): %v", err)
	}

	if got, want := committedKeys(t, root), []string{"b"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v: only checkpoint 2 was notified", got, want)
	}
	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	want := []string{"out-0-1" + stagingSuffix, "out-0-3" + stagingSuffix}
	if !slices.Equal(staged, want) {
		t.Errorf("staging holds %v, want %v", staged, want)
	}
}

// TestTwoSubtasksCommitToDifferentFiles.
//
// The other half of the name. Two subtasks of one vertex commit the same
// checkpoint, and each has to land somewhere of its own: a name that carried
// only the checkpoint would put both on one path and one subtask's whole epoch
// would vanish under the other's.
//
// Parallelism is where this can fail, so the test uses two subtasks rather than
// asserting on a string.
func TestTwoSubtasksCommitToDifferentFiles(t *testing.T) {
	root := t.TempDir()
	zero := openSink(t, root, "out", 0)
	one := openSink(t, root, "out", 1)

	write(t, zero, "from-zero")
	write(t, one, "from-one")
	snapshot(t, zero)
	snapshot(t, one)
	for _, s := range []*Transactional{zero, one} {
		if err := s.NotifyCheckpointComplete(1); err != nil {
			t.Fatalf("NotifyCheckpointComplete(1): %v", err)
		}
	}

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	want := []string{"out-0-1" + commitSuffix, "out-1-1" + commitSuffix}
	if !slices.Equal(files, want) {
		t.Errorf("committed holds %v, want %v: the subtask index is what keeps them apart", files, want)
	}
	if got, want := committedKeys(t, root), []string{"from-one", "from-zero"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v: one subtask's epoch replaced the other's", got, want)
	}
}

// TestTheSnapshotPayloadNamesTheStagedTransaction.
//
// The payload is the epoch, and the epoch plus this subtask's identity is the
// staging file's name. Restore reads it to learn which transaction is pending,
// so a payload that named the wrong epoch would commit the wrong file -- and
// both files exist, so the failure is wrong output rather than a missing file.
func TestTheSnapshotPayloadNamesTheStagedTransaction(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	for want := int64(1); want <= 3; want++ {
		write(t, s, fmt.Sprintf("epoch-%d", want))
		payload := snapshot(t, s)
		if len(payload) != 8 {
			t.Fatalf("the payload for checkpoint %d is %d bytes, want 8", want, len(payload))
		}
		if got := int64(binary.BigEndian.Uint64(payload)); got != want {
			t.Errorf("the payload for checkpoint %d names epoch %d", want, got)
		}
		if got := s.Epoch(); got != want+1 {
			t.Errorf("after snapshotting checkpoint %d the sink is in epoch %d, want %d: "+
				"the next records belong to the next checkpoint", want, got, want+1)
		}
	}
}

// TestAStagingFileIsNotReadAsOutput.
//
// ReadCommitted reads committed/ and nothing else. A reader that globbed the
// root, or that fell back to staging when committed was empty, would count
// records belonging to an epoch no checkpoint vouched for -- and those records
// are replayable, so the recovered run produces them again and the count is
// double.
func TestAStagingFileIsNotReadAsOutput(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "committed-one")
	snapshot(t, s)
	if err := s.NotifyCheckpointComplete(1); err != nil {
		t.Fatalf("NotifyCheckpointComplete(1): %v", err)
	}
	write(t, s, "staged-only")
	snapshot(t, s)

	if got, want := committedKeys(t, root), []string{"committed-one"}; !slices.Equal(got, want) {
		t.Errorf("ReadCommitted returned %v, want %v: a staging file was read as output", got, want)
	}
}

// TestCloseDoesNotCommit.
//
// Close runs on every exit path, failure included. A Close that committed would
// commit the epoch of a subtask that died, which is precisely the data no
// checkpoint vouched for.
func TestCloseDoesNotCommit(t *testing.T) {
	root := t.TempDir()
	s := NewTransactional(root)
	if err := s.Open(&sinkContext{vertexID: "out", index: 0}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	write(t, s, "a")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := committedKeys(t, root); len(got) != 0 {
		t.Errorf("Close committed %v: an epoch no checkpoint vouched for became output", got)
	}
}

// TestADriftedEpochIsReportedRatherThanCommitted.
//
// The epoch counter is a count of barriers, and it is right because a subtask
// sees barrier k as its k-th barrier. If it ever stopped being right, a
// notification would find no file under the name it expects. Reporting it is
// the alternative to the silent version, where the sink shrugs and the
// transaction is never committed at all.
//
// Constructed by hand, because a drifted counter is not something the runtime
// can be made to produce on demand: the staging file for an epoch inside the
// sink's own range is removed underneath it.
func TestADriftedEpochIsReportedRatherThanCommitted(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "a")
	snapshot(t, s)
	write(t, s, "b")
	snapshot(t, s)

	if err := os.Remove(filepath.Join(root, stagingDir, "out-0-1"+stagingSuffix)); err != nil {
		t.Fatalf("removing the staging file: %v", err)
	}
	err := s.NotifyCheckpointComplete(1)
	if !errors.Is(err, ErrStagedFileMissing) {
		t.Errorf("NotifyCheckpointComplete(1) = %v, want %v: a transaction inside this sink's own "+
			"range went missing and the commit reported success", err, ErrStagedFileMissing)
	}
}

// TestTheRecordFormatRoundTripsAndDetectsCorruption.
//
// The CRC is per record rather than per file, because a staging file is
// appended to across an epoch and a crash leaves whatever the page cache had.
// Table-driven over the shapes a record can take, then one corrupted byte.
func TestTheRecordFormatRoundTripsAndDetectsCorruption(t *testing.T) {
	for _, tt := range []struct {
		name string
		recs []*core.Record
	}{
		{"one record", []*core.Record{{Key: []byte("k"), Value: []byte("v"), EventTime: 7}}},
		{"several records", []*core.Record{
			{Key: []byte("a"), Value: []byte("1"), EventTime: 1},
			{Key: []byte("b"), Value: []byte("22"), EventTime: 2},
			{Key: []byte("c"), Value: []byte("333"), EventTime: -3},
		}},
		{"an empty key and value", []*core.Record{{Key: nil, Value: nil, EventTime: 0}}},
		{"no records at all", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			for _, rec := range tt.recs {
				if err := encodeRecord(&buf, rec); err != nil {
					t.Fatalf("encodeRecord: %v", err)
				}
			}
			got, err := decodeRecords(buf.Bytes(), tt.name)
			if err != nil {
				t.Fatalf("decodeRecords: %v", err)
			}
			if len(got) != len(tt.recs) {
				t.Fatalf("decoded %d records, want %d", len(got), len(tt.recs))
			}
			for i, rec := range tt.recs {
				if !bytes.Equal(got[i].Key, rec.Key) || !bytes.Equal(got[i].Value, rec.Value) ||
					got[i].EventTime != rec.EventTime {
					t.Errorf("record %d round-tripped as %+v, want %+v", i, got[i], rec)
				}
			}
		})
	}

	t.Run("a corrupted byte", func(t *testing.T) {
		var buf bytes.Buffer
		if err := encodeRecord(&buf, &core.Record{Key: []byte("k"), Value: []byte("value"), EventTime: 9}); err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}
		raw := buf.Bytes()
		raw[14] ^= 0xFF
		if _, err := decodeRecords(raw, "corrupted"); !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("decodeRecords over a corrupted record = %v, want %v", err, ErrCorruptRecord)
		}
	})

	t.Run("a truncated record", func(t *testing.T) {
		var buf bytes.Buffer
		if err := encodeRecord(&buf, &core.Record{Key: []byte("k"), Value: []byte("value"), EventTime: 9}); err != nil {
			t.Fatalf("encodeRecord: %v", err)
		}
		raw := buf.Bytes()
		if _, err := decodeRecords(raw[:len(raw)-3], "truncated"); !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("decodeRecords over a truncated record = %v, want %v", err, ErrCorruptRecord)
		}
	})
}

// TestReadCommittedOverAnEmptyOrAbsentRoot.
//
// A job that committed nothing leaves no committed directory, and that is an
// empty output rather than an error. The distinction matters because the chaos
// suite compares against this: an error here would be reported as a broken
// harness, and a nil-with-no-error is the honest answer to "what did this job
// commit".
func TestReadCommittedOverAnEmptyOrAbsentRoot(t *testing.T) {
	for _, tt := range []struct {
		name string
		root func(t *testing.T) string
	}{
		{"a root that does not exist", func(t *testing.T) string {
			return filepath.Join(t.TempDir(), "absent")
		}},
		{"a root with no committed directory", func(t *testing.T) string { return t.TempDir() }},
		{"an opened sink that wrote nothing", func(t *testing.T) string {
			root := t.TempDir()
			openSink(t, root, "out", 0)
			return root
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recs, err := ReadCommitted(tt.root(t))
			if err != nil {
				t.Fatalf("ReadCommitted = %v, want no error", err)
			}
			if len(recs) != 0 {
				t.Errorf("ReadCommitted returned %d records, want none", len(recs))
			}
		})
	}
}

// TestOpenRejectsAVertexIDThatIsNotAFileName, for the reason pkg/checkpoint
// rejects one: a separator would put a transaction outside the directory that
// is supposed to hold it.
func TestOpenRejectsAVertexIDThatIsNotAFileName(t *testing.T) {
	for _, id := range []string{"", ".", "..", "a/b", "out/"} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			s := NewTransactional(t.TempDir())
			if err := s.Open(&sinkContext{vertexID: id, index: 0}); err == nil {
				t.Errorf("Open with vertex ID %q succeeded", id)
			}
		})
	}
}

// A compile-time check that the sink satisfies the interface with the two
// methods Phase 5 added, and that io is used for the signature rather than
// only in the doc comment.
var _ interface {
	Snapshot(io.Writer) error
	NotifyCheckpointComplete(int64) error
} = (*Transactional)(nil)

// The three crash windows.
//
// Each is constructed on disk directly rather than by racing a job, because a
// race reproduces the window it happens to hit and these are the three that
// have to be right. The middle one is the one that produces silent loss, and it
// is asserted on the COMMITTED FILES rather than on the contents: the records
// in a lost transaction may be regenerated by replay from the source, so a
// contents comparison against a clean run does not necessarily catch it.

// stageDirectly writes a staging file for (vertexID, index, checkpointID) by
// hand, holding one record with the given key.
//
// By hand, so a test can build the state a crash leaves without arranging the
// crash. It writes through the same encoder the sink does, so the file is one
// the sink's own reader accepts.
func stageDirectly(t *testing.T, root, vertexID string, index int, checkpointID int64, key string) {
	t.Helper()
	dir := filepath.Join(root, stagingDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	var buf bytes.Buffer
	if err := encodeRecord(&buf, &core.Record{Key: []byte(key), Value: []byte("v-" + key), EventTime: 1}); err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}
	name := fmt.Sprintf("%s-%d-%d%s", vertexID, index, checkpointID, stagingSuffix)
	if err := os.WriteFile(filepath.Join(dir, name), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", name, err)
	}
}

// restoreFrom opens a sink and restores it at checkpoint k.
func restoreFrom(t *testing.T, root, vertexID string, index int, k int64) *Transactional {
	t.Helper()
	s := openSink(t, root, vertexID, index)
	var payload bytes.Buffer
	if err := binary.Write(&payload, binary.BigEndian, k); err != nil {
		t.Fatalf("building the payload: %v", err)
	}
	if err := s.Restore(&payload); err != nil {
		t.Fatalf("Restore(%d): %v", k, err)
	}
	return s
}

// TestACrashBeforeCompleteDoesNotCommitTheStagedCheckpoint.
//
// The first window: the sink staged checkpoint 3 and the crash landed before
// _COMPLETE for 3 was written, so the newest complete checkpoint is 2 and the
// run resumes there. Checkpoint 3 is not a cut anybody can restore to, and its
// records are below the replay point -- the resumed run produces them again.
// Committing it would duplicate them.
func TestACrashBeforeCompleteDoesNotCommitTheStagedCheckpoint(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 2, "belongs-to-2")
	stageDirectly(t, root, "out", 0, 3, "belongs-to-3")

	restoreFrom(t, root, "out", 0, 2)

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if want := []string{"out-0-2" + commitSuffix}; !slices.Equal(files, want) {
		t.Fatalf("committed holds %v, want %v: checkpoint 3 never completed and must not be output",
			files, want)
	}
	if got, want := committedKeys(t, root), []string{"belongs-to-2"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v", got, want)
	}
}

// TestACrashAfterCompleteCommitsOnRestore is the window that loses data.
//
// The sink staged checkpoint 3, _COMPLETE for 3 was written, and the crash
// landed before the notification reached the sink. Nothing else will ever tell
// it, so restore has to commit it -- and restoring from 3 is exactly the
// statement that 3 is committable.
//
// Asserted on the committed FILE. The records in this transaction are also
// producible by replay, so a run that lost it can still match the oracle on
// contents; what cannot be faked is the file.
func TestACrashAfterCompleteCommitsOnRestore(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 3, "belongs-to-3")

	restoreFrom(t, root, "out", 0, 3)

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if want := []string{"out-0-3" + commitSuffix}; !slices.Equal(files, want) {
		t.Fatalf("committed holds %v, want %v: checkpoint 3 completed and the notification never "+
			"arrived, so restore is the only thing that will ever commit it", files, want)
	}
	if got, want := committedKeys(t, root), []string{"belongs-to-3"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v", got, want)
	}
	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if len(staged) != 0 {
		t.Errorf("staging still holds %v: the transaction was committed and its staging file must be gone", staged)
	}
}

// TestRestoreIsIdempotentWhenTheNotificationDidArrive is the same window with
// the crash on the other side of it.
//
// The notification arrived, checkpoint 3 is already committed, and the process
// died before it could record anything about that. Restore commits it again,
// which is the same rename to the same path: one file, one copy of the records.
// A restore that treated an already-committed checkpoint as an error would fail
// every recovery whose notification got through.
func TestRestoreIsIdempotentWhenTheNotificationDidArrive(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 3, "belongs-to-3")
	first := restoreFrom(t, root, "out", 0, 3)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restoreFrom(t, root, "out", 0, 3)

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if want := []string{"out-0-3" + commitSuffix}; !slices.Equal(files, want) {
		t.Errorf("committed holds %v, want %v after two restores from checkpoint 3", files, want)
	}
	if got, want := committedKeys(t, root), []string{"belongs-to-3"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v: the second restore duplicated the transaction", got, want)
	}
}

// TestRestoreDiscardsEveryStagingFileAboveTheCut.
//
// Checkpoints 4 and 5 belong to epochs that never completed. Their records are
// below the replay point, so the resumed run produces them again: committing
// either would duplicate, and LEAVING either would be worse than it looks --
// the resumed run reuses epoch numbers above 3, so its own staging file for 4
// would be a previous run's until the first Write truncated it, and 5 would
// simply sit there until a notification for 5 committed a transaction from a
// run that no longer exists.
func TestRestoreDiscardsEveryStagingFileAboveTheCut(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 3, "belongs-to-3")
	stageDirectly(t, root, "out", 0, 4, "belongs-to-4")
	stageDirectly(t, root, "out", 0, 5, "belongs-to-5")

	s := restoreFrom(t, root, "out", 0, 3)

	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if len(staged) != 0 {
		t.Errorf("staging holds %v after restoring at checkpoint 3: every epoch above the cut "+
			"belongs to a checkpoint that never completed", staged)
	}
	if got, want := committedKeys(t, root), []string{"belongs-to-3"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v: an epoch above the cut was committed", got, want)
	}
	if got, want := s.Epoch(), int64(4); got != want {
		t.Errorf("the restored sink is in epoch %d, want %d: the next records belong to the "+
			"checkpoint after the one restored", got, want)
	}
}

// TestRestoreLeavesAnotherSubtasksStagingFilesAlone.
//
// Subtasks restore CONCURRENTLY. A sink that swept the whole staging directory
// would delete a sibling's file while the sibling was writing to it, and the
// sibling would carry on appending to a file with no name -- whose records
// reach nothing, with no error anywhere.
//
// Two subtasks, because with one there is nobody else's file to delete.
func TestRestoreLeavesAnotherSubtasksStagingFilesAlone(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 5, "zero-above-the-cut")
	stageDirectly(t, root, "out", 1, 5, "one-above-the-cut")

	restoreFrom(t, root, "out", 0, 3)

	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if want := []string{"out-1-5" + stagingSuffix}; !slices.Equal(staged, want) {
		t.Errorf("staging holds %v, want %v: subtask 0's restore reached subtask 1's files", staged, want)
	}
}

// TestRestoreOverANameFromAnotherVertexLeavesItAlone.
//
// Two sink vertices can share a root. The name is split from the right, and the
// vertex ID is COMPARED rather than trusted, so a file belonging to another
// vertex is neither committed nor discarded by this one.
func TestRestoreOverANameFromAnotherVertexLeavesItAlone(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 5, "mine")
	stageDirectly(t, root, "other", 0, 5, "not-mine")
	stageDirectly(t, root, "out-extra", 0, 5, "also-not-mine")

	restoreFrom(t, root, "out", 0, 3)

	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	want := []string{"other-0-5" + stagingSuffix, "out-extra-0-5" + stagingSuffix}
	if !slices.Equal(staged, want) {
		t.Errorf("staging holds %v, want %v", staged, want)
	}
}

// TestARestoredSinkStagesIntoTheEpochAfterTheCut.
//
// Restoring at k leaves the sink in epoch k+1, so the next records land in a
// transaction named k+1 and the notification for k+1 commits them. A sink that
// restarted its epoch count at 1 would stage into names the resumed run's
// checkpoints do not use, and every one of those transactions would be
// committed by a notification for the wrong checkpoint or by none at all.
func TestARestoredSinkStagesIntoTheEpochAfterTheCut(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 3, "belongs-to-3")
	s := restoreFrom(t, root, "out", 0, 3)

	write(t, s, "after-the-cut")
	snapshot(t, s)
	if err := s.NotifyCheckpointComplete(4); err != nil {
		t.Fatalf("NotifyCheckpointComplete(4): %v", err)
	}

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	want := []string{"out-0-3" + commitSuffix, "out-0-4" + commitSuffix}
	if !slices.Equal(files, want) {
		t.Errorf("committed holds %v, want %v", files, want)
	}
	if got, want := committedKeys(t, root), []string{"after-the-cut", "belongs-to-3"}; !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v", got, want)
	}
}

// TestRestoreFromCheckpointZeroCommitsNothing.
//
// A sink that reached no barrier records epoch 1 as its state -- checkpoint IDs
// are contiguous from 1, so zero is the payload of a sink with nothing staged.
// It is not a checkpoint anybody restores to, and a commitPending that treated
// it as one would look for a file named out-0-0 forever.
func TestRestoreFromCheckpointZeroCommitsNothing(t *testing.T) {
	root := t.TempDir()
	stageDirectly(t, root, "out", 0, 1, "belongs-to-1")

	s := restoreFrom(t, root, "out", 0, 0)

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("committed holds %v after restoring at checkpoint 0, want nothing", files)
	}
	if got, want := s.Epoch(), int64(1); got != want {
		t.Errorf("the restored sink is in epoch %d, want %d", got, want)
	}
}

// TestRestoreRejectsAPayloadItCannotRead.
//
// A truncated or absent payload is a checkpoint that did not record what this
// sink staged. Continuing from it would pick an epoch out of the air and commit
// whatever happened to be named that.
func TestRestoreRejectsAPayloadItCannotRead(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"truncated", []byte{0, 0, 0}},
		{"negative", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := openSink(t, t.TempDir(), "out", 0)
			if err := s.Restore(bytes.NewReader(tt.payload)); err == nil {
				t.Errorf("Restore over a %s payload succeeded", tt.name)
			}
		})
	}
}

// The final epoch.
//
// Records after the last barrier belong to no checkpoint, so nothing will ever
// notify for them. It exists only because the input is bounded.

// TestTheFinalEpochIsCommittedDirectly.
//
// Two epochs' worth of records: one closed by a barrier and notified, one after
// the last barrier. The second is the tail, and nothing in the checkpoint
// protocol will ever mention it -- so without this commit it stays staged and
// its records are absent from the output with nothing to point at.
func TestTheFinalEpochIsCommittedDirectly(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "covered-by-checkpoint-1")
	snapshot(t, s)
	if err := s.NotifyCheckpointComplete(1); err != nil {
		t.Fatalf("NotifyCheckpointComplete(1): %v", err)
	}
	write(t, s, "after-the-last-barrier")

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := committedKeys(t, root); !slices.Contains(got, "covered-by-checkpoint-1") ||
		slices.Contains(got, "after-the-last-barrier") {
		t.Fatalf("committed output is %v after Close: Close must not commit, because it runs on "+
			"failure as well as on success", got)
	}

	if err := s.CommitFinalEpoch(); err != nil {
		t.Fatalf("CommitFinalEpoch: %v", err)
	}
	want := []string{"after-the-last-barrier", "covered-by-checkpoint-1"}
	if got := committedKeys(t, root); !slices.Equal(got, want) {
		t.Errorf("committed output is %v, want %v", got, want)
	}
	staged, err := StagingFiles(root)
	if err != nil {
		t.Fatalf("StagingFiles: %v", err)
	}
	if len(staged) != 0 {
		t.Errorf("staging holds %v after the final epoch was committed", staged)
	}
}

// TestCommittingAFinalEpochWithNoRecordsIsANoOp.
//
// A job whose last barrier fell on its last record has an empty final epoch,
// which left no staging file. Committing it must produce nothing rather than an
// empty committed file a reader has to know to skip.
func TestCommittingAFinalEpochWithNoRecordsIsANoOp(t *testing.T) {
	root := t.TempDir()
	s := openSink(t, root, "out", 0)

	write(t, s, "a")
	snapshot(t, s)
	if err := s.NotifyCheckpointComplete(1); err != nil {
		t.Fatalf("NotifyCheckpointComplete(1): %v", err)
	}
	if err := s.CommitFinalEpoch(); err != nil {
		t.Fatalf("CommitFinalEpoch: %v", err)
	}

	files, err := CommittedFiles(root)
	if err != nil {
		t.Fatalf("CommittedFiles: %v", err)
	}
	if want := []string{"out-0-1" + commitSuffix}; !slices.Equal(files, want) {
		t.Errorf("committed holds %v, want %v: an empty final epoch produced a file", files, want)
	}
}
