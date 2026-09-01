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
