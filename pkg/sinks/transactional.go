package sinks

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AarinB1/tidemark/pkg/core"
)

// Transactional is a two-phase commit over the filesystem.
//
// # The layout
//
//	<root>/staging/<vertexID>-<index>-<checkpointID>.tmp
//	<root>/committed/<vertexID>-<index>-<checkpointID>.out
//
// # The protocol
//
// Write appends to the staging file for the CURRENT epoch. At the barrier for
// checkpoint k, Snapshot flushes and fsyncs that file, records its identity in
// the payload, and opens a new staging file for epoch k+1 -- and commits
// nothing. NotifyCheckpointComplete(k) renames staging k into committed, and
// that rename IS the commit.
//
// Everything between those two is the reason the sink exists. Data committed at
// snapshot time belongs to a checkpoint that may never complete; the run that
// recovered from the previous one would replay those records and write them
// again, so the sink whose whole purpose is exactly-once output would be the
// thing producing the duplicates. That is invariant 4.
//
// # Deduplication is the naming
//
// There is NO dedup table, and a reader looking for one should stop looking:
// the committed file name carries the checkpoint ID and the subtask, so
// committing twice is the same rename to the same path. Two runs that both
// decide to commit checkpoint 7 for subtask out[1] produce one file, because
// they produce the same name. That is why "already committed" is a success and
// not an error, and it is why the checkpoint ID may never be dropped from the
// name -- without it, two epochs of one subtask collapse onto one path and the
// second silently replaces the first.
//
// A side table would need its own durability, its own fsync ordering against
// the rename, and its own recovery. The filesystem already has all three.
//
// # Reading the output
//
// Every file under committed/, and nothing else. A file under staging/ is not
// output: it belongs to an epoch whose checkpoint has not completed, and it may
// hold records a recovered run will produce again. See ReadCommitted.
//
// # Concurrency
//
// One subtask goroutine, like every core.Sink. The runtime does not hand this
// to the coordinator; the sink's own subtask asks which checkpoints completed
// and calls NotifyCheckpointComplete itself, which is what keeps that true.
type Transactional struct {
	root string
	// vertexID and index are this subtask's identity, learned from the runtime
	// in Open. They are half of every file name this sink writes.
	vertexID string
	index    int
	// epoch is the checkpoint the CURRENT staging file belongs to.
	//
	// It is a count of barriers rather than an ID handed in, because
	// core.Sink.Snapshot is not told which checkpoint it is snapshotting -- and
	// it does not need to be for a sink whose job is "everything since the last
	// barrier". The count is correct because checkpoint IDs are contiguous from
	// 1 within a subtask and a subtask sees barrier k as its k-th barrier,
	// which is the same fact the source runner's barriersInjected rests on.
	//
	// Restore sets it, which is the only other way it moves.
	epoch int64
	// firstEpoch is the lowest epoch THIS instance owns: 1 on a fresh start,
	// k+1 after restoring from checkpoint k. It is what lets
	// NotifyCheckpointComplete tell "a checkpoint I never staged" from "a
	// checkpoint I staged and whose file has gone missing", and the second is
	// the drift this sink would otherwise commit silently.
	firstEpoch int64

	// opened guards against one instance being handed to several subtasks; see
	// Open.
	opened bool

	// file is the open staging file for epoch, and buf the writer over it. Both
	// are nil until the first Write of an epoch: an epoch with no records
	// leaves no staging file, and committing it is then correctly a no-op
	// rather than the creation of an empty committed file.
	file *os.File
	buf  *bufio.Writer
}

var _ core.Sink = (*Transactional)(nil)

// Directory and file names. The staging suffix is .tmp and the committed one is
// .out, so a half-written epoch is visible as such to a person looking at the
// directory and cannot be mistaken for output by a glob.
const (
	stagingDir    = "staging"
	committedDir  = "committed"
	stagingSuffix = ".tmp"
	commitSuffix  = ".out"
)

// Failures a caller can tell apart.
var (
	// ErrStagedFileMissing is a notification for a checkpoint this sink staged
	// and whose file is neither staged nor committed. It means the epoch
	// counter has drifted from the checkpoint IDs, which would otherwise commit
	// one epoch's records under another's name.
	ErrStagedFileMissing = errors.New("checkpoint was staged and its file is neither staged nor committed")
	// ErrCorruptRecord is a committed file that does not decode.
	ErrCorruptRecord = errors.New("committed file does not hold the record format")
)

// NewTransactional returns a sink writing under root.
//
// It learns its own identity from the runtime in Open rather than taking it
// here. A constructor told its index would put the same number in two places --
// the caller's argument and the one the executor assigns -- and the failure of
// them disagreeing is two subtasks committing to one file name, which is
// exactly the collision the naming exists to prevent.
func NewTransactional(root string) *Transactional {
	return &Transactional{root: root, epoch: 1, firstEpoch: 1}
}

// Open creates the two directories and learns this subtask's identity.
//
// Opening twice is refused, and the refusal is the guard on the one way this
// sink is easy to misuse. A graph whose NewSink returns the same instance to
// every subtask -- which is what a job wiring sinks.Collect does, and correctly,
// because Collect locks -- hands one Transactional to several goroutines. They
// would then share one buffered writer and one epoch counter, and the visible
// symptom is a short write from deep inside bufio; the invisible one is two
// subtasks writing into one staging file and committing it under one name, so
// half the output disappears.
//
// Detected here because this is the only place the runtime touches a sink once
// per subtask. NewSink must return a new instance per call.
func (t *Transactional) Open(ctx core.Context) error {
	if t.opened {
		return fmt.Errorf("transactional sink: Open called twice on one instance, most recently as "+
			"%s[%d]: every subtask needs its own sink, so NewSink must return a new one per call",
			t.vertexID, t.index)
	}
	t.opened = true
	t.vertexID, t.index = ctx.Subtask()
	if err := checkVertexID(t.vertexID); err != nil {
		return err
	}
	for _, dir := range []string{t.dir(stagingDir), t.dir(committedDir)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("transactional sink: %w", err)
		}
	}
	// Made durable now rather than at the first commit. A rename into a
	// directory whose own entry is not on the disk is a rename that a crash can
	// undo, and the commit path fsyncs the directory it renames INTO, not the
	// root that names it.
	return syncDir(t.root)
}

// Write appends rec to the staging file for the current epoch.
//
// Buffered, and the buffer is flushed and fsynced at Snapshot. Nothing between
// two barriers needs to be durable before the barrier that ends the epoch: the
// records are replayable from the source until the checkpoint that covers them
// completes, and the fsync at Snapshot is what makes them stop being.
func (t *Transactional) Write(rec *core.Record) error {
	if t.buf == nil {
		if err := t.openEpoch(); err != nil {
			return err
		}
	}
	return encodeRecord(t.buf, rec)
}

// Snapshot ends the current epoch: flush, fsync, close, and record which
// staging file belongs to this checkpoint. It commits NOTHING.
//
// The payload is the epoch number. That is the whole of a sink's state, and it
// is enough: the epoch plus this subtask's identity is the staging file's name,
// and the name is the transaction. Recording the path instead would put a
// directory a run was configured with into a checkpoint that a differently
// configured run has to read.
func (t *Transactional) Snapshot(w io.Writer) error {
	if err := t.closeEpoch(); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, t.epoch); err != nil {
		return fmt.Errorf("transactional sink: snapshot: %w", err)
	}
	// The next records belong to the next checkpoint. The file for it is not
	// created here: an epoch that receives no records leaves no staging file,
	// and a commit of it is then a no-op rather than an empty committed file
	// that a reader has to know to skip.
	t.epoch++
	return nil
}

// NotifyCheckpointComplete commits checkpoint id by renaming its staging file
// into committed.
//
// The rename is the commit and the name is the deduplication. Committing twice
// is the same rename to the same path, so "already committed" is success. So is
// a notification for a checkpoint this sink staged nothing for: an epoch with
// no records has no file, and a notification can arrive for a checkpoint that
// completed after this sink's last barrier.
//
// The one case that is NOT success is a checkpoint inside this instance's own
// staged range whose file is in neither directory. That is the epoch counter
// having drifted from the checkpoint IDs, and the alternative to reporting it
// is committing one epoch's records under another epoch's name.
func (t *Transactional) NotifyCheckpointComplete(checkpointID int64) error {
	staging, committed := t.stagingPath(checkpointID), t.committedPath(checkpointID)
	err := os.Rename(staging, committed)
	if err == nil {
		// The directory entry the rename created, made durable. Without this a
		// crash can leave a committed file whose contents are on the disk and
		// whose name is not, which reads back as lost output.
		return syncDir(t.dir(committedDir))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("transactional sink: committing checkpoint %d: %w", checkpointID, err)
	}
	if _, statErr := os.Stat(committed); statErr == nil {
		// Already committed. The same rename to the same path.
		return nil
	}
	if checkpointID >= t.firstEpoch && checkpointID < t.epoch {
		return fmt.Errorf("transactional sink: %w: checkpoint %d, subtask %s[%d]",
			ErrStagedFileMissing, checkpointID, t.vertexID, t.index)
	}
	// Never staged: an epoch with no records, or a checkpoint from before this
	// instance began.
	return nil
}

// Restore reopens this sink at the cut checkpoint k records, where k is the
// checkpoint whose _COMPLETE marker the run being resumed selected.
//
// It is called after Open and before any Write, which is the window
// core.Operator.Restore already documents and for the same reason: Open is
// where the identity arrives, and every path below is named by it.
//
// # Why it commits
//
// Restoring from k means _COMPLETE for k exists, so k IS committable. But the
// crash may have landed after _COMPLETE was written and before the notification
// reached this sink -- a window the runtime cannot close, because there is no
// atomic step spanning a durable marker and a call into a sink in another
// process's memory. So the pending transaction for k is committed HERE,
// idempotently, rather than assumed to have been committed already.
//
// This is the case that produces silent loss if it is missed, and it is not
// necessarily caught by comparing contents against a clean run: the records in
// that transaction may be regenerated by replay from the source anyway, and
// then the only trace is a committed file that is not there.
//
// # Why it discards
//
// A staging file above k belongs to an epoch that never completed. Its records
// are below the replay point, so the resumed run produces them again --
// committing such a file would duplicate them. Leaving it in place is no better
// than committing it: the resumed run reuses the epoch numbers above k, so its
// own staging file for k+1 would be written over a previous run's, and a
// notification for k+1 would commit a mixture of the two.
func (t *Transactional) Restore(r io.Reader) error {
	var k int64
	if err := binary.Read(r, binary.BigEndian, &k); err != nil {
		return fmt.Errorf("transactional sink: restore: reading the staged checkpoint: %w", err)
	}
	if k < 0 {
		return fmt.Errorf("transactional sink: restore: the payload names checkpoint %d", k)
	}

	// The order is commit, then discard, then move to the next epoch, and it is
	// not interchangeable. Discarding first over a range that included k would
	// throw away the transaction this function exists to commit; moving the
	// epoch first would put k below firstEpoch and turn a missing staging file
	// for it from a reported drift into a shrug.
	if err := t.commitPending(k); err != nil {
		return err
	}
	if err := t.discardAbove(k); err != nil {
		return err
	}
	t.epoch, t.firstEpoch = k+1, k+1
	return nil
}

// commitPending commits checkpoint k if its staging file is still there.
//
// Idempotent, and the idempotence is the naming rather than a check: the
// committed path is a function of (subtask, checkpoint), so a k that a previous
// run already committed is a rename whose source is gone and whose destination
// exists, which is success. A run that restores twice from the same checkpoint
// commits it twice and produces one file.
func (t *Transactional) commitPending(k int64) error {
	if k == 0 {
		// Checkpoint IDs are contiguous from 1, so zero is the payload of a
		// sink that never reached a barrier. There is nothing staged under that
		// name and nothing to commit.
		return nil
	}
	staging, committed := t.stagingPath(k), t.committedPath(k)
	err := os.Rename(staging, committed)
	if err == nil {
		return syncDir(t.dir(committedDir))
	}
	if errors.Is(err, os.ErrNotExist) {
		// Either the notification arrived before the crash and this is already
		// committed, or the epoch held no records and left no file. Both are
		// the state this function is trying to reach.
		return nil
	}
	return fmt.Errorf("transactional sink: restore: committing checkpoint %d: %w", k, err)
}

// discardAbove deletes THIS SUBTASK's staging files for checkpoints above k.
//
// This subtask's and no other's. Subtasks restore concurrently, so a sink that
// swept the whole directory would delete a sibling's staging file while the
// sibling was writing to it -- and the sibling would carry on appending to a
// file with no name, whose records reach nothing.
func (t *Transactional) discardAbove(k int64) error {
	dir := t.dir(stagingDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("transactional sink: restore: %w", err)
	}
	removed := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		id, ok := t.checkpointOf(e.Name())
		if !ok || id <= k {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("transactional sink: restore: discarding %s: %w", e.Name(), err)
		}
		removed = true
	}
	if !removed {
		return nil
	}
	// The removals, made durable. A crash that lost them would leave the
	// resumed run's staging namespace holding a previous run's epochs, which is
	// the collision this function exists to prevent.
	return syncDir(dir)
}

// checkpointOf reads the checkpoint ID out of a staging file name belonging to
// this subtask, and reports false for any other name.
//
// Split from the RIGHT. A checkpoint ID and an index are decimal digits and
// carry no '-', so the last two separators always mark the boundaries however
// many the vertex ID contains -- which is the same property fileName relies on
// to be injective. The vertex ID and index are then compared rather than
// trusted, because "belonging to this subtask" is the whole question.
func (t *Transactional) checkpointOf(name string) (int64, bool) {
	rest, ok := strings.CutSuffix(name, stagingSuffix)
	if !ok {
		return 0, false
	}
	cut := strings.LastIndexByte(rest, '-')
	if cut < 0 {
		return 0, false
	}
	id, err := strconv.ParseInt(rest[cut+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	if rest[:cut] != t.vertexID+"-"+strconv.Itoa(t.index) {
		return 0, false
	}
	return id, true
}

// Close releases the open staging file without committing it.
//
// Whatever is still staged stays staged. It is not output, it belongs to an
// epoch no checkpoint has vouched for, and the run that recovers will either
// commit it or discard it. Committing here would be committing on a path that
// runs on failure as well as on success.
func (t *Transactional) Close() error {
	if t.buf == nil {
		return nil
	}
	// Flushed but NOT fsynced, and not renamed. The bytes are handed to the
	// kernel so the file a person looks at is the file that was written; their
	// durability is not claimed, because nothing is going to rely on it.
	err := t.buf.Flush()
	if cerr := t.file.Close(); err == nil {
		err = cerr
	}
	t.buf, t.file = nil, nil
	return err
}

// Epoch returns the checkpoint the current staging file belongs to. It is the
// number Snapshot records, and tests read it to say which epoch a record landed
// in.
func (t *Transactional) Epoch() int64 { return t.epoch }

// openEpoch creates the staging file for the current epoch.
func (t *Transactional) openEpoch() error {
	f, err := os.OpenFile(t.stagingPath(t.epoch), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("transactional sink: staging checkpoint %d: %w", t.epoch, err)
	}
	t.file, t.buf = f, bufio.NewWriter(f)
	return nil
}

// closeEpoch flushes, fsyncs and closes the current staging file.
//
// The fsync is what makes the staged transaction durable, and it happens HERE
// rather than at the commit. A rename is atomic and cheap; it is not a flush.
// Renaming a file whose contents are still in the page cache would produce a
// committed file that a crash truncates, which is a committed file that is
// wrong rather than absent.
func (t *Transactional) closeEpoch() error {
	if t.buf == nil {
		return nil
	}
	if err := t.buf.Flush(); err != nil {
		t.file.Close()
		t.buf, t.file = nil, nil
		return fmt.Errorf("transactional sink: staging checkpoint %d: %w", t.epoch, err)
	}
	if err := t.file.Sync(); err != nil {
		t.file.Close()
		t.buf, t.file = nil, nil
		return fmt.Errorf("transactional sink: staging checkpoint %d: %w", t.epoch, err)
	}
	err := t.file.Close()
	t.buf, t.file = nil, nil
	if err != nil {
		return fmt.Errorf("transactional sink: staging checkpoint %d: %w", t.epoch, err)
	}
	// The staging directory entry, made durable alongside the contents. A crash
	// that lost the name would lose a transaction that is otherwise complete.
	return syncDir(t.dir(stagingDir))
}

func (t *Transactional) dir(name string) string { return filepath.Join(t.root, name) }

// fileName is (vertexID, index, checkpointID) and nothing else. The mapping is
// injective without escaping: an index and a checkpoint ID are decimal digits
// and carry no '-', so the last two '-' always split the name. Nothing parses
// it back inside this type; ReadCommitted reads the directory rather than the
// names.
func (t *Transactional) fileName(checkpointID int64, suffix string) string {
	return t.vertexID + "-" + strconv.Itoa(t.index) + "-" + strconv.FormatInt(checkpointID, 10) + suffix
}

func (t *Transactional) stagingPath(checkpointID int64) string {
	return filepath.Join(t.dir(stagingDir), t.fileName(checkpointID, stagingSuffix))
}

func (t *Transactional) committedPath(checkpointID int64) string {
	return filepath.Join(t.dir(committedDir), t.fileName(checkpointID, commitSuffix))
}

// checkVertexID rejects an ID that cannot be part of a file name, for the same
// reason pkg/checkpoint does: a separator would put a transaction outside the
// directory that is supposed to hold it.
func checkVertexID(id string) error {
	switch {
	case id == "":
		return fmt.Errorf("transactional sink: vertex ID is empty")
	case id == "." || id == "..":
		return fmt.Errorf("transactional sink: vertex ID %q names a directory", id)
	case strings.ContainsRune(id, '/') || strings.ContainsRune(id, filepath.Separator):
		return fmt.Errorf("transactional sink: vertex ID %q contains a path separator", id)
	}
	return nil
}

// syncDir fsyncs a directory, making the renames and creations into it durable.
//
// Syncing a file makes its CONTENTS durable; the directory entry naming it is
// separate metadata. This is the same fsync pkg/checkpoint performs for the
// same reason, written out here rather than shared because it is six lines and
// exporting it would put a filesystem helper in the public API of the package
// that owns the checkpoint format.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// The record format. Big-endian throughout, like every other encoding here:
//
//	eventTime int64
//	keyLen    uint32, then that many bytes
//	valueLen  uint32, then that many bytes
//	crc32     uint32, IEEE, over every preceding byte of this record
//
// Hand-rolled rather than encoding/gob or encoding/json, which the scope rules
// forbid anywhere in the data path.
//
// The CRC is PER RECORD and not per file, and that is what a staging file needs
// it to be. A file is appended to across an epoch and fsynced once at the end,
// so a crash leaves a file whose tail is whatever the page cache had -- and
// that tail can be a partial record or blocks of something else. A per-file
// checksum would condemn the whole transaction; a per-record one lets a reader
// say exactly where the good records stop. Nothing reads a staging file today,
// and the format is the same either way, so this is the cheaper of the two to
// have been right about.
const recordCRCBytes = 4

// encodeRecord appends one record to w.
func encodeRecord(w io.Writer, rec *core.Record) error {
	buf := make([]byte, 0, 8+4+len(rec.Key)+4+len(rec.Value)+recordCRCBytes)
	buf = binary.BigEndian.AppendUint64(buf, uint64(rec.EventTime))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.Key)))
	buf = append(buf, rec.Key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.Value)))
	buf = append(buf, rec.Value...)
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	_, err := w.Write(buf)
	return err
}

// decodeRecords reads every record in raw.
func decodeRecords(raw []byte, from string) ([]*core.Record, error) {
	var out []*core.Record
	for pos := 0; pos < len(raw); {
		rec, n, err := decodeRecord(raw[pos:])
		if err != nil {
			return nil, fmt.Errorf("%s at offset %d: %w", from, pos, err)
		}
		out = append(out, rec)
		pos += n
	}
	return out, nil
}

// decodeRecord reads one record and returns how many bytes it took.
func decodeRecord(raw []byte) (*core.Record, int, error) {
	// eventTime + keyLen + valueLen + crc is the smallest a record can be.
	if len(raw) < 8+4+4+recordCRCBytes {
		return nil, 0, fmt.Errorf("%w: %d bytes remain", ErrCorruptRecord, len(raw))
	}
	eventTime := int64(binary.BigEndian.Uint64(raw))
	keyLen := int(binary.BigEndian.Uint32(raw[8:]))
	if keyLen < 0 || 12+keyLen+4+recordCRCBytes > len(raw) {
		return nil, 0, fmt.Errorf("%w: key claims %d bytes, %d remain", ErrCorruptRecord, keyLen, len(raw)-12)
	}
	key := raw[12 : 12+keyLen]
	valueLen := int(binary.BigEndian.Uint32(raw[12+keyLen:]))
	start := 12 + keyLen + 4
	if valueLen < 0 || start+valueLen+recordCRCBytes > len(raw) {
		return nil, 0, fmt.Errorf("%w: value claims %d bytes, %d remain", ErrCorruptRecord, valueLen, len(raw)-start)
	}
	value := raw[start : start+valueLen]
	n := start + valueLen + recordCRCBytes
	body := raw[:n-recordCRCBytes]
	if got, want := crc32.ChecksumIEEE(body), binary.BigEndian.Uint32(raw[n-recordCRCBytes:n]); got != want {
		return nil, 0, fmt.Errorf("%w: computed %#08x, file records %#08x", ErrCorruptRecord, got, want)
	}
	// Copied out of raw, which is one read of a whole file: a record holding a
	// slice of it would keep the file alive for as long as any record survives.
	return &core.Record{
		Key:       append([]byte(nil), key...),
		Value:     append([]byte(nil), value...),
		EventTime: eventTime,
	}, n, nil
}

// ReadCommitted returns every record under <root>/committed, across every
// subtask and every checkpoint.
//
// This is what "the output of the job" means for this sink, and the staging
// directory is deliberately not consulted. A staging file belongs to an epoch
// no checkpoint has vouched for: its records are replayable from the source, so
// a reader that counted them would be counting records the recovered run
// produces again.
//
// The order across files is the directory's and means nothing. Delivery is
// at-least-once and ordering after a recovery differs from a clean run for
// reasons that have nothing to do with correctness; what a caller compares is
// the sorted contents.
func ReadCommitted(root string) ([]*core.Record, error) {
	dir := filepath.Join(root, committedDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A job that committed nothing leaves no directory, and that is an
			// empty output rather than an error.
			return nil, nil
		}
		return nil, fmt.Errorf("reading committed output: %w", err)
	}
	var out []*core.Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), commitSuffix) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading committed output: %w", err)
		}
		recs, err := decodeRecords(raw, e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// CommittedFiles returns the names of the committed files under root, sorted.
//
// It is what a test asserts on when the question is which TRANSACTIONS were
// committed rather than which records came out -- the two are different
// questions, and the restore cases in particular turn on the first.
func CommittedFiles(root string) ([]string, error) {
	return namesUnder(filepath.Join(root, committedDir), commitSuffix)
}

// StagingFiles returns the names of the staging files under root, sorted.
func StagingFiles(root string) ([]string, error) {
	return namesUnder(filepath.Join(root, stagingDir), stagingSuffix)
}

func namesUnder(dir, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			out = append(out, e.Name())
		}
	}
	// ReadDir already sorts by name, so this is the directory's own order and a
	// function of the contents rather than of the filesystem.
	return out, nil
}
