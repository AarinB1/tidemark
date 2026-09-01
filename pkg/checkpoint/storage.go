// Package checkpoint stores the snapshots a job takes and decides which of them
// a recovery may use.
//
// A checkpoint is a directory. It is usable only once _COMPLETE is in it, and
// the order the files reach the disk in is the whole of what makes that true;
// see Storage.Complete. That is invariant 8, and it is the invariant with the
// least forgiving failure mode in this engine: a checkpoint that looks complete
// but is not restores into a job that runs, produces output, and is wrong.
package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Layout under a checkpoint root:
//
//	<root>/chk-<id>/
//	    <vertexID>-<index>.state
//	    _METADATA
//	    _COMPLETE
//
// The state file name is (id, index) and nothing else, and the mapping is
// injective without any escaping: an index is decimal digits and carries no
// '-', so the last '-' in a name always splits the vertex ID from the index.
// Nothing parses the name back, though. Load builds the names it expects from
// _METADATA, so a name is a lookup key rather than a record, and the file
// itself carries the vertex ID and index it belongs to.
//
// Per-file format, big-endian throughout:
//
//	magic        [4]byte "TDMK"
//	version      uint16 = 1
//	checkpointID int64
//	vertexID     uint16 length, then that many bytes
//	subtaskIndex uint32
//	payloadLen   uint64
//	payload      [payloadLen]byte
//	crc32        uint32, IEEE, over every preceding byte
//
// The identity fields are in the file and not only in its name so that a file
// which somehow ends up under the wrong name, or in the wrong checkpoint
// directory, is caught rather than restored into the wrong subtask. Restoring
// subtask 2's state into subtask 1 is silent: both are valid states, and the
// job produces wrong counts for two subtasks instead of failing.
//
// The CRC and _COMPLETE guard different things and neither replaces the other.
// _COMPLETE says the checkpoint as a whole exists: every subtask acknowledged,
// every file was written. The CRC says one file is the bytes that were written:
// a torn write, or an fsync that returned success without the data reaching the
// platter, leaves a file of the right length holding some of the old contents.
// Without the CRC that decodes into a plausible state -- a shorter entry count,
// a different aggregate -- and restores.
const (
	magic       = "TDMK"
	formatVer   = uint16(1)
	metadataVer = uint16(1)

	completeName = "_COMPLETE"
	metadataName = "_METADATA"
	stateSuffix  = ".state"
	tempSuffix   = ".tmp"

	dirPrefix = "chk-"
)

// Format and validation failures. Each has its own error so a caller can tell
// a corrupt checkpoint from an incompatible one with errors.Is.
var (
	errBadMagic         = errors.New("file does not begin with the checkpoint magic")
	errBadVersion       = errors.New("unsupported checkpoint format version")
	errCRCMismatch      = errors.New("checksum does not match the file contents")
	errWrongSubtask     = errors.New("file holds a different subtask than the one requested")
	errWrongCheckpoint  = errors.New("file holds a different checkpoint than the one requested")
	errShortFile        = errors.New("file ends before the format does")
	errNoCheckpoint     = errors.New("no complete checkpoint")
	errBadVertexID      = errors.New("vertex ID cannot be used as a file name")
	errVertexMismatch   = errors.New("checkpoint and job disagree about the vertices")
	errParallelismDiff  = errors.New("checkpoint and job disagree about a vertex's parallelism")
	errCountDiff        = errors.New("checkpoint and job disagree about a source's element count")
	errMissingStateFile = errors.New("checkpoint has no state file for an expected subtask")
)

// SubtaskKey identifies one parallel instance of a vertex. It mirrors the
// runtime's subtask identity; this package holds the on-disk half of it.
type SubtaskKey struct {
	VertexID string
	Index    int
}

func (k SubtaskKey) String() string { return fmt.Sprintf("%s[%d]", k.VertexID, k.Index) }

// fileName is the name of this subtask's state file within a checkpoint
// directory.
func (k SubtaskKey) fileName() string {
	return k.VertexID + "-" + strconv.Itoa(k.Index) + stateSuffix
}

// VertexMeta is what a checkpoint records about one vertex of the job that
// wrote it.
//
// Count is the source's element count, and zero for a vertex that is not a
// source with a countable input. It is recorded because a source subtask's
// range is derived from (Count, Parallelism, index) and NOT stored anywhere in
// the checkpoint: what is stored is a resume offset, which only means the right
// thing if the same arithmetic produces the same range. Restoring at a
// different Count or a different parallelism hands subtask 1 an offset from
// somebody else's range, and the job reads a valid stream that is not the one
// it was checkpointed on.
type VertexMeta struct {
	ID          string
	Parallelism int
	Count       int64
}

// Metadata is what a checkpoint records about the job that wrote it.
//
// Vertices are held in ID order, which is the order the graph's own traversal
// produces, so the encoding is a function of the job rather than of a map's
// iteration.
//
// Seed is recorded and NOT validated, and the asymmetry is deliberate. A job's
// seed decides which records a source produces, so restoring a checkpoint into
// a job with a different seed replays a different stream from the same offsets
// and produces a wrong answer with nothing to point at -- exactly the hazard
// Parallelism and Count are validated against. It is not validated because the
// graph cannot report it: a seed lives inside a source's own configuration, and
// core.Source has no method that would surface one. Adding a Seed() to that
// interface would be an interface method with a single implementation behind
// it, which the scope rules forbid, so the seed is recorded from the job
// configuration for a person and for Phase 4 to read, and the validation this
// package can actually perform is performed.
type Metadata struct {
	Vertices []VertexMeta
	Seed     uint64
}

// CheckAgainst reports the first way m and want disagree about the shape of the
// job.
//
// Every message names BOTH values. "parallelism mismatch" sends the reader to
// read two configurations and guess which is which; "checkpoint has 4, job has
// 2" is the whole diagnosis.
func (m Metadata) CheckAgainst(want Metadata) error {
	if len(m.Vertices) != len(want.Vertices) {
		return fmt.Errorf("%w: checkpoint has %d vertices, job has %d", errVertexMismatch, len(m.Vertices), len(want.Vertices))
	}
	for i, got := range m.Vertices {
		exp := want.Vertices[i]
		if got.ID != exp.ID {
			return fmt.Errorf("%w: vertex %d is %q in the checkpoint and %q in the job", errVertexMismatch, i, got.ID, exp.ID)
		}
		if got.Parallelism != exp.Parallelism {
			return fmt.Errorf("%w: vertex %q has parallelism %d in the checkpoint and %d in the job",
				errParallelismDiff, got.ID, got.Parallelism, exp.Parallelism)
		}
		if got.Count != exp.Count {
			return fmt.Errorf("%w: source %q has count %d in the checkpoint and %d in the job",
				errCountDiff, got.ID, got.Count, exp.Count)
		}
	}
	return nil
}

// Subtasks returns every subtask the job described by m runs, in vertex order
// then index order. It is what Load reads and what a coordinator counts
// acknowledgements against.
func (m Metadata) Subtasks() []SubtaskKey {
	var out []SubtaskKey
	for _, v := range m.Vertices {
		for i := range v.Parallelism {
			out = append(out, SubtaskKey{VertexID: v.ID, Index: i})
		}
	}
	return out
}

// Storage reads and writes checkpoints under one root directory.
//
// It holds no state beyond the path and the filesystem it writes through.
// Concurrent use is the caller's to serialise: the coordinator is the only
// writer and it holds a mutex, and a restore reads a checkpoint nothing is
// writing to.
type Storage struct {
	root string
	// fs is the filesystem the writes go through. It is osFS in production and
	// a recorder in the one test that asserts the ORDER of them; see fileSystem
	// for why the order needs a seam to be observable at all.
	fs fileSystem
}

// NewStorage returns storage rooted at root. The directory is created when the
// first checkpoint is written, not here, so constructing one costs nothing and
// a job that never checkpoints leaves no directory behind.
func NewStorage(root string) *Storage { return &Storage{root: root, fs: osFS{}} }

// newStorageOn is NewStorage over a chosen filesystem. Unexported and called
// only by the ordering test; production has exactly one implementation.
func newStorageOn(root string, fs fileSystem) *Storage { return &Storage{root: root, fs: fs} }

// Root returns the directory this storage writes under.
func (s *Storage) Root() string { return s.root }

// dir is the directory holding checkpoint id.
func (s *Storage) dir(id int64) string {
	return filepath.Join(s.root, dirPrefix+strconv.FormatInt(id, 10))
}

// WriteSubtaskState records one subtask's snapshot for checkpoint id.
//
// This is step 1 of the durability sequence and it happens per acknowledgement
// rather than in a batch at the end, which is what leaves a partial directory
// behind when a checkpoint is abandoned. That directory is evidence: it says
// which subtasks got as far as a snapshot before the job came apart.
//
// The file is written to a temporary name, fsynced, and renamed. A rename over
// a complete file is atomic, so a reader never sees a half-written .state; the
// directory entry it creates is made durable by the fsync in Complete, which is
// the only thing that declares this checkpoint usable at all.
func (s *Storage) WriteSubtaskState(id int64, key SubtaskKey, payload []byte) error {
	if err := checkVertexID(key.VertexID); err != nil {
		return fmt.Errorf("checkpoint %d: subtask %s: %w", id, key, err)
	}
	dir := s.dir(id)
	if err := s.fs.MkdirAll(dir); err != nil {
		return fmt.Errorf("checkpoint %d: %w", id, err)
	}
	if err := writeFileSynced(s.fs, dir, key.fileName(), encodeSubtaskState(id, key, payload)); err != nil {
		return fmt.Errorf("checkpoint %d: subtask %s: %w", id, key, err)
	}
	return nil
}

// Complete writes the metadata and then the marker that makes checkpoint id
// usable.
//
// Steps 2, 3 and 4 of the durability sequence, and the order is the point:
//
//  1. WriteSubtaskState has already put every .state file in place, each
//     fsynced before its rename.
//  2. _METADATA is written the same way.
//  3. The DIRECTORY is fsynced, which is what makes those renames durable. A
//     file's own fsync makes its contents durable; the directory entry naming
//     it is a separate write, and without this step a crash can leave a
//     directory that does not list a file whose contents are safely on disk.
//  4. _COMPLETE is written, fsynced, renamed, and the directory fsynced again.
//
// A crash anywhere before step 4 finishes leaves a directory with no
// _COMPLETE, which Latest skips. There is no state in which _COMPLETE is
// durable and the files it vouches for are not, and that is the whole claim
// this function makes.
//
// The claim is held to by TestTheWriteOrderIsInvariantEight, which records the
// sequence through the fileSystem seam. It is worth saying why that test exists
// rather than a check on the directory afterwards: the two orders produce the
// SAME final directory, so the only thing that ever caught a marker written
// early was Load re-verifying every subtask the metadata names -- downstream of
// the write, and not on the path a transactional sink takes when it commits on
// the marker alone.
func (s *Storage) Complete(id int64, meta Metadata) error {
	dir := s.dir(id)
	if err := s.fs.MkdirAll(dir); err != nil {
		return fmt.Errorf("checkpoint %d: %w", id, err)
	}
	if err := writeFileSynced(s.fs, dir, metadataName, encodeMetadata(meta)); err != nil {
		return fmt.Errorf("checkpoint %d: metadata: %w", id, err)
	}
	if err := s.fs.SyncDir(dir); err != nil {
		return fmt.Errorf("checkpoint %d: sync directory before completing: %w", id, err)
	}
	// The marker's contents are not read by anything; its existence is the
	// signal. The ID is in it so that a directory copied to the wrong name is
	// visible to a person looking at the file.
	if err := writeFileSynced(s.fs, dir, completeName, []byte(strconv.FormatInt(id, 10)+"\n")); err != nil {
		return fmt.Errorf("checkpoint %d: complete marker: %w", id, err)
	}
	if err := s.fs.SyncDir(dir); err != nil {
		return fmt.Errorf("checkpoint %d: sync directory after completing: %w", id, err)
	}
	return nil
}

// Latest returns the highest checkpoint ID that has a _COMPLETE marker.
//
// A directory without the marker is SKIPPED and never repaired. It is either
// in flight or abandoned, and there is no way to tell those apart from here;
// repairing one would mean deciding that a checkpoint nobody finished is good
// enough, which is invariant 8 read backwards. The highest complete one wins
// even when a higher incomplete one sits next to it, which is exactly what a
// job that died mid-checkpoint leaves behind.
//
// ok is false when the root holds no complete checkpoint, including when the
// root does not exist. A caller asking to restore from a root with nothing in
// it is asking for something that cannot be done, and that is the caller's
// error to report rather than this one's to paper over.
func (s *Storage) Latest() (id int64, ok bool, err error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("checkpoint root %s: %w", s.root, err)
	}

	best := int64(0)
	found := false
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), dirPrefix) {
			continue
		}
		n, convErr := strconv.ParseInt(strings.TrimPrefix(e.Name(), dirPrefix), 10, 64)
		if convErr != nil {
			continue
		}
		if _, statErr := os.Stat(filepath.Join(s.root, e.Name(), completeName)); statErr != nil {
			continue
		}
		if !found || n > best {
			best, found = n, true
		}
	}
	return best, found, nil
}

// Load reads checkpoint id: its metadata, and every subtask's payload.
//
// The subtasks read are the ones the METADATA names, so a checkpoint is
// self-describing and a caller does not have to know the job to read one. A
// named subtask whose file is missing is an error: a checkpoint that validates
// structurally but has no state for a subtask would restore that subtask as
// empty, and an operator starting from empty state produces counts that are too
// low rather than an error.
//
// Every file is verified before any of it is used: magic, version, the identity
// it claims, and the CRC.
func (s *Storage) Load(id int64) (Metadata, map[SubtaskKey][]byte, error) {
	dir := s.dir(id)
	if _, err := os.Stat(filepath.Join(dir, completeName)); err != nil {
		return Metadata{}, nil, fmt.Errorf("checkpoint %d: %w: %s is absent", id, errNoCheckpoint, completeName)
	}

	raw, err := os.ReadFile(filepath.Join(dir, metadataName))
	if err != nil {
		return Metadata{}, nil, fmt.Errorf("checkpoint %d: metadata: %w", id, err)
	}
	meta, err := decodeMetadata(raw)
	if err != nil {
		return Metadata{}, nil, fmt.Errorf("checkpoint %d: metadata: %w", id, err)
	}

	payloads := make(map[SubtaskKey][]byte)
	for _, key := range meta.Subtasks() {
		raw, err := os.ReadFile(filepath.Join(dir, key.fileName()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return Metadata{}, nil, fmt.Errorf("checkpoint %d: %w: %s", id, errMissingStateFile, key)
			}
			return Metadata{}, nil, fmt.Errorf("checkpoint %d: subtask %s: %w", id, key, err)
		}
		payload, err := decodeSubtaskState(raw, id, key)
		if err != nil {
			return Metadata{}, nil, fmt.Errorf("checkpoint %d: subtask %s: %w", id, key, err)
		}
		payloads[key] = payload
	}
	return meta, payloads, nil
}

// encodeSubtaskState renders one subtask's state file, CRC included.
func encodeSubtaskState(id int64, key SubtaskKey, payload []byte) []byte {
	buf := make([]byte, 0, len(magic)+2+8+2+len(key.VertexID)+4+8+len(payload)+4)
	buf = append(buf, magic...)
	buf = binary.BigEndian.AppendUint16(buf, formatVer)
	buf = binary.BigEndian.AppendUint64(buf, uint64(id))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(key.VertexID)))
	buf = append(buf, key.VertexID...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(key.Index))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(payload)))
	buf = append(buf, payload...)
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

// decodeSubtaskState verifies one state file and returns its payload.
//
// The CRC is checked FIRST, before any length in the file is trusted, because a
// corrupted payloadLen is exactly what would make a decoder allocate or read
// nonsense. Everything after that is a comparison against what the caller asked
// for.
func decodeSubtaskState(raw []byte, wantID int64, wantKey SubtaskKey) ([]byte, error) {
	r, err := newReader(raw, formatVer)
	if err != nil {
		return nil, err
	}
	id, err := r.uint64()
	if err != nil {
		return nil, err
	}
	if int64(id) != wantID {
		return nil, fmt.Errorf("%w: file holds checkpoint %d, want %d", errWrongCheckpoint, int64(id), wantID)
	}
	vertexID, err := r.shortString()
	if err != nil {
		return nil, err
	}
	index, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if vertexID != wantKey.VertexID || int(index) != wantKey.Index {
		return nil, fmt.Errorf("%w: file holds %s[%d], want %s", errWrongSubtask, vertexID, index, wantKey)
	}
	payloadLen, err := r.uint64()
	if err != nil {
		return nil, err
	}
	return r.bytes(payloadLen)
}

// encodeMetadata renders _METADATA, CRC included.
//
// The same framing as a state file: magic, version, body, CRC. One framing for
// both means one decoder to be right about, and _METADATA is worth a CRC for
// the same reason a state file is -- a torn write here produces a parallelism
// that is off by one, which is the mismatch this file exists to catch.
func encodeMetadata(m Metadata) []byte {
	var buf []byte
	buf = append(buf, magic...)
	buf = binary.BigEndian.AppendUint16(buf, metadataVer)
	buf = binary.BigEndian.AppendUint64(buf, m.Seed)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.Vertices)))
	for _, v := range m.Vertices {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v.ID)))
		buf = append(buf, v.ID...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(v.Parallelism))
		buf = binary.BigEndian.AppendUint64(buf, uint64(v.Count))
	}
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

// decodeMetadata verifies and reads _METADATA.
func decodeMetadata(raw []byte) (Metadata, error) {
	r, err := newReader(raw, metadataVer)
	if err != nil {
		return Metadata{}, err
	}
	seed, err := r.uint64()
	if err != nil {
		return Metadata{}, err
	}
	n, err := r.uint32()
	if err != nil {
		return Metadata{}, err
	}
	m := Metadata{Seed: seed}
	for i := uint32(0); i < n; i++ {
		id, err := r.shortString()
		if err != nil {
			return Metadata{}, fmt.Errorf("vertex %d of %d: %w", i, n, err)
		}
		parallelism, err := r.uint32()
		if err != nil {
			return Metadata{}, fmt.Errorf("vertex %q: %w", id, err)
		}
		count, err := r.uint64()
		if err != nil {
			return Metadata{}, fmt.Errorf("vertex %q: %w", id, err)
		}
		m.Vertices = append(m.Vertices, VertexMeta{ID: id, Parallelism: int(parallelism), Count: int64(count)})
	}
	return m, nil
}

// reader walks a verified file body. Every read is bounds-checked and reports
// errShortFile rather than panicking, because a truncated file is one of the
// things this format exists to catch.
type reader struct {
	buf []byte
	pos int
}

// newReader checks the magic, the version and the CRC, and returns a reader
// positioned after the header and stopping before the checksum.
func newReader(raw []byte, wantVersion uint16) (*reader, error) {
	// magic + version + crc is the smallest a well-formed file can be.
	if len(raw) < len(magic)+2+4 {
		return nil, fmt.Errorf("%w: %d bytes", errShortFile, len(raw))
	}
	if string(raw[:len(magic)]) != magic {
		return nil, fmt.Errorf("%w: begins with %q, want %q", errBadMagic, raw[:len(magic)], magic)
	}
	body, sum := raw[:len(raw)-4], binary.BigEndian.Uint32(raw[len(raw)-4:])
	if got := crc32.ChecksumIEEE(body); got != sum {
		return nil, fmt.Errorf("%w: computed %#08x, file records %#08x", errCRCMismatch, got, sum)
	}
	version := binary.BigEndian.Uint16(raw[len(magic):])
	if version != wantVersion {
		return nil, fmt.Errorf("%w: file is version %d, this build reads %d", errBadVersion, version, wantVersion)
	}
	return &reader{buf: body, pos: len(magic) + 2}, nil
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, fmt.Errorf("%w: wanted %d bytes at offset %d of %d", errShortFile, n, r.pos, len(r.buf))
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *reader) uint32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) uint64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

// shortString reads a uint16-length-prefixed string.
func (r *reader) shortString() (string, error) {
	b, err := r.take(2)
	if err != nil {
		return "", err
	}
	s, err := r.take(int(binary.BigEndian.Uint16(b)))
	if err != nil {
		return "", err
	}
	return string(s), nil
}

// bytes reads a payload of exactly n bytes and requires that it runs to the end
// of the body.
//
// The trailing check matters: a file whose payloadLen is short leaves bytes
// between the payload and the checksum, and since the CRC covers all of them
// such a file is internally consistent. It is still not a file this format
// produced, so it is refused rather than half-read.
func (r *reader) bytes(n uint64) ([]byte, error) {
	if n > uint64(len(r.buf)-r.pos) {
		return nil, fmt.Errorf("%w: payload claims %d bytes, %d remain", errShortFile, n, len(r.buf)-r.pos)
	}
	out, err := r.take(int(n))
	if err != nil {
		return nil, err
	}
	if r.pos != len(r.buf) {
		return nil, fmt.Errorf("%w: %d bytes follow the payload", errShortFile, len(r.buf)-r.pos)
	}
	return out, nil
}

// checkVertexID rejects an ID that cannot be half of a file name.
//
// A separator would put a subtask's state outside its checkpoint directory, and
// "." or ".." would name the directory itself. Rejected rather than escaped,
// because an escaping scheme is a second encoding to keep in step with the one
// Load uses to build the name back.
func checkVertexID(id string) error {
	switch {
	case id == "":
		return fmt.Errorf("%w: it is empty", errBadVertexID)
	case id == "." || id == "..":
		return fmt.Errorf("%w: %q names a directory", errBadVertexID, id)
	case strings.ContainsRune(id, '/') || strings.ContainsRune(id, filepath.Separator):
		return fmt.Errorf("%w: %q contains a path separator", errBadVertexID, id)
	case len(id) > int(^uint16(0)):
		return fmt.Errorf("%w: %d bytes does not fit the length field", errBadVertexID, len(id))
	}
	return nil
}

// writeFileSynced writes data to dir/name atomically: to a temporary file,
// fsynced, then renamed.
//
// The fsync is BEFORE the rename and that is the whole of the guarantee. A
// rename is atomic, so a reader sees the old file or the new one; without the
// fsync it can see the new NAME pointing at contents that never reached the
// disk, which is a file of the right length full of whatever was in those
// blocks. That is the case the CRC catches and this ordering avoids.
func writeFileSynced(fs fileSystem, dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+tempSuffix)
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		fs.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		fs.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		fs.Remove(tmp)
		return err
	}
	return fs.Rename(tmp, filepath.Join(dir, name))
}

// fileSystem is the writes this package makes, as a seam.
//
// # Why it exists
//
// Invariant 8 is an ORDER: every state file, then the metadata, then the
// directory fsync that makes those renames durable, and only then _COMPLETE.
// Until this seam existed the order was unenforced in the sense that mattered
// -- a Complete that wrote the marker first was caught by Storage.Load, which
// re-verifies every subtask the metadata names, and not by anything that knew
// what order the writes went out in. Load is a fine backstop and it is the
// wrong one to rely on: from Phase 5 a transactional sink commits on the
// presence of _COMPLETE without calling Load at all, so a marker that reached
// the disk before the state it vouches for would commit output for a
// checkpoint that cannot be restored.
//
// The order cannot be observed from outside without crashing the process part
// way through the sequence, which no unit test can arrange. So the sequence is
// made observable instead: every write goes through this interface, the
// production implementation is a passthrough to package os, and the ordering
// test's implementation is the same passthrough with a log.
//
// # Why it is unexported and why there are two of them
//
// CLAUDE.md counts exported interfaces in pkg/core and asks for a justification
// per interface. This is neither: it is package-private, it has exactly one
// production implementation, and nothing outside this file names it. It is two
// interfaces rather than one because writeFileSynced's guarantee is a sequence
// WITHIN one file -- create, write, fsync, close, then rename -- and a single
// WriteFile method would collapse the four operations the test has to tell
// apart into one. "_COMPLETE is fsynced before its rename" is not a statement a
// coarser seam can make.
//
// Reads are NOT on it. Latest and Load go to package os directly: this exists
// to pin the order of the writes, and putting the reads through it would be an
// indirection bought for symmetry.
type fileSystem interface {
	MkdirAll(dir string) error
	// Create truncates or creates path and opens it for writing.
	Create(path string) (syncFile, error)
	Rename(from, to string) error
	// SyncDir fsyncs a directory, making the renames into it durable.
	SyncDir(dir string) error
	// Remove deletes path. It is the cleanup on a failed write and its error is
	// deliberately dropped by the caller, which already has a better one.
	Remove(path string) error
}

// syncFile is one file open for writing, with the fsync that makes its contents
// durable before it is renamed into place.
type syncFile interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// osFS is the production filesystem: a passthrough to package os, and the only
// implementation a job ever runs on.
type osFS struct{}

var _ fileSystem = osFS{}

func (osFS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

func (osFS) Create(path string) (syncFile, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
}

func (osFS) Rename(from, to string) error { return os.Rename(from, to) }

func (osFS) Remove(path string) error { return os.Remove(path) }

// SyncDir fsyncs a directory, making the renames into it durable.
//
// Syncing a file makes its CONTENTS durable. The directory entry that names it
// is a separate piece of metadata, and a crash between the two leaves a
// directory that does not list a file whose data is safely on disk. Every
// production checkpoint format gets this wrong at least once.
func (osFS) SyncDir(dir string) error {
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
