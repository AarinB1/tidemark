package checkpoint

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// testMetadata is the shape most rows below checkpoint: two sources of
// different lengths and an operator and a sink behind them.
func testMetadata() Metadata {
	return Metadata{
		Seed: 7,
		Vertices: []VertexMeta{
			{ID: "op", Parallelism: 2},
			{ID: "out", Parallelism: 1},
			{ID: "srcA", Parallelism: 2, Count: 1000},
			{ID: "srcB", Parallelism: 1, Count: 40},
		},
	}
}

// writeCheckpoint writes every subtask meta names, with a payload derived from
// the subtask so a mixed-up file is visible, and completes it.
func writeCheckpoint(t *testing.T, s *Storage, id int64, meta Metadata) map[SubtaskKey][]byte {
	t.Helper()
	want := make(map[SubtaskKey][]byte)
	for _, key := range meta.Subtasks() {
		payload := []byte(key.String() + " at checkpoint")
		if err := s.WriteSubtaskState(id, key, payload); err != nil {
			t.Fatalf("WriteSubtaskState(%d, %s): %v", id, key, err)
		}
		want[key] = payload
	}
	if err := s.Complete(id, meta); err != nil {
		t.Fatalf("Complete(%d): %v", id, err)
	}
	return want
}

// TestCheckpointRoundTrips is the base case: what is written comes back, per
// subtask, with the metadata that described it.
func TestCheckpointRoundTrips(t *testing.T) {
	s := NewStorage(t.TempDir())
	meta := testMetadata()
	want := writeCheckpoint(t, s, 3, meta)

	gotMeta, payloads, err := s.Load(3)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !slices.Equal(gotMeta.Vertices, meta.Vertices) || gotMeta.Seed != meta.Seed {
		t.Fatalf("metadata came back as %+v, want %+v", gotMeta, meta)
	}
	if len(payloads) != len(want) {
		t.Fatalf("Load returned %d payloads, want %d", len(payloads), len(want))
	}
	for key, w := range want {
		if got, ok := payloads[key]; !ok {
			t.Errorf("no payload for %s", key)
		} else if !bytes.Equal(got, w) {
			t.Errorf("payload for %s is %q, want %q", key, got, w)
		}
	}
}

// TestEmptyPayloadRoundTrips is the sink's case. A sink acknowledges with
// nothing, and a format that could not distinguish "no state" from "no file"
// would make a sink's checkpoint indistinguishable from a missing one.
func TestEmptyPayloadRoundTrips(t *testing.T) {
	s := NewStorage(t.TempDir())
	meta := Metadata{Vertices: []VertexMeta{{ID: "out", Parallelism: 1}}}
	key := SubtaskKey{VertexID: "out", Index: 0}

	if err := s.WriteSubtaskState(1, key, nil); err != nil {
		t.Fatalf("WriteSubtaskState: %v", err)
	}
	if err := s.Complete(1, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	_, payloads, err := s.Load(1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	payload, ok := payloads[key]
	if !ok {
		t.Fatal("no payload for the sink subtask")
	}
	if len(payload) != 0 {
		t.Errorf("sink payload is %q, want empty", payload)
	}
}

// TestLatestIgnoresACheckpointWithoutComplete is invariant 8 stated as a test.
//
// The higher directory is complete in every way except the marker, which is
// exactly what a job that died between writing its last state file and writing
// _COMPLETE leaves behind. Selecting it would restore a cut that no subtask
// ever agreed on.
func TestLatestIgnoresACheckpointWithoutComplete(t *testing.T) {
	s := NewStorage(t.TempDir())
	meta := testMetadata()

	writeCheckpoint(t, s, 1, meta)
	writeCheckpoint(t, s, 2, meta)

	// Checkpoint 3: every state file, and the metadata, but no marker.
	for _, key := range meta.Subtasks() {
		if err := s.WriteSubtaskState(3, key, []byte("partial")); err != nil {
			t.Fatalf("WriteSubtaskState(3, %s): %v", key, err)
		}
	}
	if err := writeFileSynced(s.fs, s.dir(3), metadataName, encodeMetadata(meta)); err != nil {
		t.Fatalf("writing metadata for 3: %v", err)
	}

	id, ok, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if !ok {
		t.Fatal("Latest found no complete checkpoint")
	}
	if id != 2 {
		t.Errorf("Latest selected checkpoint %d, want 2: an incomplete checkpoint was chosen over a complete one", id)
	}

	// And it is still there afterwards. An abandoned checkpoint is evidence,
	// not litter.
	if _, err := os.Stat(s.dir(3)); err != nil {
		t.Errorf("the incomplete checkpoint directory was removed: %v", err)
	}

	// Loading it directly is refused too, so the marker is a gate rather than
	// only a hint to the selector.
	if _, _, err := s.Load(3); !errors.Is(err, errNoCheckpoint) {
		t.Errorf("Load of an incomplete checkpoint = %v, want %v", err, errNoCheckpoint)
	}
}

// TestLatestOverAnEmptyOrAbsentRoot covers the two ways there is nothing to
// restore from. Neither is an error here: a caller that asked to restore is the
// one who knows that finding nothing is a problem.
func TestLatestOverAnEmptyOrAbsentRoot(t *testing.T) {
	tests := []struct {
		name string
		root func(t *testing.T) string
	}{
		{name: "absent", root: func(t *testing.T) string { return filepath.Join(t.TempDir(), "nothing-here") }},
		{name: "empty", root: func(t *testing.T) string { return t.TempDir() }},
		{
			name: "holds unrelated directories",
			root: func(t *testing.T) string {
				dir := t.TempDir()
				for _, name := range []string{"chk-notanumber", "logs", "chk-"} {
					if err := os.Mkdir(filepath.Join(dir, name), 0o755); err != nil {
						t.Fatalf("Mkdir(%s): %v", name, err)
					}
				}
				return dir
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, ok, err := NewStorage(tt.root(t)).Latest()
			if err != nil {
				t.Fatalf("Latest: %v", err)
			}
			if ok {
				t.Errorf("Latest reported checkpoint %d, want none", id)
			}
		})
	}
}

// TestLoadRejectsACorruptedFile is what the CRC is for.
//
// Every byte position is flipped in turn, so the coverage is not "a corruption
// somewhere" but "a corruption anywhere". A flip in the payload is the one that
// matters most: without a checksum it decodes into a state that is valid,
// different, and restores.
func TestLoadRejectsACorruptedFile(t *testing.T) {
	meta := Metadata{Vertices: []VertexMeta{{ID: "op", Parallelism: 1}}}
	key := SubtaskKey{VertexID: "op", Index: 0}
	payload := []byte("an aggregate a recovery would believe")

	// One good file, to corrupt a copy of at every offset.
	good := encodeSubtaskState(4, key, payload)
	if _, err := decodeSubtaskState(good, 4, key); err != nil {
		t.Fatalf("the uncorrupted file does not decode: %v", err)
	}

	for i := range good {
		corrupt := bytes.Clone(good)
		corrupt[i] ^= 0x01

		s := NewStorage(t.TempDir())
		if err := os.MkdirAll(s.dir(4), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(s.dir(4), key.fileName()), corrupt, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := s.Complete(4, meta); err != nil {
			t.Fatalf("Complete: %v", err)
		}

		_, _, err := s.Load(4)
		if err == nil {
			t.Fatalf("a file with byte %d of %d flipped loaded without error", i, len(good))
		}
		// A flip in the magic is caught by the magic check; everything else by
		// the checksum. Both are refusals with a cause, which is the point:
		// nothing decodes into plausible state.
		if !errors.Is(err, errCRCMismatch) && !errors.Is(err, errBadMagic) {
			t.Fatalf("a file with byte %d flipped failed with %v, want a magic or checksum failure", i, err)
		}
	}
}

// TestLoadRejectsATruncatedFile covers a write that stopped part way.
//
// Truncation is not the same failure as corruption: the bytes that are there
// are the right bytes, so a decoder that read what it could would return a
// SHORTER payload rather than a wrong one, and a short state is a job that
// starts with some of its aggregates missing.
func TestLoadRejectsATruncatedFile(t *testing.T) {
	meta := Metadata{Vertices: []VertexMeta{{ID: "op", Parallelism: 1}}}
	key := SubtaskKey{VertexID: "op", Index: 0}
	good := encodeSubtaskState(5, key, []byte("payload bytes"))

	for n := 0; n < len(good); n++ {
		s := NewStorage(t.TempDir())
		if err := os.MkdirAll(s.dir(5), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(filepath.Join(s.dir(5), key.fileName()), good[:n], 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := s.Complete(5, meta); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		if _, payloads, err := s.Load(5); err == nil {
			t.Fatalf("the first %d of %d bytes loaded without error, returning %q", n, len(good), payloads[key])
		}
	}
}

// TestLoadRejectsAFileFromAnotherSubtask covers the file that is intact and in
// the wrong place.
//
// Restoring subtask 1's aggregates into subtask 0 is silent: both states are
// well-formed, the job runs, and two subtasks produce wrong counts. The
// identity in the file, rather than only in its name, is what catches it.
func TestLoadRejectsAFileFromAnotherSubtask(t *testing.T) {
	meta := Metadata{Vertices: []VertexMeta{{ID: "op", Parallelism: 2}}}
	zero := SubtaskKey{VertexID: "op", Index: 0}
	one := SubtaskKey{VertexID: "op", Index: 1}

	s := NewStorage(t.TempDir())
	if err := s.WriteSubtaskState(6, zero, []byte("zero")); err != nil {
		t.Fatalf("WriteSubtaskState: %v", err)
	}
	// Subtask 1's file holds subtask 0's contents under subtask 1's name.
	if err := os.WriteFile(filepath.Join(s.dir(6), one.fileName()), encodeSubtaskState(6, zero, []byte("zero")), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := s.Complete(6, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if _, _, err := s.Load(6); !errors.Is(err, errWrongSubtask) {
		t.Errorf("Load with a misplaced state file = %v, want %v", err, errWrongSubtask)
	}
}

// TestLoadRejectsAFileFromAnotherCheckpoint covers a state file left over from
// an earlier checkpoint in a directory that was reused.
func TestLoadRejectsAFileFromAnotherCheckpoint(t *testing.T) {
	meta := Metadata{Vertices: []VertexMeta{{ID: "op", Parallelism: 1}}}
	key := SubtaskKey{VertexID: "op", Index: 0}

	s := NewStorage(t.TempDir())
	if err := os.MkdirAll(s.dir(8), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(s.dir(8), key.fileName()), encodeSubtaskState(7, key, []byte("stale")), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := s.Complete(8, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if _, _, err := s.Load(8); !errors.Is(err, errWrongCheckpoint) {
		t.Errorf("Load with a stale state file = %v, want %v", err, errWrongCheckpoint)
	}
}

// TestLoadRejectsAMissingSubtaskFile is the checkpoint that validates
// structurally and cannot be used.
//
// An absent file would restore that subtask from empty state, which produces
// counts that are too low. It is refused so that the failure is a job that will
// not start rather than a job with a quiet hole in it.
func TestLoadRejectsAMissingSubtaskFile(t *testing.T) {
	meta := Metadata{Vertices: []VertexMeta{{ID: "op", Parallelism: 2}}}
	s := NewStorage(t.TempDir())

	if err := s.WriteSubtaskState(9, SubtaskKey{VertexID: "op", Index: 0}, []byte("zero")); err != nil {
		t.Fatalf("WriteSubtaskState: %v", err)
	}
	if err := s.Complete(9, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if _, _, err := s.Load(9); !errors.Is(err, errMissingStateFile) {
		t.Errorf("Load of a checkpoint missing a subtask = %v, want %v", err, errMissingStateFile)
	}
}

// TestMetadataCheckAgainstNamesBothValues is the Phase 1 constraint coming due.
//
// A source subtask's range is derived from (Count, parallelism, index) and only
// its resume OFFSET is checkpointed, so restoring at a different shape hands a
// subtask an offset out of somebody else's range. The job reads a valid stream
// that is not the one it was checkpointed on and produces a wrong answer with
// nothing to point at. Every message names both numbers, because a reader who
// has to go and look them up is a reader who guesses.
func TestMetadataCheckAgainstNamesBothValues(t *testing.T) {
	base := testMetadata()

	tests := []struct {
		name     string
		mutate   func(Metadata) Metadata
		wantErr  error
		contains []string
	}{
		{
			name:    "identical",
			mutate:  func(m Metadata) Metadata { return m },
			wantErr: nil,
		},
		{
			name: "a different parallelism",
			mutate: func(m Metadata) Metadata {
				m.Vertices = slices.Clone(m.Vertices)
				m.Vertices[0].Parallelism = 4
				return m
			},
			wantErr:  errParallelismDiff,
			contains: []string{"op", "2", "4"},
		},
		{
			name: "a different source count",
			mutate: func(m Metadata) Metadata {
				m.Vertices = slices.Clone(m.Vertices)
				m.Vertices[2].Count = 999
				return m
			},
			wantErr:  errCountDiff,
			contains: []string{"srcA", "1000", "999"},
		},
		{
			name: "a renamed vertex",
			mutate: func(m Metadata) Metadata {
				m.Vertices = slices.Clone(m.Vertices)
				m.Vertices[1].ID = "sink"
				return m
			},
			wantErr:  errVertexMismatch,
			contains: []string{"out", "sink"},
		},
		{
			name: "a vertex added",
			mutate: func(m Metadata) Metadata {
				m.Vertices = append(slices.Clone(m.Vertices), VertexMeta{ID: "zzz", Parallelism: 1})
				return m
			},
			wantErr:  errVertexMismatch,
			contains: []string{"4", "5"},
		},
		{
			// The seed is recorded and not validated; see the note on Metadata.
			name: "a different seed",
			mutate: func(m Metadata) Metadata {
				m.Seed = 99
				return m
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := base.CheckAgainst(tt.mutate(base))
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("CheckAgainst = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CheckAgainst = %v, want %v", err, tt.wantErr)
			}
			for _, want := range tt.contains {
				if !bytes.Contains([]byte(err.Error()), []byte(want)) {
					t.Errorf("the message %q does not name %q", err, want)
				}
			}
		})
	}
}

// TestMetadataRoundTripsAndDetectsCorruption. _METADATA carries a checksum for
// the same reason a state file does: a torn write here produces a parallelism
// that is off by one, which is precisely the mismatch the file exists to catch.
func TestMetadataRoundTripsAndDetectsCorruption(t *testing.T) {
	meta := testMetadata()
	raw := encodeMetadata(meta)

	got, err := decodeMetadata(raw)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if got.Seed != meta.Seed || !slices.Equal(got.Vertices, meta.Vertices) {
		t.Fatalf("metadata round-tripped to %+v, want %+v", got, meta)
	}

	for i := range raw {
		corrupt := bytes.Clone(raw)
		corrupt[i] ^= 0x80
		if _, err := decodeMetadata(corrupt); err == nil {
			t.Fatalf("metadata with byte %d of %d flipped decoded without error", i, len(raw))
		}
	}
}

// TestWriteSubtaskStateRejectsAVertexIDThatIsNotAFileName. A separator would
// put a subtask's state outside its own checkpoint directory, where Load will
// not look for it and where it may overwrite something.
func TestWriteSubtaskStateRejectsAVertexIDThatIsNotAFileName(t *testing.T) {
	s := NewStorage(t.TempDir())
	for _, id := range []string{"", ".", "..", "a/b", "../escape"} {
		err := s.WriteSubtaskState(1, SubtaskKey{VertexID: id, Index: 0}, nil)
		if !errors.Is(err, errBadVertexID) {
			t.Errorf("WriteSubtaskState with vertex ID %q = %v, want %v", id, err, errBadVertexID)
		}
	}
}

// TestCompleteIsWrittenLast reads the directory back and checks the ordering
// claim from the outside.
//
// What is checked here is the state the sequence produces: before Complete
// there is no marker and Latest finds nothing; after it there is a marker,
// every state file, and the metadata. That is a weaker claim than the order
// itself, and deliberately kept: it is the claim a reader can check against the
// directory in front of them. The order is TestTheWriteOrderIsInvariantEight,
// which records the sequence through the filesystem seam rather than inferring
// it from the state left behind -- a Complete that wrote the marker first
// leaves exactly this state and fails that one.
func TestCompleteIsWrittenLast(t *testing.T) {
	s := NewStorage(t.TempDir())
	meta := testMetadata()

	for _, key := range meta.Subtasks() {
		if err := s.WriteSubtaskState(1, key, []byte("state")); err != nil {
			t.Fatalf("WriteSubtaskState(%s): %v", key, err)
		}
	}

	if _, ok, err := s.Latest(); err != nil || ok {
		t.Fatalf("Latest before Complete = (ok %t, err %v), want no checkpoint", ok, err)
	}
	for _, name := range []string{completeName, metadataName} {
		if _, err := os.Stat(filepath.Join(s.dir(1), name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists before Complete was called", name)
		}
	}

	if err := s.Complete(1, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if _, ok, err := s.Latest(); err != nil || !ok {
		t.Fatalf("Latest after Complete = (ok %t, err %v), want the checkpoint", ok, err)
	}

	// No temporary files survive. One left behind means a rename did not
	// happen, and the next writer would find a file where it expects to create
	// one.
	entries, err := os.ReadDir(s.dir(1))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == tempSuffix {
			t.Errorf("temporary file %s survived", e.Name())
		}
	}
}

// The ordering seam and the test that holds invariant 8 to it.
//
// TestCompleteIsWrittenLast above checks the STATE the sequence produces:
// before Complete there is no marker, after it there is one and everything it
// vouches for. That is worth having and it is not invariant 8. A Complete that
// wrote _COMPLETE first and the state files afterwards produces the same final
// state, and the only thing that ever caught it was Storage.Load re-verifying
// every subtask the metadata names -- a backstop, downstream of the write, and
// one that a transactional sink committing on the marker does not go through.
//
// So the sequence is recorded instead. See fileSystem.

// fsOpKind is one filesystem operation, in the order a correct sequence
// performs them within a single file.
type fsOpKind uint8

const (
	opMkdirAll fsOpKind = iota
	opCreate
	opWrite
	opSync
	opClose
	opRename
	opSyncDir
	opRemove
)

func (k fsOpKind) String() string {
	switch k {
	case opMkdirAll:
		return "mkdir"
	case opCreate:
		return "create"
	case opWrite:
		return "write"
	case opSync:
		return "fsync"
	case opClose:
		return "close"
	case opRename:
		return "rename"
	case opSyncDir:
		return "fsync-dir"
	case opRemove:
		return "remove"
	default:
		return "unknown"
	}
}

// fsOp is one recorded operation. Path is the file or directory it acted on,
// and for a rename it is the DESTINATION: what the ordering assertions are
// about is when a name became visible, not which temporary file it came from.
type fsOp struct {
	Kind fsOpKind
	Path string
}

func (o fsOp) String() string { return o.Kind.String() + " " + filepath.Base(o.Path) }

// recordingFS is osFS with a log. Every operation is performed for real, so the
// checkpoint this test writes is a checkpoint Load can read back afterwards --
// which it does, because an ordering assertion over a sequence that produced
// nothing usable would be an assertion about a broken write.
type recordingFS struct {
	inner osFS
	ops   []fsOp
}

var _ fileSystem = (*recordingFS)(nil)

func (r *recordingFS) record(kind fsOpKind, path string) {
	r.ops = append(r.ops, fsOp{Kind: kind, Path: path})
}

func (r *recordingFS) MkdirAll(dir string) error {
	r.record(opMkdirAll, dir)
	return r.inner.MkdirAll(dir)
}

func (r *recordingFS) Create(path string) (syncFile, error) {
	r.record(opCreate, path)
	f, err := r.inner.Create(path)
	if err != nil {
		return nil, err
	}
	return &recordingFile{inner: f, fs: r, path: path}, nil
}

func (r *recordingFS) Rename(from, to string) error {
	r.record(opRename, to)
	return r.inner.Rename(from, to)
}

func (r *recordingFS) SyncDir(dir string) error {
	r.record(opSyncDir, dir)
	return r.inner.SyncDir(dir)
}

func (r *recordingFS) Remove(path string) error {
	r.record(opRemove, path)
	return r.inner.Remove(path)
}

// recordingFile logs what happens to one open file and forwards it.
//
// Every method is forwarded EXPLICITLY rather than promoted from an embedded
// syncFile. Embedding would compile and would leave a recorder that silently
// stopped logging the moment syncFile grew a method, which is the trap
// CLAUDE.md records against decorators -- and here the consequence is an
// ordering assertion over a sequence with a hole in it.
type recordingFile struct {
	inner syncFile
	fs    *recordingFS
	path  string
}

func (f *recordingFile) Write(p []byte) (int, error) {
	f.fs.record(opWrite, f.path)
	return f.inner.Write(p)
}

func (f *recordingFile) Sync() error {
	f.fs.record(opSync, f.path)
	return f.inner.Sync()
}

func (f *recordingFile) Close() error {
	f.fs.record(opClose, f.path)
	return f.inner.Close()
}

// indexOf returns the position of the first op matching kind and base name, or
// -1.
func (r *recordingFS) indexOf(kind fsOpKind, base string) int {
	for i, op := range r.ops {
		if op.Kind == kind && filepath.Base(op.Path) == base {
			return i
		}
	}
	return -1
}

// lastIndexOf returns the position of the LAST op of this kind, or -1.
func (r *recordingFS) lastIndexOf(kind fsOpKind) int {
	last := -1
	for i, op := range r.ops {
		if op.Kind == kind {
			last = i
		}
	}
	return last
}

// renamesOfStateFiles returns the positions of every .state rename.
func (r *recordingFS) renamesOfStateFiles() []int {
	var out []int
	for i, op := range r.ops {
		if op.Kind == opRename && filepath.Ext(op.Path) == stateSuffix {
			out = append(out, i)
		}
	}
	return out
}

func (r *recordingFS) trace() string {
	var b []byte
	for i, op := range r.ops {
		b = append(b, []byte("\n    "+strconv.Itoa(i)+"  "+op.String())...)
	}
	return string(b)
}

// TestTheWriteOrderIsInvariantEight records the whole sequence a checkpoint is
// written in and asserts the four orderings that make _COMPLETE mean something.
//
// A checkpoint is usable for recovery only after _COMPLETE is written, so
// _COMPLETE must be the last thing that becomes visible and everything it
// vouches for must be durable before it does. Each assertion below is one half
// of that, and each names the failure it is for -- a reader who changes the
// sequence and trips one should not have to work out what it was protecting.
func TestTheWriteOrderIsInvariantEight(t *testing.T) {
	root := t.TempDir()
	fs := &recordingFS{}
	s := newStorageOn(root, fs)
	meta := testMetadata()

	for _, key := range meta.Subtasks() {
		if err := s.WriteSubtaskState(1, key, []byte(key.String())); err != nil {
			t.Fatalf("WriteSubtaskState(%s): %v", key, err)
		}
	}
	if err := s.Complete(1, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// The recorded sequence is only evidence if it is the sequence that wrote a
	// checkpoint somebody can restore. Read it back through the ordinary path
	// first: an ordering assertion over a write that produced nothing usable
	// would pass on a Storage that wrote the right operations to the wrong
	// files.
	if _, payloads, err := NewStorage(root).Load(1); err != nil {
		t.Fatalf("Load after the recorded write: %v", err)
	} else if len(payloads) != len(meta.Subtasks()) {
		t.Fatalf("Load returned %d payloads, want %d", len(payloads), len(meta.Subtasks()))
	}

	stateRenames := fs.renamesOfStateFiles()
	if want := len(meta.Subtasks()); len(stateRenames) != want {
		t.Fatalf("the sequence holds %d .state renames and the metadata names %d subtasks: "+
			"a subtask whose file never landed is one _COMPLETE vouches for and Load cannot find%s",
			len(stateRenames), want, fs.trace())
	}
	metadataRename := fs.indexOf(opRename, metadataName)
	completeRename := fs.indexOf(opRename, completeName)
	completeSync := fs.indexOf(opSync, completeName+tempSuffix)
	if metadataRename < 0 || completeRename < 0 || completeSync < 0 {
		t.Fatalf("the sequence is missing one of: %s rename (%d), %s rename (%d), %s fsync (%d)%s",
			metadataName, metadataRename, completeName, completeRename, completeName, completeSync, fs.trace())
	}

	// 1. Every .state file is renamed into place before _METADATA.
	//
	// _METADATA is what names the subtasks a restore will look for. A metadata
	// file that landed first would, at a crash between the two, name a subtask
	// whose state is not there -- and it is _METADATA, not the state files,
	// that Load reads to decide what it is missing.
	for _, at := range stateRenames {
		if at > metadataRename {
			t.Errorf("%s was renamed at step %d and a .state file at step %d: the metadata names "+
				"subtasks whose state had not landed yet%s",
				metadataName, metadataRename, at, fs.trace())
		}
	}

	// 2. The checkpoint directory is fsynced after those renames.
	//
	// A file's own fsync makes its CONTENTS durable. The directory entry naming
	// it is separate metadata, so without this the directory can come back not
	// listing a file whose data is safely on disk.
	dirSyncAfterRenames := -1
	for i, op := range fs.ops {
		if op.Kind == opSyncDir && i > metadataRename {
			dirSyncAfterRenames = i
			break
		}
	}
	if dirSyncAfterRenames < 0 {
		t.Errorf("nothing fsynced the checkpoint directory after the state files and %s were renamed "+
			"into it: their contents are durable and the entries naming them need not be%s",
			metadataName, fs.trace())
	}

	// 3. _COMPLETE's rename is the LAST rename in the sequence.
	//
	// This is invariant 8 stated as an order. The marker declares the whole
	// checkpoint usable, so anything that became visible after it is something
	// the marker vouched for before it existed.
	if last := fs.lastIndexOf(opRename); last != completeRename {
		t.Errorf("%s was renamed at step %d and the last rename in the sequence is step %d (%s): "+
			"the marker declared the checkpoint usable before that name existed%s",
			completeName, completeRename, last, fs.ops[last], fs.trace())
	}

	// 4. _COMPLETE is fsynced, and the directory is fsynced after it.
	//
	// The marker's own fsync is what puts its contents on the disk; the
	// directory fsync after the rename is what puts the ENTRY there. Without
	// the second one a crash can leave a checkpoint that is complete on the
	// platter and absent from its directory, which Latest skips -- losing a
	// recovery point rather than corrupting one, but losing it silently.
	if completeSync > completeRename {
		t.Errorf("%s was fsynced at step %d, after its rename at step %d: the name can point at "+
			"contents that never reached the disk%s",
			completeName, completeSync, completeRename, fs.trace())
	}
	if last := fs.lastIndexOf(opSyncDir); last < completeRename {
		t.Errorf("the last directory fsync is step %d and %s was renamed at step %d: the marker's "+
			"contents are durable and the entry naming it need not be%s",
			last, completeName, completeRename, fs.trace())
	}
}

// TestEveryFileIsFsyncedBeforeItsRename is the within-file half of the order.
//
// Separate from the sequence assertions above because it is a different claim:
// those are about which name becomes visible first, this is about what is
// behind a name when it does. A rename is atomic, so a reader sees the old file
// or the new one; without the fsync it can see the new NAME pointing at
// contents that never reached the disk, which is a file of the right length
// full of whatever was in those blocks.
func TestEveryFileIsFsyncedBeforeItsRename(t *testing.T) {
	fs := &recordingFS{}
	s := newStorageOn(t.TempDir(), fs)
	meta := testMetadata()
	for _, key := range meta.Subtasks() {
		if err := s.WriteSubtaskState(1, key, []byte(key.String())); err != nil {
			t.Fatalf("WriteSubtaskState(%s): %v", key, err)
		}
	}
	if err := s.Complete(1, meta); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	renames := 0
	for i, op := range fs.ops {
		if op.Kind != opRename {
			continue
		}
		renames++
		// The four operations on the temporary file, in order, immediately
		// before the rename that gives it its final name.
		want := []fsOpKind{opCreate, opWrite, opSync, opClose}
		if i < len(want) {
			t.Fatalf("the rename at step %d has only %d operations before it%s", i, i, fs.trace())
		}
		tmp := op.Path + tempSuffix
		for n, kind := range want {
			at := i - len(want) + n
			if fs.ops[at].Kind != kind || fs.ops[at].Path != tmp {
				t.Errorf("step %d before the rename of %s is %q, want %q on the temporary file%s",
					at, filepath.Base(op.Path), fs.ops[at], fsOp{Kind: kind, Path: tmp}, fs.trace())
			}
		}
	}
	if want := len(meta.Subtasks()) + 2; renames != want {
		t.Errorf("the sequence holds %d renames, want %d: one per subtask plus %s and %s%s",
			renames, want, metadataName, completeName, fs.trace())
	}
}
