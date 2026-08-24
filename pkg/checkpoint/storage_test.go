package checkpoint

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
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
	if err := writeFileSynced(s.dir(3), metadataName, encodeMetadata(meta)); err != nil {
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
// The sequence itself cannot be observed without crashing the process part way
// through it, which no unit test can arrange. What can be checked is the state
// it is supposed to produce: before Complete there is no marker and Latest
// finds nothing; after it there is a marker, every state file, and the
// metadata. A Complete that wrote the marker first would pass the second half
// and fail the first.
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
