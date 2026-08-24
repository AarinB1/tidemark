package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"testing"
)

// entry is one key/value pair a test writes into a state.
type entry struct{ key, value string }

// fill puts every entry of es into s, in the order given, and returns it.
func fill(s KeyedState, es []entry) KeyedState {
	for _, e := range es {
		s.Put([]byte(e.key), []byte(e.value))
	}
	return s
}

// forEachSerializeBackend runs fn against each implementation. The serialised
// form has to be a function of the state's contents and not of which backend
// held it, which is exactly what Phase 4 compares two runs on.
func forEachSerializeBackend(t *testing.T, fn func(t *testing.T, b backend)) {
	t.Helper()
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) { fn(t, b) })
	}
}

// TestSerializeRoundTrips covers the shapes a subtask's state can be in.
//
// The empty state is in the table because it is what a sink and an unstarted
// operator snapshot, and it is the case a format with no explicit count would
// get wrong. The zero-length value is there because Get distinguishes an absent
// key from one holding nothing, and a round trip that lost the difference would
// turn a real entry into a missing one.
func TestSerializeRoundTrips(t *testing.T) {
	tests := []struct {
		name    string
		entries []entry
	}{
		{name: "empty"},
		{name: "one entry", entries: []entry{{key: "k", value: "v"}}},
		{name: "zero-length value", entries: []entry{{key: "k", value: ""}}},
		{name: "zero-length key", entries: []entry{{key: "", value: "v"}}},
		{
			name: "binary keys and values",
			entries: []entry{
				{key: "\x00\x01\x02", value: "\xff\xfe"},
				{key: "\x00", value: ""},
				{key: "\xff", value: "\x00\x00\x00\x00"},
			},
		},
		{
			name: "composite keys across both partitions",
			entries: []entry{
				{key: string([]byte{PrefixUserState}) + "key\x00\x00\x00\x00\x00\x00\x00\x64", value: "\x00\x00\x00\x00\x00\x00\x00\x07"},
				{key: string([]byte{PrefixTimer}) + "key", value: "t"},
			},
		},
	}

	forEachSerializeBackend(t, func(t *testing.T, b backend) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var buf bytes.Buffer
				if err := WriteTo(fill(b.make(t), tt.entries), &buf); err != nil {
					t.Fatalf("WriteTo: %v", err)
				}

				restored := b.make(t)
				if err := ReadFrom(restored, bytes.NewReader(buf.Bytes())); err != nil {
					t.Fatalf("ReadFrom: %v", err)
				}

				want := slices.Clone(tt.entries)
				slices.SortFunc(want, func(x, y entry) int { return bytes.Compare([]byte(x.key), []byte(y.key)) })

				got := collect(restored)
				if len(got) != len(want) {
					t.Fatalf("restored state holds %d entries, want %d", len(got), len(want))
				}
				for i := range want {
					if got[i].key != want[i].key || got[i].value != want[i].value {
						t.Errorf("entry %d is (%q, %q), want (%q, %q)", i, got[i].key, got[i].value, want[i].key, want[i].value)
					}
				}
			})
		}
	})
}

// TestWriteToIsIndependentOfInsertionOrder is the property Phase 4 needs.
//
// The same logical state, built in three different orders, must serialise to
// three identical byte streams. WriteTo gets that from Iterate visiting keys in
// sorted byte order and from nothing else: an unordered scan would still
// restore correctly, so a checkpoint would look fine and only a comparison
// between two runs would come apart -- and a comparison that fails for a reason
// unrelated to the thing under test is worse than no comparison.
//
// The three orders are chosen so that no two of them share a prefix: forwards,
// backwards, and interleaved from both ends.
func TestWriteToIsIndependentOfInsertionOrder(t *testing.T) {
	forEachSerializeBackend(t, testWriteToIsIndependentOfInsertionOrder)
}

func testWriteToIsIndependentOfInsertionOrder(t *testing.T, b backend) {
	entries := []entry{
		{key: "\x00alpha", value: "1"},
		{key: "\x00beta", value: "22"},
		{key: "\x00gamma", value: "333"},
		{key: "\x01delta", value: ""},
		{key: "\xffomega", value: "4444"},
	}

	forward := slices.Clone(entries)
	backward := slices.Clone(entries)
	slices.Reverse(backward)

	// Ends inward: first, last, second, second to last, and so on.
	var interleaved []entry
	for i, j := 0, len(entries)-1; i <= j; i, j = i+1, j-1 {
		interleaved = append(interleaved, entries[i])
		if i != j {
			interleaved = append(interleaved, entries[j])
		}
	}

	orders := []struct {
		name    string
		entries []entry
	}{
		{name: "forward", entries: forward},
		{name: "backward", entries: backward},
		{name: "interleaved", entries: interleaved},
	}

	var first []byte
	for _, o := range orders {
		var buf bytes.Buffer
		if err := WriteTo(fill(b.make(t), o.entries), &buf); err != nil {
			t.Fatalf("%s: WriteTo: %v", o.name, err)
		}
		if first == nil {
			first = buf.Bytes()
			continue
		}
		if !bytes.Equal(first, buf.Bytes()) {
			t.Fatalf("%s produced a different byte stream from forward: %x vs %x", o.name, buf.Bytes(), first)
		}
	}
}

// TestWriteToWritesTheDocumentedBytes reads the format back by hand.
//
// The round-trip test above passes against any self-consistent encoding, so it
// would not notice the count moving to the end or the lengths becoming
// little-endian. This one states the bytes, because the format outlives the
// code that writes it.
func TestWriteToWritesTheDocumentedBytes(t *testing.T) {
	forEachSerializeBackend(t, testWriteToWritesTheDocumentedBytes)
}

func testWriteToWritesTheDocumentedBytes(t *testing.T, b backend) {
	var buf bytes.Buffer
	if err := WriteTo(fill(b.make(t), []entry{{key: "ab", value: "xyz"}}), &buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	want := make([]byte, 0, 8+4+2+4+3)
	want = binary.BigEndian.AppendUint64(want, 1)
	want = binary.BigEndian.AppendUint32(want, 2)
	want = append(want, "ab"...)
	want = binary.BigEndian.AppendUint32(want, 3)
	want = append(want, "xyz"...)

	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("WriteTo produced %x, want %x", buf.Bytes(), want)
	}
}

// TestReadFromRefusesANonEmptyState pins the refusal rather than a merge.
//
// A merge produces a job whose aggregates are the sum of a recovered run and a
// partial one: every key present, every window present, every count too high.
// Nothing downstream can tell that from a correct answer.
func TestReadFromRefusesANonEmptyState(t *testing.T) {
	forEachSerializeBackend(t, testReadFromRefusesANonEmptyState)
}

func testReadFromRefusesANonEmptyState(t *testing.T, b backend) {
	var buf bytes.Buffer
	if err := WriteTo(fill(b.make(t), []entry{{key: "a", value: "1"}}), &buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	dirty := fill(b.make(t), []entry{{key: "b", value: "2"}})
	err := ReadFrom(dirty, bytes.NewReader(buf.Bytes()))
	if !errors.Is(err, errStateNotEmpty) {
		t.Fatalf("ReadFrom into occupied state = %v, want %v", err, errStateNotEmpty)
	}
	// And it did not half-restore on its way to the refusal.
	if got := count(dirty); got != 1 {
		t.Errorf("the refused state holds %d entries, want the 1 it started with", got)
	}
}

// TestReadFromRejectsATruncatedStream covers every point the stream can stop.
//
// A truncated snapshot must fail rather than restore what it could reach. The
// prefix that stops after the count is the dangerous one: it decodes as a state
// with entries that are simply not there, so a job would restore cleanly and
// run on a fraction of its state.
func TestReadFromRejectsATruncatedStream(t *testing.T) {
	forEachSerializeBackend(t, testReadFromRejectsATruncatedStream)
}

func testReadFromRejectsATruncatedStream(t *testing.T, b backend) {
	var buf bytes.Buffer
	if err := WriteTo(fill(b.make(t), []entry{{key: "ab", value: "xyz"}, {key: "cd", value: "w"}}), &buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	full := buf.Bytes()

	for n := 0; n < len(full); n++ {
		restored := b.make(t)
		if err := ReadFrom(restored, bytes.NewReader(full[:n])); err == nil {
			t.Errorf("ReadFrom over the first %d of %d bytes returned nil, restoring %d entries", n, len(full), count(restored))
		}
	}

	// The whole stream still restores, so the loop above is not passing because
	// the decoder rejects everything.
	restored := b.make(t)
	if err := ReadFrom(restored, bytes.NewReader(full)); err != nil {
		t.Fatalf("ReadFrom over the whole stream: %v", err)
	}
	if got := count(restored); got != 2 {
		t.Errorf("the whole stream restored %d entries, want 2", got)
	}
}

// failingWriter fails on the nth Write, counting from one.
type failingWriter struct {
	failOn int
	writes int
}

var errWriteFailed = errors.New("write failed")

func (f *failingWriter) Write(p []byte) (int, error) {
	f.writes++
	if f.writes == f.failOn {
		return 0, errWriteFailed
	}
	return len(p), nil
}

// TestWriteToReportsAFailedWrite covers a full disk part way through a
// snapshot. Every write position is tried, because the header, the lengths and
// the payloads are three different code paths.
func TestWriteToReportsAFailedWrite(t *testing.T) {
	forEachSerializeBackend(t, testWriteToReportsAFailedWrite)
}

func testWriteToReportsAFailedWrite(t *testing.T, b backend) {
	s := fill(b.make(t), []entry{{key: "ab", value: "xyz"}, {key: "cd", value: "w"}})

	// Header, then four length/payload pairs per entry: nine writes in total.
	for n := 1; n <= 9; n++ {
		w := &failingWriter{failOn: n}
		if err := WriteTo(s, w); !errors.Is(err, errWriteFailed) {
			t.Errorf("WriteTo with write %d failing = %v, want %v", n, err, errWriteFailed)
		}
	}

	// Nothing beyond the ninth write happens, so a tenth-write failure is never
	// reached and the snapshot succeeds. That pins the count above.
	if err := WriteTo(s, &failingWriter{failOn: 10}); err != nil {
		t.Errorf("WriteTo with only a tenth write failing = %v, want nil", err)
	}
}

// TestSerializeReportsABackendError checks that a backend failing mid-scan
// stops the snapshot instead of writing a short one.
//
// A short snapshot is the worst outcome available: it is a valid stream that
// restores cleanly into a state missing whatever the scan did not reach.
func TestSerializeReportsABackendError(t *testing.T) {
	failing := &iterateFailsState{inner: fill(NewMemory(), []entry{{key: "a", value: "1"}}).(*Memory)}
	if err := WriteTo(failing, io.Discard); !errors.Is(err, errIterateFailed) {
		t.Errorf("WriteTo over a failing backend = %v, want %v", err, errIterateFailed)
	}

	var buf bytes.Buffer
	if err := WriteTo(fill(NewMemory(), []entry{{key: "a", value: "1"}}), &buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	target := &iterateFailsState{inner: NewMemory()}
	if err := ReadFrom(target, bytes.NewReader(buf.Bytes())); !errors.Is(err, errIterateFailed) {
		t.Errorf("ReadFrom into a failing backend = %v, want %v", err, errIterateFailed)
	}
}

var errIterateFailed = errors.New("iterate failed")

// iterateFailsState is a KeyedState whose Iterate records an error and visits
// nothing. Every method is written out rather than promoted from an embedded
// interface, so a method this fake forgets is a compile error rather than a
// silent pass-through.
type iterateFailsState struct {
	inner *Memory
	err   error
}

func (s *iterateFailsState) Get(key []byte) ([]byte, bool) { return s.inner.Get(key) }
func (s *iterateFailsState) Put(key, value []byte)         { s.inner.Put(key, value) }
func (s *iterateFailsState) Delete(key []byte)             { s.inner.Delete(key) }
func (s *iterateFailsState) Err() error                    { return s.err }

func (s *iterateFailsState) Iterate(fn func(key, value []byte) bool) {
	if s.err == nil {
		s.err = errIterateFailed
	}
}

var _ KeyedState = (*iterateFailsState)(nil)
