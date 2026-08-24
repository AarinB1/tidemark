package state

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The shared KeyedState properties are in state_test.go and serialize_test.go,
// which run every case against both backends. What is here is what only the
// disk backend has: a directory, a lifetime, and a way to fail.

// TestPebblePersistsAcrossAReopen is what makes this a disk backend rather than
// a map with extra steps.
//
// It also pins that OpenPebble does not clear what it finds. A backend that
// silently started empty would pass every other test in this package -- they
// all begin with an empty state -- and would lose a subtask's aggregates at
// exactly the moment they mattered.
func TestPebblePersistsAcrossAReopen(t *testing.T) {
	dir := t.TempDir()

	first, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	entries := []entry{
		{key: string([]byte{PrefixUserState}) + "alpha", value: "1"},
		{key: string([]byte{PrefixUserState}) + "beta", value: "22"},
		{key: string([]byte{PrefixTimer}) + "gamma", value: "333"},
	}
	fill(first, entries)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	for _, e := range entries {
		v, ok := second.Get([]byte(e.key))
		if !ok || string(v) != e.value {
			t.Errorf("after a reopen, key %q = (%q, %t), want (%q, true)", e.key, v, ok, e.value)
		}
	}
	if got := count(second); got != len(entries) {
		t.Errorf("the reopened state holds %d entries, want %d", got, len(entries))
	}
}

// TestTempPebbleRemovesItsDirectoryOnClose. A job's subtask state is scratch --
// the durable record is the checkpoint -- so the directory is worth exactly as
// long as the process that owns it. A backend that left one behind would leak a
// directory per subtask per run, which nothing reports and nothing cleans up.
func TestTempPebbleRemovesItsDirectoryOnClose(t *testing.T) {
	p, err := NewTempPebble()
	if err != nil {
		t.Fatalf("NewTempPebble: %v", err)
	}
	dir := p.Dir()
	if dir == "" {
		t.Fatal("NewTempPebble reported no directory")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("the temporary directory is not there: %v", err)
	}

	p.Put([]byte{PrefixUserState, 'k'}, []byte("v"))
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the temporary directory survived Close: %v", err)
	}
}

// TestOpenPebbleKeepsADirectoryItDidNotMake is the other half of the lifetime
// rule: a caller who named the directory owns it.
func TestOpenPebbleKeepsADirectoryItDidNotMake(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("Close removed a directory it did not create: %v", err)
	}
}

// TestOpenPebbleReportsAnUnusableDirectory. A job that cannot make its state
// has to fail at startup rather than run on something else.
func TestOpenPebbleReportsAnUnusableDirectory(t *testing.T) {
	// A regular file where the database should be.
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("occupied"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if p, err := OpenPebble(path); err == nil {
		_ = p.Close()
		t.Error("OpenPebble over a regular file returned no error")
	}
}

// TestPebbleUseAfterCloseIsAnErrorAndNotAPanic.
//
// A subtask closes its state on the way out, so a call arriving afterwards
// means something outlived the subtask that owned it. That is a bug either way,
// and it surfaces through Err like every other backend failure rather than as a
// nil dereference in whichever goroutine happened to reach it.
func TestPebbleUseAfterCloseIsAnErrorAndNotAPanic(t *testing.T) {
	p, err := NewTempPebble()
	if err != nil {
		t.Fatalf("NewTempPebble: %v", err)
	}
	p.Put([]byte{PrefixUserState, 'k'}, []byte("v"))
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Err(); err != nil {
		t.Fatalf("Err after a clean Close = %v, want nil", err)
	}

	// Each operation in turn, from a fresh close each time, so one recording the
	// error does not cover for another that would have panicked.
	tests := []struct {
		name string
		call func(p *Pebble)
	}{
		{name: "Get", call: func(p *Pebble) { p.Get([]byte("k")) }},
		{name: "Put", call: func(p *Pebble) { p.Put([]byte("k"), []byte("v")) }},
		{name: "Delete", call: func(p *Pebble) { p.Delete([]byte("k")) }},
		{name: "Iterate", call: func(p *Pebble) { p.Iterate(func(k, v []byte) bool { return true }) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			closed, err := NewTempPebble()
			if err != nil {
				t.Fatalf("NewTempPebble: %v", err)
			}
			if err := closed.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			tt.call(closed)
			if !errors.Is(closed.Err(), errClosed) {
				t.Errorf("%s after Close recorded %v, want %v", tt.name, closed.Err(), errClosed)
			}
		})
	}
}

// TestPebbleSurfacesAFailureThroughErrAndKeepsAcceptingCalls is the contract
// KeyedState.Err documents, checked on the implementation that can actually
// fail.
//
// After a failure the backend returns zero values instead of panicking or
// blocking, because an operator is not written to check after each call -- the
// runtime collects the stash between calls, and only reaches that point if the
// operator returned.
func TestPebbleSurfacesAFailureThroughErrAndKeepsAcceptingCalls(t *testing.T) {
	p, err := NewTempPebble()
	if err != nil {
		t.Fatalf("NewTempPebble: %v", err)
	}
	defer func() { _ = p.Close() }()

	p.Put([]byte{PrefixUserState, 'a'}, []byte("1"))
	if err := p.Err(); err != nil {
		t.Fatalf("Err after a normal write = %v, want nil", err)
	}

	// Injected directly: the failures this backend has are I/O ones, and a test
	// that arranged a real disk failure would be testing the filesystem.
	injected := errors.New("injected")
	p.fail(injected)

	if v, ok := p.Get([]byte{PrefixUserState, 'a'}); ok || v != nil {
		t.Errorf("Get after a failure = (%q, %t), want (nil, false)", v, ok)
	}
	p.Put([]byte{PrefixUserState, 'b'}, []byte("2"))
	p.Delete([]byte{PrefixUserState, 'a'})
	visited := 0
	p.Iterate(func(k, v []byte) bool { visited++; return true })
	if visited != 0 {
		t.Errorf("Iterate after a failure visited %d entries, want 0", visited)
	}

	// The FIRST error survives. A later one replacing it would leave the
	// reader with the consequence rather than the cause.
	if !errors.Is(p.Err(), injected) {
		t.Errorf("Err = %v, want the first error %v", p.Err(), injected)
	}
}

// TestPebbleSnapshotsMatchMemoryByteForByte is the claim step 3's determinism
// rests on, stated across implementations.
//
// The two backends hold the same logical state in completely different
// structures -- a Go map and an LSM -- and the checkpoint written from either
// has to be the same bytes. If it were not, a job that changed backend would
// produce checkpoints that could not be compared with the ones before it, and
// Phase 4's reproduction from a seed would be a comparison between two
// implementations rather than between two runs.
func TestPebbleSnapshotsMatchMemoryByteForByte(t *testing.T) {
	entries := []entry{
		{key: string([]byte{PrefixUserState}) + "zeta", value: "4"},
		{key: string([]byte{PrefixUserState}) + "alpha", value: "1"},
		{key: string([]byte{PrefixTimer}) + "\xff", value: ""},
		{key: "", value: "empty key"},
		{key: "\xff\xff", value: "\x00\x01\x02"},
	}

	p, err := NewTempPebble()
	if err != nil {
		t.Fatalf("NewTempPebble: %v", err)
	}
	defer func() { _ = p.Close() }()

	var fromMemory, fromPebble bytes.Buffer
	if err := WriteTo(fill(NewMemory(), entries), &fromMemory); err != nil {
		t.Fatalf("WriteTo(memory): %v", err)
	}
	// Inserted in the reverse order, so a match cannot come from the two having
	// been filled the same way.
	reversed := make([]entry, len(entries))
	for i, e := range entries {
		reversed[len(entries)-1-i] = e
	}
	if err := WriteTo(fill(p, reversed), &fromPebble); err != nil {
		t.Fatalf("WriteTo(pebble): %v", err)
	}

	if !bytes.Equal(fromMemory.Bytes(), fromPebble.Bytes()) {
		t.Errorf("the two backends serialised the same state differently:\n memory %x\n pebble %x",
			fromMemory.Bytes(), fromPebble.Bytes())
	}

	// And each restores into the other, which is what a job changing backend
	// between runs would do.
	restored, err := NewTempPebble()
	if err != nil {
		t.Fatalf("NewTempPebble: %v", err)
	}
	defer func() { _ = restored.Close() }()
	if err := ReadFrom(restored, bytes.NewReader(fromMemory.Bytes())); err != nil {
		t.Fatalf("ReadFrom(memory snapshot -> pebble): %v", err)
	}
	if got, want := count(restored), len(entries); got != want {
		t.Errorf("restoring a memory snapshot into pebble gave %d entries, want %d", got, want)
	}
}
