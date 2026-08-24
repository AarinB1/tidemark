package state

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// collect drains an Iterate into the pairs it visited, in the order it visited
// them.
type pair struct{ key, value string }

func collect(s KeyedState) []pair {
	var out []pair
	s.Iterate(func(k, v []byte) bool {
		out = append(out, pair{key: string(k), value: string(v)})
		return true
	})
	return out
}

func keysOf(ps []pair) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.key
	}
	return out
}

// TestMemoryGetPutDelete covers the three single-key operations against an
// empty state, an occupied one, and a replaced one.
func TestMemoryGetPutDelete(t *testing.T) {
	m := NewMemory()

	if v, ok := m.Get([]byte("absent")); ok || v != nil {
		t.Errorf("Get on empty state = (%q, %t), want (nil, false)", v, ok)
	}
	// Deleting what is not there is a no-op rather than a panic: the window
	// operator's purge deletes whatever its scan reaches and has no reason to
	// check first.
	m.Delete([]byte("absent"))

	m.Put([]byte("k"), []byte("one"))
	if v, ok := m.Get([]byte("k")); !ok || string(v) != "one" {
		t.Errorf("Get after Put = (%q, %t), want (\"one\", true)", v, ok)
	}

	m.Put([]byte("k"), []byte("two"))
	if v, ok := m.Get([]byte("k")); !ok || string(v) != "two" {
		t.Errorf("Get after a replacing Put = (%q, %t), want (\"two\", true)", v, ok)
	}
	if got := m.Len(); got != 1 {
		t.Errorf("Len after replacing one key = %d, want 1", got)
	}

	m.Delete([]byte("k"))
	if _, ok := m.Get([]byte("k")); ok {
		t.Error("Get returned a deleted key")
	}
	if got := m.Len(); got != 0 {
		t.Errorf("Len after deleting the only key = %d, want 0", got)
	}

	// An empty key and an empty value are keys and values like any other. The
	// window operator never writes either, but a backend that special-cased
	// them would be a backend Pebble does not match.
	m.Put(nil, nil)
	if v, ok := m.Get(nil); !ok || len(v) != 0 {
		t.Errorf("Get of the empty key = (%q, %t), want (\"\", true)", v, ok)
	}
}

// TestMemoryCopiesKeyAndValue is the property that lets the window operator
// build its composite key into one reused scratch buffer.
//
// If Put stored the caller's slices, every entry would alias that buffer and
// the whole of state would read back as whatever the last record wrote. Nothing
// errors when that happens: the counts are simply all the same, which looks
// like an aggregation bug rather than an aliasing one.
func TestMemoryCopiesKeyAndValue(t *testing.T) {
	m := NewMemory()
	scratch := []byte("key-a")
	value := []byte("value-a")

	m.Put(scratch, value)

	// The caller reuses both buffers for the next record, as the operator does.
	copy(scratch, []byte("key-b"))
	copy(value, []byte("value-b"))

	m.Put(scratch, value)

	if v, ok := m.Get([]byte("key-a")); !ok || string(v) != "value-a" {
		t.Errorf("after the caller reused its buffers, key-a = (%q, %t), want (\"value-a\", true)", v, ok)
	}
	if v, ok := m.Get([]byte("key-b")); !ok || string(v) != "value-b" {
		t.Errorf("key-b = (%q, %t), want (\"value-b\", true)", v, ok)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2: one buffer reused twice produced one entry", got)
	}
}

// TestMemoryIterateIsSortedByByteOrder is the property Phase 3b and Phase 4
// both rest on.
//
// Go randomises map iteration, so an Iterate that walked the map directly would
// hand back a different order on every run and the snapshot written from it
// would be a different byte stream every time. Nothing fails at the point that
// happens; it fails later, in whatever compares two snapshots or replays a
// seed, and it looks like a bug there.
//
// The keys are inserted in several different orders, including sorted,
// reversed, and shuffled, because an implementation that happened to preserve
// insertion order would pass a test that only ever inserted in one.
//
// Byte order, not text order: the keys below include bytes above 0x7f and an
// empty key, which a collation-aware comparison would place differently.
func TestMemoryIterateIsSortedByByteOrder(t *testing.T) {
	keys := []string{
		"", "\x00", "\x00\x00", "\x01", "A", "AA", "Ab", "a", "ab", "b",
		"\x7f", "\x80", "\xff", "\xff\x00", "\xff\xff",
	}
	want := slices.Clone(keys)
	slices.Sort(want)
	// Sanity: the fixture must not already be in order for a shuffle to mean
	// anything, and slices.Sort on strings is the byte order being asserted.
	if !slices.IsSorted(want) {
		t.Fatal("the expected order is not sorted")
	}

	orders := map[string][]string{
		"sorted":   slices.Clone(want),
		"reversed": nil,
		"shuffled": nil,
		"rotated":  nil,
	}
	orders["reversed"] = slices.Clone(want)
	slices.Reverse(orders["reversed"])
	orders["rotated"] = append(slices.Clone(want[7:]), want[:7]...)
	shuffled := slices.Clone(want)
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	orders["shuffled"] = shuffled

	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			m := NewMemory()
			for i, k := range order {
				m.Put([]byte(k), []byte(fmt.Sprintf("v%d", i)))
			}
			got := keysOf(collect(m))
			if !slices.Equal(got, want) {
				t.Fatalf("Iterate visited %q, want %q", got, want)
			}
			// And the values travelled with their keys rather than with their
			// positions.
			for _, p := range collect(m) {
				i := slices.Index(order, p.key)
				if got, want := p.value, fmt.Sprintf("v%d", i); got != want {
					t.Errorf("key %q carried value %q, want %q", p.key, got, want)
				}
			}
		})
	}

	// Every insertion order produced the same sequence. Stated once more as a
	// direct comparison between two states filled differently, since that is
	// the claim Phase 3b makes about two snapshots.
	forward, backward := NewMemory(), NewMemory()
	for _, k := range want {
		forward.Put([]byte(k), []byte("v"))
	}
	for i := len(want) - 1; i >= 0; i-- {
		backward.Put([]byte(want[i]), []byte("v"))
	}
	if a, b := keysOf(collect(forward)), keysOf(collect(backward)); !slices.Equal(a, b) {
		t.Errorf("two states holding the same keys iterated as %q and %q", a, b)
	}
}

// TestMemoryIterateRepeatsItsOrder pins that the order does not vary between
// two iterations of the SAME state, which is the form the randomised map order
// would show up in first.
func TestMemoryIterateRepeatsItsOrder(t *testing.T) {
	m := NewMemory()
	for i := range 64 {
		m.Put([]byte{byte(i * 3), byte(i)}, []byte{byte(i)})
	}
	first := keysOf(collect(m))
	for range 16 {
		if got := keysOf(collect(m)); !slices.Equal(got, first) {
			t.Fatalf("a second Iterate over unchanged state gave a different order:\n %q\n %q", got, first)
		}
	}
	if !slices.IsSorted(first) {
		t.Errorf("Iterate order %q is stable but not sorted", first)
	}
}

// TestMemoryIterateStopsWhenFnReturnsFalse covers the early exit.
func TestMemoryIterateStopsWhenFnReturnsFalse(t *testing.T) {
	m := NewMemory()
	for _, k := range []string{"a", "b", "c", "d"} {
		m.Put([]byte(k), []byte(k))
	}

	var seen []string
	m.Iterate(func(k, v []byte) bool {
		seen = append(seen, string(k))
		return string(k) != "b"
	})
	if want := []string{"a", "b"}; !slices.Equal(seen, want) {
		t.Errorf("Iterate visited %q after stopping at b, want %q", seen, want)
	}
}

// TestMemoryIterateToleratesDeletionByTheCallback is what the window operator's
// purge does: one scan of the open windows that removes the ones the watermark
// has moved past.
//
// A key removed by an earlier call must be skipped rather than handed back with
// a stale value, and the scan must reach every key that is still there.
func TestMemoryIterateToleratesDeletionByTheCallback(t *testing.T) {
	m := NewMemory()
	for i := range 32 {
		m.Put([]byte{byte(i)}, []byte{byte(i)})
	}

	// Delete the entry being visited, and the one after it.
	var visited []byte
	m.Iterate(func(k, v []byte) bool {
		visited = append(visited, k[0])
		if k[0]%2 == 0 {
			m.Delete(k)
			m.Delete([]byte{k[0] + 1})
		}
		return true
	})

	// Only the evens are visited: each one removes the odd that would follow.
	var wantVisited []byte
	for i := 0; i < 32; i += 2 {
		wantVisited = append(wantVisited, byte(i))
	}
	if !bytes.Equal(visited, wantVisited) {
		t.Errorf("Iterate visited %v, want %v: a key deleted during the scan was still handed back", visited, wantVisited)
	}
	if got := m.Len(); got != 0 {
		t.Errorf("Len = %d after deleting every entry during one scan, want 0", got)
	}
}

// TestMemoryIterateOverEmptyStateDoesNothing covers the case the window
// operator hits on every watermark before the first record.
func TestMemoryIterateOverEmptyStateDoesNothing(t *testing.T) {
	calls := 0
	NewMemory().Iterate(func(k, v []byte) bool {
		calls++
		return true
	})
	if calls != 0 {
		t.Errorf("Iterate over empty state made %d calls", calls)
	}
}

// TestReservedPrefixesAreTheDocumentedBytes pins the discriminator values.
//
// These are not internal constants. From this phase on they are part of the
// snapshot format: a checkpoint holds composite keys byte for byte, so changing
// what 0x00 means reinterprets every checkpoint already on disk, and nothing in
// the restore path can tell that it happened. A test that reads like a
// tautology is the point — it makes the change loud.
func TestReservedPrefixesAreTheDocumentedBytes(t *testing.T) {
	tests := []struct {
		name string
		got  byte
		want byte
	}{
		{name: "user state", got: PrefixUserState, want: 0x00},
		{name: "timer", got: PrefixTimer, want: 0x01},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s prefix is %#x, want %#x", tt.name, tt.got, tt.want)
		}
	}
	if PrefixUserState == PrefixTimer {
		t.Fatal("the two prefixes are the same byte, so the partitions are not partitioned")
	}
}

// TestPrefixOrdersPartitionsIntoContiguousRuns is why the discriminator leads a
// key rather than trailing it.
//
// Sorted iteration is part of the KeyedState contract, so a leading byte makes
// each partition one contiguous run and a scan for the timers is a scan of that
// run. The entries below are chosen so that the user-state key sorts ABOVE the
// timer key on every byte except the discriminator: with the byte on the front
// the partitions still separate, and with it anywhere else they interleave.
func TestPrefixOrdersPartitionsIntoContiguousRuns(t *testing.T) {
	m := NewMemory()
	m.Put([]byte{PrefixTimer, 0x00}, []byte("t0"))
	m.Put([]byte{PrefixUserState, 0xff}, []byte("u1"))
	m.Put([]byte{PrefixTimer, 0xff}, []byte("t1"))
	m.Put([]byte{PrefixUserState, 0x00}, []byte("u0"))

	var order []string
	m.Iterate(func(k, v []byte) bool {
		order = append(order, string(v))
		return true
	})
	want := []string{"u0", "u1", "t0", "t1"}
	if !slices.Equal(order, want) {
		t.Errorf("sorted iteration visited %v, want %v: the partitions are not contiguous runs", order, want)
	}
}
