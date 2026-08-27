package state

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"
)

// orderedValues is the value set the sorting claims are made over.
//
// Hand-picking three numbers proves nothing here: plain big-endian agrees with
// the int64 order over any set that is entirely non-negative, and it agrees
// over any set that is entirely negative too. The pairs that break it are the
// ones that STRADDLE ZERO and the ones at the extremes, so the set is built to
// contain those pairs rather than to look varied.
func orderedValues() []int64 {
	values := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		math.MinInt64 / 2,
		-1 << 32,
		-1000000,
		-256,
		-2,
		-1,
		0,
		1,
		2,
		256,
		1000000,
		1 << 32,
		math.MaxInt64 / 2,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}
	// A deterministic spread on top of the boundaries, so the claim is not
	// only about the values someone thought to write down. A linear congruence
	// on a fixed seed, never math/rand: this file must produce the same set on
	// every run for the same reason the sources do.
	x := uint64(0x9e3779b97f4a7c15)
	for range 200 {
		x = x*6364136223846793005 + 1442695040888963407
		values = append(values, int64(x))
	}
	return values
}

// TestOrderedInt64SortsLikeTheInt64 is the property the encoding exists for.
//
// Sorting the encoded forms BYTEWISE must produce the same sequence as sorting
// the int64s numerically. It is asserted by generating the values, sorting both
// ways and comparing, rather than by naming pairs: the encoding is a total
// order or it is not one, and a spot check on three values passes for a plain
// big-endian encoding that reverses the whole negative half.
func TestOrderedInt64SortsLikeTheInt64(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{name: "boundaries and a deterministic spread", values: orderedValues()},
		{
			// The minimal breaking pair. Big-endian puts -1 (0xffff...) above
			// 0 and above MaxInt64.
			name:   "straddling zero",
			values: []int64{-1, 0, 1},
		},
		{
			name:   "the extremes",
			values: []int64{math.MinInt64, math.MaxInt64},
		},
		{
			name:   "adjacent at every boundary",
			values: []int64{math.MinInt64, math.MinInt64 + 1, -1, 0, 1, math.MaxInt64 - 1, math.MaxInt64},
		},
		{
			// Duplicates encode identically, so the byte sort has ties in the
			// same places the numeric sort does. A layout that appended
			// anything to disambiguate would show up as a length difference.
			name:   "duplicates",
			values: []int64{5, -5, 5, 0, -5, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			numeric := slices.Clone(tt.values)
			slices.Sort(numeric)

			encoded := make([][]byte, 0, len(tt.values))
			for _, v := range tt.values {
				b := EncodeOrderedInt64(v)
				encoded = append(encoded, b[:])
			}
			slices.SortFunc(encoded, bytes.Compare)

			byByteOrder := make([]int64, len(encoded))
			for i, b := range encoded {
				byByteOrder[i] = DecodeOrderedInt64(b)
			}
			if !slices.Equal(byByteOrder, numeric) {
				t.Errorf("sorting the encoded forms gives\n%v\nand sorting the int64s gives\n%v", byByteOrder, numeric)
			}
		})
	}
}

// TestOrderedInt64RoundTrips covers every value in the set, in both directions.
//
// A longer slice is accepted and only its first eight bytes are read, because
// the caller in pkg/operators decodes a fire time out of the head of a longer
// composite key rather than out of a slice cut to width.
func TestOrderedInt64RoundTrips(t *testing.T) {
	for _, v := range orderedValues() {
		b := EncodeOrderedInt64(v)
		if len(b) != OrderedInt64Bytes {
			t.Fatalf("EncodeOrderedInt64(%d) is %d bytes, want %d", v, len(b), OrderedInt64Bytes)
		}
		if got := DecodeOrderedInt64(b[:]); got != v {
			t.Errorf("DecodeOrderedInt64(EncodeOrderedInt64(%d)) = %d", v, got)
		}
		// The head of a composite key: the trailing bytes must not be read.
		withTail := append(b[:], 0xff, 0x00, 0xff)
		if got := DecodeOrderedInt64(withTail); got != v {
			t.Errorf("decoding %d out of the head of a longer key = %d", v, got)
		}
	}
}

// TestPlainBigEndianDoesNotSortLikeTheInt64 states what the sign flip is for.
//
// Without it the code still round-trips, still looks like a byte encoding of an
// int64, and still sorts correctly over any all-positive set -- which is every
// set an epoch-based test produces. The failure only appears once a negative
// fire time exists, and then it appears as a window that fires at the end of
// input with the right count. This is the assertion that would fail if someone
// "simplified" EncodeOrderedInt64 back to binary.BigEndian.
func TestPlainBigEndianDoesNotSortLikeTheInt64(t *testing.T) {
	plain := func(v int64) []byte {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v))
		return b[:]
	}

	// Every pair here is ordered one way numerically and the other way in
	// plain big-endian bytes.
	pairs := [][2]int64{
		{-1, 0},
		{-1, math.MaxInt64},
		{math.MinInt64, 0},
		{math.MinInt64, math.MaxInt64},
		{-1000000, 1},
	}
	for _, p := range pairs {
		lo, hi := p[0], p[1]
		if lo >= hi {
			t.Fatalf("the pair (%d, %d) is not ordered, so it tests nothing", lo, hi)
		}
		if bytes.Compare(plain(lo), plain(hi)) <= 0 {
			t.Errorf("plain big-endian already sorts %d below %d, so this pair does not pin the sign flip", lo, hi)
		}
		encLo, encHi := EncodeOrderedInt64(lo), EncodeOrderedInt64(hi)
		if bytes.Compare(encLo[:], encHi[:]) >= 0 {
			t.Errorf("EncodeOrderedInt64 sorts %d at or above %d", lo, hi)
		}
	}
}
