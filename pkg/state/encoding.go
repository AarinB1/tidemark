package state

import "encoding/binary"

// OrderedInt64Bytes is the width of an encoded int64. Named so that a parser
// splitting a composite key states the width it is skipping rather than
// repeating an 8 that has to be kept in step with this file by hand.
const OrderedInt64Bytes = 8

// signBit is the bit that has to move for byte order and numeric order to
// agree. See EncodeOrderedInt64.
const signBit = uint64(1) << 63

// EncodeOrderedInt64 renders v as eight bytes that sort, BYTEWISE, in the same
// order as the int64s they came from.
//
// Big-endian alone does not do this, and the reason is two's complement: a
// negative int64 has its top bit set, so its big-endian bytes begin at 0x80 or
// above while every non-negative value begins below 0x80. Sorting the raw
// big-endian forms therefore puts every negative value AFTER every positive
// one. Flipping the sign bit maps the int64 range onto uint64 order:
// math.MinInt64 becomes 0x0000..., -1 becomes 0x7fff..., 0 becomes 0x8000...,
// and math.MaxInt64 becomes 0xffff.... The mapping is a bijection, so nothing
// collides and nothing is lost.
//
// This matters here rather than in the abstract. Timers are keyed by fire time
// and fired by scanning the timer partition in key order, so the encoding of a
// fire time IS the firing order. Negative event times are a supported input --
// pkg/operators.floorMod exists precisely so that they land in the window that
// contains them -- and a window at a negative fire time encoded as plain
// big-endian would sort above every positive one and fire only at the
// MaxInt64 flush at end of input, with the correct count and at the wrong time.
//
// It is not used for the window START in the aggregate key, and that is not an
// inconsistency: the aggregate layout groups by RECORD KEY and only needs one
// key's windows to be contiguous, which any total order on the start gives.
// The timer layout orders BY the encoded number, which is a stronger
// requirement.
func EncodeOrderedInt64(v int64) [OrderedInt64Bytes]byte {
	var b [OrderedInt64Bytes]byte
	binary.BigEndian.PutUint64(b[:], uint64(v)^signBit)
	return b
}

// DecodeOrderedInt64 reads back what EncodeOrderedInt64 wrote.
//
// b must be at least OrderedInt64Bytes long; only the first eight bytes are
// read, so a longer slice is the head of a composite key and is accepted. A
// shorter one panics inside binary.BigEndian.Uint64, which is the same
// contract every other fixed-width read in this engine has: the callers are
// parsers that have already checked the length of the key they are splitting,
// and an error return here would be checked at exactly those call sites and
// nowhere else.
func DecodeOrderedInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b) ^ signBit)
}
