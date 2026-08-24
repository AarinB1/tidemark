// Package state holds the keyed state an operator subtask accumulates.
//
// State is per SUBTASK, not per vertex and not per job. A subtask is the unit
// of scheduling, state and failure, and a record's key partitions to exactly
// one subtask of a vertex, so a key's state lives in exactly one place and is
// touched by exactly one goroutine.
package state

import "slices"

// KeyedState is the state one operator subtask holds.
//
// It is the fifth exported interface in this project, and CLAUDE.md requires a
// justification for each. Two implementations exist as committed work rather
// than as speculation: Memory below, and the Pebble backend in Phase 3b. The
// choice between them is what decides whether a job's state fits in RAM, so it
// has to be selectable at the point where an operator reaches for state, which
// is here.
//
// The interface is bytes on both sides, and the reason is the second
// implementation rather than taste. Pebble stores byte keys in byte order, so
// an interface phrased in terms of Go strings or typed keys would need a
// translation at the boundary and would let the two backends disagree about
// ordering. Bytes leave nothing to translate.
//
// # Ownership
//
// Put COPIES both key and value, so a caller may hand it a reused scratch
// buffer. Get returns a slice that ALIASES stored state: it is valid until the
// next Put or Delete of that key, and a caller that modifies it corrupts the
// state silently. The same holds for the key and value handed to an Iterate
// callback, which are valid only for the duration of that call.
//
// No locking. One subtask owns one KeyedState and the runtime calls the
// operator from a single goroutine.
type KeyedState interface {
	// Get returns the value stored under key, and whether there was one.
	Get(key []byte) (value []byte, ok bool)
	// Put stores value under key, replacing any previous value.
	Put(key, value []byte)
	// Delete removes key. Deleting a key that is not there is a no-op.
	Delete(key []byte)
	// Iterate calls fn for every entry in ASCENDING BYTE ORDER of key,
	// stopping early if fn returns false.
	//
	// The order is part of the contract and not an implementation detail. Go
	// randomises map iteration, so an unordered Iterate would produce a
	// different byte stream from the same state on every run: Phase 3b compares
	// snapshots and Phase 4 reproduces a run from a seed, and both fail quietly
	// rather than loudly if this is not deterministic. A snapshot that differs
	// run to run looks like a checkpointing bug in whichever component reads it
	// next.
	//
	// fn may Delete the entry it is given, or any other. It must not Put.
	Iterate(fn func(key, value []byte) bool)
}

// Memory is the in-process KeyedState: a map, plus a sort on iteration.
//
// Sorting on every Iterate rather than holding the keys sorted is the obvious
// implementation and is the one chosen. Keeping a sorted slice in step would
// make every Put an insertion into it, which is O(n) on the path that runs once
// per record per window, to save an O(n log n) on the path that runs once per
// watermark. Phase 3b's Pebble backend iterates in order natively and neither
// cost survives it.
type Memory struct {
	// entries is keyed by string because a []byte cannot be a Go map key. The
	// conversion is confined to this type: nothing above it sees a string, so
	// nothing above it can disagree with Pebble about how keys order.
	entries map[string][]byte
}

var _ KeyedState = (*Memory)(nil)

// NewMemory returns empty state.
func NewMemory() *Memory {
	return &Memory{entries: make(map[string][]byte)}
}

// Get returns the value stored under key. The returned slice aliases the stored
// value; see the ownership note on KeyedState.
func (m *Memory) Get(key []byte) ([]byte, bool) {
	// m[string(b)] is the form the compiler lowers to a lookup with no
	// allocation. Assigning the conversion to a variable first would allocate
	// on every read.
	v, ok := m.entries[string(key)]
	return v, ok
}

// Put stores a copy of value under a copy of key.
//
// Both are copied because the caller is expected to hand over a reused buffer:
// the window operator builds one composite key into a scratch slice and
// overwrites it on the next record. Storing the caller's slice would leave
// every entry aliasing that one buffer, so the whole of state would read back
// as whatever the last record happened to write, and nothing would error.
func (m *Memory) Put(key, value []byte) {
	m.entries[string(key)] = slices.Clone(value)
}

// Delete removes key, if it is there.
func (m *Memory) Delete(key []byte) {
	delete(m.entries, string(key))
}

// Iterate calls fn for every entry in ascending byte order of key.
//
// The key order is taken as a snapshot before the first call, so fn may delete
// entries as it goes; each is looked up again at the moment it is visited, so
// one deleted by an earlier call is skipped rather than handed back stale. That
// is what the window operator's purge does.
//
// Go compares strings bytewise, so sorting the map's keys as strings is
// ascending byte order and not a text collation.
func (m *Memory) Iterate(fn func(key, value []byte) bool) {
	keys := make([]string, 0, len(m.entries))
	for k := range m.entries {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		v, ok := m.entries[k]
		if !ok {
			continue
		}
		if !fn([]byte(k), v) {
			return
		}
	}
}

// Len returns the number of entries held. It is for tests and for the state
// size Phase 6 is measured on; nothing on the data path calls it.
func (m *Memory) Len() int { return len(m.entries) }

// Reserved key prefixes. Every composite key a subtask stores begins with one
// of these bytes, so the one key space a subtask owns is partitioned by what is
// stored in it rather than by convention.
//
//	0x00        operator user state (window aggregates)
//	0x01        event-time timers (RESERVED, unused in this phase)
//	0x02..0xFF  reserved
//
// Nothing writes 0x01 yet, and nothing in this phase should: the timer service
// in pkg/operators holds its timers in RAM. The byte is claimed now because the
// snapshot format in pkg/checkpoint is written in this phase, and partitioning
// a key space AFTER a format exists is a format change: every checkpoint
// already on disk decodes into the wrong partition, and the restore path has to
// learn to tell an old layout from a new one. Claiming it costs one byte per
// entry today and makes Phase 6's move of timers onto disk a change to one
// operator rather than to the on-disk format.
//
// The discriminator is FIRST rather than last so that a scan can be confined to
// one partition by prefix. Sorted iteration is part of the KeyedState contract,
// so a leading byte groups a partition into one contiguous run; a trailing byte
// would interleave the partitions and leave a timer scan reading every
// aggregate.
const (
	// PrefixUserState is the discriminator on operator state: the aggregates a
	// user-defined operator accumulates.
	PrefixUserState byte = 0x00
	// PrefixTimer is the discriminator reserved for event-time timers. Nothing
	// writes it in this phase.
	PrefixTimer byte = 0x01
)
