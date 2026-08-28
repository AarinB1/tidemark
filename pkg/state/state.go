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
	// fn may Delete the entry it is GIVEN, and must not Put.
	//
	// Deleting any OTHER entry during a scan is undefined: whether the scan
	// still visits it depends on the backend, and the two here disagree.
	// Memory looks each key up again as it reaches it, so an entry deleted by
	// an earlier call is skipped; Pebble's iterator reads a view fixed when the
	// scan began and hands it back. The interface promises what both can do,
	// because a contract only one backend honours is a trap for whoever writes
	// the next operator against it -- and the only caller in this engine, the
	// window operator's purge, deletes exactly the entry it is handed.
	Iterate(fn func(key, value []byte) bool)
	// Err returns the FIRST error the implementation encountered, or nil.
	//
	// Get, Put, Delete and Iterate cannot fail, which is honest for a map and
	// a lie for a disk backend: the Pebble implementation later in this phase
	// can fail on a read. Widening those four signatures would put an error
	// return on every call site in every operator for a case that is rare and
	// terminal, and an operator that has to check an error per record will
	// eventually drop one.
	//
	// So a failing implementation stashes its first error and keeps going,
	// returning zero values, and the RUNTIME collects the stash after each
	// operator call and fails the subtask. That is the same shape opContext
	// already uses for a failed Emit, which is why it is this shape and not a
	// new one.
	//
	// FIRST rather than last, and sticky rather than cleared: once a backend
	// has failed, every value it hands back afterwards is suspect, and the
	// error worth reporting is the one that explains why. A later error is a
	// consequence.
	Err() error
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

// Err returns nil, always. A map cannot fail: there is no read to go wrong and
// no allocation this type recovers from. Memory exists partly so that a test
// separates a bug in an operator from a bug in a backend, and a Memory that
// could fail would blur that.
func (m *Memory) Err() error { return nil }

// Len returns the number of entries held. It is for tests and for the state
// size Phase 6 is measured on; nothing on the data path calls it.
func (m *Memory) Len() int { return len(m.entries) }

// Reserved key prefixes. Every composite key a subtask stores begins with one
// of these bytes, so the one key space a subtask owns is partitioned by what is
// stored in it rather than by convention.
//
//	0x00        operator user state (window aggregates)
//	0x01        event-time timers
//	0x02        operator scalar state, keyed 0x02 || name
//	0x03..0xFF  reserved
//
// All three are written today, by the window operator in pkg/operators. That is
// the point of the partitioning rather than an accident of it: a subtask has
// ONE key space, and without a discriminator a timer key, an aggregate key and
// a scalar would be distinguishable only by length and by luck, so the purge
// scan that walks the aggregates on every watermark would read the other two as
// aggregates and delete them.
//
// The discriminator is FIRST rather than last so that a scan can be confined to
// one partition by prefix. Sorted iteration is part of the KeyedState contract,
// so a leading byte groups a partition into one contiguous run; a trailing byte
// would interleave the partitions and leave a timer scan reading every
// aggregate. It also fixes the order the partitions are visited in, which is
// what lets each scan STOP at the first key outside its own.
//
// 0x01 and 0x02 were claimed before anything wrote them, in the phase that
// wrote the snapshot format in pkg/checkpoint. Partitioning a key space AFTER a
// format exists is a format change: every checkpoint already on disk decodes
// into the wrong partition, and the restore path has to learn to tell an old
// layout from a new one. Claiming them cost one byte per entry and made moving
// timers and the operator watermark into state a change to one operator rather
// than to the on-disk format.
const (
	// PrefixUserState is the discriminator on operator state: the aggregates a
	// user-defined operator accumulates.
	PrefixUserState byte = 0x00
	// PrefixTimer is the discriminator on event-time timers.
	//
	// A timer is STATE, and that is why it is here rather than in a heap beside
	// the operator: an operator whose timers live in a Go field is restored with
	// its aggregates and no timer to fire them, so a (key, window) complete
	// before the checkpoint is silently never emitted. Snapshotting the key
	// space snapshots the timers with it and nothing in this package or in
	// pkg/checkpoint has to know that is what it is doing.
	PrefixTimer byte = 0x01
	// PrefixOperatorState is the discriminator on an operator's named scalars,
	// keyed PrefixOperatorState || name.
	//
	// One name exists: the window operator's current watermark. It is here for
	// the same reason the timers are -- a watermark held in a Go field comes
	// back as MinInt64 after a restore, so until the restored sources produce
	// one the operator accepts records it should be treating as late.
	//
	// There is deliberately no scalar-state API over this. It is a byte and a
	// name; a helper would be an abstraction layer over two calls.
	PrefixOperatorState byte = 0x02
)
