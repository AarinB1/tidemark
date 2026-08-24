package state

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire format, and it is a format rather than an implementation detail: a
// checkpoint written by one build is read by the next one.
//
//	entryCount  uint64
//	repeated entryCount times:
//	    keyLen  uint32
//	    key     [keyLen]byte
//	    valLen  uint32
//	    value   [valLen]byte
//
// Big-endian throughout, matching every other encoding in this engine, so that
// a dump of a checkpoint reads in the same byte order as a dump of a state key.
//
// Hand-written, because encoding/gob and encoding/json are out of scope
// anywhere on the data path. That is not only a performance rule here: gob
// writes type information derived from Go declarations, so renaming a field
// would change the bytes on disk and a checkpoint would stop being readable for
// a reason no test would connect to the rename.
//
// These are package-level functions and not methods on KeyedState. Serialising
// is something the RUNTIME does to a backend, not something a backend does; a
// method would put it on the interface, and the interface is already at the
// size CLAUDE.md asks for a justification for.
//
// # Cost, and what a later phase does about it
//
// WriteTo iterates the whole of a subtask's state and copies every entry into
// the stream, so a checkpoint costs a full scan and a full copy no matter how
// little changed since the last one. Pebble can do far better: it is an LSM,
// and a snapshot of one is a reference to a set of immutable SSTables, so a
// backend-aware path could hard-link or copy those files and never walk a key.
// Incremental checkpoints in production systems are exactly that.
//
// The generic path is chosen here on purpose. This phase is where checkpointing
// has to be shown correct, and one path that both backends take is one path to
// be right about; a Pebble-specific snapshot would mean the recovery suite ran
// against a mechanism the memory backend never exercises, so a bug in it would
// only appear under the backend that is harder to reason about. The cost is
// real and Phase 6 is where it is paid.

// errStateNotEmpty is returned by ReadFrom when the state it is asked to
// restore into already holds something.
var errStateNotEmpty = errors.New("restore into state that is not empty")

// errNegativeLength is returned for a length that does not fit in an int on
// this platform.
var errNegativeLength = errors.New("length does not fit in an int")

// WriteTo writes every entry of s to w in the format above.
//
// It relies on Iterate visiting keys in ASCENDING BYTE ORDER, which is part of
// the KeyedState contract. That is what makes the byte stream a function of the
// state's CONTENTS and not of the order the entries were inserted in: the same
// logical state written twice produces the same bytes, so two checkpoints can
// be compared byte for byte and a run reproduced from a seed produces a
// checkpoint identical to the one the first run produced. An unordered Iterate
// would break that quietly -- the checkpoint would still restore correctly, and
// only the comparison would stop meaning anything.
//
// The entries are counted in a first pass and written in a second, rather than
// buffered so the count can be filled in at the end. A buffer holds the whole
// of a subtask's state a second time at the moment it is largest, which is the
// number Phase 6 is measured on; a second scan costs time instead of memory,
// and nothing mutates the state between the two passes because one subtask owns
// its state and calls this from its own goroutine.
func WriteTo(s KeyedState, w io.Writer) error {
	var entries uint64
	s.Iterate(func(key, value []byte) bool {
		entries++
		return true
	})
	if err := s.Err(); err != nil {
		return fmt.Errorf("state: snapshot: counting entries: %w", err)
	}

	var header [8]byte
	binary.BigEndian.PutUint64(header[:], entries)
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("state: snapshot: entry count: %w", err)
	}

	var written uint64
	var writeErr error
	var lenBuf [4]byte
	s.Iterate(func(key, value []byte) bool {
		for _, b := range [][]byte{key, value} {
			if uint64(len(b)) > uint64(^uint32(0)) {
				writeErr = fmt.Errorf("state: snapshot: entry is %d bytes, which does not fit a uint32 length", len(b))
				return false
			}
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			if _, err := w.Write(lenBuf[:]); err != nil {
				writeErr = fmt.Errorf("state: snapshot: length: %w", err)
				return false
			}
			if _, err := w.Write(b); err != nil {
				writeErr = fmt.Errorf("state: snapshot: bytes: %w", err)
				return false
			}
		}
		written++
		return true
	})
	if writeErr != nil {
		return writeErr
	}
	if err := s.Err(); err != nil {
		return fmt.Errorf("state: snapshot: writing entries: %w", err)
	}
	// The two passes disagreeing means the state changed underneath a snapshot,
	// which is a concurrency bug rather than an I/O one. Reported here because
	// the header is already on the stream and a reader would take the count at
	// its word.
	if written != entries {
		return fmt.Errorf("state: snapshot: counted %d entries and wrote %d: the state changed during the snapshot", entries, written)
	}
	return nil
}

// ReadFrom restores the entries in r into s.
//
// s must be EMPTY. A restore onto state that already holds something is a bug
// in the caller -- the runtime makes a subtask's state, restores into it, and
// only then processes an element -- and merging would produce a job whose
// aggregates are the sum of a recovered run and a partial one. That is wrong in
// a way nothing downstream can detect: the counts are plausible, no key is
// missing, and every window is present. Refusing turns it into a job that will
// not start.
//
// The emptiness check is an Iterate that stops on the first entry rather than a
// Len, so that KeyedState does not grow a method for it. It costs one seek on a
// disk backend.
//
// This decoder is not a parser for untrusted bytes. A length field it reads is
// trusted enough to allocate against, because the only thing that produces this
// format is WriteTo and the only thing that stores it is the checkpoint format
// in pkg/checkpoint, which verifies a CRC over the whole payload before handing
// any of it here. A corrupted stream reaching this function means the CRC check
// was skipped.
func ReadFrom(s KeyedState, r io.Reader) error {
	empty := true
	s.Iterate(func(key, value []byte) bool {
		empty = false
		return false
	})
	if err := s.Err(); err != nil {
		return fmt.Errorf("state: restore: checking that the state is empty: %w", err)
	}
	if !empty {
		return fmt.Errorf("state: restore: %w", errStateNotEmpty)
	}

	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return fmt.Errorf("state: restore: entry count: %w", err)
	}
	entries := binary.BigEndian.Uint64(header[:])

	for i := uint64(0); i < entries; i++ {
		key, err := readBytes(r)
		if err != nil {
			return fmt.Errorf("state: restore: entry %d of %d: key: %w", i, entries, err)
		}
		value, err := readBytes(r)
		if err != nil {
			return fmt.Errorf("state: restore: entry %d of %d: value: %w", i, entries, err)
		}
		s.Put(key, value)
	}
	if err := s.Err(); err != nil {
		return fmt.Errorf("state: restore: writing entries: %w", err)
	}
	return nil
}

// readBytes reads one length-prefixed byte string.
//
// A truncated stream is reported rather than returning what was there: io.EOF
// on the length and io.ErrUnexpectedEOF partway through the bytes, both of
// which the caller wraps with the entry it was on. Returning a short value
// would restore a state whose entries are silently wrong.
func readBytes(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	// On a 32-bit platform a length above MaxInt32 is not a slice this process
	// can make. Reported rather than left to a panic in make.
	if uint64(n) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%d bytes: %w", n, errNegativeLength)
	}
	if n == 0 {
		// Distinguished from nil on purpose: Put copies what it is given and a
		// zero-length value is a value, not an absent one.
		return []byte{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
