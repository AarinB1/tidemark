package state

import (
	"errors"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

// Pebble is the on-disk KeyedState.
//
// It is the second implementation the interface was justified by, and the
// reason the interface is phrased in bytes on both sides: Pebble stores byte
// keys in byte order, so there is no translation at the boundary and no way for
// the two backends to disagree about how keys sort. Iterate is a scan of the
// LSM in key order, which is native here and a sort-per-call in Memory.
//
// # Durability, and why writes are not synced
//
// Every write uses pebble.NoSync. This database is NOT the durable artefact: a
// checkpoint is, and it is written by pkg/checkpoint with its own fsync
// sequence out of a snapshot taken through state.WriteTo. The working state of
// a running subtask has no value after a crash -- recovery restores from the
// last complete checkpoint and replays -- so syncing every Put would buy
// nothing and cost a disk round trip per record on the hottest path in the
// engine.
//
// # Errors
//
// Get, Put, Delete and Iterate cannot fail in their signatures, so the first
// error is stashed and returned by Err, which the runtime collects after every
// call it makes into an operator. See KeyedState.Err. Once it has failed this
// type keeps accepting calls and returns zero values, because an operator is
// not written to check after each one.
//
// No locking, like Memory: one subtask owns one KeyedState and the runtime
// calls the operator from a single goroutine.
type Pebble struct {
	db  *pebble.DB
	dir string
	// ownsDir is true when this instance created its own directory and must
	// remove it on Close. A job's subtask states are scratch and there is
	// nothing to keep once the job is over.
	ownsDir bool
	err     error
}

var _ KeyedState = (*Pebble)(nil)

// errClosed is stashed when a closed database is used.
//
// The alternative is a nil dereference. A subtask closes its state on the way
// out, so a use after close means something outlived the subtask that owned it,
// and a panic in whatever goroutine that is says far less than an error naming
// the backend.
var errClosed = errors.New("state: pebble: use after close")

// OpenPebble opens or creates a state database in dir.
func OpenPebble(dir string) (*Pebble, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("state: opening pebble at %s: %w", dir, err)
	}
	return &Pebble{db: db, dir: dir}, nil
}

// NewTempPebble creates a state database in a new temporary directory, which
// Close removes.
//
// This is what a job uses. A subtask's state is scratch: the durable record of
// it is the checkpoint, and the directory is worth exactly as long as the
// process that owns it. Each subtask gets its OWN database, because a subtask
// is the unit of state and two subtasks sharing one would put two key spaces in
// one file with nothing separating them.
func NewTempPebble() (*Pebble, error) {
	dir, err := os.MkdirTemp("", "tidemark-state-")
	if err != nil {
		return nil, fmt.Errorf("state: creating a temporary directory for pebble: %w", err)
	}
	p, err := OpenPebble(dir)
	if err != nil {
		// The directory is this function's to clean up: nothing else knows
		// about it yet.
		_ = os.RemoveAll(dir)
		return nil, err
	}
	p.ownsDir = true
	return p, nil
}

// Dir returns the directory this database lives in.
func (p *Pebble) Dir() string { return p.dir }

// fail records err as the first error, if there is not one already.
func (p *Pebble) fail(err error) {
	if p.err == nil {
		p.err = err
	}
}

// closed reports whether this backend can no longer serve a call: either it has
// already failed, or it has been closed. A closed database records the fact as
// an error, so a use after close surfaces through Err like any other failure
// rather than through a nil dereference in whichever goroutine reached it.
func (p *Pebble) closed() bool {
	if p.err != nil {
		return true
	}
	if p.db == nil {
		p.fail(errClosed)
		return true
	}
	return false
}

// Get returns the value stored under key.
//
// The value is COPIED before the read is released, which is stricter than the
// interface asks for: KeyedState documents a returned slice as aliasing stored
// state and valid only until the next write to that key. Pebble has nothing to
// alias -- the bytes live in a memtable or an SSTable behind a handle that has
// to be closed -- so the copy is not a choice. It means a caller can hold this
// slice for longer than the interface promises, which no caller should rely on,
// because Memory does not offer it.
func (p *Pebble) Get(key []byte) ([]byte, bool) {
	if p.closed() {
		return nil, false
	}
	value, closer, err := p.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false
		}
		p.fail(fmt.Errorf("state: pebble: get: %w", err))
		return nil, false
	}
	out := make([]byte, len(value))
	copy(out, value)
	if err := closer.Close(); err != nil {
		p.fail(fmt.Errorf("state: pebble: get: releasing the read: %w", err))
		return nil, false
	}
	return out, true
}

// Put stores value under key. Pebble copies both into its batch, so the caller
// may hand over a reused scratch buffer exactly as with Memory.
func (p *Pebble) Put(key, value []byte) {
	if p.closed() {
		return
	}
	if err := p.db.Set(key, value, pebble.NoSync); err != nil {
		p.fail(fmt.Errorf("state: pebble: put: %w", err))
	}
}

// Delete removes key. Deleting a key that is not there is a no-op, as it is for
// Memory: an LSM records a tombstone either way.
func (p *Pebble) Delete(key []byte) {
	if p.closed() {
		return
	}
	if err := p.db.Delete(key, pebble.NoSync); err != nil {
		p.fail(fmt.Errorf("state: pebble: delete: %w", err))
	}
}

// Iterate calls fn for every entry in ascending byte order of key.
//
// The order is native. Memory sorts its keys on every call to produce it;
// an LSM is stored in key order, so this walks it.
//
// The key and value handed to fn point into the iterator and are valid only for
// the duration of that call, which is what KeyedState documents. fn may Delete
// the entry it is given: the iterator reads a consistent view taken when it was
// created, so a write does not disturb the scan.
//
// It may NOT usefully delete a different entry, and that is the one place the
// two backends part company. Memory looks each key up again as it reaches it,
// so an entry deleted by an earlier call is skipped; this iterator was fixed
// when it was created and still hands it back. Neither is more correct, so the
// interface promises only what both do -- see the note on KeyedState.Iterate.
// Emulating Memory here would mean a Get per entry on the scan that runs on
// every watermark, to reproduce an accident of the map implementation that no
// operator relies on.
func (p *Pebble) Iterate(fn func(key, value []byte) bool) {
	if p.closed() {
		return
	}
	iter, err := p.db.NewIter(nil)
	if err != nil {
		p.fail(fmt.Errorf("state: pebble: iterate: %w", err))
		return
	}
	for ok := iter.First(); ok; ok = iter.Next() {
		if !fn(iter.Key(), iter.Value()) {
			break
		}
	}
	// The scan's own error before the close: an iterator that stopped early
	// because of a read failure looks exactly like one that reached the end,
	// and treating the two alike would snapshot a fraction of a subtask's state
	// as though it were all of it.
	if err := iter.Error(); err != nil {
		p.fail(fmt.Errorf("state: pebble: iterate: %w", err))
	}
	if err := iter.Close(); err != nil {
		p.fail(fmt.Errorf("state: pebble: iterate: closing: %w", err))
	}
}

// Err returns the first error this backend encountered.
func (p *Pebble) Err() error { return p.err }

// Close releases the database, and removes its directory when this instance
// created it.
//
// The stashed error is NOT returned here. It has already failed the subtask
// that hit it, and returning it again would make an unwinding subtask report
// the state failure a second time in place of whatever it was closing for.
func (p *Pebble) Close() error {
	var err error
	if p.db != nil {
		err = p.db.Close()
		p.db = nil
	}
	if p.ownsDir && p.dir != "" {
		if rerr := os.RemoveAll(p.dir); rerr != nil && err == nil {
			err = rerr
		}
		p.dir = ""
	}
	if err != nil {
		return fmt.Errorf("state: pebble: close: %w", err)
	}
	return nil
}
