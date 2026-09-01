// Package sinks holds the Sink implementations.
package sinks

import (
	"io"
	"slices"
	"sync"

	"github.com/AarinB1/tidemark/pkg/core"
)

// Collect accumulates every record it is given, for tests and small demos.
//
// The mutex guards against the test goroutine calling Records while the subtask
// goroutine is still writing. It is not a claim that the data path is shared:
// one subtask owns a sink, and Write is called from that goroutine only.
type Collect struct {
	mu      sync.Mutex
	records []*core.Record
}

var _ core.Sink = (*Collect)(nil)

// NewCollect returns an empty Collect.
func NewCollect() *Collect { return &Collect{} }

func (c *Collect) Open(ctx core.Context) error { return nil }

func (c *Collect) Write(rec *core.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, rec)
	return nil
}

// Snapshot writes nothing. Collect holds its records in memory, so there is no
// staged transaction to name: what an aborted run wrote is still in the slice
// when the recovered run appends to it, which is exactly the at-least-once
// behaviour the recovery tests compare against.
func (c *Collect) Snapshot(w io.Writer) error { return nil }

// NotifyCheckpointComplete does nothing, because Collect commits on Write.
//
// That makes it an AT-LEAST-ONCE sink and it is kept that way on purpose. It is
// what the oracle comparison ran against for three phases: a recovered run
// replays and the duplicates it leaves have to agree with each other, which is
// an assertion a transactional sink cannot make because it collapses them.
// Phase 5 adds sinks.Transactional beside this rather than in place of it.
func (c *Collect) NotifyCheckpointComplete(checkpointID int64) error { return nil }

func (c *Collect) Close() error { return nil }

// Records returns a snapshot of what has been written so far. It is a copy, so
// a caller holding the result does not race with a subtask that keeps writing.
func (c *Collect) Records() []*core.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.records)
}

// Discard throws every record away. It exists so a throughput run is not
// measuring the allocator.
type Discard struct{}

var _ core.Sink = (*Discard)(nil)

// NewDiscard returns a Discard.
func NewDiscard() *Discard { return &Discard{} }

func (d *Discard) Open(ctx core.Context) error { return nil }

func (d *Discard) Write(rec *core.Record) error { return nil }

func (d *Discard) Snapshot(w io.Writer) error { return nil }

func (d *Discard) NotifyCheckpointComplete(checkpointID int64) error { return nil }

func (d *Discard) Close() error { return nil }
