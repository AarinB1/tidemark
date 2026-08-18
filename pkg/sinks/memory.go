// Package sinks holds the Sink implementations.
package sinks

import (
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

func (d *Discard) Close() error { return nil }
