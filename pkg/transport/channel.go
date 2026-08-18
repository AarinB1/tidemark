// Package transport moves stream elements between subtasks. Phase 0 has one
// implementation, an in-process channel; a cross-process transport arrives in
// Phase 7.
package transport

import (
	"context"

	"github.com/AarinB1/tidemark/pkg/core"
)

// DefaultCapacity is the number of elements a channel buffers before a sender
// blocks.
const DefaultCapacity = 1024

// Channel carries stream elements from one producing subtask to one consuming
// subtask.
//
// Send blocks when the buffer is full, and that blocking is the backpressure
// mechanism: a slow consumer stalls its producer, which stalls the producer's
// producer, all the way back to the source. There is deliberately no drop path,
// no timeout, and no non-blocking send. Any of those would silently lose
// elements, and losing a watermark or a barrier corrupts results without
// producing an error anywhere.
//
// Exactly one goroutine produces and exactly one consumes. Only the producer
// calls Close.
type Channel struct {
	elements chan core.StreamElement
}

// NewChannel returns a channel buffering capacity elements. A capacity below 1
// is treated as DefaultCapacity.
func NewChannel(capacity int) *Channel {
	if capacity < 1 {
		capacity = DefaultCapacity
	}
	return &Channel{elements: make(chan core.StreamElement, capacity)}
}

// Send delivers e, blocking until the buffer has room. It returns ctx.Err() if
// ctx is cancelled while it waits, which is how a blocked producer is unwound
// when the job is shutting down.
func (c *Channel) Send(ctx context.Context, e core.StreamElement) error {
	select {
	case c.elements <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recv returns the next element, blocking until one arrives. ok is false once
// the channel has been closed and every buffered element has been received.
func (c *Channel) Recv() (e core.StreamElement, ok bool) {
	e, ok = <-c.elements
	return e, ok
}

// Close signals that no further elements will be sent. Only the producing
// goroutine may call it, and only once; a consumer that closed its input would
// race with the producer's Send.
func (c *Channel) Close() {
	close(c.elements)
}
