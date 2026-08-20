package transport

import (
	"context"

	"github.com/AarinB1/tidemark/pkg/core"
)

// Output is the sending half of a transport, and Input the receiving half.
//
// These exist because the coordinator/worker split in Phase 7 is a committed
// phase, not a speculative one. The record writer below and the input gate in
// pkg/runtime are the two places that touch a transport on the data path; if
// either named *Channel directly, Phase 7 would have to rewrite both to add a
// second implementation. Naming the seam once, now, costs nothing and confines
// that change to the wiring.
//
// *Channel is the only implementation in Phase 1 and keeps every method it has.
//
// The one-producer, one-consumer contract on Channel carries over: an Output is
// sent to by exactly one goroutine, which is also the only one that may Close
// it, and an Input is received from by exactly one goroutine.
type Output interface {
	// Send delivers e, blocking until the transport has room. It returns
	// ctx.Err() if ctx is cancelled while it waits.
	Send(ctx context.Context, e core.StreamElement) error
	// Close signals that no further elements will be sent.
	Close()
}

// Input is the receiving half of a transport. See Output.
type Input interface {
	// Recv returns the next element, blocking until one arrives. ok is false
	// once the transport has been closed and drained.
	Recv() (e core.StreamElement, ok bool)
}

var (
	_ Output = (*Channel)(nil)
	_ Input  = (*Channel)(nil)
)
