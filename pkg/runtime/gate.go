package runtime

import (
	"context"
	"sync"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// tagged is an element paired with the index of the input it arrived on.
//
// The tag is not cosmetic. Phase 2 takes min(perInputWatermark) and cannot do
// that without knowing which input advanced; Phase 3 aligns barriers and cannot
// do that without knowing which input a barrier came from. A merged channel
// that dropped the tag would foreclose both, and neither failure would look
// like a merge bug: the watermark one produces slightly wrong window counts and
// the barrier one deadlocks.
type tagged struct {
	input int
	elem  core.StreamElement
}

// Gate merges the inputs of one subtask into a single stream.
//
// A subtask with N inputs cannot select over a slice of channels without
// reflect, so each input gets a forwarder goroutine that reads it and sends
// onto one merged channel, tagging each element with its input index. The gate
// itself reads only the merged channel.
//
// Phase 1 responsibilities, and nothing else:
//
//   - deliver records in the order they arrive off the merged channel
//   - track which inputs have delivered end-of-stream
//   - report end-of-stream downstream exactly once, after every input has
//     delivered one
//
// It does not compute a watermark. Watermarks are Phase 2; the per-input state
// exists so that phase can add min(perInputWatermark), but taking a minimum now
// would be building a phase early. Nothing here is allowed to write a min.
//
// Phase 3 alignment, recorded now so this design is not accidentally wrong:
// when barrier alignment arrives, the gate must keep consuming from inputs that
// have already delivered their barrier and buffer those post-barrier elements
// per input until alignment completes, then drain them. The alternative,
// pausing a forwarder, needs a handshake with the producer and can deadlock;
// buffering is the standard approach and is the reason unaligned checkpoints
// exist later as a separate feature. Do not implement that buffer now. Do not
// build anything that prevents it: in particular, the forwarders must stay
// per-input, because a single shared reader could not hold back one input while
// draining another.
type Gate struct {
	merged chan tagged

	// endOfStream[i] is true once input i has delivered its end-of-stream.
	// Indexed by input rather than counted, so a second end-of-stream on one
	// input cannot stand in for a missing one on another.
	endOfStream []bool
	remaining   int
	closed      bool

	wg sync.WaitGroup
}

// NewGate starts one forwarder per input and returns the gate reading them.
//
// The merged channel buffers len(inputs) elements, so every forwarder can hold
// one element in flight without blocking. That adds exactly one slot per input
// on top of each transport's own capacity; backpressure still reaches upstream,
// because a gate that stops receiving fills the merged channel and then every
// input behind it.
//
// The caller must drain the gate until Recv reports closure, or cancel ctx. The
// forwarders select on ctx.Done when sending, so a gate abandoned after a
// failure does not strand them.
func NewGate(ctx context.Context, inputs []transport.Input) *Gate {
	g := &Gate{
		merged:      make(chan tagged, len(inputs)),
		endOfStream: make([]bool, len(inputs)),
		remaining:   len(inputs),
	}

	g.wg.Add(len(inputs))
	for i, in := range inputs {
		go func() {
			defer g.wg.Done()
			for {
				e, ok := in.Recv()
				if !ok {
					// The producer closed this input. Whether it finished or
					// failed is the producer's to report; this forwarder just
					// stops.
					return
				}
				select {
				case g.merged <- tagged{input: i, elem: e}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// One closer rather than a close in each forwarder: the merged channel has
	// several senders, and only the last of them may close it.
	go func() {
		g.wg.Wait()
		close(g.merged)
	}()

	return g
}

// Recv returns the next element the subtask should process.
//
// End-of-stream is absorbed: an input's end-of-stream is recorded and not
// returned, until the last input delivers one, at which point exactly one
// end-of-stream element is returned. A subtask with N inputs must call
// OnEndOfStream once, not N times, and must not forward N of them downstream.
//
// ok is false once every input has closed without the gate having delivered an
// end-of-stream. That is the quiet exit: an upstream subtask failed or the job
// was cancelled, that goroutine reports the cause, and this one has nothing to
// add.
func (g *Gate) Recv() (e core.StreamElement, ok bool) {
	for {
		t, open := <-g.merged
		if !open {
			return core.StreamElement{}, false
		}
		if t.elem.Kind != core.KindEndOfStream {
			return t.elem, true
		}
		// A second end-of-stream on one input is ignored rather than counted:
		// counting would let one chatty input satisfy the gate on behalf of a
		// quiet one, and the job would finish on partial data with no error.
		if g.endOfStream[t.input] {
			continue
		}
		g.endOfStream[t.input] = true
		g.remaining--
		if g.remaining == 0 && !g.closed {
			g.closed = true
			return core.NewEndOfStreamElement(), true
		}
	}
}

// Wait blocks until every forwarder has exited.
//
// A forwarder is blocked in one of two places, and Wait returns only once
// neither can hold: in Recv on its input, which ends when that input's producer
// closes it, or sending to merged, which ends when ctx is cancelled. So Wait
// returns once every input has been closed, or once ctx has been cancelled and
// every forwarder has reached its send.
//
// That is why the subtask itself must not call Wait on its way out. A subtask
// that failed mid-stream still has producers upstream of it that have not
// closed anything, because nothing has told them to stop yet: waiting there
// would block the very return that reports the failure and cancels them. The
// executor waits instead, after every subtask has unwound and therefore after
// every channel has been closed, which is the point at which Wait is guaranteed
// to return and where "no goroutine outlives Run" becomes assertable.
func (g *Gate) Wait() { g.wg.Wait() }
