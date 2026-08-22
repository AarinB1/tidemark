package runtime

import (
	"context"
	"math"
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
// Responsibilities, and nothing else:
//
//   - deliver records in the order they arrive off the merged channel
//   - track which inputs have delivered end-of-stream
//   - report end-of-stream downstream exactly once, after every input has
//     delivered one
//   - forward min(perInputWatermark) over the inputs that are still live
//
// The watermark is a MINIMUM and never a maximum. This is invariant 1, and it
// is the reason the merged channel carries a tag: a gate that could not tell
// which input a watermark arrived on could only track one number, and one
// number over several inputs is a maximum however it is written. Taking the max
// declares event time complete as soon as the fastest input says so, which
// fires every window on that input's data alone. Nothing crashes; the counts
// come out slightly low.
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

	// watermarks[i] is the last watermark input i delivered, math.MinInt64
	// until it delivers one. An input at MinInt64 pins the minimum, which is
	// correct: nothing is known to be complete on a channel that has said
	// nothing about event time.
	watermarks []int64
	// out is the last watermark forwarded downstream.
	out int64
	// pending holds the end-of-stream element across the one Recv that returns
	// the final MaxInt64 watermark, since Recv hands back one element at a
	// time and those two must arrive in that order.
	pending *core.StreamElement

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
		watermarks:  make([]int64, len(inputs)),
		out:         math.MinInt64,
	}
	for i := range g.watermarks {
		g.watermarks[i] = math.MinInt64
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
// returned, until the last input delivers one, at which point the gate returns
// a watermark of math.MaxInt64 followed by exactly one end-of-stream element. A
// subtask with N inputs must call OnEndOfStream once, not N times, and must not
// forward N of them downstream.
//
// That MaxInt64 is what flushes the windows still open at end of input, and it
// is the only thing that does so. No source emits one; see watermarkGenerator.
//
// Watermarks are absorbed too, and re-derived: what comes out is the minimum
// across the live inputs, forwarded only when it strictly increases.
// core.Operator.ProcessWatermark documents wm as monotonically non-decreasing,
// and every window operator downstream is written against that.
//
// ok is false once every input has closed without the gate having delivered an
// end-of-stream. That is the quiet exit: an upstream subtask failed or the job
// was cancelled, that goroutine reports the cause, and this one has nothing to
// add.
func (g *Gate) Recv() (e core.StreamElement, ok bool) {
	if g.pending != nil {
		e, g.pending = *g.pending, nil
		return e, true
	}
	for {
		t, open := <-g.merged
		if !open {
			return core.StreamElement{}, false
		}
		switch t.elem.Kind {
		case core.KindWatermark:
			g.watermarks[t.input] = t.elem.Watermark
			if wm, advanced := g.advance(); advanced {
				return core.NewWatermarkElement(wm), true
			}
		case core.KindEndOfStream:
			// A second end-of-stream on one input is ignored rather than
			// counted: counting would let one chatty input satisfy the gate on
			// behalf of a quiet one, and the job would finish on partial data
			// with no error.
			if g.endOfStream[t.input] {
				continue
			}
			g.endOfStream[t.input] = true
			g.remaining--

			// Recomputed here and not only on a watermark. Dropping an input
			// out of the minimum can raise it, and that is the whole reason
			// exhausted inputs are excluded rather than frozen.
			wm, advanced := g.advance()
			if g.remaining == 0 && !g.closed {
				g.closed = true
				eos := core.NewEndOfStreamElement()
				if !advanced {
					return eos, true
				}
				g.pending = &eos
				return core.NewWatermarkElement(wm), true
			}
			if advanced {
				return core.NewWatermarkElement(wm), true
			}
		default:
			return t.elem, true
		}
	}
}

// advance returns the gate's output watermark, and whether it moved.
//
// The minimum is taken over the inputs that have NOT delivered end-of-stream.
// An exhausted input is excluded, not frozen at its last value. Freezing it
// pins the minimum at whatever that input happened to reach, so the tail
// windows never fire and the run ends with its last windows missing from the
// sink. That reads like a window assignment bug and is not one.
//
// Excluding every input leaves the minimum at its starting math.MaxInt64, which
// is exactly the end-of-input flush value. That is not a coincidence worth
// hiding behind a special case: "no live input can contradict me" and "event
// time is over" are the same statement.
func (g *Gate) advance() (wm int64, advanced bool) {
	min := int64(math.MaxInt64)
	for i, w := range g.watermarks {
		if g.endOfStream[i] {
			continue
		}
		if w < min {
			min = w
		}
	}
	if min <= g.out {
		return 0, false
	}
	g.out = min
	return min, true
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
