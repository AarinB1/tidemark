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
//   - align barriers: hold back everything an input sends after its barrier
//     for checkpoint k until every live input has delivered that barrier,
//     then forward one barrier k and release what was held
//
// The watermark is a MINIMUM and never a maximum. This is invariant 1, and it
// is the reason the merged channel carries a tag: a gate that could not tell
// which input a watermark arrived on could only track one number, and one
// number over several inputs is a maximum however it is written. Taking the max
// declares event time complete as soon as the fastest input says so, which
// fires every window on that input's data alone. Nothing crashes; the counts
// come out slightly low.
//
// # Alignment
//
// A barrier for checkpoint k separates the elements belonging to k from the
// elements belonging to k+1. When it arrives on input i, everything input i
// sends afterwards is in the next epoch and must not reach the operator until k
// is complete, or the operator's state at the moment of the snapshot would
// contain elements from both sides of the line.
//
// The gate keeps consuming from an aligned input and BUFFERS what it sends,
// per input. It does not pause the forwarder. Pausing a producer needs a
// handshake with it, and a handshake between a consumer that is waiting and a
// producer that is blocked sending is a deadlock; the forwarders being
// per-input is what makes holding one back while draining another possible at
// all.
//
// The buffer is UNBOUNDED. A skewed input, one that runs far ahead of the
// slowest, grows it without limit, and there is no backpressure to stop it
// because the gate is still reading. That is a real cost and it is the reason
// unaligned checkpoints exist as a separate feature in production systems: they
// snapshot the in-flight data instead of holding it. Not built here.
//
// An input that has delivered end-of-stream is EXCLUDED from alignment, exactly
// as it is excluded from the watermark minimum, and through the same
// live() helper for the same reason. Two source vertices with different record
// counts inject different numbers of barriers, so a gate that waited on an
// exhausted input would wait for a barrier nobody is going to send. There is no
// timeout: the job would stop producing output with no error anywhere.
//
// Phase 3a does not snapshot. The gate aligns and forwards; taking a snapshot
// when alignment completes is Phase 3b.
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

	// aligning is true between the first barrier of a checkpoint and the last,
	// and barrier is the element to forward once every live input has sent one.
	// The FIRST barrier of the checkpoint is what goes downstream, so exactly
	// one leaves the gate however many inputs delivered it.
	aligning   bool
	barrier    core.StreamElement
	alignedFor []bool
	// buffered[i] is what input i has sent since its barrier, held back until
	// alignment completes. Unbounded; see the type comment.
	buffered [][]core.StreamElement
	// replay is elements taken back out of buffered, waiting to pass through
	// the gate again as though they had just arrived. A drained barrier starts
	// the next alignment, so what follows it has to be classified afresh rather
	// than delivered; going back through the same path is what makes a run of
	// buffered checkpoints behave like the stream it came from.
	replay []tagged
	// pending holds elements the gate has decided to deliver but has not
	// returned yet, in order, since Recv hands back one at a time. The
	// end-of-input MaxInt64 watermark and the end-of-stream that follows it are
	// the smallest case; a completed alignment is the largest.
	pending []core.StreamElement

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
		alignedFor:  make([]bool, len(inputs)),
		buffered:    make([][]core.StreamElement, len(inputs)),
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
// Barriers are absorbed and re-emitted the same way: the N copies of barrier k
// that arrive, one per input, become exactly one going out, and it goes out
// only once every live input has delivered it. See the alignment section on the
// type.
//
// ok is false once every input has closed without the gate having delivered an
// end-of-stream. That is the quiet exit: an upstream subtask failed or the job
// was cancelled, that goroutine reports the cause, and this one has nothing to
// add.
func (g *Gate) Recv() (e core.StreamElement, ok bool) {
	for {
		if len(g.pending) > 0 {
			e := g.pending[0]
			g.pending = g.pending[1:]
			if len(g.pending) == 0 {
				// Dropped rather than left as an empty slice over the consumed
				// region, so a long run of alignments does not keep growing the
				// backing array behind a queue that is repeatedly emptied.
				g.pending = nil
			}
			return e, true
		}
		// Drained buffers before the merged channel. They are older than
		// anything still in flight, and taking a new element first would put it
		// ahead of elements the gate has already accepted.
		if len(g.replay) > 0 {
			t := g.replay[0]
			g.replay = g.replay[1:]
			if len(g.replay) == 0 {
				g.replay = nil
			}
			g.handle(t)
			continue
		}
		t, open := <-g.merged
		if !open {
			return core.StreamElement{}, false
		}
		g.handle(t)
	}
}

// handle routes one arriving element, either holding it back for the current
// alignment or letting it through to deliver.
//
// This is the whole of the alignment decision: an input that has already
// delivered its barrier for the checkpoint in progress is in the next epoch, so
// everything it sends is buffered. That includes its watermarks and its
// end-of-stream. Letting a watermark past would be worse than pointless: the
// records held in that input's buffer are BELOW it in the stream, so forwarding
// it first would declare their event time complete before they arrived, and the
// window operator would drop every one of them as late. In-band order is
// invariant 5, and buffering an input means buffering all of it.
func (g *Gate) handle(t tagged) {
	if g.aligning && g.alignedFor[t.input] {
		g.buffered[t.input] = append(g.buffered[t.input], t.elem)
		return
	}
	g.deliver(t)
}

// deliver interprets one element from an input that is not currently held back.
func (g *Gate) deliver(t tagged) {
	switch t.elem.Kind {
	case core.KindWatermark:
		g.watermarks[t.input] = t.elem.Watermark
		if wm, advanced := g.advance(); advanced {
			g.pending = append(g.pending, core.NewWatermarkElement(wm))
		}
	case core.KindBarrier:
		g.onBarrier(t)
	case core.KindEndOfStream:
		g.onEndOfStream(t)
	default:
		g.pending = append(g.pending, t.elem)
	}
}

// onBarrier records that input t.input has reached the checkpoint boundary.
//
// The first barrier of a checkpoint starts the alignment and is the element
// that will eventually go downstream; the rest only mark their inputs. A gate
// with one input completes on that same call and the barrier passes straight
// through, which is the general rule rather than a case handled separately.
//
// Checkpoint IDs are contiguous and identical on every channel by construction:
// a source injects them in order and broadcasts each to every downstream
// channel, and an operator forwards exactly the aligned sequence it received.
// So an input cannot deliver a barrier for a different checkpoint than the one
// being aligned without an upstream bug. There is nowhere to report one from
// here, since Recv has no error to return; the checkpoint coordinator in Phase
// 3b is what gets to compare IDs against what it asked for.
func (g *Gate) onBarrier(t tagged) {
	if !g.aligning {
		g.aligning = true
		g.barrier = t.elem
	}
	g.alignedFor[t.input] = true
	g.completeAlignment()
}

// onEndOfStream records that input t.input is exhausted.
func (g *Gate) onEndOfStream(t tagged) {
	// A second end-of-stream on one input is ignored rather than counted:
	// counting would let one chatty input satisfy the gate on behalf of a quiet
	// one, and the job would finish on partial data with no error.
	if g.endOfStream[t.input] {
		return
	}
	g.endOfStream[t.input] = true
	g.remaining--

	// Recomputed here and not only on a watermark. Dropping an input out of the
	// minimum can raise it, and that is the whole reason exhausted inputs are
	// excluded rather than frozen.
	if wm, advanced := g.advance(); advanced {
		g.pending = append(g.pending, core.NewWatermarkElement(wm))
	}
	if g.remaining == 0 && !g.closed {
		g.closed = true
		g.pending = append(g.pending, core.NewEndOfStreamElement())
	}
	// An exhausted input is excluded from alignment as well as from the
	// minimum, so losing it can be what completes a checkpoint. Two source
	// vertices with different record counts inject different numbers of
	// barriers; without this the gate waits on one that will never arrive.
	g.completeAlignment()
}

// completeAlignment forwards the barrier and releases the buffers, if every
// live input has now delivered it.
//
// remaining can only reach zero once every input's end-of-stream has been
// DELIVERED, and an aligned input's end-of-stream is buffered rather than
// delivered, so the final MaxInt64 watermark and end-of-stream cannot be queued
// ahead of buffered records. The tail of the run comes out in order without
// this function having to know about it.
func (g *Gate) completeAlignment() {
	if !g.aligning {
		return
	}
	for i := range g.alignedFor {
		if g.live(i) && !g.alignedFor[i] {
			return
		}
	}

	g.pending = append(g.pending, g.barrier)
	g.aligning = false
	g.barrier = core.StreamElement{}
	clear(g.alignedFor)
	g.drainBuffers()
}

// drainBuffers moves everything held back into the replay queue, in INPUT INDEX
// order, and empties the buffers.
//
// Input index order is a choice, and any fixed order would do for correctness
// since these elements were concurrent on separate channels. It is fixed rather
// than arbitrary because the order records reach an operator decides the order
// its windows accumulate, and an order that varied between runs would make a
// recovered run diverge from a clean one for a reason that has nothing to do
// with recovery.
//
// The elements go back through handle rather than straight out, because a
// buffered barrier starts the NEXT alignment and everything behind it on that
// input has to be held back again. Prepended to any replay already queued: what
// is queued came from an earlier point in the stream on some input, and what is
// being added was buffered behind the barrier just forwarded.
func (g *Gate) drainBuffers() {
	var out []tagged
	for i, buf := range g.buffered {
		for _, e := range buf {
			out = append(out, tagged{input: i, elem: e})
		}
		g.buffered[i] = nil
	}
	if len(out) == 0 {
		return
	}
	g.replay = append(out, g.replay...)
}

// live reports whether input i can still contribute anything.
//
// One helper, used by the watermark minimum and by barrier alignment alike.
// They exclude an exhausted input for the same reason: it will send nothing
// further, so waiting on it is waiting forever. Tracked separately the two
// would drift, and the drift shows up as a job that hangs at the tail under one
// topology and not another, which is a long way from looking like a
// bookkeeping mistake.
func (g *Gate) live(i int) bool { return !g.endOfStream[i] }

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
		if !g.live(i) {
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
