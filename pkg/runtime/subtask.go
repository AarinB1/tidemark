package runtime

import (
	"context"
	"fmt"
	"math"

	"github.com/AarinB1/tidemark/pkg/core"
	"github.com/AarinB1/tidemark/pkg/graph"
	"github.com/AarinB1/tidemark/pkg/state"
	"github.com/AarinB1/tidemark/pkg/transport"
)

// subtaskID identifies one parallel instance of a vertex. It is the unit of
// scheduling, state, and failure.
type subtaskID struct {
	vertexID string
	index    int
}

func (id subtaskID) String() string { return fmt.Sprintf("%s[%d]", id.vertexID, id.index) }

// runSubtask runs one parallel instance of v to completion.
//
// It closes the subtask's outputs on every exit path, including a failure, so a
// downstream Recv always unblocks: Recv has no context to watch, and closure is
// the only signal it has. A subtask closes its own outputs and nothing else,
// which is what keeps one producer per channel true at P*Q channels.
//
// gate is nil for a source, which has no inputs, and groups is empty for a
// sink, which has no outputs. groups holds one group of outputs per downstream
// vertex; see transport.NewWriter for what the grouping buys.
func runSubtask(ctx context.Context, v graph.Vertex, index int, gate *Gate, groups [][]transport.Output) error {
	id := subtaskID{vertexID: v.ID, index: index}
	w := transport.NewWriter(groups)
	defer w.CloseAll()

	switch v.Kind {
	case graph.VertexSource:
		return runSourceSubtask(ctx, v, id, w)
	case graph.VertexOperator:
		return runOperatorSubtask(ctx, v, id, gate, w)
	case graph.VertexSink:
		return runSinkSubtask(ctx, v, id, gate, w)
	default:
		return fmt.Errorf("subtask %s: unknown kind %d", id, uint8(v.Kind))
	}
}

// runSourceSubtask reads this subtask's slice of the offset space and emits it.
func runSourceSubtask(ctx context.Context, v graph.Vertex, id subtaskID, w *transport.Writer) (err error) {
	src := v.NewSource()
	oc := newOpContext(ctx, w)
	if err := src.Open(oc); err != nil {
		return fmt.Errorf("source %s: open: %w", id, err)
	}
	defer func() {
		if cerr := src.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("source %s: close: %w", id, cerr)
		}
	}()

	emitRecord := func(rec *core.Record) error { return w.EmitRecord(ctx, rec) }
	// Watermarks broadcast; they never partition. A watermark that reached one
	// subtask of a downstream vertex would leave the others with no event-time
	// signal at all, so their windows would sit open until end of input and the
	// gate's minimum would be pinned by the silent ones (invariants 1 and 2).
	emitWatermark := func(t int64) error {
		return w.Broadcast(ctx, core.NewWatermarkElement(t))
	}
	// Barriers broadcast for the same reason watermarks do, and the consequence
	// of getting it wrong is worse. A barrier that reached only some of a
	// downstream vertex's subtasks leaves the others aligning on a checkpoint
	// they will never see completed, and alignment has no timeout: the job stops
	// producing output with no error anywhere (invariants 2 and 3).
	emitBarrier := func(b *core.Barrier) error {
		return w.Broadcast(ctx, core.NewBarrierElement(b))
	}
	wm := newWatermarkGenerator(v.WatermarkIntervalElements, v.MaxOutOfOrderness)
	if err := sourceLoop(ctx, src, v.Parallelism, id.index, wm, v.BarrierIntervalElements, emitRecord, emitWatermark, emitBarrier); err != nil {
		return fmt.Errorf("source %s: %w", id, err)
	}

	// End-of-stream broadcasts. Each downstream subtask needs to know this
	// producer is finished; a partitioned end-of-stream would leave the others
	// waiting on an input that will never speak again.
	return w.Broadcast(ctx, core.NewEndOfStreamElement())
}

func runOperatorSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer) (err error) {
	op := v.NewOperator()
	oc := newOpContext(ctx, w)
	if err := op.Open(oc); err != nil {
		return fmt.Errorf("operator %s: open: %w", id, err)
	}
	defer func() {
		if cerr := op.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("operator %s: close: %w", id, cerr)
		}
	}()

	for {
		e, ok := gate.Recv()
		if !ok {
			// Every input closed without the gate delivering an end-of-stream:
			// an upstream subtask failed, or the job was cancelled. That
			// goroutine reports the cause, so this one exits quietly.
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := op.ProcessElement(e.Record, oc); err != nil {
				return fmt.Errorf("operator %s: process: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
			if err := op.ProcessWatermark(e.Watermark, oc); err != nil {
				return fmt.Errorf("operator %s: watermark: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			// Forwarded by the runtime, and broadcast rather than partitioned:
			// a watermark that reached only one downstream channel would let
			// the others fall behind without any signal.
			if err := w.Broadcast(ctx, e); err != nil {
				return err
			}
		case core.KindEndOfStream:
			// The gate delivers exactly one of these, after every input has
			// sent one, so OnEndOfStream runs once per subtask rather than once
			// per input.
			if err := op.OnEndOfStream(oc); err != nil {
				return fmt.Errorf("operator %s: end of stream: %w", id, err)
			}
			if err := oc.takeErr(); err != nil {
				return err
			}
			return w.Broadcast(ctx, e)
		case core.KindBarrier:
			// Forwarded and broadcast, like a watermark, and NOT handed to the
			// operator. Phase 3a aligns barriers at the gate and forwards them;
			// taking a snapshot when one arrives is Phase 3b, and core.Operator
			// has no method for it precisely so that this line cannot quietly
			// grow one.
			if err := w.Broadcast(ctx, e); err != nil {
				return err
			}
		default:
			return fmt.Errorf("operator %s: unexpected %s element", id, e.Kind)
		}
	}
}

func runSinkSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer) (err error) {
	snk := v.NewSink()
	oc := newOpContext(ctx, w)
	if err := snk.Open(oc); err != nil {
		return fmt.Errorf("sink %s: open: %w", id, err)
	}
	defer func() {
		if cerr := snk.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("sink %s: close: %w", id, cerr)
		}
	}()

	for {
		e, ok := gate.Recv()
		if !ok {
			return ctx.Err()
		}
		switch e.Kind {
		case core.KindRecord:
			if err := snk.Write(e.Record); err != nil {
				return fmt.Errorf("sink %s: write: %w", id, err)
			}
		case core.KindWatermark:
			oc.watermark = e.Watermark
		case core.KindBarrier:
			// Ignored, deliberately. A sink commits on NotifyCheckpointComplete
			// and never at snapshot time, because data committed at snapshot
			// time belongs to a checkpoint that may never complete and comes
			// back as duplicates on recovery (invariant 4). Neither the
			// notification nor the transactional sink exists yet, so there is
			// nothing here to do but let the barrier pass.
		case core.KindEndOfStream:
			return nil
		default:
			return fmt.Errorf("sink %s: unexpected %s element", id, e.Kind)
		}
	}
}

// opContext is the runtime's implementation of core.Context.
//
// Emit cannot return an error, so a failed send is held here and collected by
// the caller after each operator call. Dropping it instead would turn a
// cancelled job into a silently truncated output.
type opContext struct {
	ctx       context.Context
	writer    *transport.Writer
	watermark int64
	err       error
	// state is this subtask's keyed state. One per subtask, made here rather
	// than by the operator, because a subtask is the unit of state and the
	// runtime is what decides which backend a job runs on: Memory now, Pebble
	// in Phase 3b. Sources and sinks get one too, and neither uses it; giving
	// them a different Context to keep it away would be a second Context
	// implementation to keep in step for no gain.
	state state.KeyedState
}

var _ core.Context = (*opContext)(nil)

func newOpContext(ctx context.Context, w *transport.Writer) *opContext {
	return &opContext{
		ctx:    ctx,
		writer: w,
		state:  state.NewMemory(),
		// No watermark has been delivered, so nothing is complete yet. Starting
		// at zero would claim that every event before 1970 had arrived.
		watermark: math.MinInt64,
	}
}

// Emit sends rec downstream, to exactly one of this subtask's outputs, chosen
// by the hash of the record's key.
//
// Once a send has failed every remaining Emit in the same operator call is a
// no-op. The runtime only collects the stash between calls, so an operator
// emitting in a loop would otherwise grind through a send per record that
// cannot succeed, and a later send that did succeed would clear the error
// explaining why the job is stopping.
func (c *opContext) Emit(rec *core.Record) {
	if c.err != nil {
		return
	}
	c.err = c.writer.EmitRecord(c.ctx, rec)
}

// CurrentWatermark returns the last watermark delivered to this subtask.
func (c *opContext) CurrentWatermark() int64 { return c.watermark }

// State returns this subtask's keyed state.
func (c *opContext) State() state.KeyedState { return c.state }

// takeErr returns and clears any error stashed by Emit.
func (c *opContext) takeErr() error {
	err := c.err
	c.err = nil
	return err
}
