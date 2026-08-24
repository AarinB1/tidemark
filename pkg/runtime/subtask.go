package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/AarinB1/tidemark/pkg/checkpoint"
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

// checkpointKey is this subtask's identity on disk. Two types for one identity
// because pkg/checkpoint must not import the runtime: the on-disk half of a
// subtask's name lives with the format that writes it.
func (id subtaskID) checkpointKey() checkpoint.SubtaskKey {
	return checkpoint.SubtaskKey{VertexID: id.vertexID, Index: id.index}
}

// subtaskConfig is what a subtask needs from the job beyond its own vertex and
// its channels.
//
// A struct rather than another parameter on runSubtask because the restore
// half arrives in the next step and a function with eight positional arguments
// is one where a caller eventually passes them in the wrong order.
type subtaskConfig struct {
	// coordinator is nil when the job is not checkpointing. Barriers still flow
	// in that case -- they are part of the element stream whether or not
	// anybody is recording snapshots -- and every subtask simply has nothing to
	// acknowledge.
	coordinator *checkpoint.Coordinator
	// restore is the payload this subtask snapshotted at the checkpoint being
	// resumed from, and restored says whether there is one. They are separate
	// because a sink's payload is legitimately empty, so a nil slice cannot
	// stand for "not restoring".
	restore  []byte
	restored bool
	// newState makes the keyed state for an operator subtask. nil means the
	// in-memory backend.
	//
	// Only OPERATOR subtasks take their state from here. Sources and sinks get
	// a Memory they never touch: a source's recovery state is its offset and a
	// sink stages nothing until Phase 5, so opening a database per source
	// subtask would be a file handle and a directory bought for nothing. If
	// either ever grows real state, this is the line that has to change with
	// it.
	newState func() (state.KeyedState, error)
	// restoredCheckpoint is the ID of the checkpoint being resumed from, and
	// zero when the job is starting fresh.
	//
	// A source subtask needs it as well as its offset. Checkpoint IDs are
	// contiguous from 1 within a subtask and a subtask injects barrier k as its
	// k-th barrier, so restoring from checkpoint k means exactly k barriers
	// have been injected. Restarting the count at zero would make the resumed
	// run emit a second barrier 1 at a different logical position, and a
	// coordinator counting acknowledgements would be told about two different
	// cuts under one name.
	restoredCheckpoint int64
}

// makeState returns the keyed state for one operator subtask.
func (c subtaskConfig) makeState() (state.KeyedState, error) {
	if c.newState == nil {
		return state.NewMemory(), nil
	}
	return c.newState()
}

// closableState is a state backend holding resources the runtime must release.
//
// An optional interface, asserted for, rather than a Close on KeyedState: a map
// has nothing to close, and putting the method on the interface would make
// every fake in every test implement a no-op to satisfy it. This is the same
// shape as splittableSource, which the source runner asserts for the same
// reason.
type closableState interface {
	Close() error
}

// checkpointer is a subtask's half of the checkpoint protocol.
//
// It exists so that "the job is not checkpointing" is handled in ONE place. The
// alternative is a nil check at each of the four sites that snapshot, and the
// site that forgot one would panic only in a job with checkpointing switched
// off, which is most of the existing test suite.
type checkpointer struct {
	co  *checkpoint.Coordinator
	key checkpoint.SubtaskKey
}

// enabled reports whether anything is recording checkpoints.
func (c checkpointer) enabled() bool { return c.co != nil }

// snapshot records payload as this subtask's state for checkpoint id.
func (c checkpointer) snapshot(id int64, payload []byte) error {
	if c.co == nil {
		return nil
	}
	return c.co.Acknowledge(id, c.key, payload)
}

// fail abandons checkpoint id because this subtask could not snapshot.
//
// Called when the snapshot itself fails, before anything was handed to the
// coordinator. A failure INSIDE Acknowledge abandons the checkpoint on its own,
// because the coordinator is the one that knows the write did not land.
func (c checkpointer) fail(id int64, cause error) {
	if c.co == nil {
		return
	}
	c.co.Fail(id, c.key, cause)
}

// finished tells the coordinator this subtask will not acknowledge again.
func (c checkpointer) finished(payload []byte) error {
	if c.co == nil {
		return nil
	}
	return c.co.Finished(c.key, payload)
}

// positionBytes is the width of a source subtask's snapshot payload.
const positionBytes = 8

// encodePosition renders a source subtask's resume offset.
//
// The whole of a source subtask's checkpoint is one integer, and that is what
// contiguous ranges buy: a strided split would have to record a stride and a
// phase beside it. Big-endian, like every other encoding here.
func encodePosition(offset int64) []byte {
	var buf [positionBytes]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	return buf[:]
}

// decodePosition reads a resume offset written by encodePosition.
func decodePosition(payload []byte) (int64, error) {
	if len(payload) != positionBytes {
		return 0, fmt.Errorf("source position payload is %d bytes, want %d", len(payload), positionBytes)
	}
	return int64(binary.BigEndian.Uint64(payload)), nil
}

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
func runSubtask(ctx context.Context, v graph.Vertex, index int, gate *Gate, groups [][]transport.Output, cfg subtaskConfig) error {
	id := subtaskID{vertexID: v.ID, index: index}
	w := transport.NewWriter(groups)
	defer w.CloseAll()

	cp := checkpointer{co: cfg.coordinator, key: id.checkpointKey()}
	switch v.Kind {
	case graph.VertexSource:
		return runSourceSubtask(ctx, v, id, w, cp, cfg)
	case graph.VertexOperator:
		return runOperatorSubtask(ctx, v, id, gate, w, cp, cfg)
	case graph.VertexSink:
		// A sink restores nothing. Its payload is empty by construction, and it
		// will be until the transactional sink in Phase 5 has a staging area to
		// record. Ignored rather than checked, because a sink that found
		// something there could not do anything with it either.
		return runSinkSubtask(ctx, v, id, gate, w, cp)
	default:
		return fmt.Errorf("subtask %s: unknown kind %d", id, uint8(v.Kind))
	}
}

// runSourceSubtask reads this subtask's slice of the offset space and emits it.
func runSourceSubtask(ctx context.Context, v graph.Vertex, id subtaskID, w *transport.Writer, cp checkpointer, cfg subtaskConfig) (err error) {
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
	//
	// The SNAPSHOT COMES FIRST. A source is the initiator of a checkpoint, and
	// what it records is the offset it will resume from, which is the position
	// immediately after the last element belonging to this checkpoint. That
	// position is handed in by the loop, captured at the injection point rather
	// than read here: reading it after the broadcast would be reading it after
	// nothing has moved, which is right today and would stop being right the
	// moment anything else touches the source between the two.
	emitBarrier := func(b *core.Barrier, position int64) error {
		if cp.enabled() {
			if err := cp.snapshot(b.CheckpointID, encodePosition(position)); err != nil {
				return fmt.Errorf("checkpoint %d: %w", b.CheckpointID, err)
			}
		}
		return w.Broadcast(ctx, core.NewBarrierElement(b))
	}
	wm := newWatermarkGenerator(v.WatermarkIntervalElements, v.MaxOutOfOrderness)

	// A restored source resumes at the offset it recorded and continues its
	// barrier numbering. The watermark generator is NOT restored and does not
	// need to be: it derives its value from the records it sees, and the
	// records it sees from here are the ones it would have seen anyway.
	var resume *sourceResume
	if cfg.restored {
		position, err := decodePosition(cfg.restore)
		if err != nil {
			return fmt.Errorf("source %s: %w", id, err)
		}
		resume = &sourceResume{position: position, barriersInjected: cfg.restoredCheckpoint}
	}

	if err := sourceLoop(ctx, src, v.Parallelism, id.index, wm, v.BarrierIntervalElements, resume, emitRecord, emitWatermark, emitBarrier); err != nil {
		return fmt.Errorf("source %s: %w", id, err)
	}

	// The source is done and will not inject another barrier. Tell the
	// coordinator before broadcasting end-of-stream, so a later barrier from a
	// longer source can complete without waiting for an acknowledgement that
	// will never come. The position is the end of the range: elements past the
	// last barrier already went downstream and belong to whatever checkpoint
	// the remaining sources close next.
	if cp.enabled() {
		if err := cp.finished(encodePosition(src.Position())); err != nil {
			return fmt.Errorf("source %s: %w", id, err)
		}
	}

	// End-of-stream broadcasts. Each downstream subtask needs to know this
	// producer is finished; a partitioned end-of-stream would leave the others
	// waiting on an input that will never speak again.
	return w.Broadcast(ctx, core.NewEndOfStreamElement())
}

func runOperatorSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer, cfg subtaskConfig) (err error) {
	st, err := cfg.makeState()
	if err != nil {
		return fmt.Errorf("operator %s: state: %w", id, err)
	}
	if closable, ok := st.(closableState); ok {
		defer func() {
			if cerr := closable.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("operator %s: state: close: %w", id, cerr)
			}
		}()
	}

	op := v.NewOperator()
	oc := newOpContextWithState(ctx, w, st)
	if err := op.Open(oc); err != nil {
		return fmt.Errorf("operator %s: open: %w", id, err)
	}
	defer func() {
		if cerr := op.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("operator %s: close: %w", id, cerr)
		}
	}()

	// AFTER Open and BEFORE any element, which is the window
	// core.Operator.Restore already documents. Open is where an operator takes
	// the state handle, so restoring earlier would fill a state the operator
	// has not asked for yet; restoring later would fold a checkpointed
	// aggregate into one that has already started accumulating, and
	// state.ReadFrom refuses exactly that.
	if cfg.restored {
		if err := state.ReadFrom(oc.state, bytes.NewReader(cfg.restore)); err != nil {
			return fmt.Errorf("operator %s: restore: %w", id, err)
		}
	}

	return runOperatorLoop(ctx, op, oc, id, gate, w, cp)
}

// runOperatorLoop is the element loop of an operator subtask, split from the
// open and close around it so that a test can drive it with a context whose
// state backend is one that fails on demand. Nothing else calls it.
func runOperatorLoop(ctx context.Context, op core.Operator, oc *opContext, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer) error {
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
			// The gate has completed alignment: every live input has delivered
			// this barrier, and everything they sent afterwards is held in the
			// gate's buffers. So the operator's state right now is exactly the
			// records BELOW the barrier on every input, which is the cut this
			// checkpoint records.
			//
			// Snapshot, then acknowledge, THEN forward. That order is
			// Chandy-Lamport and it is not interchangeable. Forwarding first
			// would let a downstream operator record a state that includes
			// effects of elements this operator has not recorded yet: this
			// subtask could process more elements and emit from them while the
			// barrier was already ahead of them downstream, so the downstream
			// snapshot would contain records that the upstream snapshot's
			// resume point will replay. Those records arrive twice on recovery.
			//
			// Draining the gate's buffers stays after all of it, and it does so
			// without this loop arranging anything: the gate queues the barrier
			// for delivery before the elements it releases, so the next Recv is
			// what starts epoch k+1.
			//
			// The state is the RUNTIME's, read straight out of the subtask's
			// context, and core.Operator.Snapshot is not called. An operator's
			// state IS its KeyedState; an operator that kept a second copy
			// somewhere else would checkpoint half of itself.
			if err := snapshotOperatorState(cp, e.Barrier.CheckpointID, oc.state); err != nil {
				return fmt.Errorf("operator %s: %w", id, err)
			}
			if err := w.Broadcast(ctx, e); err != nil {
				return err
			}
		default:
			return fmt.Errorf("operator %s: unexpected %s element", id, e.Kind)
		}
	}
}

func runSinkSubtask(ctx context.Context, v graph.Vertex, id subtaskID, gate *Gate, w *transport.Writer, cp checkpointer) (err error) {
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
			// Acknowledged with an EMPTY payload, and nothing is committed. A
			// sink commits on NotifyCheckpointComplete and never at snapshot
			// time, because data committed at snapshot time belongs to a
			// checkpoint that may never complete and comes back as duplicates
			// on recovery (invariant 4). Neither the notification nor the
			// transactional sink exists yet, so there is nothing to record.
			//
			// It still acknowledges. A sink that stayed out of the count would
			// let a checkpoint complete while the sink was arbitrarily far
			// behind the cut, and Phase 5 would have to add a participant to
			// the protocol rather than content to a payload.
			if err := cp.snapshot(e.Barrier.CheckpointID, nil); err != nil {
				return fmt.Errorf("sink %s: checkpoint %d: %w", id, e.Barrier.CheckpointID, err)
			}
		case core.KindEndOfStream:
			return nil
		default:
			return fmt.Errorf("sink %s: unexpected %s element", id, e.Kind)
		}
	}
}

// snapshotOperatorState serialises st and hands it to the coordinator as this
// subtask's state for checkpoint id.
//
// A snapshot that cannot be taken abandons the checkpoint before the error goes
// back to the subtask, because the subtask is about to stop and will never
// acknowledge: a checkpoint still waiting for it would sit outstanding forever
// while later ones completed on top of it. A failure inside Acknowledge
// abandons it too, from the coordinator, which is the side that knows the write
// did not land.
func snapshotOperatorState(cp checkpointer, id int64, st state.KeyedState) error {
	if !cp.enabled() {
		return nil
	}
	// Buffered rather than streamed to the file, because the coordinator is
	// what owns the on-disk layout and a subtask handing it a reader would be a
	// subtask that decides when the write happens. The cost is one copy of a
	// subtask's state per checkpoint; see the note on state.WriteTo.
	var buf bytes.Buffer
	if err := state.WriteTo(st, &buf); err != nil {
		cp.fail(id, err)
		return fmt.Errorf("checkpoint %d: snapshot: %w", id, err)
	}
	if err := cp.snapshot(id, buf.Bytes()); err != nil {
		return fmt.Errorf("checkpoint %d: %w", id, err)
	}
	return nil
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
	return newOpContextWithState(ctx, w, state.NewMemory())
}

// newOpContextWithState is newOpContext over a chosen backend. Operator
// subtasks use it; sources and sinks take the memory one above.
func newOpContextWithState(ctx context.Context, w *transport.Writer, st state.KeyedState) *opContext {
	return &opContext{
		ctx:    ctx,
		writer: w,
		state:  st,
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

// takeErr returns the first error stashed since the last call: one from a
// failed Emit, or one the state backend recorded.
//
// The two are collected together because they are the same kind of thing. Emit
// has no error return and KeyedState's four operations have none either, so
// both stash and both need somebody to look. The runtime looks after every call
// it makes into the operator, which is the granularity at which a failure is
// still attributable to a record.
//
// The Emit stash is CLEARED and the state's is not. Emit's is per call by
// construction: a send that failed because a downstream channel was momentarily
// gone is the operator call's error and nothing later. A state error is sticky
// on the backend, because every value read after a backend fails is suspect;
// the subtask returns on the first one it sees, so it is read once either way.
//
// Emit's error wins when both are set. It is the one with a cause the reader
// can act on; a state error surfacing in the same call is more likely a
// consequence of the same cancellation.
func (c *opContext) takeErr() error {
	err := c.err
	c.err = nil
	if err != nil {
		return err
	}
	return c.state.Err()
}
