// Package operators holds the stateless Operator implementations. Anything that
// keeps state across records waits for the state backend in Phase 3.
package operators

import (
	"io"

	"github.com/AarinB1/tidemark/pkg/core"
)

// Map applies a function to every record and emits the result. Returning a nil
// record drops it, which is how a map expresses "no output" without a second
// operator type.
type Map struct {
	fn func(*core.Record) (*core.Record, error)
}

var _ core.Operator = (*Map)(nil)

// NewMap returns a Map applying fn.
func NewMap(fn func(*core.Record) (*core.Record, error)) *Map {
	return &Map{fn: fn}
}

func (m *Map) Open(ctx core.Context) error { return nil }

func (m *Map) ProcessElement(rec *core.Record, ctx core.Context) error {
	out, err := m.fn(rec)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	ctx.Emit(out)
	return nil
}

// ProcessWatermark emits nothing. Watermarks broadcast to every downstream
// channel and the runtime does that forwarding; an operator that also emitted
// one here would double it.
func (m *Map) ProcessWatermark(wm int64, ctx core.Context) error { return nil }

// OnEndOfStream has nothing to flush: Map holds no state.
func (m *Map) OnEndOfStream(ctx core.Context) error { return nil }

// Snapshot writes zero bytes.
func (m *Map) Snapshot(w io.Writer) error { return nil }

// Restore consumes the snapshot and discards it. Draining rather than ignoring
// the reader keeps the stream positioned for whatever follows it.
func (m *Map) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (m *Map) Close() error { return nil }

// Filter emits the records its predicate accepts.
type Filter struct {
	pred func(*core.Record) bool
}

var _ core.Operator = (*Filter)(nil)

// NewFilter returns a Filter using pred.
func NewFilter(pred func(*core.Record) bool) *Filter {
	return &Filter{pred: pred}
}

func (f *Filter) Open(ctx core.Context) error { return nil }

func (f *Filter) ProcessElement(rec *core.Record, ctx core.Context) error {
	if f.pred(rec) {
		ctx.Emit(rec)
	}
	return nil
}

// ProcessWatermark emits nothing; the runtime forwards watermarks.
func (f *Filter) ProcessWatermark(wm int64, ctx core.Context) error { return nil }

// OnEndOfStream has nothing to flush: Filter holds no state.
func (f *Filter) OnEndOfStream(ctx core.Context) error { return nil }

// Snapshot writes zero bytes.
func (f *Filter) Snapshot(w io.Writer) error { return nil }

// Restore consumes the snapshot and discards it.
func (f *Filter) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

func (f *Filter) Close() error { return nil }
