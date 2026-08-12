// Package core defines the element and operator types shared by every stage of
// the pipeline.
package core

// ElementKind discriminates the payload carried by a StreamElement.
type ElementKind uint8

const (
	KindRecord ElementKind = iota
	KindWatermark
	KindBarrier
	KindEndOfStream
)

// String renders the kind for debugging output.
func (k ElementKind) String() string {
	switch k {
	case KindRecord:
		return "Record"
	case KindWatermark:
		return "Watermark"
	case KindBarrier:
		return "Barrier"
	case KindEndOfStream:
		return "EndOfStream"
	default:
		return "Unknown"
	}
}

// Record is a single data element. EventTime is millis since the Unix epoch.
type Record struct {
	Key       []byte
	Value     []byte
	EventTime int64
}

// Barrier marks a checkpoint boundary in the stream.
type Barrier struct {
	CheckpointID int64
	Timestamp    int64
}

// StreamElement is the unit transported on every channel.
//
// Records, watermarks, and barriers share one struct so that they stay ordered
// relative to one another on each channel. Moving control elements to a side
// channel would break the time model: a watermark could overtake or fall behind
// the records it is meant to bound.
//
// Only the field named by Kind is meaningful; the others are zero.
type StreamElement struct {
	Kind      ElementKind
	Record    *Record
	Watermark int64
	Barrier   *Barrier
}

// NewRecordElement wraps rec as a KindRecord element.
func NewRecordElement(rec *Record) StreamElement {
	return StreamElement{Kind: KindRecord, Record: rec}
}

// NewWatermarkElement wraps wm as a KindWatermark element.
func NewWatermarkElement(wm int64) StreamElement {
	return StreamElement{Kind: KindWatermark, Watermark: wm}
}

// NewBarrierElement wraps b as a KindBarrier element.
func NewBarrierElement(b *Barrier) StreamElement {
	return StreamElement{Kind: KindBarrier, Barrier: b}
}

// NewEndOfStreamElement returns the element signalling that no further records
// will arrive on a channel.
//
// It exists in Phase 0 because input is bounded and aggregating operators need a
// signal to flush. Declaring it now avoids editing this enum once checkpointing
// lands. It broadcasts to all downstream channels like a watermark; it never
// partitions to one channel like a record.
func NewEndOfStreamElement() StreamElement {
	return StreamElement{Kind: KindEndOfStream}
}
