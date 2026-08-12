package core

import "testing"

func TestElementKindString(t *testing.T) {
	tests := []struct {
		kind ElementKind
		want string
	}{
		{KindRecord, "Record"},
		{KindWatermark, "Watermark"},
		{KindBarrier, "Barrier"},
		{KindEndOfStream, "EndOfStream"},
		{ElementKind(99), "Unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.kind.String(); got != tt.want {
				t.Errorf("ElementKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}

func TestConstructors(t *testing.T) {
	rec := &Record{Key: []byte("k"), Value: []byte("v"), EventTime: 1700000000000}
	bar := &Barrier{CheckpointID: 7, Timestamp: 1700000000001}

	tests := []struct {
		name string
		got  StreamElement
		want StreamElement
	}{
		{
			name: "record",
			got:  NewRecordElement(rec),
			want: StreamElement{Kind: KindRecord, Record: rec},
		},
		{
			name: "watermark",
			got:  NewWatermarkElement(42),
			want: StreamElement{Kind: KindWatermark, Watermark: 42},
		},
		{
			name: "barrier",
			got:  NewBarrierElement(bar),
			want: StreamElement{Kind: KindBarrier, Barrier: bar},
		},
		{
			name: "end of stream",
			got:  NewEndOfStreamElement(),
			want: StreamElement{Kind: KindEndOfStream},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %+v, want %+v", tt.got, tt.want)
			}
		})
	}
}
