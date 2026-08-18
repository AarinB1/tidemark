package core

import (
	"io"
	"testing"
)

// nopSource and nopSink exist to pin the interface shapes at compile time.
// There is no behaviour to exercise yet: Source and Sink are contracts, and
// their first real implementations land in pkg/sources and pkg/sinks.
type nopSource struct {
	pos int64
}

func (s *nopSource) Open(ctx Context) error       { return nil }
func (s *nopSource) Next() (*Record, bool, error) { return nil, false, io.EOF }
func (s *nopSource) Seek(offset int64) error      { s.pos = offset; return nil }
func (s *nopSource) Position() int64              { return s.pos }
func (s *nopSource) Close() error                 { return nil }

type nopSink struct{}

func (s *nopSink) Open(ctx Context) error  { return nil }
func (s *nopSink) Write(rec *Record) error { return nil }
func (s *nopSink) Close() error            { return nil }

var (
	_ Source = (*nopSource)(nil)
	_ Sink   = (*nopSink)(nil)
)

// TestContractsSatisfied is a compile-time assertion. If this package builds,
// the fakes above implement Source and Sink.
func TestContractsSatisfied(t *testing.T) {
	var src Source = &nopSource{}
	if got := src.Position(); got != 0 {
		t.Errorf("fresh Position() = %d, want 0", got)
	}
}
