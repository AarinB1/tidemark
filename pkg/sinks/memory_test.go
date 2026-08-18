package sinks

import (
	"sync"
	"testing"

	"github.com/AarinB1/tidemark/pkg/core"
)

func rec(key string) *core.Record {
	return &core.Record{Key: []byte(key), Value: []byte(key)}
}

func keys(recs []*core.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Key)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCollect(t *testing.T) {
	tests := []struct {
		name  string
		input []*core.Record
		want  []string
	}{
		{name: "empty", input: nil, want: nil},
		{name: "one", input: []*core.Record{rec("a")}, want: []string{"a"}},
		{
			name:  "many, including a repeat",
			input: []*core.Record{rec("a"), rec("b"), rec("a")},
			want:  []string{"a", "b", "a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCollect()
			if err := c.Open(nil); err != nil {
				t.Fatalf("Open: %v", err)
			}
			for _, r := range tt.input {
				if err := c.Write(r); err != nil {
					t.Fatalf("Write: %v", err)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := keys(c.Records()); !equalStrings(got, tt.want) {
				t.Errorf("Records() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCollectRecordsIsASnapshot: a caller holding the result must not see later
// writes, and must not be able to corrupt the sink through it.
func TestCollectRecordsIsASnapshot(t *testing.T) {
	c := NewCollect()
	if err := c.Write(rec("a")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapshot := c.Records()
	if err := c.Write(rec("b")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(snapshot) != 1 {
		t.Fatalf("snapshot grew to %d records after a later Write", len(snapshot))
	}

	snapshot[0] = rec("z")
	if got := keys(c.Records()); !equalStrings(got, []string{"a", "b"}) {
		t.Errorf("mutating the snapshot changed the sink: %v", got)
	}
}

// TestCollectConcurrentReadAndWrite is the case the mutex exists for: a test
// goroutine calling Records while a subtask goroutine is still writing. Run
// under -race, this fails without the lock.
func TestCollectConcurrentReadAndWrite(t *testing.T) {
	const n = 1000
	c := NewCollect()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range n {
			if err := c.Write(rec(string(rune('a' + i%26)))); err != nil {
				t.Errorf("Write: %v", err)
				return
			}
		}
	}()

	for range n {
		_ = c.Records()
	}
	wg.Wait()

	if got := len(c.Records()); got != n {
		t.Errorf("collected %d records, want %d", got, n)
	}
}

func TestDiscard(t *testing.T) {
	d := NewDiscard()
	if err := d.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, r := range []*core.Record{rec("a"), nil} {
		if err := d.Write(r); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
