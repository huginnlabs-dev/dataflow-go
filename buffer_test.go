package dataflow

import (
	"testing"

	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

func TestBufferSequencingAndAck(t *testing.T) {
	b := newEventBuffer(100)
	for range 5 {
		b.Add(&pb.TraceEvent{})
	}
	if b.base != 1 {
		t.Fatalf("base = %d, want 1", b.base)
	}
	pending := b.After(0)
	if len(pending) != 5 || pending[0].Seq != 1 || pending[4].Seq != 5 {
		t.Fatalf("After(0) returned %d events, first seq %d", len(pending), pending[0].Seq)
	}
	b.Acked(3)
	if got := b.Len(); got != 2 {
		t.Fatalf("len after ack = %d, want 2", got)
	}
	if pending = b.After(3); len(pending) != 2 || pending[0].Seq != 4 {
		t.Fatalf("After(3) returned %d events", len(pending))
	}
	next := b.Add(&pb.TraceEvent{})
	if next != 6 {
		t.Fatalf("next seq = %d, want 6", next)
	}
}

func TestBufferDropsOldestOnOverflow(t *testing.T) {
	b := newEventBuffer(3)
	for i := range 5 {
		b.Add(&pb.TraceEvent{EventId: string(rune('a' + i))})
	}
	if b.Len() != 3 {
		t.Fatalf("len = %d, want 3", b.Len())
	}
	pending := b.After(0)
	if pending[0].EventId != "c" {
		t.Fatalf("oldest surviving event = %q, want c", pending[0].EventId)
	}
	if b.base != 3 {
		t.Fatalf("base = %d, want 3", b.base)
	}
}
