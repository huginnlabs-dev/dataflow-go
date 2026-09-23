package dataflow

import (
	"sync"

	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// eventBuffer is a bounded FIFO of events awaiting acknowledgement. It backs
// the replay protocol: the sender retransmits everything above the last
// acked sequence number after a reconnect, and the oldest events are dropped
// once the buffer overflows (configurable policy per design).
type eventBuffer struct {
	mu       sync.Mutex
	events   []*pb.TraceEvent // sorted by seq, contiguous
	base     int64            // seq of events[0]
	capacity int
}

func newEventBuffer(capacity int) *eventBuffer {
	if capacity <= 0 {
		capacity = 10000
	}
	return &eventBuffer{capacity: capacity, base: 1}
}

// Add appends an event, dropping the oldest when full. The event is stamped
// with the next sequence number.
func (b *eventBuffer) Add(ev *pb.TraceEvent) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	seq := b.base + int64(len(b.events))
	ev.Seq = seq
	b.events = append(b.events, ev)
	if len(b.events) > b.capacity {
		b.events = b.events[1:]
		b.base++
	}
	return seq
}

// After returns the events with seq > acked, copying the slice header so the
// sender never holds the lock while writing to the wire.
func (b *eventBuffer) After(acked int64) []*pb.TraceEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	// acked is the last durably accepted seq on the server side.
	offset := max(acked+1-b.base, 0)
	if offset > int64(len(b.events)) {
		return nil
	}
	return b.events[offset:]
}

// Acked trims everything up to and including acked once the server confirmed
// durability.
func (b *eventBuffer) Acked(acked int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	drop := min(max(acked+1-b.base, 0), int64(len(b.events)))
	b.events = b.events[drop:]
	b.base += drop
}

// Len reports the number of buffered events.
func (b *eventBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}
