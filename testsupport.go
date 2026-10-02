package dataflow

// Test-support surface: mirrors of the SDK's own test seams for integration
// packages (contrib/*) and host test suites that need to assert on emitted
// spans and log lines without running an ingestion server. Nothing here
// starts network traffic: both captures swap the delivery paths for
// in-memory buffers the test can drain. Restore puts the previous state
// back (pair it with t.Cleanup).

import (
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// EventCapture records the spans ended while it is installed, backed by an
// in-memory replay buffer instead of the gRPC stream.
type EventCapture struct {
	prev *pipeline
	p    *pipeline
}

// CaptureEvents replaces the event delivery path with an in-memory buffer.
// Requires the SDK to be active (call Configure with an empty Endpoint
// first to stay offline) — with it disabled, spans are never enqueued.
func CaptureEvents() *EventCapture {
	c := &EventCapture{prev: globalPipeline.Load()}
	c.p = &pipeline{buf: newEventBuffer(1000), cfg: current()}
	globalPipeline.Store(c.p)
	return c
}

// Events drains and returns the captured events, oldest first.
func (c *EventCapture) Events() []*pb.TraceEvent { return c.p.buf.After(0) }

// Restore removes the capture; spans end nowhere until a new capture or a
// real pipeline is installed.
func (c *EventCapture) Restore() { globalPipeline.Store(c.prev) }

// LogCapture buffers application log lines the way the log pipeline does,
// without starting the background flusher (nothing is POSTed).
type LogCapture struct {
	prev *logPipeline
	p    *logPipeline
}

// CaptureLogs replaces the log pipeline with an in-memory buffer.
func CaptureLogs() *LogCapture {
	c := &LogCapture{prev: globalLogs.Load()}
	c.p = newLogPipeline("", "test-key", false)
	globalLogs.Store(c.p)
	return c
}

// Records drains and returns the buffered log lines, oldest first.
func (c *LogCapture) Records() []LogRecord {
	c.p.mu.Lock()
	defer c.p.mu.Unlock()
	out := make([]LogRecord, len(c.p.pending))
	copy(out, c.p.pending)
	return out
}

// Restore removes the capture.
func (c *LogCapture) Restore() { globalLogs.Store(c.prev) }

// SetEnabled flips the SDK's active flag (what Enabled reports) and returns
// the restore function. Test-support for integrations that must behave
// correctly with tracing on and off.
func SetEnabled(on bool) (restore func()) {
	prev := active.Load()
	active.Store(on)
	return func() { active.Store(prev) }
}
