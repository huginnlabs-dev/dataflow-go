package redistrace

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

func TestMain(m *testing.M) {
	// Offline, active SDK: empty Endpoint keeps the gRPC stream down; the
	// tests install in-memory captures per test.
	dataflow.Configure(dataflow.Config{APIKey: "test-key", ServiceName: "redistrace-test"})
	m.Run()
}

func newClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := Wrap(redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	// Prime the connection pool before any capture is installed: the first
	// command dials a connection and emits a HELLO span that would
	// otherwise pollute the per-test span counts.
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return mr, client
}

// Every command emits a DB_QUERY span named after the command verb with the
// rendered command as the statement.
func TestCommandSpans(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	ctx := context.Background()
	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := client.Get(ctx, "greeting").Err(); err != nil {
		t.Fatalf("get: %v", err)
	}

	events := capture.Events()
	if len(events) != 2 {
		t.Fatalf("captured %d events, want 2", len(events))
	}

	set := events[0]
	if set.Type != pb.EventType_EVENT_TYPE_DB_QUERY {
		t.Errorf("set type = %v, want DB_QUERY", set.Type)
	}
	if set.Name != "SET" {
		t.Errorf("set name = %q, want SET", set.Name)
	}
	if got := set.Metadata["db.system"]; got != "redis" {
		t.Errorf("db.system = %q, want redis", got)
	}
	if stmt := set.Metadata["db.statement"]; stmt != "set greeting hello" {
		t.Errorf("db.statement = %q, want %q", stmt, "set greeting hello")
	}
	if set.StatusCode != 200 || set.ErrorMessage != "" {
		t.Errorf("set status/error = %d/%q, want 200/\"\"", set.StatusCode, set.ErrorMessage)
	}

	get := events[1]
	if get.Name != "GET" {
		t.Errorf("get name = %q, want GET", get.Name)
	}
	if stmt := get.Metadata["db.statement"]; stmt != "get greeting" {
		t.Errorf("get db.statement = %q, want %q", stmt, "get greeting")
	}
	if get.StatusCode != 200 {
		t.Errorf("get status = %d, want 200", get.StatusCode)
	}
}

// A failed command records the error and status 500.
func TestErrorSpan(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	if err := client.Get(context.Background(), "missing").Err(); err == nil {
		t.Fatal("expected redis.Nil for a missing key")
	}

	events := capture.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.StatusCode != 500 {
		t.Errorf("status = %d, want 500", ev.StatusCode)
	}
	if ev.ErrorMessage == "" {
		t.Error("error message must be recorded on the span")
	}
	// The statement captured before execution carries the command, not the
	// error text.
	if stmt := ev.Metadata["db.statement"]; stmt != "get missing" {
		t.Errorf("db.statement = %q, want %q", stmt, "get missing")
	}
}

// Pipelines are one span named after the first command with the batch in
// the statement.
func TestPipelineSpan(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	pipe := client.Pipeline()
	pipe.Set(context.Background(), "a", "1", 0)
	pipe.Get(context.Background(), "a")
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}

	events := capture.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1 (the pipeline is one span)", len(events))
	}
	ev := events[0]
	if ev.Name != "PIPELINE SET" {
		t.Errorf("pipeline name = %q, want %q", ev.Name, "PIPELINE SET")
	}
	if stmt := ev.Metadata["db.statement"]; stmt != "set a 1; get a" {
		t.Errorf("pipeline db.statement = %q, want %q", stmt, "set a 1; get a")
	}
	if ev.StatusCode != 200 {
		t.Errorf("status = %d, want 200", ev.StatusCode)
	}
}

// Commands issued with a traced context join the trace.
func TestSpansJoinTrace(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	ctx := context.Background()
	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := dataflow.Trace(context.Background(), "cache.Warm", func(ctx context.Context) (string, error) {
		return client.Get(ctx, "greeting").Result()
	}); err != nil {
		t.Fatal(err)
	}

	events := capture.Events()
	if len(events) != 3 {
		t.Fatalf("captured %d events, want 3 (set span + redis get span + root span)", len(events))
	}
	child, root := events[1], events[2]
	if root.Name != "cache.Warm" || child.Name != "GET" {
		t.Fatalf("spans = %q/%q, want GET under cache.Warm", child.Name, root.Name)
	}
	if child.TraceId != root.TraceId || child.ParentSpanId != root.SpanId {
		t.Errorf("child %v/%v not a child of root %v/%v",
			child.TraceId, child.ParentSpanId, root.TraceId, root.SpanId)
	}
}

// Long statements clip to 200 characters.
func TestStatementClips(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	big := make([]byte, 600)
	for i := range big {
		big[i] = 'x'
	}
	if err := client.Set(context.Background(), "big", big, 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}

	events := capture.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1", len(events))
	}
	if stmt := events[0].Metadata["db.statement"]; len(stmt) != 200 {
		t.Errorf("db.statement length = %d, want 200", len(stmt))
	}
}

// With the SDK disabled nothing is emitted and commands still work.
func TestDisabledNoSpans(t *testing.T) {
	_, client := newClient(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)
	restore := dataflow.SetEnabled(false)
	t.Cleanup(restore)

	if err := client.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("set while disabled: %v", err)
	}
	if events := capture.Events(); len(events) != 0 {
		t.Errorf("captured %d events while disabled, want 0", len(events))
	}
}
