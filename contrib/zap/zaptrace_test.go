package zaptrace

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

func TestMain(m *testing.M) {
	// Offline, active SDK: empty Endpoint keeps the gRPC stream down; the
	// tests install in-memory log captures per test.
	dataflow.Configure(dataflow.Config{APIKey: "test-key", ServiceName: "zaptrace-test"})
	m.Run()
}

// Core() alone forwards entries and fields to the Dataflow pipeline with
// the documented level mapping.
func TestCoreShipsEntries(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	logger := zap.New(Core())
	logger.Debug("cache warm", zap.String("area", "db"))
	logger.Info("order placed", zap.Int("order_id", 42), zap.Strings("tags", []string{"a", "b"}))
	logger.Warn("token expired")
	logger.Error("payment declined", zap.Error(errors.New("card declined")))
	// DPanic/Fatal map to error too, but they terminate/panic — exercised
	// via the level switch, not the real methods.

	records := capture.Records()
	if len(records) != 4 {
		t.Fatalf("buffered %d records, want 4", len(records))
	}
	wantLevels := []string{"debug", "info", "warn", "error"}
	wantMessages := []string{"cache warm", "order placed", "token expired", "payment declined"}
	for i, rec := range records {
		if rec.Level != wantLevels[i] {
			t.Errorf("record %d level = %q, want %q", i, rec.Level, wantLevels[i])
		}
		if rec.Message != wantMessages[i] {
			t.Errorf("record %d message = %q, want %q", i, rec.Message, wantMessages[i])
		}
	}
	if records[0].Fields["area"] != "db" {
		t.Errorf("debug fields = %v, want area=db", records[0].Fields)
	}
	if records[1].Fields["order_id"] != "42" {
		t.Errorf("info fields = %v, want order_id=42 (stringified)", records[1].Fields)
	}
	if records[1].Fields["tags"] != "[a b]" {
		t.Errorf("info fields = %v, want tags=[a b]", records[1].Fields)
	}
	if records[3].Fields["error"] != "card declined" {
		t.Errorf("error fields = %v, want error=card declined", records[3].Fields)
	}
}

// With accumulates fields onto every record.
func TestWithAccumulatesFields(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	logger := zap.New(Core()).With(zap.String("component", "auth"))
	logger.Info("signed in", zap.String("user_id", "u42"))

	records := capture.Records()
	if len(records) != 1 {
		t.Fatalf("buffered %d records, want 1", len(records))
	}
	rec := records[0]
	if rec.Fields["component"] != "auth" || rec.Fields["user_id"] != "u42" {
		t.Errorf("fields = %v, want component=auth user_id=u42", rec.Fields)
	}
}

// Records inside a Trace carry the active trace/span ids.
func TestCoreCorrelatesInsideTrace(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	// TraceVoid registers the ambient span for the callback goroutine, so
	// the log written inside it is stamped with the trace ids.
	if err := dataflow.TraceVoid(context.Background(), "zap.Traced", func(ctx context.Context) error {
		zap.New(Core()).Info("processing")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	records := capture.Records()
	if len(records) != 1 {
		t.Fatalf("buffered %d records, want 1", len(records))
	}
	if records[0].TraceID == "" || records[0].SpanID == "" {
		t.Errorf("ids = %q/%q, want the active trace ids", records[0].TraceID, records[0].SpanID)
	}
}

// Wrap tees entries into Dataflow and the wrapped core.
func TestWrapTees(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	var buf bytes.Buffer
	sink := zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg", LevelKey: "lvl"}),
		zapcore.AddSync(&buf),
		zapcore.DebugLevel,
	)
	logger := zap.New(Wrap(sink))
	logger.Warn("both places", zap.String("k", "v"))

	records := capture.Records()
	if len(records) != 1 {
		t.Fatalf("buffered %d records, want 1", len(records))
	}
	if records[0].Level != "warn" || records[0].Fields["k"] != "v" {
		t.Errorf("dataflow copy = %q/%v", records[0].Level, records[0].Fields)
	}
	if out := buf.String(); !strings.Contains(out, `"both places"`) {
		t.Errorf("wrapped core output = %q, want the message", out)
	}
}

// Outside any trace the ids stay empty; without a pipeline nothing panics.
func TestCoreNoTraceAndDisabled(t *testing.T) {
	capture := dataflow.CaptureLogs()
	zap.New(Core()).Info("correlated")
	records := capture.Records()
	if len(records) != 1 || records[0].TraceID != "" || records[0].SpanID != "" {
		t.Fatalf("records = %v, want one with empty ids", records)
	}
	capture.Restore()

	// No pipeline installed: the core must swallow the entry silently.
	zap.New(Core()).Info("nowhere to go")
	zap.New(Wrap(zap.NewNop().Core())).Error("still nowhere")
}
