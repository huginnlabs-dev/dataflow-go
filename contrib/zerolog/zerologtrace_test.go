package zerologtrace

import (
	"context"
	"io"
	"testing"

	"github.com/rs/zerolog"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

func TestMain(m *testing.M) {
	// Offline, active SDK: empty Endpoint keeps the gRPC stream down; the
	// tests install in-memory log captures per test.
	dataflow.Configure(dataflow.Config{APIKey: "test-key", ServiceName: "zerologtrace-test"})
	m.Run()
}

func hookedLogger() zerolog.Logger {
	return zerolog.New(io.Discard).Hook(Hook())
}

// Hook() forwards events at every level with the documented mapping.
func TestHookShipsEntries(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	logger := hookedLogger()
	logger.Trace().Msg("seek")
	logger.Debug().Msg("cache warm")
	logger.Info().Msg("order placed")
	logger.Warn().Msg("token expired")
	logger.Error().Msg("payment declined")
	logger.Log().Msg("no level")

	records := capture.Records()
	if len(records) != 6 {
		t.Fatalf("buffered %d records, want 6", len(records))
	}
	wantLevels := []string{"debug", "debug", "info", "warn", "error", "info"}
	wantMessages := []string{"seek", "cache warm", "order placed", "token expired", "payment declined", "no level"}
	for i, rec := range records {
		if rec.Level != wantLevels[i] {
			t.Errorf("record %d level = %q, want %q", i, rec.Level, wantLevels[i])
		}
		if rec.Message != wantMessages[i] {
			t.Errorf("record %d message = %q, want %q", i, rec.Message, wantMessages[i])
		}
	}
}

// Records inside a Trace carry the active trace/span ids.
func TestHookCorrelatesInsideTrace(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	// TraceVoid registers the ambient span for the callback goroutine, so
	// the log written inside it is stamped with the trace ids.
	if err := dataflow.TraceVoid(context.Background(), "zlog.Traced", func(ctx context.Context) error {
		logger := hookedLogger()
		logger.Info().Msg("processing")
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

// Outside any trace the ids stay empty.
func TestHookNoTrace(t *testing.T) {
	capture := dataflow.CaptureLogs()
	t.Cleanup(capture.Restore)

	logger := hookedLogger()
	logger.Info().Msg("bare")
	records := capture.Records()
	if len(records) != 1 || records[0].TraceID != "" || records[0].SpanID != "" {
		t.Fatalf("records = %v, want one with empty ids", records)
	}
}

// Without a pipeline installed (or with the SDK disabled) the hook must
// swallow events silently.
func TestHookDisabledIsNoOp(t *testing.T) {
	capture := dataflow.CaptureLogs()
	capture.Restore() // back to no pipeline

	logger := hookedLogger()
	logger.Info().Msg("nowhere to go")
	Hook().Run(nil, zerolog.ErrorLevel, "nil event")
	logger.Error().Msg("still nowhere")
}
