package dataflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// useTestLogs swaps in a log pipeline pointed at base ("" for buffer-only
// tests) without starting the background flush loop; tests trigger flushes
// explicitly via FlushLogs.
func useTestLogs(t *testing.T, base string) *logPipeline {
	t.Helper()
	p := newLogPipeline(base, "test-key", false)
	globalLogs.Store(p)
	t.Cleanup(func() { globalLogs.Store(nil) })
	return p
}

// pendingLogs drains the test pipeline's buffered log lines.
func pendingLogs(p *logPipeline) []LogRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]LogRecord, len(p.pending))
	copy(out, p.pending)
	return out
}

type logServer struct {
	srv     *httptest.Server
	mu      sync.Mutex
	batches [][]LogRecord
	paths   []string
	keys    []string
	codes   []int
}

// newLogServer records every POST /api/v1/logs batch. When failFirst is
// positive, that many requests are answered 500 before turning healthy.
func newLogServer(t *testing.T, failFirst int) *logServer {
	t.Helper()
	ls := &logServer{}
	ls.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ls.mu.Lock()
		defer ls.mu.Unlock()
		ls.paths = append(ls.paths, r.URL.Path)
		ls.keys = append(ls.keys, r.Header.Get("X-Api-Key"))
		if n := len(ls.codes); n < failFirst {
			ls.codes = append(ls.codes, http.StatusInternalServerError)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ls.codes = append(ls.codes, http.StatusOK)
		var b logBatch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Errorf("bad log batch JSON: %v", err)
		}
		ls.batches = append(ls.batches, b.Logs)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ls.srv.Close)
	return ls
}

func (ls *logServer) total() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	n := 0
	for _, b := range ls.batches {
		n += len(b)
	}
	return n
}

// snapshot copies the recorded requests/batches under one lock so callers
// never hold ls.mu while inspecting (sync.Mutex is not reentrant).
func (ls *logServer) snapshot() (paths, keys []string, batches [][]LogRecord) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	paths = append(paths, ls.paths...)
	keys = append(keys, ls.keys...)
	batches = append(batches, ls.batches...)
	return paths, keys, batches
}

func TestLogHelpersAttachCurrentTraceIDs(t *testing.T) {
	setEnabled(t, true)
	p := useTestLogs(t, "")

	// Exact match via the ambient span seam.
	span := StartSpan(context.Background(), "log.Span")
	pushCurrentSpan(span)
	Info("with span", map[string]any{"k": "v"})
	popCurrentSpan()
	span.mu.Lock()
	traceID, spanID := span.ev.TraceId, span.ev.SpanId
	span.mu.Unlock()

	// The Trace helpers must register the ambient span for the callback's
	// goroutine on their own.
	if err := TraceVoid(context.Background(), "log.Traced", func(ctx context.Context) error {
		Warn("traced")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Outside any trace the ids stay empty.
	Info("bare")

	logs := pendingLogs(p)
	if len(logs) != 3 {
		t.Fatalf("buffered %d records, want 3", len(logs))
	}
	if logs[0].TraceID != traceID || logs[0].SpanID != spanID {
		t.Errorf("ids = %q/%q, want span's %q/%q", logs[0].TraceID, logs[0].SpanID, traceID, spanID)
	}
	if logs[1].TraceID == "" || logs[1].SpanID == "" {
		t.Error("TraceVoid callback record must carry the active trace ids")
	}
	if logs[2].TraceID != "" || logs[2].SpanID != "" {
		t.Errorf("record without an active trace has ids %q/%q, want empty", logs[2].TraceID, logs[2].SpanID)
	}
	if logs[0].Fields["k"] != "v" {
		t.Errorf("fields = %v, want k=v", logs[0].Fields)
	}
}

func TestLogsPostBatchToServer(t *testing.T) {
	setEnabled(t, true)
	ls := newLogServer(t, 0)
	p := useTestLogs(t, ls.srv.URL)

	svc := current().cfg.ServiceName
	// One record inside a trace (trace correlation over the wire) plus
	// enough plain lines to force batching past the 1000-line server cap.
	_, err := Trace(context.Background(), "log.Serve", func(ctx context.Context) (int, error) {
		Info("order placed", map[string]any{"order_id": 42})
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1004; i++ {
		Info(fmt.Sprintf("line %d", i))
	}
	FlushLogs()

	paths, keys, batches := ls.snapshot()
	if len(paths) == 0 {
		t.Fatal("no POST reached the server")
	}
	for i, path := range paths {
		if path != "/api/v1/logs" {
			t.Errorf("request %d path = %q, want /api/v1/logs", i, path)
		}
		if keys[i] != "test-key" {
			t.Errorf("request %d api key = %q, want test-key", i, keys[i])
		}
	}
	total := 0
	for i, b := range batches {
		total += len(b)
		if len(b) > logMaxBatch {
			t.Errorf("batch %d has %d records, cap is %d", i, len(b), logMaxBatch)
		}
	}
	if total != 1005 {
		t.Errorf("server received %d records, want 1005", total)
	}
	// Wire shape of the correlated record.
	rec := batches[0][0]
	if rec.Level != "info" {
		t.Errorf("level = %q, want info", rec.Level)
	}
	if rec.Message != "order placed" {
		t.Errorf("message = %q, want %q", rec.Message, "order placed")
	}
	if rec.TraceID == "" || rec.SpanID == "" {
		t.Error("posted record must carry trace/span ids inside a Trace")
	}
	if rec.ServiceName != svc {
		t.Errorf("service_name = %q, want %q", rec.ServiceName, svc)
	}
	if rec.Timestamp <= 0 {
		t.Errorf("timestamp = %d, want unix ms", rec.Timestamp)
	}
	if rec.Fields["order_id"] != "42" {
		t.Errorf("fields = %v, want order_id=42", rec.Fields)
	}
	// The pipeline buffer must be empty after the flush.
	if n := len(pendingLogs(p)); n != 0 {
		t.Errorf("%d records left buffered after FlushLogs", n)
	}
}

func TestLogBufferOverflowDropsOldest(t *testing.T) {
	setEnabled(t, true)
	p := useTestLogs(t, "") // no server, nothing flushes

	for i := 0; i < logBufferSize+50; i++ {
		Info(fmt.Sprintf("m%d", i))
	}
	if got := p.dropped.Load(); got != 50 {
		t.Errorf("dropped = %d, want 50", got)
	}
	logs := pendingLogs(p)
	if len(logs) != logBufferSize {
		t.Fatalf("buffered %d records, want %d", len(logs), logBufferSize)
	}
	// The oldest lines made room for the newest.
	if logs[0].Message != "m50" {
		t.Errorf("oldest survivor = %q, want m50", logs[0].Message)
	}
	if last := logs[len(logs)-1].Message; last != fmt.Sprintf("m%d", logBufferSize+49) {
		t.Errorf("newest = %q, want m%d", last, logBufferSize+49)
	}
}

func TestLogLevelNormalization(t *testing.T) {
	setEnabled(t, true)
	p := useTestLogs(t, "")

	Debug("d")
	Info("i")
	Warn("w")
	Error("e")
	Logf("WARN", "upper")
	Logf("  info ", "spaced")
	Logf("warning", "alias")
	Logf("banana", "unknown")

	want := []string{"debug", "info", "warn", "error", "warn", "info", "warn", "info"}
	logs := pendingLogs(p)
	if len(logs) != len(want) {
		t.Fatalf("buffered %d records, want %d", len(logs), len(want))
	}
	for i, level := range want {
		if logs[i].Level != level {
			t.Errorf("record %d level = %q, want %q", i, logs[i].Level, level)
		}
	}
}

func TestSlogHandlerEmitsFields(t *testing.T) {
	setEnabled(t, true)
	p := useTestLogs(t, "")

	logger := slog.New(NewSlogHandler().WithAttrs([]slog.Attr{slog.String("component", "auth")}))
	logger.Warn("token expired", slog.Int("user_id", 42))
	logger.WithGroup("http").Error("upstream failed", slog.String("status", "503"))

	logs := pendingLogs(p)
	if len(logs) != 2 {
		t.Fatalf("buffered %d records, want 2", len(logs))
	}
	rec := logs[0]
	if rec.Level != "warn" || rec.Message != "token expired" {
		t.Errorf("level/message = %q/%q", rec.Level, rec.Message)
	}
	if rec.Fields["component"] != "auth" {
		t.Errorf("With attr missing: %v", rec.Fields)
	}
	if rec.Fields["user_id"] != "42" {
		t.Errorf("record attr not stringified: %v", rec.Fields)
	}
	if rec.TraceID != "" {
		t.Errorf("trace_id = %q with no active trace, want empty", rec.TraceID)
	}
	rec = logs[1]
	if rec.Level != "error" {
		t.Errorf("level = %q, want error", rec.Level)
	}
	if rec.Fields["http.status"] != "503" {
		t.Errorf("group prefix not applied: %v", rec.Fields)
	}
}

func TestSlogHandlerCorrelatesInsideTrace(t *testing.T) {
	setEnabled(t, true)
	p := useTestLogs(t, "")

	span := StartSpan(context.Background(), "slog.Span")
	pushCurrentSpan(span)
	logger := slog.New(NewSlogHandler())
	logger.Info("processing")
	popCurrentSpan()

	logs := pendingLogs(p)
	if len(logs) != 1 {
		t.Fatalf("buffered %d records, want 1", len(logs))
	}
	span.mu.Lock()
	traceID, spanID := span.ev.TraceId, span.ev.SpanId
	span.mu.Unlock()
	if logs[0].TraceID != traceID || logs[0].SpanID != spanID {
		t.Errorf("ids = %q/%q, want %q/%q", logs[0].TraceID, logs[0].SpanID, traceID, spanID)
	}
}

func TestLogsDisabledAreNoOps(t *testing.T) {
	// No pipeline installed (nothing configured / DATAFLOW_DISABLED): the
	// helpers, FlushLogs and the slog handler must not panic or buffer.
	prev := globalLogs.Load()
	globalLogs.Store(nil)
	t.Cleanup(func() { globalLogs.Store(prev) })

	Debug("d")
	Info("i", map[string]any{"k": "v"})
	Warn("w")
	Error("e")
	Logf("info", "f: %d", 1)
	FlushLogs()
	slog.New(NewSlogHandler()).Info("via slog")
	if err := NewSlogHandler().Handle(context.Background(),
		slog.NewRecord(time.Now(), slog.LevelInfo, "direct", 0)); err != nil {
		t.Errorf("Handle = %v, want nil", err)
	}
}

func TestLogsRetryOnceThenDeliverOrDrop(t *testing.T) {
	setEnabled(t, true)

	// First POST fails (500), the retry delivers.
	ls := newLogServer(t, 1)
	useTestLogs(t, ls.srv.URL)
	Info("survivor")
	FlushLogs()
	if got := ls.total(); got != 1 {
		t.Errorf("server received %d records after retry, want 1", got)
	}

	// Server always failing: the batch is dropped and counted.
	dead := newLogServer(t, 99)
	p := useTestLogs(t, dead.srv.URL)
	Warn("doomed")
	FlushLogs()
	if n := len(pendingLogs(p)); n != 0 {
		t.Errorf("%d records left buffered, want the failed batch dropped", n)
	}
	if got := p.dropped.Load(); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
}

func TestStringifyFieldsClamps(t *testing.T) {
	fields := map[string]any{}
	for i := 0; i < maxLogFields+10; i++ {
		fields[fmt.Sprintf("k%d", i)] = i
	}
	out := stringifyFields([]map[string]any{fields})
	if len(out) != maxLogFields {
		t.Errorf("fields = %d, want clamp at %d", len(out), maxLogFields)
	}
	long := stringifyFields([]map[string]any{{"big": strings.Repeat("x", 1000)}})
	if got := long["big"]; len(got) != maxLogFieldValue {
		t.Errorf("value length = %d, want clamp at %d", len(got), maxLogFieldValue)
	}
}
