package dataflow

// Application log shipping with trace correlation. The Debug/Info/Warn/Error
// helpers and the slog handler buffer log lines in-process and POST them to
// the server's REST log endpoint in batches; every line is stamped with the
// current span's trace/span ids (when one is active on the calling
// goroutine) so logs line up with traces in the dashboard's Logs tab.
//
// Logging is strictly best-effort: it never blocks, never panics, and drops
// the oldest lines on overflow. It uses the manifest's HTTP base resolution
// — a bare host:port DATAFLOW_ENDPOINT with no DATAFLOW_HTTP_URL override
// has no derivable HTTP base and logging stays silently off.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// logBufferSize bounds the in-memory log lines; the oldest are dropped
	// once it overflows (same policy as the event replay buffer).
	logBufferSize = 1024
	// logFlushLines triggers a flush once this many lines are buffered;
	// logFlushInterval flushes them regardless.
	logFlushLines    = 50
	logFlushInterval = 500 * time.Millisecond
	// logMaxBatch caps one POST body; the server accepts up to 1000 lines.
	logMaxBatch = 1000
	// logHTTPTimeout bounds each POST attempt.
	logHTTPTimeout = 5 * time.Second
	// Client-side clamps mirroring the server's limits.
	maxLogMessageBytes = 8192
	maxLogFields       = 50
	maxLogFieldValue   = 512
)

// logRecord is one shipped application log line (wire shape of
// POST /api/v1/logs entries).
type logRecord struct {
	Timestamp   int64             `json:"timestamp"`
	Level       string            `json:"level"`
	Message     string            `json:"message"`
	TraceID     string            `json:"trace_id"`
	SpanID      string            `json:"span_id"`
	ServiceName string            `json:"service_name"`
	Fields      map[string]string `json:"fields"`
}

type logBatch struct {
	Logs []logRecord `json:"logs"`
}

// logPipeline buffers log lines and flushes them to the REST endpoint from
// a background goroutine, independently of the gRPC trace stream.
type logPipeline struct {
	mu      sync.Mutex
	pending []logRecord
	dropped atomic.Int64

	signal chan struct{}
	stop   chan struct{}
	done   chan struct{}

	base     string
	apiKey   string
	insecure bool
	client   *http.Client
}

var globalLogs atomic.Pointer[logPipeline]

// startLogs turns on log shipping when an HTTP base and API key exist. A
// bare host:port gRPC endpoint with no DATAFLOW_HTTP_URL override resolves
// no HTTP base — logging stays silently off, like the manifest report.
func startLogs(s *settings) {
	base := httpBaseURL(s.endpoint, s.useTLS)
	if base == "" || s.cfg.APIKey == "" {
		return
	}
	p := newLogPipeline(base, s.cfg.APIKey, s.cfg.Insecure)
	globalLogs.Store(p)
	go p.run()
}

func newLogPipeline(base, apiKey string, insecure bool) *logPipeline {
	p := &logPipeline{
		pending:  make([]logRecord, 0, logBufferSize),
		signal:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		base:     base,
		apiKey:   apiKey,
		insecure: insecure,
		client:   &http.Client{Timeout: logHTTPTimeout},
	}
	if insecure {
		p.client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // explicit dev opt-in
		}
	}
	return p
}

// run flushes on the ticker or the line-count signal until stopped, then
// makes one best-effort final flush.
func (p *logPipeline) run() {
	defer close(p.done)
	t := time.NewTicker(logFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			p.flushNow()
			return
		case <-t.C:
			p.flushNow()
		case <-p.signal:
			p.flushNow()
		}
	}
}

// logLine buffers one record; a no-op when log shipping is off. Never
// blocks and never panics: on overflow the oldest line is dropped and
// counted.
func logLine(rec logRecord) {
	p := globalLogs.Load()
	if p == nil {
		return
	}
	defer func() { _ = recover() }() // logging must never take the process down
	p.mu.Lock()
	if len(p.pending) >= logBufferSize {
		copy(p.pending, p.pending[1:])
		p.pending[len(p.pending)-1] = rec
		p.dropped.Add(1)
	} else {
		p.pending = append(p.pending, rec)
	}
	full := len(p.pending) >= logFlushLines
	p.mu.Unlock()
	if full {
		select {
		case p.signal <- struct{}{}:
		default:
		}
	}
}

// take swaps out the buffered lines under the lock.
func (p *logPipeline) take() []logRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	recs := p.pending
	p.pending = make([]logRecord, 0, logBufferSize)
	return recs
}

// flushNow drains the buffer and POSTs it in batches of ≤ logMaxBatch. A
// failed batch is retried once, then dropped and counted.
func (p *logPipeline) flushNow() {
	defer func() { _ = recover() }()
	for {
		recs := p.take()
		if len(recs) == 0 {
			return
		}
		for len(recs) > 0 {
			n := min(len(recs), logMaxBatch)
			if err := p.post(recs[:n]); err != nil {
				p.dropped.Add(int64(n))
			}
			recs = recs[n:]
		}
	}
}

// post sends one batch; one retry on failure, then the batch is dropped.
func (p *logPipeline) post(recs []logRecord) error {
	body, err := json.Marshal(logBatch{Logs: recs})
	if err != nil {
		return err // unreachable for string-only records
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequest(http.MethodPost, p.base+"/api/v1/logs", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Api-Key", p.apiKey)
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("log ingest returned %s", resp.Status)
	}
	return lastErr
}

// Debug ships an application log line at debug level. Fields are
// stringified; the record is stamped with the current span's trace/span ids
// when a trace is active on the calling goroutine. A no-op when log
// shipping is disabled.
func Debug(msg string, fields ...map[string]any) { logAt("debug", msg, fields) }

// Info is the info-level flavour of Debug.
func Info(msg string, fields ...map[string]any) { logAt("info", msg, fields) }

// Warn is the warn-level flavour of Debug.
func Warn(msg string, fields ...map[string]any) { logAt("warn", msg, fields) }

// Error is the error-level flavour of Debug.
func Error(msg string, fields ...map[string]any) { logAt("error", msg, fields) }

// Logf ships a printf-formatted log line at the given level ("debug",
// "info", "warn", "error" — case-insensitive; unknown levels log as info).
func Logf(level string, format string, args ...any) {
	logAt(level, fmt.Sprintf(format, args...), nil)
}

// FlushLogs synchronously ships any buffered log lines (one HTTP round trip
// with a single retry per batch). It is a no-op when log shipping is
// disabled; explicit shutdown paths can call it before exit.
func FlushLogs() {
	if p := globalLogs.Load(); p != nil {
		p.flushNow()
	}
}

// logAt builds a record and buffers it. Disabled (no pipeline) → no-op.
func logAt(level, msg string, fields []map[string]any) {
	p := globalLogs.Load()
	if p == nil {
		return
	}
	logLine(buildRecord(level, msg, fields))
}

// buildRecord stamps a log line with time, service and — when a trace is
// active on the calling goroutine — the current span's ids.
func buildRecord(level, msg string, fields []map[string]any) logRecord {
	rec := logRecord{
		Timestamp:   time.Now().UnixMilli(),
		Level:       normalizeLevel(level),
		Message:     truncateString(msg, maxLogMessageBytes),
		ServiceName: current().cfg.ServiceName,
		Fields:      stringifyFields(fields),
	}
	if s := currentSpan(); s != nil {
		rec.TraceID, rec.SpanID = s.ids()
	}
	return rec
}

// normalizeLevel maps input to the wire vocabulary debug|info|warn|error.
// Unknown levels degrade to info rather than being dropped.
func normalizeLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return "debug"
	case "warn", "warning":
		return "warn"
	case "error":
		return "error"
	default:
		return "info"
	}
}

// stringifyFields merges the field maps, stringifying each value and
// clipping to the server's 50×512 limits.
func stringifyFields(fields []map[string]any) map[string]string {
	out := make(map[string]string)
	for _, m := range fields {
		for k, v := range m {
			if k == "" {
				continue
			}
			if len(out) >= maxLogFields {
				return out
			}
			out[k] = truncateString(fmt.Sprint(v), maxLogFieldValue)
		}
	}
	return out
}

// ids returns the span's trace and span identifiers.
func (s *Span) ids() (traceID, spanID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ev.TraceId, s.ev.SpanId
}

// The ambient current-span registry: a per-goroutine stack of active spans.
// Trace and the HTTP middlewares push the span for the duration of the user
// code they wrap, so ctx-free helpers (the log shippers) can correlate with
// the trace active on the calling goroutine. Go goroutine ids are unique
// and never reused, so the map needs no cleanup beyond pop.

var (
	ambientMu    sync.Mutex
	ambientSpans = map[uint64][]*Span{}
)

// pushCurrentSpan marks s as the innermost active span on the calling
// goroutine. Must be paired with popCurrentSpan on the same goroutine.
func pushCurrentSpan(s *Span) {
	id := goroutineID()
	ambientMu.Lock()
	ambientSpans[id] = append(ambientSpans[id], s)
	ambientMu.Unlock()
}

// popCurrentSpan removes the innermost active span from the calling
// goroutine's stack.
func popCurrentSpan() {
	id := goroutineID()
	ambientMu.Lock()
	stack := ambientSpans[id]
	if n := len(stack); n > 0 {
		stack = stack[:n-1]
	}
	if len(stack) == 0 {
		delete(ambientSpans, id)
	} else {
		ambientSpans[id] = stack
	}
	ambientMu.Unlock()
}

// currentSpan returns the innermost span active on the calling goroutine,
// or nil when no trace is active.
func currentSpan() *Span {
	id := goroutineID()
	ambientMu.Lock()
	stack := ambientSpans[id]
	var s *Span
	if n := len(stack); n > 0 {
		s = stack[n-1]
	}
	ambientMu.Unlock()
	return s
}

// goroutineID parses the current goroutine's id from its stack header
// ("goroutine 123 [running]:"). Best effort; 0 on the impossible parse
// failure.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	head := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	if i := strings.IndexByte(head, ' '); i > 0 {
		if id, err := strconv.ParseUint(head[:i], 10, 64); err == nil {
			return id
		}
	}
	return 0
}

// NewSlogHandler returns an slog.Handler that ships log records through the
// Dataflow pipeline with trace correlation:
//
//	logger := slog.New(dataflow.NewSlogHandler())
//	slog.SetDefault(logger)
//
// Levels map onto the wire vocabulary (DEBUG→debug, INFO→info,
// WARN→warn, ERROR→error) and attributes become stringified fields.
// With/WithGroup state is kept simple: attributes are stored as given and
// group names are joined into the field keys ("group.key"). When log
// shipping is disabled the handler accepts records and drops them.
func NewSlogHandler() slog.Handler { return &slogHandler{} }

type slogHandler struct {
	attrs  []slog.Attr // preformatted attrs accumulated via WithAttrs
	groups []string    // group name stack from WithGroup
}

func (h *slogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *slogHandler) Handle(_ context.Context, r slog.Record) error {
	defer func() { _ = recover() }() // a bad attr must never crash the app
	fields := make(map[string]any, len(h.attrs)+4)
	prefix := strings.Join(h.groups, ".")
	for _, a := range h.attrs {
		if a.Key != "" {
			fields[prefixedKey(prefix, a.Key)] = clipSlogValue(a.Value)
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "" {
			fields[prefixedKey(prefix, a.Key)] = clipSlogValue(a.Value)
		}
		return true
	})
	logAt(slogLevel(r.Level), r.Message, []map[string]any{fields})
	return nil
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	nh := h.clone()
	nh.attrs = append(nh.attrs, attrs...)
	return nh
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := h.clone()
	nh.groups = append(nh.groups, name)
	return nh
}

func (h *slogHandler) clone() *slogHandler {
	nh := &slogHandler{attrs: make([]slog.Attr, len(h.attrs)), groups: make([]string, len(h.groups))}
	copy(nh.attrs, h.attrs)
	copy(nh.groups, h.groups)
	return nh
}

// slogLevel folds a slog.Level onto the four wire levels.
func slogLevel(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "debug"
	case l < slog.LevelWarn:
		return "info"
	case l < slog.LevelError:
		return "warn"
	default:
		return "error"
	}
}

// clipSlogValue renders an attr value as a string within the field clamp.
func clipSlogValue(v slog.Value) string {
	return truncateString(v.String(), maxLogFieldValue)
}

func prefixedKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
