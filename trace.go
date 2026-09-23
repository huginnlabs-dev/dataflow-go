package dataflow

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

type spanCtxKey struct{}

// Span is one measured unit of work. Spans are created with StartSpan (or
// the Trace helpers) and shipped to the SaaS when End is called. A Span is
// safe for concurrent use.
type Span struct {
	mu      sync.Mutex
	ev      *pb.TraceEvent
	payload map[string]any
	base    context.Context
	start   time.Time
	sampled bool
	ended   atomic.Bool
}

// StartSpan opens a child span named name (e.g. "auth.Login" or an HTTP
// route template). When ctx carries a parent span — because the request is
// already being traced by middleware — the new span joins the same trace.
func StartSpan(ctx context.Context, name string) *Span {
	s := &Span{start: time.Now(), sampled: shouldSample(), base: ctx}
	s.ev = &pb.TraceEvent{
		EventId:     newID(),
		Timestamp:   s.start.UnixMilli(),
		Type:        pb.EventType_EVENT_TYPE_FUNCTION_CALL,
		Name:        name,
		ServiceName: current().cfg.ServiceName,
		TraceId:     newID(),
		SpanId:      newID(),
	}
	if parent, ok := ctx.Value(spanCtxKey{}).(*Span); ok && parent != nil {
		parent.mu.Lock()
		s.ev.TraceId = parent.ev.TraceId
		s.ev.ParentSpanId = parent.ev.SpanId
		parent.mu.Unlock()
	}

	// Package-boundary attribution: the callee is the package calling
	// StartSpan, the caller the nearest foreign frame on the stack.
	if pkg := packageAtCaller(2); pkg != "" {
		s.ev.CalleePackage = pkg
	}
	s.ev.CallerPackage = foreignCallerPackage(s.ev.CalleePackage, 2)
	return s
}

// SpanFromContext returns the span attached to ctx by StartSpan, or nil.
func SpanFromContext(ctx context.Context) *Span {
	if s, ok := ctx.Value(spanCtxKey{}).(*Span); ok {
		return s
	}
	return nil
}

// Context returns a context that links spans created from it as children of
// s. It derives from the context StartSpan received, so cancellation and
// deadlines keep propagating to the instrumented code. The context handed
// to Trace callbacks is already linked.
func (s *Span) Context() context.Context {
	return context.WithValue(s.base, spanCtxKey{}, s)
}

// SetAttr records a plaintext attribute (metrics-grade metadata).
func (s *Span) SetAttr(key, value string) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ev.Metadata == nil {
		s.ev.Metadata = map[string]string{}
	}
	s.ev.Metadata[key] = value
	return s
}

// SetData captures input/output data. Payloads are encrypted client-side
// when an encryption key is configured, otherwise shipped as JSON.
func (s *Span) SetData(key string, value any) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.payload == nil {
		s.payload = map[string]any{}
	}
	s.payload[key] = value
	return s
}

// RecordError attaches an error to the span.
func (s *Span) RecordError(err error) *Span {
	if err == nil {
		return s
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ev.ErrorMessage = err.Error()
	return s
}

// SetStatus records a numeric status (HTTP status code or gRPC code).
func (s *Span) SetStatus(code int32) *Span {
	s.mu.Lock()
	s.ev.StatusCode = code
	s.mu.Unlock()
	return s
}

// End closes the span and enqueues it for delivery. Calling End twice is a
// no-op; unsampled spans are dropped.
func (s *Span) End() {
	if !s.ended.CompareAndSwap(false, true) || !s.sampled {
		return
	}
	s.mu.Lock()
	ev := s.ev
	payload := s.payload
	s.mu.Unlock()

	ev.DurationMs = time.Since(s.start).Milliseconds()
	// Field-name lineage: key names (never values) travel as plaintext
	// metadata even when payload values are encrypted. PII categories are
	// classified client-side the same way.
	if len(payload) > 0 {
		keys := make([]string, 0, len(payload))
		for k := range payload {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if ev.Metadata == nil {
			ev.Metadata = map[string]string{}
		}
		ev.Metadata["data.fields"] = strings.Join(keys, ",")
		if pii := classifyPII(keys); pii != "" {
			ev.Metadata["data.pii"] = pii
		}
	}
	if payload != nil {
		attachPayload(ev, payload)
	}
	enqueue(ev)
}

// Trace runs fn as a child span named name, propagating the trace through
// the context handed to fn. The function name doubles as the callee package
// ("host/path/pkg.Func" -> package "host/path/pkg").
func Trace[T any](ctx context.Context, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	span := StartSpan(ctx, name)
	if pkg := packageOfFuncName(name); pkg != "" {
		span.mu.Lock()
		span.ev.CalleePackage = pkg
		span.ev.CallerPackage = foreignCallerPackage(pkg, 2)
		span.mu.Unlock()
	}
	val, err := fn(span.Context())
	if err != nil {
		span.RecordError(err)
	}
	span.End()
	return val, err
}

// TraceVoid is the void-returning flavour of Trace.
func TraceVoid(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	span := StartSpan(ctx, name)
	if pkg := packageOfFuncName(name); pkg != "" {
		span.mu.Lock()
		span.ev.CalleePackage = pkg
		span.mu.Unlock()
	}
	err := fn(span.Context())
	if err != nil {
		span.RecordError(err)
	}
	span.End()
	return err
}

func shouldSample() bool {
	s := current()
	return s.sampleRatio >= 1 || rand.Float64() < s.sampleRatio
}

func attachPayload(ev *pb.TraceEvent, payload map[string]any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		raw, _ = json.Marshal(map[string]any{"capture_error": "payload marshal failed"})
	}
	set := current()
	if !set.encrypted {
		ev.Payload = &pb.PayloadData{Encrypted: false, Data: raw}
		return
	}
	saltHex := encryptionSalt()
	if saltHex == "" {
		ev.Payload = &pb.PayloadData{Encrypted: false, Data: raw}
		return
	}
	ct, iv, err := encryptPayload(raw)
	if err != nil {
		set.cfg.Logger.Printf("dataflow: payload encryption failed: %v", err)
		ev.Payload = &pb.PayloadData{Encrypted: false, Data: raw}
		return
	}
	ev.Payload = &pb.PayloadData{Encrypted: true, Data: ct, Iv: iv, KeySalt: saltHex}
}

// packageAtCaller returns the import path of the package of the frame at
// runtime.Caller depth skip (0 = the caller of packageAtCaller).
func packageAtCaller(skip int) string {
	pc, _, _, ok := runtime.Caller(skip + 1)
	if !ok {
		return ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return ""
	}
	return packageOfFuncName(fn.Name())
}

// foreignCallerPackage walks the stack from depth skip (0 = the caller of
// foreignCallerPackage) and returns the package of the first frame that is
// neither the SDK, runtime, nor callee — i.e. the code that called into the
// traced function from outside its package.
func foreignCallerPackage(callee string, skip int) string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(skip+2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		pkg := packageOfFuncName(frame.Function)
		if pkg != "" && pkg != callee && !isSDKPackage(pkg) && pkg != "runtime" {
			return pkg
		}
		if !more {
			break
		}
	}
	// Empty on roots: caller==callee self-edges would corrupt the graph.
	return ""
}

func isSDKPackage(pkg string) bool {
	return pkg == "github.com/huginnlabs-dev/dataflow-go" ||
		strings.HasPrefix(pkg, "github.com/huginnlabs-dev/dataflow-go/")
}

// packageOfFuncName derives a package import path from a fully qualified
// function name like "host/path/to/pkg.Receiver.Method".
func packageOfFuncName(full string) string {
	if full == "" {
		return ""
	}
	rest := full
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		rest = full[i+1:]
	}
	dot := strings.IndexByte(rest, '.')
	if dot < 0 {
		return "" // bare name without a package qualifier
	}
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[:i+1] + rest[:dot]
	}
	return rest[:dot]
}

// Transport wraps an http.RoundTripper so outgoing HTTP calls become
// HTTP_CLIENT spans (service-boundary tracing) and carry the trace id.
func Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &tracingTransport{base: base}
}

type tracingTransport struct{ base http.RoundTripper }

func (t *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !Enabled() {
		return t.base.RoundTrip(req)
	}
	span := StartSpan(req.Context(), req.Method+" "+req.URL.Host+req.URL.Path)
	span.mu.Lock()
	span.ev.Type = pb.EventType_EVENT_TYPE_HTTP_CLIENT
	span.ev.CalleePackage = req.URL.Host
	traceID := span.ev.TraceId
	span.mu.Unlock()
	span.SetAttr("http.method", req.Method)
	span.SetAttr("http.url", req.URL.String())

	out := req.Clone(req.Context())
	out.Header.Set("X-Dataflow-Trace-Id", traceID)

	resp, err := t.base.RoundTrip(out)
	if err != nil {
		span.RecordError(err)
		span.End()
		return resp, err
	}
	span.SetStatus(int32(resp.StatusCode))
	span.End()
	return resp, nil
}
