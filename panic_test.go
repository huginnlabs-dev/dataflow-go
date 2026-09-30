package dataflow

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// useTestPipeline swaps in a pipeline whose buffer the test can inspect
// directly, bypassing gRPC entirely.
func useTestPipeline(t *testing.T) *pipeline {
	t.Helper()
	p := &pipeline{buf: newEventBuffer(100), cfg: current()}
	globalPipeline.Store(p)
	t.Cleanup(func() { globalPipeline.Store(nil) })
	return p
}

// setEnabled toggles the SDK active flag (the package init may have turned
// it on from the environment) and restores the previous value on cleanup.
func setEnabled(t *testing.T, on bool) {
	t.Helper()
	prev := active.Load()
	active.Store(on)
	t.Cleanup(func() { active.Store(prev) })
}

// emitted drains the test pipeline buffer.
func emitted(p *pipeline) []*pb.TraceEvent {
	return p.buf.After(0)
}

func TestCapturePanicRecordsOnRequestSpanAndRepanics(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)

	span := StartSpan(context.Background(), "op")

	func() {
		defer func() {
			v := recover()
			if v != "boom" {
				t.Fatalf("recovered %v, want the original value \"boom\"", v)
			}
		}()
		CapturePanic(span.Context(), func() { panic("boom") })
	}()
	span.End()

	events := emitted(p)
	if len(events) == 0 {
		t.Fatal("no event emitted for the crashing span")
	}
	ev := events[0]
	if ev.ErrorMessage != "boom" {
		t.Errorf("error_message = %q, want \"boom\"", ev.ErrorMessage)
	}
	if ev.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", ev.StatusCode)
	}
	stack := ev.Metadata["error.stack"]
	if stack == "" {
		t.Fatal("error.stack attribute is empty")
	}
	if len(stack) > maxStackBytes {
		t.Errorf("error.stack is %d bytes, cap is %d", len(stack), maxStackBytes)
	}
	if !strings.Contains(stack, "panic_test") {
		t.Errorf("error.stack %q does not name the test frames", stack)
	}
}

func TestCapturePanicCreatesStandaloneSpanWithoutCtxSpan(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)

	func() {
		defer func() {
			if v := recover(); v != 42 {
				t.Fatalf("recovered %v, want the original value 42", v)
			}
		}()
		CapturePanic(context.Background(), func() { panic(42) })
	}()

	events := emitted(p)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Name != "panic" {
		t.Errorf("span name = %q, want \"panic\"", ev.Name)
	}
	if ev.Type != pb.EventType_EVENT_TYPE_FUNCTION_CALL {
		t.Errorf("span type = %s, want FUNCTION_CALL", ev.Type)
	}
	if ev.ErrorMessage != "42" {
		t.Errorf("error_message = %q, want \"42\"", ev.ErrorMessage)
	}
	if ev.Metadata["error.stack"] == "" {
		t.Fatal("error.stack attribute is empty")
	}
}

func TestCapturePanicTruncatesLongValuesAndStacks(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)

	var deep func(int)
	deep = func(n int) {
		if n == 0 {
			panic(strings.Repeat("x", 600))
		}
		deep(n - 1)
	}

	span := StartSpan(context.Background(), "deep")
	func() {
		defer func() { _ = recover() }()
		CapturePanic(span.Context(), func() { deep(500) })
	}()
	span.End()

	ev := emitted(p)[0]
	if got := ev.ErrorMessage; len(got) != maxPanicValueChars || !strings.HasPrefix(got, "xxxxx") {
		t.Errorf("error_message length = %d, want %d (prefix %q)", len(got), maxPanicValueChars, got[:10])
	}
	stack := ev.Metadata["error.stack"]
	if len(stack) != maxStackBytes {
		t.Errorf("error.stack length = %d, want exactly %d", len(stack), maxStackBytes)
	}
	if !strings.HasPrefix(stack, "goroutine ") {
		t.Errorf("error.stack must keep the top of the stack, got prefix %q", stack[:min(20, len(stack))])
	}
}

func TestCapturePanicKeepsMultibyteValuesIntact(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)

	span := StartSpan(context.Background(), "op")
	func() {
		defer func() { _ = recover() }()
		CapturePanic(span.Context(), func() { panic(strings.Repeat("é", 600)) }) // 1200 bytes
	}()
	span.End()

	ev := emitted(p)[0]
	if len(ev.ErrorMessage) != maxPanicValueChars {
		t.Errorf("error_message = %d bytes, want %d", len(ev.ErrorMessage), maxPanicValueChars)
	}
	if !utf8Valid(ev.ErrorMessage) {
		t.Error("truncation split a multi-byte rune")
	}
}

func utf8Valid(s string) bool { return strings.ToValidUTF8(s, "?") == s }

func TestCapturePanicDisabledRecordsNothing(t *testing.T) {
	setEnabled(t, false)
	p := useTestPipeline(t)

	span := StartSpan(context.Background(), "op")
	func() {
		defer func() {
			if v := recover(); v != "boom" {
				t.Fatalf("recovered %v, want \"boom\" even when disabled", v)
			}
		}()
		CapturePanic(span.Context(), func() { panic("boom") })
	}()
	span.End()

	events := emitted(p)
	for _, ev := range events {
		if ev.ErrorMessage != "" {
			t.Errorf("error_message = %q while disabled, want empty (no panic recording)", ev.ErrorMessage)
		}
		if stack := ev.Metadata["error.stack"]; stack != "" {
			t.Errorf("error.stack recorded while disabled, want empty")
		}
	}
}

func TestPanicMiddlewareAnswers500AndRecords(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)

	outer := Middleware(PanicMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("kaboom")
	})))

	req := httptest.NewRequest(http.MethodGet, "/things", nil)
	rec := httptest.NewRecorder()
	outer.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal server error") {
		t.Errorf("body = %q, want the 500 page", rec.Body.String())
	}

	events := emitted(p)
	if len(events) == 0 {
		t.Fatal("no event emitted for the panicking request")
	}
	ev := events[0]
	if ev.ErrorMessage != "kaboom" {
		t.Errorf("error_message = %q, want the panic value \"kaboom\"", ev.ErrorMessage)
	}
	if ev.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", ev.StatusCode)
	}
	stack := ev.Metadata["error.stack"]
	if stack == "" {
		t.Fatal("error.stack attribute is empty")
	}
	if len(stack) > maxStackBytes {
		t.Errorf("error.stack is %d bytes, cap is %d", len(stack), maxStackBytes)
	}
}

func TestPanicMiddlewareDisabledStillAnswers500WithoutEvents(t *testing.T) {
	setEnabled(t, false)
	p := useTestPipeline(t)

	outer := Middleware(PanicMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("kaboom")
	})))

	rec := httptest.NewRecorder()
	outer.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/things", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 even while disabled", rec.Code)
	}
	if n := len(emitted(p)); n != 0 {
		t.Errorf("emitted %d events while disabled, want 0", n)
	}
}

func TestGinMiddlewareRecordsPanicForRecovery(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)
	gin.SetMode(gin.TestMode)

	// gin.Recovery first: it sits in the outer frame, so the panic reaches
	// GinMiddleware's deferred recover (record + re-panic) before Recovery
	// turns it into the 500.
	r := gin.New()
	r.Use(gin.RecoveryWithWriter(io.Discard))
	r.Use(GinMiddleware())
	r.GET("/boom", func(c *gin.Context) { panic("gin-boom") })

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 from gin.Recovery", rec.Code)
	}

	events := emitted(p)
	if len(events) == 0 {
		t.Fatal("no event emitted for the panicking gin request")
	}
	ev := events[0]
	if ev.ErrorMessage != "gin-boom" {
		t.Errorf("error_message = %q, want \"gin-boom\"", ev.ErrorMessage)
	}
	if ev.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", ev.StatusCode)
	}
	if ev.Metadata["error.stack"] == "" {
		t.Fatal("error.stack attribute is empty")
	}
}

func TestGinMiddlewareWithoutOuterRecoveryDegradesGracefully(t *testing.T) {
	setEnabled(t, true)
	p := useTestPipeline(t)
	gin.SetMode(gin.TestMode)

	// Reverse order: gin.Recovery swallows the panic before our deferred
	// recover sees it. The request still answers 500 and the span still
	// ships — with the generic message and without a stack.
	r := gin.New()
	r.Use(GinMiddleware())
	r.Use(gin.RecoveryWithWriter(io.Discard))
	r.GET("/boom", func(c *gin.Context) { panic("gin-boom") })

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	events := emitted(p)
	if len(events) == 0 {
		t.Fatal("no event emitted for the panicking gin request")
	}
	if stack := events[0].Metadata["error.stack"]; stack != "" {
		t.Errorf("error.stack = %q, want empty when Recovery swallows the panic first", stack)
	}
}
