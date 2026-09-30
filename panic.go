package dataflow

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"unicode/utf8"

	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

const (
	// maxStackBytes caps a captured panic stack. The server clamps at the
	// same size; the top of the stack names the panicking frames, so the
	// clip keeps the head and drops deep tail frames.
	maxStackBytes = 8192
	// maxPanicValueChars caps the panic value stored as the span's
	// error_message, matching the server's error-group rendering.
	maxPanicValueChars = 500
)

// CapturePanic runs fn and turns a panic inside it into a recorded error on
// the current trace before re-panicking with the original value:
//
//	dataflow.CapturePanic(ctx, func() { process(job) })
//
// The span is found in ctx the same way middleware stores the request span,
// so the panic lands on the crashing request's span. When ctx carries no
// span (background goroutines, batch jobs), the crash gets its own
// FUNCTION_CALL span named "panic". Either way the panic is re-raised
// unchanged — the caller (or runtime) stays in charge of crashing; only the
// evidence is captured first. Panic values and stacks surface on the Errors
// page in the Dataflow dashboard.
func CapturePanic(ctx context.Context, fn func()) {
	defer func() {
		v := recover()
		if v == nil {
			return
		}
		switch span := SpanFromContext(ctx); {
		case span != nil:
			recordPanic(span, v)
		case Enabled():
			// No request span in ctx: give the crash its own span so it
			// still ships.
			crash := StartSpan(ctx, "panic")
			crash.mu.Lock()
			crash.ev.Type = pb.EventType_EVENT_TYPE_FUNCTION_CALL
			crash.mu.Unlock()
			recordPanic(crash, v)
			crash.End()
		}
		panic(v)
	}()
	fn()
}

// PanicMiddleware recovers panics from downstream net/http handlers so a
// crash answers 500 instead of dropping the connection, and records the
// panic on the request's span for the Errors page:
//
//	http.ListenAndServe(addr, dataflow.Middleware(dataflow.PanicMiddleware(mux)))
//
// Wrap PanicMiddleware inside Middleware (as above) so the crash lands on
// the request span; without a span in the context the 500 still goes out
// but nothing is recorded. When the SDK is disabled the middleware still
// recovers and answers 500 — its presence alone guarantees no crash — it
// just records nothing.
func PanicMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				recordPanic(SpanFromContext(r.Context()), v)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// recordPanic stamps the panic value and a clipped stack onto span. It is
// best-effort by construction: the whole body is recover-protected so a
// failure during recording can never double-panic and mask the original
// crash, and it records nothing while the SDK is disabled.
func recordPanic(span *Span, value any) {
	if span == nil || !Enabled() {
		return
	}
	defer func() { _ = recover() }()
	stack := debug.Stack()
	if len(stack) > maxStackBytes {
		stack = stack[:maxStackBytes] // keep the top: the panicking frames
	}
	span.SetAttr("error.stack", string(stack))
	span.RecordError(errString(truncateString(fmt.Sprint(value), maxPanicValueChars)))
	span.SetStatus(http.StatusInternalServerError)
}

// truncateString clips msg to max bytes without splitting a multi-byte rune.
func truncateString(msg string, max int) string {
	if len(msg) <= max {
		return msg
	}
	for max > 0 && !utf8.RuneStart(msg[max]) {
		max--
	}
	return msg[:max]
}
