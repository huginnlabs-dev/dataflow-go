package dataflow

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// defaultRedactedHeaders are dropped from captured metadata and replaced by
// [REDACTED]; their values never leave the host process.
var defaultRedactedHeaders = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"set-cookie":          {},
	"x-api-key":           {},
}

// Middleware instruments a net/http handler. Because chi, gorilla/mux and
// echo all expose standard http.Handler, wrapping the router covers every
// major framework; a gin-specific middleware with route templates lives in
// GinMiddleware.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Enabled() {
			next.ServeHTTP(w, r)
			return
		}
		span := StartSpan(r.Context(), r.Method+" "+r.URL.Path)
		span.mu.Lock()
		span.ev.Type = pb.EventType_EVENT_TYPE_HTTP_SERVER
		if incoming := r.Header.Get("X-Dataflow-Trace-Id"); incoming != "" {
			span.ev.TraceId = incoming
		}
		span.mu.Unlock()
		stampAgent(span)
		span.SetAttr("http.method", r.Method)
		span.SetAttr("http.path", r.URL.Path)
		span.SetAttr("http.remote_addr", r.RemoteAddr)
		captureHeaders(span, r)
		captureBody(r, span)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(span.Context()))

		span.SetStatus(int32(rec.status))
		span.SetAttr("http.status_code", strconv.Itoa(rec.status))
		span.SetAttr("http.response_bytes", strconv.FormatInt(rec.bytes, 10))
		if r.ContentLength > 0 {
			span.SetAttr("http.request_bytes", strconv.FormatInt(r.ContentLength, 10))
		}
		if rec.status >= 500 {
			span.RecordError(errString("http " + strconv.Itoa(rec.status)))
		}
		span.End()
	})
}

// GinMiddleware is the gin-native flavour of Middleware: it uses the
// matched route template ("GET /api/users/:id") as the span name, keeping
// cardinality bounded on parameterised routes.
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !Enabled() {
			c.Next()
			return
		}
		span := StartSpan(c.Request.Context(), c.Request.Method+" "+c.Request.URL.Path)
		span.mu.Lock()
		span.ev.Type = pb.EventType_EVENT_TYPE_HTTP_SERVER
		if incoming := c.Request.Header.Get("X-Dataflow-Trace-Id"); incoming != "" {
			span.ev.TraceId = incoming
		}
		span.mu.Unlock()
		stampAgent(span)
		if route := c.FullPath(); route != "" {
			span.mu.Lock()
			span.ev.Name = c.Request.Method + " " + route
			span.mu.Unlock()
			span.SetAttr("http.route", route)
		}
		span.SetAttr("http.method", c.Request.Method)
		span.SetAttr("http.path", c.Request.URL.Path)
		captureHeaders(span, c.Request)
		captureBody(c.Request, span)

		// Expose the span so StartSpan/Trace calls inside handlers join the
		// same trace.
		c.Request = c.Request.WithContext(span.Context())
		c.Next()

		status := c.Writer.Status()
		span.SetStatus(int32(status))
		span.SetAttr("http.status_code", strconv.Itoa(status))
		span.SetAttr("http.response_bytes", strconv.Itoa(c.Writer.Size()))
		switch {
		case len(c.Errors) > 0:
			span.RecordError(errString(c.Errors.String()))
		case status >= 500:
			span.RecordError(errString("http " + strconv.Itoa(status)))
		}
		span.End()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wrote = true
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// captureHeaders stores redacted request headers as plaintext attributes.
func captureHeaders(span *Span, r *http.Request) {
	for name, vals := range r.Header {
		lower := strings.ToLower(name)
		key := "http.header." + lower
		if _, redact := defaultRedactedHeaders[lower]; redact {
			span.SetAttr(key, "[REDACTED]")
			continue
		}
		span.SetAttr(key, strings.Join(vals, ", "))
	}
}

// captureBody tees up to MaxBodyBytes of the request body into the span
// payload and restores a live reader for the handler.
func captureBody(r *http.Request, span *Span) {
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	limit := int64(current().maxBodyBytes)
	var excerpt bytes.Buffer
	tee := io.TeeReader(io.LimitReader(r.Body, limit), &excerpt)
	rest, err := io.ReadAll(tee)
	if err != nil {
		return
	}
	r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(rest), r.Body))
	if excerpt.Len() == 0 {
		return
	}
	span.SetData("request", map[string]any{
		"body_excerpt": excerpt.String(),
		"truncated":    r.ContentLength > limit,
	})
}

type errString string

func (e errString) Error() string { return string(e) }
