# dataflow-go — HuginnLabs Dataflow SDK for Go

One blank import instruments a Go service for [HuginnLabs Dataflow](https://github.com/huginnlabs-dev):
HTTP entry points (gin / net/http), package-boundary function calls and outgoing
HTTP calls become live traces, metrics and a data-flow graph in the Dataflow
dashboard. Captured payloads are end-to-end encrypted (AES-256-GCM, PBKDF2) —
the server stores ciphertext only; you hold the key.

```go
import _ "github.com/huginnlabs-dev/dataflow-go"
```

That's it — with the `DATAFLOW_*` environment variables set (or a config
file), the SDK streams traces to your Dataflow endpoint over gRPC.

## Install

```sh
go get github.com/huginnlabs-dev/dataflow-go
```

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATAFLOW_ENDPOINT` | — | gRPC ingest endpoint, e.g. `localhost:25090` |
| `DATAFLOW_API_KEY` | — | project API key (from the Dataflow dashboard) |
| `DATAFLOW_SERVICE_NAME` | hostname | service name shown in the dashboard |
| `DATAFLOW_APP_VERSION` | — | deployment tag (drives the release-diff views) |
| `DATAFLOW_ENV` | — | environment label (`prod`, `staging`, …) |
| `DATAFLOW_ENCRYPTION_KEY` | — | payload encryption key (keep secret, keep local) |
| `DATAFLOW_SALT` | — | encryption salt |
| `DATAFLOW_DISABLED` | — | set to disable tracing entirely |

Full SDK documentation, per-language pages and an agent-ready prompt live in
the product docs (the `/docs` section of your Dataflow deployment).

## Route scanning (dataflow-scan)

`dataflow-scan` statically extracts the HTTP endpoints a Go service declares
in its source — no build, no run — and posts them to the Dataflow server
catalog (`POST /api/v1/catalog`), so routes show up in the dashboard even
before the service ships its first trace.

```sh
go run github.com/huginnlabs-dev/dataflow-go/cmd/dataflow-scan@latest \
  --url https://dataflow.example.com \
  --api-key "$DATAFLOW_API_KEY" \
  --service shop \
  --dir ./cmd/shop
```

CI usage sketch (e.g. GitLab CI):

```yaml
scan:
  stage: catalog
  image: golang:1.25
  script:
    - go run github.com/huginnlabs-dev/dataflow-go/cmd/dataflow-scan@latest
        --url "$DATAFLOW_HTTP_URL"
        --api-key "$DATAFLOW_API_KEY"
        --service "$CI_PROJECT_NAME"
        --dir .
```

Flags mirror the SDK environment:

| Flag | Default | Meaning |
|------|---------|---------|
| `--dir` | `.` | Go service directory to scan |
| `--service` | `DATAFLOW_SERVICE_NAME`, then dir base name | service name in the catalog |
| `--url` | `DATAFLOW_HTTP_URL`, then URL-form `DATAFLOW_ENDPOINT` | Dataflow HTTP base URL |
| `--api-key` | `DATAFLOW_API_KEY` | project API key |
| `--print` | off | print the routes as JSON and exit without posting |

A bare `host:port` `DATAFLOW_ENDPOINT` is the gRPC ingest address with no
derivable HTTP base — the upload is then skipped with exit code 1. Exit
codes: `0` ok, `1` scan failure or skipped upload, `2` catalog POST rejected.

Recognized registrations (literal string paths only; dynamic paths are
skipped; results are deduplicated and capped at 1000 routes):

| Framework | Patterns |
|-----------|----------|
| gin | `r.GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS(...)`, `Any`, `g := r.Group("/api")` prefixes |
| echo | `e.GET/POST/...(...)`, `Any`, `g := e.Group("/api")` prefixes |
| chi | `r.Get/Post/...(...)`, `r.Route("/api", func(r chi.Router) {...})` nesting |
| fiber | `app.Get/Post/...(...)`, `All`, group prefixes |
| gorilla/mux | `r.HandleFunc("/x", h)` (→ `ANY`), chained `.Methods("GET", ...)` |
| net/http | `mux.HandleFunc("GET /x", h)` (Go 1.22 patterns), plain path → `ANY` |

## Panic capture

Crashes become first-class errors: the panicking span is stamped with the
panic value as its message, status 500, and an `error.stack` attribute
(≤ 8 KB, top of the stack) — so panics land on the **Errors** page in the
Dataflow dashboard, grouped and with their stack traces.

For `net/http` (and anything built on it — chi, gorilla/mux, echo), wrap
`PanicMiddleware` inside `Middleware`; a panic then answers a 500 and lands
on the request's span:

```go
http.ListenAndServe(addr,
    dataflow.Middleware(
        dataflow.PanicMiddleware(mux)))
```

For background work and goroutines, wrap the risky call in `CapturePanic`.
It records the crash on the span carried by `ctx` (or a standalone `panic`
span when there is none) and then re-panics with the original value — it
captures the evidence, it never swallows the crash:

```go
dataflow.CapturePanic(ctx, func() { process(job) })
```

With gin, register `gin.Recovery()` **before** `GinMiddleware()`:

```go
r := gin.New()
r.Use(gin.Recovery())               // outer: answers the 500
r.Use(dataflow.GinMiddleware())     // inner: records the panic, re-panics
```

In that order the panic reaches the SDK's deferred recover first, is
recorded on the request span, and is re-raised for `gin.Recovery()` to turn
into the 500. In the reverse order Recovery swallows the panic before the
SDK sees it — requests still answer 500, but only a generic `http 500`
lands on the span, without a stack. When the SDK is disabled
(`DATAFLOW_DISABLED`), the middlewares still recover and answer 500 — they
just record nothing.

## Log capture

The SDK ships your application logs to the Dataflow dashboard (**Logs**
tab) and stamps every line with the current span's trace/span ids, so a log
line shows up next to the trace it happened in.

Four level helpers take a message plus optional fields (stringified,
clamped to 50×512 like the server side):

```go
dataflow.Info("order placed", map[string]any{"order_id": order.ID})
dataflow.Warn("cache miss", map[string]any{"key": key})
dataflow.Error("payment declined", map[string]any{"reason": reason})
dataflow.Debug("cache warm")
dataflow.Logf("warn", "retry %d/%d for %s", attempt, max, op) // printf flavour
```

Trace correlation is automatic: inside a `dataflow.Trace` callback or an
HTTP handler wrapped by `Middleware`/`GinMiddleware`, records carry the
active trace/span ids; outside a trace they ship with empty ids.

For `log/slog` users, swap the default logger and every record — including
third-party code logging through `log/slog` — flows into Dataflow with
attributes as fields:

```go
logger := slog.New(dataflow.NewSlogHandler())
slog.SetDefault(logger)
slog.Warn("token expired", "user_id", 42)
```

Shipping is batched and best-effort: lines buffer in-process (1024 cap,
oldest dropped on overflow) and POST to `/api/v1/logs` every 500 ms or 50
lines; a failed batch is retried once, then dropped — logging never blocks
your application. `dataflow.FlushLogs()` ships the buffer synchronously
(useful on shutdown). Log shipping reuses the manifest's HTTP base
resolution: a URL-form `DATAFLOW_ENDPOINT` (or `DATAFLOW_HTTP_URL` for a
bare gRPC `host:port`) is required — with no derivable HTTP base, logging
stays silently off. When the SDK is disabled (`DATAFLOW_DISABLED`, or no
API key) all helpers and the slog handler are no-ops.

## Integrations (contrib modules)

Optional integrations ship as **separate Go modules under `contrib/`**, so
their library never becomes a dependency of the SDK core — add the one you
use, and the core stays dependency-free:

| Module | Import | What it adds |
|--------|--------|--------------|
| [contrib/gorm](contrib/gorm) | `gormtrace` | GORM plugin — `DB_QUERY` spans for every query/exec/create/update/delete (`SELECT orders`, `db.system`, `db.statement`) |
| [contrib/redis](contrib/redis) | `redistrace` | go-redis v9 hook — one `DB_QUERY` span per command (`GET`, `PIPELINE SET`), command text as the statement |
| [contrib/zap](contrib/zap) | `zaptrace` | zap core (`zaptrace.Core()`, `Wrap`) — entries forwarded to the log pipeline with fields |
| [contrib/zerolog](contrib/zerolog) | `zerologtrace` | zerolog hook (`Hook()`) — events forwarded to the log pipeline |

```go
import gormtrace "github.com/huginnlabs-dev/dataflow-go/contrib/gorm"

db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
_ = gormtrace.Register(db) // every query now emits a DB_QUERY span
```

Each module has its own `go.mod` (requiring the SDK via a `replace ../..`)
and its own README — they carry no effect on builds that don't import them.

For custom instrumentations the SDK also exports the DB-span primitives the
SQL driver proxy uses: `dataflow.StartDBSpan(ctx, name, system, statement)`
opens a `DB_QUERY` span, `dataflow.FinishDBSpan(span, err)` records the
error/status and closes it, and `dataflow.StmtSummary(sql)` renders the
"verb + table" span name. Test suites can capture spans and log lines
in-memory with `dataflow.CaptureEvents()` / `dataflow.CaptureLogs()`.

## Versioning & compatibility

Both the Dataflow server and this SDK follow [SemVer](https://semver.org).
The **wire contract** between them is `proto/dataflow.proto` (package
`dataflow.v1`) plus the REST ingest API `/api/v1/ingest` — that contract is
what the matrix below tracks:

- **MAJOR** — breaking wire-protocol change. Server and all SDKs bump major
  together.
- **MINOR** — additive protocol change (new event types, new optional
  metadata). Older SDKs keep working; a new SDK minor may need a recent
  server minor.
- **PATCH** — fixes with no protocol impact.

Tags: server releases are `dataflow/vX.Y.Z`, SDK releases are
`sdk-go/vX.Y.Z` (this file's repo also accepts bare `vX.Y.Z` — required for
the Go module proxy). CI refuses a tag that doesn't match the version
constant in the source (`SDKVersion` in `agent.go`).

### Compatibility matrix

| Dataflow server | sdk-go | Wire protocol | Status |
|-----------------|--------|---------------|--------|
| 0.5.x | 0.7.x | + optional contrib modules (GORM/redis DB_QUERY spans, zap/zerolog log forwarding) — no wire change | ✅ active |
| 0.5.x | 0.6.x | + REST `/api/v1/logs` application log shipping with trace correlation | ✅ active |
| 0.4.x | 0.5.x | + `error.stack` panic capture metadata (stacks on the Errors page) | ✅ active |
| 0.4.x | 0.3.x – 0.4.x | + `EVENT_TYPE_DB_QUERY` (SQL spans), `EVENT_TYPE_LLM_CALL`, `/api/v1/catalog` route scan | ✅ active |
| 0.1.x – 0.3.x | 0.1.x – 0.2.x | gRPC `dataflow.v1` + REST ingest v1, OTLP `/v1/traces` | ✅ active |

Rules of thumb:

- The server never breaks same-major SDK minors — upgrade the server
  freely within a major line.
- An SDK talks to any server of the same major; check this table when a new
  major appears on either side.
- The SDK stamps its version into every trace (`agent.sdk` metadata), so a
  deployed fleet is auditable from the dashboard.
