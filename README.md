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
| 0.4.x | 0.3.x – 0.4.x | + `EVENT_TYPE_DB_QUERY` (SQL spans), `EVENT_TYPE_LLM_CALL`, `/api/v1/catalog` route scan | ✅ active |
| 0.1.x – 0.3.x | 0.1.x – 0.2.x | gRPC `dataflow.v1` + REST ingest v1, OTLP `/v1/traces` | ✅ active |

Rules of thumb:

- The server never breaks same-major SDK minors — upgrade the server
  freely within a major line.
- An SDK talks to any server of the same major; check this table when a new
  major appears on either side.
- The SDK stamps its version into every trace (`agent.sdk` metadata), so a
  deployed fleet is auditable from the dashboard.
