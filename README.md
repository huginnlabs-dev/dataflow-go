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
| 0.1.x | 0.1.x | gRPC `dataflow.v1` + REST ingest v1, OTLP `/v1/traces` | ✅ active |

Rules of thumb:

- The server never breaks same-major SDK minors — upgrade the server
  freely within a major line.
- An SDK talks to any server of the same major; check this table when a new
  major appears on either side.
- The SDK stamps its version into every trace (`agent.sdk` metadata), so a
  deployed fleet is auditable from the dashboard.
