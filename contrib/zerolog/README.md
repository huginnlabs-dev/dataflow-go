# dataflow-go/contrib/zerolog

Optional [zerolog](https://github.com/rs/zerolog) integration for the
[Dataflow Go SDK](https://github.com/huginnlabs-dev/dataflow-go): a
`zerolog.Hook` that forwards events into the Dataflow log pipeline, so
existing zerolog loggers show up in the dashboard's **Logs** tab with trace
correlation (the active trace/span ids of the logging goroutine are stamped
onto every record, like all Dataflow log lines).

Levels map onto the wire vocabulary — `Trace`/`Debug`→debug, `Info`→info,
`Warn`→warn, `Error`/`Fatal`/`Panic`→error; level-less events log as info.

This is a separate Go module on purpose: adding it never makes
`github.com/rs/zerolog` a dependency of the SDK core.

## Install

```sh
go get github.com/huginnlabs-dev/dataflow-go/contrib/zerolog
```

## Usage

```go
import (
    zerologtrace "github.com/huginnlabs-dev/dataflow-go/contrib/zerolog"
    "github.com/rs/zerolog"
)

logger := zerolog.New(os.Stdout).Hook(zerologtrace.Hook())
logger.Info().Msg("order placed")
```

Inside a traced handler (`dataflow.Trace`, `dataflow.Middleware`) records
carry the active trace/span ids; outside a trace they ship with empty ids.
With the SDK disabled or log shipping off, the hook drops events — logging
never blocks or panics.

## Fields are not captured

zerolog hooks receive the event's level and message only — the library does
not expose an event's fields to hooks (they live in the encoded JSON
buffer, not the hook callback). Field capture is out of scope for this
integration; if you need fields in Dataflow, see
[contrib/zap](../zap) (a fields-capable zap core) or the SDK's built-in
`Debug/Info/Warn/Error` helpers.
