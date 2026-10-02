# dataflow-go/contrib/zap

Optional [zap](https://go.uber.org/zap) integration for the
[Dataflow Go SDK](https://github.com/huginnlabs-dev/dataflow-go): a
`zapcore.Core` that forwards entries into the Dataflow log pipeline, so
existing zap loggers show up in the dashboard's **Logs** tab with trace
correlation (the active trace/span ids of the logging goroutine are stamped
onto every record, like all Dataflow log lines).

Levels map onto the wire vocabulary — `Debug`→debug, `Info`→info,
`Warn`→warn, `Error`/`DPanic`/`Panic`/`Fatal`→error — and fields become
stringified record fields, clamped to the server's 50×512 limits.

This is a separate Go module on purpose: adding it never makes
`go.uber.org/zap` a dependency of the SDK core.

## Install

```sh
go get github.com/huginnlabs-dev/dataflow-go/contrib/zap
```

## Usage

Dataflow only (records go nowhere else):

```go
import (
    zaptrace "github.com/huginnlabs-dev/dataflow-go/contrib/zap"
    "go.uber.org/zap"
)

logger := zap.New(zaptrace.Core())
logger.Info("order placed", zap.Int("order_id", order.ID))
```

Tee into Dataflow and your previous core (console/file output keeps working):

```go
previous := zapcore.NewCore(zapcore.NewConsoleEncoder(enc), zapcore.AddSync(os.Stdout), zapcore.InfoLevel)
logger := zap.New(zaptrace.Wrap(previous))
```

`With` accumulates fields exactly like any other zap core. Inside a traced
handler (`dataflow.Trace`, `dataflow.Middleware`) records carry the active
trace/span ids; outside a trace they ship with empty ids. With the SDK
disabled or log shipping off, the core accepts and drops entries — logging
never blocks or panics.
