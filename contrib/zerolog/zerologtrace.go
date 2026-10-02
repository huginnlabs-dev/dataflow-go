// Package zerologtrace forwards zerolog events into the Dataflow log
// pipeline (the same pipeline the SDK's Debug/Info/Warn/Error helpers write
// to), so existing zerolog loggers show up in the dashboard's Logs tab with
// trace correlation.
//
//	logger := zerolog.New(os.Stdout).Hook(zerologtrace.Hook())
//
// Levels map onto the wire vocabulary — Trace/Debug→debug, Info→info,
// Warn→warn, Error/Fatal/Panic→error; level-less events log as info — and
// every record carries the active trace/span ids of the logging goroutine,
// like all Dataflow log lines.
//
// Note: zerolog hooks receive the level and the message only — the library
// does not expose an event's fields to hooks, so field capture is out of
// scope for this integration (see contrib/zap for a fields-capable core).
//
// The package is an optional companion module: it lives in its own Go
// module (contrib/zerolog) so zerolog never becomes a dependency of the
// SDK core. Import it alongside github.com/huginnlabs-dev/dataflow-go.
package zerologtrace

import (
	"github.com/rs/zerolog"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

// Hook returns a zerolog.Hook forwarding events to the Dataflow log
// pipeline. Attach it with logger.Hook(zerologtrace.Hook()).
func Hook() zerolog.Hook { return hook{} }

type hook struct{}

// Run implements zerolog.Hook. It never panics: a delivery path that fails
// must not take the application down with it.
func (hook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	defer func() { _ = recover() }()
	if e == nil || level == zerolog.Disabled {
		return
	}
	switch {
	case level <= zerolog.DebugLevel: // TraceLevel, DebugLevel
		dataflow.Debug(msg)
	case level == zerolog.InfoLevel, level == zerolog.NoLevel:
		dataflow.Info(msg)
	case level == zerolog.WarnLevel:
		dataflow.Warn(msg)
	default: // ErrorLevel, FatalLevel, PanicLevel
		dataflow.Error(msg)
	}
}
