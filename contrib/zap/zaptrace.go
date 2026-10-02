// Package zaptrace forwards zap log entries into the Dataflow log pipeline
// (the same pipeline the SDK's Debug/Info/Warn/Error helpers write to), so
// existing zap loggers show up in the dashboard's Logs tab with trace
// correlation.
//
//	logger := zap.New(zaptrace.Core())                 // dataflow only
//	logger := zap.New(zaptrace.Wrap(previousCore))     // dataflow + previous
//
// Levels map onto the wire vocabulary (Debug→debug, Info→info, Warn→warn,
// Error/DPanic/Panic/Fatal→error) and fields become stringified record
// fields, clamped to the server's 50×512 limits. Entries carry the active
// trace/span ids of the logging goroutine, like every Dataflow log line.
//
// The package is an optional companion module: it lives in its own Go
// module (contrib/zap) so zap never becomes a dependency of the SDK core.
// Import it alongside github.com/huginnlabs-dev/dataflow-go.
package zaptrace

import (
	"go.uber.org/zap/zapcore"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

// Core returns a zapcore.Core that ships every accepted entry to the
// Dataflow log pipeline. It writes nothing anywhere else — combine it with
// your previous core via Wrap when you need both destinations.
func Core() zapcore.Core { return &core{} }

// Wrap returns a core that forwards entries to the Dataflow pipeline AND to
// next (tee): records keep going wherever next writes them, and a copy
// lands in the Dataflow Logs tab.
func Wrap(next zapcore.Core) zapcore.Core { return &wrapped{core: &core{}, next: next} }

// core is the Dataflow-only zapcore.Core.
type core struct {
	fields []zapcore.Field
}

func (c *core) Enabled(zapcore.Level) bool { return true }

func (c *core) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *core) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	c.write(ent, fields)
	return nil
}

func (c *core) Sync() error { return nil }

// With returns a copy accumulating fields (the zapcore.Core contract).
func (c *core) With(fields []zapcore.Field) zapcore.Core {
	return c.with(fields)
}

func (c *core) with(fields []zapcore.Field) *core {
	nc := &core{fields: make([]zapcore.Field, 0, len(c.fields)+len(fields))}
	nc.fields = append(nc.fields, c.fields...)
	nc.fields = append(nc.fields, fields...)
	return nc
}

// write stringifies the entry's fields and dispatches to the Dataflow
// pipeline at the mapped level. Never panics: a bad field must not take
// the application down with it.
func (c *core) write(ent zapcore.Entry, fields []zapcore.Field) {
	defer func() { _ = recover() }()
	all := make([]zapcore.Field, 0, len(c.fields)+len(fields))
	all = append(all, c.fields...)
	all = append(all, fields...)
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range all {
		f.AddTo(enc)
	}
	out := make(map[string]any, len(enc.Fields))
	for k, v := range enc.Fields {
		out[k] = v
	}
	switch ent.Level {
	case zapcore.DebugLevel:
		dataflow.Debug(ent.Message, out)
	case zapcore.InfoLevel:
		dataflow.Info(ent.Message, out)
	case zapcore.WarnLevel:
		dataflow.Warn(ent.Message, out)
	default: // Error, DPanic, Panic, Fatal
		dataflow.Error(ent.Message, out)
	}
}

// wrapped tees entries into Dataflow and the wrapped core.
type wrapped struct {
	*core
	next zapcore.Core
}

func (w *wrapped) Enabled(l zapcore.Level) bool { return w.next.Enabled(l) }

func (w *wrapped) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if w.next.Enabled(ent.Level) {
		ce = ce.AddCore(ent, w)
		return w.next.Check(ent, ce)
	}
	return ce
}

func (w *wrapped) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	w.core.write(ent, fields)
	return w.next.Write(ent, fields)
}

func (w *wrapped) Sync() error { return w.next.Sync() }

func (w *wrapped) With(fields []zapcore.Field) zapcore.Core {
	return &wrapped{core: w.core.with(fields), next: w.next.With(fields)}
}
