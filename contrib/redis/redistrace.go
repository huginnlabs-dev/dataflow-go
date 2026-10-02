// Package redistrace instruments go-redis v9 clients with Dataflow
// DB_QUERY spans.
//
//	client := redistrace.Wrap(redis.Options{Addr: "localhost:6379"})
//	// or, for an existing client: client.AddHook(redistrace.Hook())
//
// Every command emits one DB_QUERY span named after the command verb
// ("GET", "SET", "CONFIG GET"), with "redis" in db.system and the rendered
// command (clipped to 200) in db.statement. Pipelines are one span named
// after their first command; failures record the error and status 500.
// Spans join the trace active on the command's context.
//
// The package is an optional companion module: it lives in its own Go
// module (contrib/redis) so go-redis never becomes a dependency of the SDK
// core. Import it alongside github.com/huginnlabs-dev/dataflow-go.
package redistrace

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

// maxStatement mirrors the SDK's server-side statement clamp.
const maxStatement = 200

// Wrap opens a go-redis v9 client with the Dataflow hook installed.
func Wrap(opt redis.Options) *redis.Client {
	client := redis.NewClient(&opt)
	client.AddHook(Hook())
	return client
}

// Hook returns a go-redis v9 hook emitting one DB_QUERY span per command
// (and per pipeline execution). Add it with client.AddHook(Hook()).
func Hook() redis.Hook { return hook{} }

type hook struct{}

// DialHook passes dialing through untouched.
func (hook) DialHook(next redis.DialHook) redis.DialHook { return next }

// ProcessHook spans a single command round trip.
func (hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		span := dataflow.StartDBSpan(ctx, commandName(cmd), "redis", clip(cmdString(cmd)))
		err := next(ctx, cmd)
		dataflow.FinishDBSpan(span, err)
		return err
	}
}

// ProcessPipelineHook spans the whole pipeline as one DB_QUERY span named
// after its first command.
func (hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		name := "PIPELINE"
		var b strings.Builder
		if len(cmds) > 0 {
			name = "PIPELINE " + strings.ToUpper(cmds[0].FullName())
			for i, cmd := range cmds {
				if i > 0 {
					b.WriteString("; ")
				}
				b.WriteString(cmdString(cmd))
			}
		}
		span := dataflow.StartDBSpan(ctx, name, "redis", clip(b.String()))
		err := next(ctx, cmds)
		dataflow.FinishDBSpan(span, err)
		return err
	}
}

// commandName is the span name for one command: the canonical command
// ("get", "config get") uppercased.
func commandName(cmd redis.Cmder) string { return strings.ToUpper(cmd.FullName()) }

// cmdString renders the command with its arguments ("get mykey"), the way
// go-redis logs commands — minus the result/error suffix their String()
// appends, since the statement is captured before execution.
func cmdString(cmd redis.Cmder) string {
	var b strings.Builder
	for i, arg := range cmd.Args() {
		if i > 0 {
			b.WriteByte(' ')
		}
		writeArg(&b, arg)
	}
	return b.String()
}

// writeArg mirrors go-redis' argument rendering: strings verbatim, byte
// slices as text, durations as whole seconds, everything else via fmt.
func writeArg(b *strings.Builder, arg any) {
	switch v := arg.(type) {
	case nil:
		b.WriteString("<nil>")
	case string:
		b.WriteString(v)
	case []byte:
		b.Write(v)
	case time.Duration:
		b.WriteString(strconv.FormatInt(int64(v/time.Second), 10))
	default:
		b.WriteString(fmt.Sprint(v))
	}
}

// clip collapses whitespace and clips to maxStatement (db.statement clamp).
func clip(s string) string {
	one := strings.Join(strings.Fields(s), " ")
	if len(one) > maxStatement {
		one = one[:maxStatement]
	}
	return one
}
