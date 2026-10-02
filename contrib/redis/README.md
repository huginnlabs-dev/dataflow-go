# dataflow-go/contrib/redis

Optional go-redis v9 instrumentation for the
[Dataflow Go SDK](https://github.com/huginnlabs-dev/dataflow-go): every
command emits a `DB_QUERY` span named after the command verb (`GET`, `SET`,
`CONFIG GET`), with `db.system=redis` and the rendered command (clipped to
200) in `db.statement`. Pipelines are one span named after their first
command (`PIPELINE SET`) with the batch joined in the statement. Failures
record the error and status 500. Spans join the trace active on the
command's context.

This is a separate Go module on purpose: adding it never makes
`github.com/redis/go-redis/v9` a dependency of the SDK core.

## Install

```sh
go get github.com/huginnlabs-dev/dataflow-go/contrib/redis
```

## Usage

Wrap a new client:

```go
import (
    redistrace "github.com/huginnlabs-dev/dataflow-go/contrib/redis"
    "github.com/redis/go-redis/v9"
)

client := redistrace.Wrap(redis.Options{Addr: "localhost:6379"})
```

…or hook an existing one:

```go
client.AddHook(redistrace.Hook())
```

Inside a traced handler (`dataflow.Trace`, `dataflow.Middleware`), commands
join the request's trace:

```go
dataflow.Trace(ctx, "cart.Load", func(ctx context.Context) (string, error) {
    return client.Get(ctx, "cart:"+userID).Result()
})
// → cart.Load
//   └─ GET   db.system=redis db.statement="get cart:u42" 200
```

## Notes

- The statement text includes command arguments (key names, values) — the
  same data go-redis itself logs. Drop the hook if argument values must not
  leave the process.
- Cluster and sentinel clients accept the hook the same way
  (`clusterClient.AddHook(redistrace.Hook())`).
- Tests in this module run against [miniredis](https://github.com/alicebob/miniredis)
  — no Redis server is needed to build or test it.
