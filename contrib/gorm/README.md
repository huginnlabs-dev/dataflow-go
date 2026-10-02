# dataflow-go/contrib/gorm

Optional GORM instrumentation for the [Dataflow Go SDK](https://github.com/huginnlabs-dev/dataflow-go):
every query, exec, create, update, delete and raw operation emits a
`DB_QUERY` span — named like the SDK's SQL driver proxy names them
(`SELECT orders`, `INSERT users`), with the dialector name in `db.system`,
the statement text (clipped to 200) in `db.statement` and affected rows in
`db.rows_affected`. Failures record the error and status 500
(`gorm.ErrRecordNotFound` counts as success). Spans join the trace active on
the context you pass to `db.WithContext`.

This is a separate Go module on purpose: adding it never makes `gorm.io/gorm`
a dependency of the SDK core.

## Install

```sh
go get github.com/huginnlabs-dev/dataflow-go/contrib/gorm
```

## Usage

```go
import (
    gormtrace "github.com/huginnlabs-dev/dataflow-go/contrib/gorm"
    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)

db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
if err != nil { ... }
if err := gormtrace.Register(db); err != nil { ... }
```

That's it. Inside a traced handler (`dataflow.Trace`, `dataflow.Middleware`,
gin middleware), a query joins the request's trace:

```go
dataflow.Trace(ctx, "orders.List", func(ctx context.Context) ([]Order, error) {
    var orders []Order
    err := db.WithContext(ctx).Limit(20).Find(&orders).Error
    return orders, err
})
// → trace.Serve
//   └─ SELECT orders        db.system=postgres db.statement="SELECT * FROM \"orders\" LIMIT 20" 200
```

`Register` is idempotent — calling it twice on the same `*gorm.DB` is
tolerated. With the SDK disabled (`DATAFLOW_DISABLED`) the plugin stays
installed but emits nothing.

## Notes

- The plugin is a GORM callback pair (before/after) per operation processor;
  the span name is derived before GORM builds the SQL (verb + table), and the
  built statement text is attached when the operation finishes.
- Prefer `db.WithContext(ctx)` so spans parent onto your traces; queries
  without a context ship as standalone DB_QUERY spans.
- Tests in this module run against a no-op dialector backed by a fake
  `database/sql` driver — no database is needed to build or test it.
