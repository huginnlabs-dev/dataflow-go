// Package gormtrace instruments GORM with Dataflow DB_QUERY spans.
//
//	db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
//	if err := gormtrace.Register(db); err != nil { ... }
//
// Every query, exec, create, update, delete, raw and row operation then
// emits one DB_QUERY span named like the SQL driver proxy names them
// ("SELECT orders", "INSERT users"), with the dialector name in db.system
// and the statement text (clipped to 200) in db.statement. Spans join the
// trace active on the context passed to db.WithContext.
//
// The package is an optional companion module: it lives in its own Go
// module (contrib/gorm) so gorm never becomes a dependency of the SDK
// core. Import it alongside github.com/huginnlabs-dev/dataflow-go.
package gormtrace

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"gorm.io/gorm"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
)

// Register installs the Dataflow tracing plugin on db (db.Use). Registering
// twice on the same db is tolerated (gorm's ErrRegistered is swallowed), so
// shared constructors can call it unconditionally.
func Register(db *gorm.DB) error {
	if err := db.Use(&plugin{}); err != nil && !errors.Is(err, gorm.ErrRegistered) {
		return err
	}
	return nil
}

// plugin implements gorm.Plugin.
type plugin struct {
	system string
}

// Name satisfies gorm.Plugin.
func (p *plugin) Name() string { return "dataflow" }

// spanKey is the per-instance slot the before callback stashes the open
// span in for the after callback to finish.
const spanKey = "dataflow:span"

// maxStatement mirrors the SDK's server-side statement clamp.
const maxStatement = 200

// Initialize wires before/after callbacks around every operation kind gorm
// dispatches (exec and raw share the raw processor). Row hooks are
// registered best-effort — the "gorm:row" hook name varies across gorm
// versions, and row queries mostly fall through to the query processor.
func (p *plugin) Initialize(db *gorm.DB) error {
	p.system = "db"
	if db.Dialector != nil {
		if n := db.Dialector.Name(); n != "" {
			p.system = n
		}
	}

	cb := db.Callback()
	if err := cb.Query().Before("gorm:query").Register("dataflow:before_query", p.before("SELECT")); err != nil {
		return err
	}
	if err := cb.Query().After("gorm:query").Register("dataflow:after_query", p.after()); err != nil {
		return err
	}
	_ = cb.Row().Before("gorm:row").Register("dataflow:before_row", p.before("SELECT"))
	_ = cb.Row().After("gorm:row").Register("dataflow:after_row", p.after())
	if err := cb.Raw().Before("gorm:raw").Register("dataflow:before_raw", p.before("EXEC")); err != nil {
		return err
	}
	if err := cb.Raw().After("gorm:raw").Register("dataflow:after_raw", p.after()); err != nil {
		return err
	}
	if err := cb.Create().Before("gorm:create").Register("dataflow:before_create", p.before("INSERT")); err != nil {
		return err
	}
	if err := cb.Create().After("gorm:create").Register("dataflow:after_create", p.after()); err != nil {
		return err
	}
	if err := cb.Update().Before("gorm:update").Register("dataflow:before_update", p.before("UPDATE")); err != nil {
		return err
	}
	if err := cb.Update().After("gorm:update").Register("dataflow:after_update", p.after()); err != nil {
		return err
	}
	if err := cb.Delete().Before("gorm:delete").Register("dataflow:before_delete", p.before("DELETE")); err != nil {
		return err
	}
	if err := cb.Delete().After("gorm:delete").Register("dataflow:after_delete", p.after()); err != nil {
		return err
	}
	return nil
}

// before opens the span. For model queries gorm has not built the SQL yet
// at this point, so the name derives from the operation verb plus the
// table (schema or Table()); raw statements already carry their SQL.
func (p *plugin) before(verb string) func(*gorm.DB) {
	return func(db *gorm.DB) {
		defer func() { _ = recover() }() // tracing must never break a query
		if db == nil || db.Statement == nil {
			return
		}
		ctx := db.Statement.Context
		if ctx == nil {
			ctx = context.Background()
		}
		span := dataflow.StartDBSpan(ctx, p.summary(db, verb), p.system, "")
		_ = db.InstanceSet(spanKey, span)
	}
}

// after records the built SQL, affected rows and the error, then closes
// the span. gorm.ErrRecordNotFound is a normal outcome, not a failure.
func (p *plugin) after() func(*gorm.DB) {
	return func(db *gorm.DB) {
		defer func() { _ = recover() }()
		if db == nil {
			return
		}
		v, ok := db.InstanceGet(spanKey)
		if !ok {
			return
		}
		span, _ := v.(*dataflow.Span)
		if s := clip(db.Statement.SQL.String()); s != "" {
			span.SetAttr("db.statement", s)
		}
		span.SetAttr("db.rows_affected", strconv.FormatInt(db.RowsAffected, 10))
		err := db.Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = nil
		}
		dataflow.FinishDBSpan(span, err)
	}
}

// summary renders the span name: StmtSummary when SQL exists, otherwise
// the operation verb plus the table gorm is operating on.
func (p *plugin) summary(db *gorm.DB, verb string) string {
	if s := db.Statement.SQL.String(); s != "" {
		return dataflow.StmtSummary(s)
	}
	if table := db.Statement.Table; table != "" {
		return verb + " " + table
	}
	if db.Statement.Schema != nil && db.Statement.Schema.Table != "" {
		return verb + " " + db.Statement.Schema.Table
	}
	return verb
}

// clip collapses whitespace and clips to maxStatement (db.statement clamp).
func clip(sql string) string {
	one := strings.Join(strings.Fields(sql), " ")
	if len(one) > maxStatement {
		one = one[:maxStatement]
	}
	return one
}
