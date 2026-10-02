package dataflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"strings"
	"sync"

	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// SQL tracing: connections opened through Driver() emit one DB_QUERY span
// per query/execution — verb + table as the name (e.g. "SELECT orders"),
// the SQL dialect in db.system, and the statement text (truncated) in
// db.statement. Parameter values are never captured.

// Driver returns the name of a registered tracing proxy of base. Register
// the result with database/sql and open it instead of the raw driver:
//
//	sql.Register("dataflow-postgres", dataflow.Driver("postgres", &stdlib.Driver{}))
//	db, _ := sql.Open("dataflow-postgres", dsn)
//
// The name is stable per base driver, so calling it repeatedly is safe.
func Driver(name string, base driver.Driver) string {
	wrapped := "dataflow-" + name
	wrapMu.Lock()
	defer wrapMu.Unlock()
	if _, ok := wrapRegistered[wrapped]; ok {
		return wrapped
	}
	sql.Register(wrapped, &tracedDriver{system: name, base: base})
	wrapRegistered[wrapped] = true
	return wrapped
}

var (
	wrapMu         sync.Mutex
	wrapRegistered = map[string]bool{}
)

type tracedDriver struct {
	system string
	base   driver.Driver
}

func (d *tracedDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.base.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &tracedConn{system: d.system, base: conn}, nil
}

type tracedConn struct {
	system string
	base   driver.Conn
}

func (c *tracedConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.base.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &tracedStmt{system: c.system, summary: StmtSummary(query), statement: clipStatement(query), base: stmt}, nil
}

func (c *tracedConn) Close() error { return c.base.Close() }

func (c *tracedConn) Begin() (driver.Tx, error) { return c.base.Begin() }

// QueryContext spans the fast path when the wrapped driver supports it.
func (c *tracedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.base.(driver.QueryerContext)
	if !ok {
		// Legacy driver: fall through to Prepare + traced statement.
		stmt, err := c.Prepare(query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		return stmt.(*tracedStmt).QueryContext(ctx, args)
	}
	span := startSQLSpan(ctx, c.system, query)
	rows, err := qc.QueryContext(ctx, query, args)
	finishSQLSpan(span, err)
	return rows, err
}

func (c *tracedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.base.(driver.ExecerContext)
	if !ok {
		stmt, err := c.Prepare(query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		return stmt.(*tracedStmt).ExecContext(ctx, args)
	}
	span := startSQLSpan(ctx, c.system, query)
	result, err := ec.ExecContext(ctx, query, args)
	finishSQLSpan(span, err)
	return result, err
}

type tracedStmt struct {
	system    string
	summary   string
	statement string
	base      driver.Stmt
}

func (s *tracedStmt) Close() error  { return s.base.Close() }
func (s *tracedStmt) NumInput() int { return s.base.NumInput() }

func (s *tracedStmt) Exec(args []driver.Value) (driver.Result, error) {
	span := startSQLSpan(context.Background(), s.system, s.statement)
	result, err := s.base.Exec(args)
	finishSQLSpan(span, err)
	return result, err
}

func (s *tracedStmt) Query(args []driver.Value) (driver.Rows, error) {
	span := startSQLSpan(context.Background(), s.system, s.statement)
	rows, err := s.base.Query(args)
	finishSQLSpan(span, err)
	return rows, err
}

func (s *tracedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := s.base.(driver.ExecerContext)
	if !ok {
		vals, err := namedToValues(args)
		if err != nil {
			return nil, err
		}
		return s.Exec(vals)
	}
	span := startSQLSpan(ctx, s.system, s.statement)
	result, err := ec.ExecContext(ctx, s.statement, args)
	finishSQLSpan(span, err)
	return result, err
}

func (s *tracedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := s.base.(driver.QueryerContext)
	if !ok {
		vals, err := namedToValues(args)
		if err != nil {
			return nil, err
		}
		return s.Query(vals)
	}
	span := startSQLSpan(ctx, s.system, s.statement)
	rows, err := qc.QueryContext(ctx, s.statement, args)
	finishSQLSpan(span, err)
	return rows, err
}

func namedToValues(args []driver.NamedValue) ([]driver.Value, error) {
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	return vals, nil
}

// startSQLSpan opens a DB_QUERY span; nil when tracing is disabled.
func startSQLSpan(ctx context.Context, system, query string) *Span {
	return StartDBSpan(ctx, StmtSummary(query), system, query)
}

func finishSQLSpan(span *Span, err error) { FinishDBSpan(span, err) }

// StartDBSpan opens a DB_QUERY span for one database operation and returns
// it for the caller to finish with FinishDBSpan. name is the short span
// name (use StmtSummary for SQL statements, an uppercased command verb for
// key-value stores), system the backend identifier recorded as db.system
// ("postgres", "sqlite", "redis", …) and statement the operation text,
// clipped and recorded as db.statement. The returned span is nil when the
// SDK is disabled — FinishDBSpan accepts a nil span, so callers can ignore
// that case.
func StartDBSpan(ctx context.Context, name, system, statement string) *Span {
	if !Enabled() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	span := StartSpan(ctx, name)
	span.mu.Lock()
	span.ev.Type = pb.EventType_EVENT_TYPE_DB_QUERY
	span.ev.CalleePackage = system
	span.mu.Unlock()
	span.SetAttr("db.system", system)
	if s := clipStatement(statement); s != "" {
		span.SetAttr("db.statement", s)
	}
	return span
}

// FinishDBSpan records err (error message + status 500 when non-nil, 200
// otherwise) and closes the span. A nil span — StartDBSpan with the SDK
// disabled — is a no-op.
func FinishDBSpan(span *Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(500)
	} else {
		span.SetStatus(200)
	}
	span.End()
}

func clipStatement(query string) string {
	one := strings.Join(strings.Fields(query), " ")
	if len(one) > 200 {
		one = one[:200]
	}
	return one
}

var (
	stmtVerbRe  = regexp.MustCompile(`(?is)^\s*\(?\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH|BEGIN|COMMIT|ROLLBACK|SET|CALL|EXEC|SHOW|EXPLAIN)\b`)
	stmtTableRe = regexp.MustCompile(`(?is)\b(?:FROM|INTO|UPDATE|TABLE|JOIN)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?[` + "`" + `"'\[]?([A-Za-z_][\w$.]*)`)
)

// StmtSummary renders a short human name for a statement: the verb plus
// the first table reference when one exists ("SELECT orders",
// "INSERT users"); bare verbs and non-SQL fall back to the first word.
// Exported for integrations that name DB_QUERY spans the same way the
// database/sql driver proxy does.
func StmtSummary(query string) string {
	one := strings.Join(strings.Fields(query), " ")
	m := stmtVerbRe.FindStringSubmatch(one)
	if m == nil {
		if i := strings.IndexAny(one, " \t\n("); i > 0 {
			return strings.ToUpper(one[:i])
		}
		return "QUERY"
	}
	verb := strings.ToUpper(m[1])
	if t := stmtTableRe.FindStringSubmatch(one); t != nil {
		// Schema-qualified names ("public.items") report the bare table.
		table := t[1]
		if i := strings.LastIndexAny(table, ".$"); i >= 0 {
			table = table[i+1:]
		}
		return verb + " " + table
	}
	return verb
}
