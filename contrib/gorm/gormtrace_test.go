package gormtrace

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	gormio "gorm.io/gorm"

	dataflow "github.com/huginnlabs-dev/dataflow-go"
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

func TestMain(m *testing.M) {
	// Offline, active SDK: an empty Endpoint keeps the gRPC stream and the
	// log flusher down; the tests install in-memory captures per test.
	dataflow.Configure(dataflow.Config{APIKey: "test-key", ServiceName: "gormtrace-test"})
	m.Run()
}

// ---- fake database/sql driver ------------------------------------------

var registerOnce sync.Once

// fakeDriver answers every query with one row (id=1, name="ada"); prepare
// fails for statements containing "broken".
type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{}, nil }

type fakeConn struct{}

func (*fakeConn) Prepare(q string) (driver.Stmt, error) {
	if strings.Contains(q, "broken") {
		return nil, errors.New("fake: broken statement")
	}
	return &fakeStmt{}, nil
}
func (*fakeConn) Close() error { return nil }
func (*fakeConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

type fakeStmt struct{}

func (*fakeStmt) Close() error  { return nil }
func (*fakeStmt) NumInput() int { return -1 }
func (*fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (*fakeStmt) Query([]driver.Value) (driver.Rows, error) { return &fakeRows{}, nil }

type fakeRows struct{ done bool }

func (*fakeRows) Columns() []string { return []string{"id", "name"} }
func (*fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0], dest[1] = int64(1), "ada"
	return nil
}

// ---- fake gorm dialector ------------------------------------------------

type fakeDialector struct{ pool *sql.DB }

func (d fakeDialector) Name() string { return "fake" }
func (d fakeDialector) Initialize(db *gormio.DB) error {
	db.ConnPool = d.pool
	// Real dialectors wire the default callback chain from Initialize; the
	// plugin's Before/After hooks need those "gorm:*" anchors to exist.
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		LastInsertIDReversed: true,
	})
	return nil
}
func (fakeDialector) Migrator(*gormio.DB) gormio.Migrator { return nil }
func (fakeDialector) DataTypeOf(*schema.Field) string     { return "text" }
func (fakeDialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}
func (fakeDialector) BindVarTo(writer clause.Writer, _ *gormio.Statement, _ interface{}) {
	writer.WriteByte('?')
}
func (fakeDialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	writer.WriteString(str)
	writer.WriteByte('`')
}
func (fakeDialector) Explain(sql string, vars ...interface{}) string { return sql }

// openTestDB opens a gorm DB over the fake driver with the plugin
// registered. A second Register must be tolerated.
func openTestDB(t *testing.T) *gormio.DB {
	t.Helper()
	registerOnce.Do(func() { sql.Register("dataflow-gormtrace-fake", fakeDriver{}) })
	pool, err := sql.Open("dataflow-gormtrace-fake", "")
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	db, err := gormio.Open(fakeDialector{pool: pool}, &gormio.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := Register(db); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := Register(db); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	return db
}

type userRow struct {
	ID   int64
	Name string
}

// Spans must be emitted for exec and query with the SQL-proxy naming
// convention, db.system from the dialector and the built statement text.
func TestSpansForExecAndQuery(t *testing.T) {
	db := openTestDB(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	if err := db.Exec("INSERT INTO users (name) VALUES ('ada')").Error; err != nil {
		t.Fatalf("exec: %v", err)
	}
	var users []userRow
	if err := db.Table("users").Where("id = ?", 7).Scan(&users).Error; err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(users) != 1 || users[0].Name != "ada" {
		t.Fatalf("rows = %v, want one ada", users)
	}

	events := capture.Events()
	if len(events) != 2 {
		t.Fatalf("captured %d events, want 2", len(events))
	}

	exec := events[0]
	if exec.Type != pb.EventType_EVENT_TYPE_DB_QUERY {
		t.Errorf("exec type = %v, want DB_QUERY", exec.Type)
	}
	if exec.Name != "INSERT users" {
		t.Errorf("exec name = %q, want %q", exec.Name, "INSERT users")
	}
	if got := exec.Metadata["db.system"]; got != "fake" {
		t.Errorf("db.system = %q, want fake", got)
	}
	if stmt := exec.Metadata["db.statement"]; stmt != "INSERT INTO users (name) VALUES ('ada')" {
		t.Errorf("db.statement = %q", stmt)
	}
	if exec.StatusCode != 200 {
		t.Errorf("exec status = %d, want 200", exec.StatusCode)
	}
	if exec.ErrorMessage != "" {
		t.Errorf("exec error = %q, want empty", exec.ErrorMessage)
	}

	query := events[1]
	if query.Name != "SELECT users" {
		t.Errorf("query name = %q, want %q (derived before gorm builds the SQL)", query.Name, "SELECT users")
	}
	if stmt := query.Metadata["db.statement"]; stmt == "" {
		t.Error("query db.statement must carry the built SQL")
	} else if len(stmt) > 200 {
		t.Errorf("query db.statement = %q, want clipped to 200", stmt)
	}
	if query.StatusCode != 200 {
		t.Errorf("query status = %d, want 200", query.StatusCode)
	}
}

// A failing statement must record the error and status 500.
func TestErrorSpan(t *testing.T) {
	db := openTestDB(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	if err := db.Exec("DELETE FROM broken WHERE id = 1").Error; err == nil {
		t.Fatal("expected the fake driver error to surface")
	}

	events := capture.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.StatusCode != 500 {
		t.Errorf("status = %d, want 500", ev.StatusCode)
	}
	if ev.ErrorMessage == "" {
		t.Error("error message must be recorded on the span")
	}
}

// Queries run with a context from dataflow.Trace must join the trace.
func TestSpansJoinTrace(t *testing.T) {
	db := openTestDB(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)

	if _, err := dataflow.Trace(context.Background(), "trace.Serve", func(ctx context.Context) (int, error) {
		var users []userRow
		if err := db.WithContext(ctx).Table("users").Scan(&users).Error; err != nil {
			return 0, err
		}
		return len(users), nil
	}); err != nil {
		t.Fatal(err)
	}

	events := capture.Events()
	if len(events) != 2 {
		t.Fatalf("captured %d events, want 2 (db span + root span)", len(events))
	}
	// The db span ends first, so it lands in the buffer before the root.
	child, root := events[0], events[1]
	if root.Name != "trace.Serve" {
		t.Fatalf("root = %q, want trace.Serve", root.Name)
	}
	if child.Name != "SELECT users" {
		t.Fatalf("child = %q, want SELECT users", child.Name)
	}
	if child.TraceId != root.TraceId || child.ParentSpanId != root.SpanId {
		t.Errorf("child %v/%v not a child of root %v/%v",
			child.TraceId, child.ParentSpanId, root.TraceId, root.SpanId)
	}
}

// With the SDK disabled the plugin must not break queries and emit nothing.
func TestDisabledNoSpans(t *testing.T) {
	db := openTestDB(t)
	capture := dataflow.CaptureEvents()
	t.Cleanup(capture.Restore)
	restore := dataflow.SetEnabled(false)
	t.Cleanup(restore)

	if err := db.Exec("INSERT INTO users (name) VALUES ('bob')").Error; err != nil {
		t.Fatalf("exec while disabled: %v", err)
	}
	var users []userRow
	if err := db.Table("users").Scan(&users).Error; err != nil {
		t.Fatalf("query while disabled: %v", err)
	}
	if events := capture.Events(); len(events) != 0 {
		t.Errorf("captured %d events while disabled, want 0", len(events))
	}
}
