package dataflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"testing"
)

// fakeDriver is a minimal database/sql driver whose rows carry one value.
type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{}, nil }

type fakeConn struct{}

func (*fakeConn) Prepare(q string) (driver.Stmt, error) { return &fakeStmt{q: q}, nil }
func (*fakeConn) Close() error                          { return nil }
func (*fakeConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }

func TestStmtSummary(t *testing.T) {
	cases := map[string]string{
		"SELECT id, total FROM orders WHERE id = $1":                   "SELECT orders",
		"  insert into users (email) values ($1)":                      "INSERT users",
		"UPDATE public.items SET total = total - 1":                    "UPDATE items",
		"DELETE FROM sessions WHERE expires < now()":                   "DELETE sessions",
		"CREATE TABLE IF NOT EXISTS migrations (id int)":               "CREATE migrations",
		"select u.id\nfrom users u\njoin orders o on o.user_id = u.id": "SELECT users",
		"PRAGMA journal_mode=WAL":                                      "PRAGMA",
	}
	for q, want := range cases {
		if got := stmtSummary(q); got != want {
			t.Errorf("stmtSummary(%q) = %q, want %q", q, got, want)
		}
	}
}

func TestClipStatement(t *testing.T) {
	long := "SELECT " + strings.Repeat("x, ", 100) + "1"
	if got := clipStatement(long); len(got) != 200 {
		t.Errorf("clip length = %d, want 200", len(got))
	}
}

// The driver proxy must keep the database/sql contract working end to
// end even when tracing is disabled (the default in tests).
func TestDriverProxyPassesThrough(t *testing.T) {
	name := Driver("fake", fakeDriver{})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query: %v", err)
	}
	if one != 1 {
		t.Errorf("row = %d", one)
	}
	if _, err := db.Exec("CREATE TABLE x (id int)"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	// Second call must be stable (sql.Register panics on duplicates).
	if again := Driver("fake", fakeDriver{}); again != name {
		t.Errorf("driver name not stable: %q vs %q", again, name)
	}
}

// Unused helpers kept minimal.
type fakeStmt struct{ q string }

func (*fakeStmt) Close() error  { return nil }
func (*fakeStmt) NumInput() int { return -1 }
func (*fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (*fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return &fakeRows{}, nil
}

type fakeRows struct{ done bool }

func (*fakeRows) Columns() []string { return []string{"one"} }
func (*fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	if len(dest) > 0 {
		dest[0] = int64(1)
	}
	return nil
}

var (
	_ driver.Driver = fakeDriver{}
	_ driver.Conn   = (*fakeConn)(nil)
	_ driver.Stmt   = (*fakeStmt)(nil)
	_ driver.Rows   = (*fakeRows)(nil)
	_               = context.Background
)
