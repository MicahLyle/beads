package dolt

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

type rbdFake struct {
	queries                    []string
	begins, commits, rollbacks int
	fail                       string
}

func (f *rbdFake) Prepare(string) (driver.Stmt, error)                          { return nil, errors.New("unused") }
func (f *rbdFake) Close() error                                                 { return nil }
func (f *rbdFake) Begin() (driver.Tx, error)                                    { f.begins++; return f, nil }
func (f *rbdFake) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) { return f.Begin() }
func (f *rbdFake) Commit() error                                                { f.commits++; return nil }
func (f *rbdFake) Rollback() error                                              { f.rollbacks++; return nil }
func (f *rbdFake) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	f.queries = append(f.queries, q)
	if f.fail != "" && strings.Contains(q, f.fail) {
		return nil, errors.New("fixture SQL failure")
	}
	return driver.RowsAffected(1), nil
}
func (f *rbdFake) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	f.queries = append(f.queries, q)
	return &rbdFakeRows{}, nil
}

type rbdFakeRows struct{ read bool }

func (*rbdFakeRows) Columns() []string { return []string{"table_name"} }
func (*rbdFakeRows) Close() error      { return nil }
func (r *rbdFakeRows) Next(v []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	v[0] = []byte("metadata")
	return nil
}

func rbdFixture(t *testing.T) (*rbdConn, *rbdFake) {
	t.Helper()
	rbdOperation = nil
	t.Cleanup(func() { rbdOperation = nil })
	request, _ := json.Marshal(map[string]any{"id": strings.Repeat("a", 32), "digest": strings.Repeat("b", 64), "message": "bd: fixture"})
	if err := BeginRBDOperation(string(request)); err != nil {
		t.Fatal(err)
	}
	fake := &rbdFake{}
	return &rbdConn{Conn: fake}, fake
}

func TestRBDCommandGroupsLogicalTransactionsAndReadRollbacks(t *testing.T) {
	c, f := rbdFixture(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		tx, err := c.BeginTx(ctx, driver.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		query := "UPDATE issues SET notes = 'fixture'"
		if i == 0 {
			query = "SELECT notes FROM issues"
		}
		if _, err := c.ExecContext(ctx, query, nil); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}
	if f.commits != 0 || f.rollbacks != 0 || f.begins != 1 {
		t.Fatalf("logical boundaries escaped: %+v", f)
	}
	if err := FinishRBDOperation(ctx); err != nil {
		t.Fatal(err)
	}
	if f.commits != 1 || f.rollbacks != 0 {
		t.Fatalf("wrong terminal boundary: %+v", f)
	}
	joined := strings.Join(f.queries, "\n")
	if strings.Index(joined, "INSERT INTO metadata") > strings.Index(joined, "DOLT_COMMIT") {
		t.Fatal("marker followed commit")
	}
}

func TestRBDSQLFailureAbortsWithoutRetryOrMarker(t *testing.T) {
	c, f := rbdFixture(t)
	f.fail = "UPDATE"
	if _, err := c.ExecContext(context.Background(), "UPDATE issues", nil); err == nil {
		t.Fatal("SQL failure ignored")
	}
	if err := FinishRBDOperation(context.Background()); err == nil {
		t.Fatal("failed command committed")
	}
	if f.commits != 0 || f.rollbacks != 1 || len(f.queries) != 1 {
		t.Fatalf("failed command escaped rollback: %+v", f)
	}
}

func TestRBDSecondConnectionAndPrematureCommitRefuse(t *testing.T) {
	for _, second := range []bool{false, true} {
		c, f := rbdFixture(t)
		if _, err := c.ExecContext(context.Background(), "UPDATE issues", nil); err != nil {
			t.Fatal(err)
		}
		if second {
			c = &rbdConn{Conn: &rbdFake{}}
		}
		query := "CALL DOLT_CHECKOUT('unexpected')"
		if second {
			query = "UPDATE issues"
		}
		if _, err := c.ExecContext(context.Background(), query, nil); err == nil {
			t.Fatal("unsafe boundary accepted")
		}
		if err := FinishRBDOperation(context.Background()); err == nil {
			t.Fatal("unsafe command committed")
		}
		if f.commits != 0 || f.rollbacks != 1 {
			t.Fatalf("unsafe command escaped rollback: %+v", f)
		}
	}
}

func TestRBDMarkerFailureRollsBackMutation(t *testing.T) {
	c, f := rbdFixture(t)
	_, _ = c.ExecContext(context.Background(), "UPDATE issues", nil)
	f.fail = "INSERT INTO metadata"
	if err := FinishRBDOperation(context.Background()); err == nil {
		t.Fatal("marker failure ignored")
	}
	if f.commits != 0 || f.rollbacks != 1 {
		t.Fatalf("mutation without marker: %+v", f)
	}
}

func TestRBDLogicalMutationRollbackCannotBeSilentlyCommitted(t *testing.T) {
	c, f := rbdFixture(t)
	tx, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.ExecContext(context.Background(), "UPDATE issues", nil)
	_ = tx.Rollback()
	if err := FinishRBDOperation(context.Background()); err == nil {
		t.Fatal("logical rollback silently committed")
	}
	if f.commits != 0 || f.rollbacks != 1 {
		t.Fatalf("rollback escaped: %+v", f)
	}
}

func TestRBDLegacyStagingAndVersionCommitsAreDeferred(t *testing.T) {
	c, f := rbdFixture(t)
	ctx := context.Background()
	_, _ = c.ExecContext(ctx, "UPDATE issues", nil)
	if _, err := c.ExecContext(ctx, "CALL DOLT_ADD('issues')", nil); err != nil {
		t.Fatal(err)
	}
	rows, err := c.QueryContext(ctx, "CALL DOLT_COMMIT('-m','legacy')", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if len(f.queries) != 1 {
		t.Fatal("legacy version boundary escaped")
	}
	if err := FinishRBDOperation(ctx); err != nil {
		t.Fatal(err)
	}
	if f.commits != 1 {
		t.Fatal("command did not commit once")
	}
}

func TestRBDPreparedVersionCallsCannotEscapeFinalCommit(t *testing.T) {
	c, f := rbdFixture(t)
	s := &rbdStmt{conn: c, query: "CALL DOLT_COMMIT('-m','prepared before operation')"}
	if _, err := s.Exec(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecContext(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.QueryContext(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(f.queries) != 0 {
		t.Fatal("prepared version call escaped")
	}
	if err := FinishRBDOperation(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.commits != 1 {
		t.Fatal("command did not commit once")
	}
}
