package dolt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type RBDOperation struct {
	ID       string   `json:"id"`
	Digest   string   `json:"digest"`
	Message  string   `json:"message"`
	CloseIDs []string `json:"close_ids,omitempty"`
	conn     *rbdConn
	tx       driver.Tx
	failure  error
	writes   int
}

var rbdOperation *RBDOperation
var rbdDriverEnabled bool
var rbdDeferredVersionCall = regexp.MustCompile(`^CALL\s+DOLT_(ADD|COMMIT)\s*\(`)

func init() { sql.Register("rbd-mysql", rbdDriver{}) }

func ConfigureRBDDriver(enabled bool) { rbdDriverEnabled = enabled }

func BeginRBDOperation(raw string) error {
	if raw == "" {
		return nil
	}
	var op RBDOperation
	if err := json.Unmarshal([]byte(raw), &op); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(op.ID) || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(op.Digest) || op.Message == "" || len(op.Message) > 2048 {
		return errors.New("invalid rbd operation identity")
	}
	for _, id := range op.CloseIDs {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(id) {
			return errors.New("invalid rbd closing id")
		}
	}
	if rbdOperation != nil {
		return errors.New("rbd operation already active")
	}
	rbdOperation = &op
	return nil
}

func RBDOperationActive() bool { return rbdOperation != nil }

func FinishRBDOperation(ctx context.Context) error {
	op := rbdOperation
	if op == nil {
		return nil
	}
	defer func() { rbdOperation = nil }()
	if op.conn == nil {
		return errors.New("rbd operation opened no managed connection")
	}
	defer func() {
		if op.tx != nil {
			_ = op.tx.Rollback()
		}
	}()
	if op.failure != nil {
		return op.failure
	}
	c := op.conn
	for _, id := range op.CloseIDs {
		if _, err := c.rawExec(ctx, "DELETE FROM labels WHERE issue_id = ? AND label LIKE 'stage:%' AND EXISTS (SELECT 1 FROM issues WHERE id = ? AND status = 'closed')", []driver.NamedValue{{Ordinal: 1, Value: id}, {Ordinal: 2, Value: id}}); err != nil {
			return err
		}
	}
	body, _ := json.Marshal(map[string]any{"version": 1, "id": op.ID, "digest": op.Digest})
	if _, err := c.rawExec(ctx, "INSERT INTO metadata (`key`, value) VALUES (?, ?)", []driver.NamedValue{{Ordinal: 1, Value: "rbd.operation." + op.ID}, {Ordinal: 2, Value: string(body)}}); err != nil {
		return err
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, "SELECT table_name FROM dolt_status WHERE table_name <> 'config'", nil)
	if err != nil {
		return err
	}
	var tables []driver.NamedValue
	values := make([]driver.Value, len(rows.Columns()))
	for {
		err = rows.Next(values)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = rows.Close()
			return err
		}
		name := fmt.Sprint(values[0])
		if b, ok := values[0].([]byte); ok {
			name = string(b)
		}
		tables = append(tables, driver.NamedValue{Ordinal: len(tables) + 1, Value: name})
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(tables) == 0 {
		return errors.New("rbd operation marker is not versioned")
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(tables)), ",")
	if _, err := c.rawExec(ctx, "CALL DOLT_ADD("+marks+")", tables); err != nil {
		return err
	}
	author := os.Getenv("RBD_COMMIT_AUTHOR")
	if author == "" {
		author = "beads <beads@local>"
	}
	if _, err := c.rawExec(ctx, "CALL DOLT_COMMIT('-m', ?, '--author', ?)", []driver.NamedValue{{Ordinal: 1, Value: op.Message}, {Ordinal: 2, Value: author}}); err != nil {
		return fmt.Errorf("rbd commit outcome uncertain: %v", err)
	}
	if err := op.tx.Commit(); err != nil {
		return fmt.Errorf("rbd commit outcome uncertain: %v", err)
	}
	op.tx = nil
	return nil
}

type rbdDriver struct{}

func (rbdDriver) Open(name string) (driver.Conn, error) {
	c, err := (&mysql.MySQLDriver{}).Open(name)
	if err != nil {
		return nil, err
	}
	return &rbdConn{Conn: c}, nil
}

type rbdConn struct{ driver.Conn }

func (c *rbdConn) ensure(ctx context.Context) error {
	op := rbdOperation
	if op == nil {
		return nil
	}
	if op.failure != nil {
		return op.failure
	}
	if op.conn != nil && op.conn != c {
		return c.failed(errors.New("rbd operation refused a second SQL connection"))
	}
	if op.tx == nil {
		tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, driver.TxOptions{})
		if err != nil {
			return c.failed(err)
		}
		op.conn, op.tx = c, tx
	}
	return nil
}

func (c *rbdConn) failed(err error) error {
	if err != nil && err != driver.ErrSkip && rbdOperation != nil {
		rbdOperation.failure = fmt.Errorf("rbd transaction aborted without replay: %v", err)
		return rbdOperation.failure
	}
	return err
}

func (c *rbdConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *rbdConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if rbdOperation == nil {
		return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	}
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	return rbdLogicalTx{op: rbdOperation, writes: rbdOperation.writes}, nil
}

type rbdLogicalTx struct {
	op     *RBDOperation
	writes int
}

func (rbdLogicalTx) Commit() error { return nil }
func (t rbdLogicalTx) Rollback() error {
	if t.op.writes > t.writes && t.op.failure == nil {
		t.op.failure = errors.New("rbd command rolled back a logical mutation; entire command aborted")
	}
	return nil
}

func (c *rbdConn) statement(query string, executing bool) (bool, error) {
	if rbdOperation == nil {
		return false, nil
	}
	q := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(query)), ";")
	if q == "COMMIT" || q == "START TRANSACTION" || q == "BEGIN" {
		return true, nil
	}
	if rbdDeferredVersionCall.MatchString(q) {
		return true, nil
	}
	if strings.HasPrefix(q, "ROLLBACK TO ") {
		return false, nil
	}
	words := strings.Fields(q)
	if len(words) == 0 {
		return false, c.failed(errors.New("empty SQL in rbd command"))
	}
	switch words[0] {
	case "SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "WITH", "SAVEPOINT", "RELEASE":
		return false, nil
	case "INSERT", "UPDATE", "REPLACE", "DELETE":
		if executing {
			rbdOperation.writes++
		}
		return false, nil
	default:
		return false, c.failed(errors.New("unexpected transaction boundary or SQL statement in rbd command: " + words[0]))
	}
}

func (c *rbdConn) rawExec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *rbdConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	noop, err := c.statement(query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return driver.RowsAffected(0), nil
	}
	r, err := c.rawExec(ctx, query, args)
	return r, c.failed(err)
}

func (c *rbdConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	noop, err := c.statement(query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return &rbdNoopRows{}, nil
	}
	r, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	return r, c.failed(err)
}

func (c *rbdConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *rbdConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	noop, err := c.statement(query, false)
	if err != nil {
		return nil, err
	}
	if noop {
		return rbdNoopStmt{}, nil
	}
	s, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		return nil, c.failed(err)
	}
	return &rbdStmt{Stmt: s, conn: c, query: query}, nil
}

func (c *rbdConn) ResetSession(ctx context.Context) error {
	if rbdOperation != nil {
		return nil
	}
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *rbdConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }
func (c *rbdConn) CheckNamedValue(v *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(v)
}

func (c *rbdConn) Close() error {
	if op := rbdOperation; op != nil && op.conn == c {
		c.failed(errors.New("rbd command closed its transaction connection"))
		if op.tx != nil {
			_ = op.tx.Rollback()
			op.tx = nil
		}
	}
	return c.Conn.Close()
}

type rbdStmt struct {
	driver.Stmt
	conn  *rbdConn
	query string
}

type rbdNoopRows struct{ read bool }

func (*rbdNoopRows) Columns() []string { return []string{"status"} }
func (*rbdNoopRows) Close() error      { return nil }
func (r *rbdNoopRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0] = int64(0)
	return nil
}

type rbdNoopStmt struct{}

func (rbdNoopStmt) Close() error                               { return nil }
func (rbdNoopStmt) NumInput() int                              { return -1 }
func (rbdNoopStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(0), nil }
func (rbdNoopStmt) Query([]driver.Value) (driver.Rows, error)  { return &rbdNoopRows{}, nil }
func (rbdNoopStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (rbdNoopStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return &rbdNoopRows{}, nil
}

func (s *rbdStmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.conn.ensure(context.Background()); err != nil {
		return nil, err
	}
	noop, err := s.conn.statement(s.query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return driver.RowsAffected(0), nil
	}
	r, err := s.Stmt.Exec(args)
	return r, s.conn.failed(err)
}

func (s *rbdStmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.conn.ensure(context.Background()); err != nil {
		return nil, err
	}
	noop, err := s.conn.statement(s.query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return &rbdNoopRows{}, nil
	}
	r, err := s.Stmt.Query(args)
	return r, s.conn.failed(err)
}

func (s *rbdStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.ensure(ctx); err != nil {
		return nil, err
	}
	noop, err := s.conn.statement(s.query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return driver.RowsAffected(0), nil
	}
	r, err := s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	return r, s.conn.failed(err)
}

func (s *rbdStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.conn.ensure(ctx); err != nil {
		return nil, err
	}
	noop, err := s.conn.statement(s.query, true)
	if err != nil {
		return nil, err
	}
	if noop {
		return &rbdNoopRows{}, nil
	}
	r, err := s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	return r, s.conn.failed(err)
}
