package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/steveyegge/beads/internal/storage/schema"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:"+os.Getenv("RARE_SEED_PORT")+")/rare?parseTime=true&multiStatements=true")
	if err != nil { panic(err) }
	defer db.Close()
	db.SetMaxOpenConns(1)
	applied, err := schema.MigrateUp(ctx, db)
	if err != nil { panic(err) }
	for _, query := range []string{
		"REPLACE INTO config (`key`, value) VALUES ('issue_prefix', 'rare')",
		"CALL DOLT_ADD('-A')",
		"CALL DOLT_COMMIT('-m', 'synthetic machine-output regression seed', '--author', 'fixture <fixture@local>')",
	} {
		if _, err := db.ExecContext(ctx, query); err != nil { panic(err) }
	}
	fmt.Printf("synthetic v1.3.4 seed: %d applied migrations\n", applied)
}
