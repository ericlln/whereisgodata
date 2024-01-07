package db

import (
	"context"
	"github.com/ericlln/whereisgodata/internal/config"
	"github.com/jackc/pgx/v5"

	"testing"
)

func TestInsertRow(t *testing.T) {
	ctx := context.Background()

	pg, err := NewPG(ctx, config.GetConfig().DatabaseUrl)
	if err != nil {
		t.Fatalf("Expected no error for pg, got %v", err)
	}

	countQuery := "SELECT COUNT(*) FROM test"
	insertQuery := "INSERT INTO test (test) VALUES (@test)"

	initial := 0
	err = pg.Db.QueryRow(ctx, countQuery).Scan(&initial)
	if err != nil {
		t.Fatalf("Error getting row count from test table, with error: %v", err)
	}

	// Insert rows into table
	args := pgx.NamedArgs{
		"test": "test1",
	}
	_, err = pg.Db.Exec(ctx, insertQuery, args)
	if err != nil {
		t.Fatalf("Unable to insert row, with error: %v", err)
	}

	// Assert count of rows after insertion is as expected
	final := 0
	err = pg.Db.QueryRow(ctx, countQuery).Scan(&final)
	if err != nil {
		t.Fatalf("Error getting row count from test table, with error: %v", err)
	}

	if initial+1 != final {
		t.Fatalf("Error inserting row into test table")
	}

	pg.Db.Close()
}

func TestSingletonBehavior(t *testing.T) {
	ctx := context.Background()
	dbUrl := config.GetConfig().DatabaseUrl

	pg1, err := NewPG(ctx, dbUrl)
	if err != nil {
		t.Fatalf("Expected no error for pg1, got %v", err)
	}

	pg2, err := NewPG(ctx, dbUrl)
	if err != nil {
		t.Fatalf("Expected no error for pg2, got %v", err)
	}

	if pg1 != pg2 {
		t.Fatalf("Expected pg1 and pg2 to point to the same database connection instance")
	}

	pg1.Db.Close()
	pg2.Db.Close()
}
