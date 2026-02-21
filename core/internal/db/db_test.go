package db

import (
	"testing"

	"github.com/inkwell/spacedb/core/internal/config"
)

func TestRebindPostgresPlaceholders(t *testing.T) {
	store := Store{cfg: config.Config{Database: config.DatabaseConfig{Driver: "postgres"}}}

	got := store.rebind("SELECT * FROM users WHERE id = ? AND name = '?' AND note = ?")
	want := "SELECT * FROM users WHERE id = $1 AND name = '?' AND note = $2"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRebindLeavesMySQLPlaceholders(t *testing.T) {
	store := Store{cfg: config.Config{Database: config.DatabaseConfig{Driver: "mysql"}}}

	got := store.rebind("SELECT * FROM users WHERE id = ?")
	want := "SELECT * FROM users WHERE id = ?"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestSplitInsertValuesBuildsMultiRowInsert(t *testing.T) {
	build, placeholders, ok := splitInsertValues("INSERT INTO users (name, score) VALUES (?, ?)")
	if !ok {
		t.Fatal("expected insert values query to use fast path")
	}
	if placeholders != 2 {
		t.Fatalf("got %d placeholders want 2", placeholders)
	}

	got, params, err := build(3)
	if err != nil {
		t.Fatal(err)
	}
	want := "INSERT INTO users (name, score) VALUES (?, ?),(?, ?),(?, ?)"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if cap(params) != 6 {
		t.Fatalf("got params cap %d want 6", cap(params))
	}
}
