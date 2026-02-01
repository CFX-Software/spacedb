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
