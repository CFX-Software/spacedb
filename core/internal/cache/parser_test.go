package cache

import "testing"

func TestParseUpdateByIdMySQLPlaceholder(t *testing.T) {
	r := Parse("UPDATE users SET name = ? WHERE id = ?", []interface{}{"Jane", 5})
	if r.FullTable != "" {
		t.Fatalf("expected entry parse, got FullTable=%q", r.FullTable)
	}
	if len(r.Entries) != 1 {
		t.Fatalf("entries = %d want 1", len(r.Entries))
	}
	if r.Entries[0].Table != "users" || r.Entries[0].Key != "5" {
		t.Fatalf("entry = %+v want users/5", r.Entries[0])
	}
}

func TestParseUpdateByIdPostgresPlaceholder(t *testing.T) {
	r := Parse("UPDATE users SET name = $1 WHERE id = $2", []interface{}{"Jane", "abc"})
	if len(r.Entries) != 1 || r.Entries[0].Key != "abc" {
		t.Fatalf("got %+v want one entry users/abc", r)
	}
}

func TestParseUpdateByIdLiteral(t *testing.T) {
	r := Parse("UPDATE users SET name = 'Jane' WHERE id = 5", nil)
	if len(r.Entries) != 1 || r.Entries[0].Key != "5" {
		t.Fatalf("got %+v", r)
	}
}

func TestParseUpdateByIdQuotedString(t *testing.T) {
	r := Parse("UPDATE players SET score = 100 WHERE license = 'steam:xyz'", nil)
	if len(r.Entries) != 1 || r.Entries[0].Key != "steam:xyz" {
		t.Fatalf("got %+v", r)
	}
}

func TestParseDeleteByIdMySQL(t *testing.T) {
	r := Parse("DELETE FROM users WHERE id = ?", []interface{}{7})
	if len(r.Entries) != 1 || r.Entries[0].Table != "users" || r.Entries[0].Key != "7" {
		t.Fatalf("got %+v", r)
	}
}

func TestParseUpdateMultiRowFallsBackToFullTable(t *testing.T) {
	r := Parse("UPDATE users SET name = ? WHERE score > ?", []interface{}{"x", 100})
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users, got %+v", r)
	}
}

func TestParseUpdateJoinFallsBackToFullTable(t *testing.T) {
	r := Parse("UPDATE users u JOIN orders o ON u.id = o.user_id SET u.flag = 1", nil)
	if r.FullTable == "" {
		t.Fatalf("expected FullTable, got %+v", r)
	}
}

func TestParseInsertIsNoOp(t *testing.T) {
	r := Parse("INSERT INTO users (name, score) VALUES (?, ?)", []interface{}{"Jane", 1})
	if !r.NoOp {
		t.Fatalf("plain INSERT should be NoOp; got %+v", r)
	}
}

func TestParseInsertOnDuplicateInvalidatesTable(t *testing.T) {
	r := Parse(
		"INSERT INTO users (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)",
		[]interface{}{5, "Jane"},
	)
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users for upsert; got %+v", r)
	}
}

func TestParseInsertOnConflictInvalidatesTable(t *testing.T) {
	r := Parse(
		"INSERT INTO users (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
		[]interface{}{5, "Jane"},
	)
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users for postgres upsert; got %+v", r)
	}
}

func TestParseTruncate(t *testing.T) {
	r := Parse("TRUNCATE TABLE users", nil)
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users; got %+v", r)
	}
}

func TestParseReplaceInto(t *testing.T) {
	r := Parse("REPLACE INTO users (id, name) VALUES (?, ?)", []interface{}{5, "Jane"})
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users; got %+v", r)
	}
}

func TestParseUnknownStatementSafeDefault(t *testing.T) {
	r := Parse("BEGIN", nil)
	if len(r.Entries) != 0 || r.FullTable != "" || r.NoOp {
		t.Fatalf("unknown SQL should fall through to empty result; got %+v", r)
	}
}

func TestParseFormatsFloatKeyAsInt(t *testing.T) {
	// JSON unmarshals numeric IDs as float64. Make sure we don't get "5.0".
	r := Parse("UPDATE users SET name = ? WHERE id = ?", []interface{}{"Jane", float64(5)})
	if r.Entries[0].Key != "5" {
		t.Fatalf("key = %q want 5", r.Entries[0].Key)
	}
}

func TestParseUpdateBacktickedIdentifiers(t *testing.T) {
	r := Parse("UPDATE `users` SET `name` = ? WHERE `id` = ?", []interface{}{"Jane", 5})
	if len(r.Entries) != 1 || r.Entries[0].Table != "users" || r.Entries[0].Key != "5" {
		t.Fatalf("got %+v want users/5", r)
	}
}

func TestParseDeleteBacktickedTable(t *testing.T) {
	r := Parse("DELETE FROM `users` WHERE id = ?", []interface{}{9})
	if len(r.Entries) != 1 || r.Entries[0].Table != "users" || r.Entries[0].Key != "9" {
		t.Fatalf("got %+v want users/9", r)
	}
}

func TestParseReplaceIntoBackticked(t *testing.T) {
	r := Parse("REPLACE INTO `users` (id, name) VALUES (?, ?)", []interface{}{5, "Jane"})
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users; got %+v", r)
	}
}

func TestParseUpdateDoubleQuotedPostgres(t *testing.T) {
	r := Parse(`UPDATE "users" SET name = $1 WHERE id = $2`, []interface{}{"Jane", "abc"})
	if len(r.Entries) != 1 || r.Entries[0].Table != "users" || r.Entries[0].Key != "abc" {
		t.Fatalf("got %+v want users/abc", r)
	}
}

func TestParseInsertOnDuplicateBackticked(t *testing.T) {
	r := Parse(
		"INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)",
		[]interface{}{5, "Jane"},
	)
	if r.FullTable != "users" {
		t.Fatalf("expected FullTable=users; got %+v", r)
	}
}
