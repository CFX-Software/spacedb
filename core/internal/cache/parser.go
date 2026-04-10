package cache

import (
	"fmt"
	"regexp"
	"strings"
)

// Affected names a (table, key) pair that should be evicted from the cache
// after a write completes.
type Affected struct {
	Table string
	Key   string
}

// ParseResult describes the cache impact of a write statement.
type ParseResult struct {
	// Entries are specific rows we know are dirty. Empty when the parser
	// can't be sure (FullTable will be set instead).
	Entries []Affected
	// FullTable, when non-empty, means the parser could not pin down
	// individual rows and the caller should invalidate the entire table.
	FullTable string
	// NoOp means the SQL is a write but the cache cannot possibly hold a
	// stale entry for it (e.g., INSERT without an ON DUPLICATE KEY clause —
	// the row didn't exist before, so it can't be in the cache).
	NoOp bool
}

// Lowercased + whitespace-collapsed view of a SQL statement, used so the
// regexes don't need (?i) and can stay simple.
var spaceRe = regexp.MustCompile(`\s+`)

func normalize(sql string) string {
	return spaceRe.ReplaceAllString(strings.TrimSpace(strings.ToLower(sql)), " ")
}

// Match `update <table> set ... where <col> = <value>` where <value> is a
// placeholder `?`, `$N`, a numeric literal, or a single-quoted string.
var updateRe = regexp.MustCompile(
	`^update\s+([a-z_][a-z0-9_]*)\s+set\s+(.+?)\s+where\s+([a-z_][a-z0-9_]*)\s*=\s*(\?|\$\d+|\d+|'[^']*')\s*;?\s*$`,
)

var deleteRe = regexp.MustCompile(
	`^delete\s+from\s+([a-z_][a-z0-9_]*)\s+where\s+([a-z_][a-z0-9_]*)\s*=\s*(\?|\$\d+|\d+|'[^']*')\s*;?\s*$`,
)

var simpleInsertRe = regexp.MustCompile(
	`^insert\s+into\s+([a-z_][a-z0-9_]*)\s*\(`,
)

// onDuplicateRe spots the MySQL upsert tail. Postgres uses ON CONFLICT.
var onDuplicateRe = regexp.MustCompile(`on\s+duplicate\s+key`)
var onConflictRe = regexp.MustCompile(`on\s+conflict`)

// Parse inspects an UPDATE/DELETE/INSERT statement and returns the cache
// impact. params are the bound values for `?` placeholders, in order.
//
// The parser is intentionally conservative: when the WHERE clause is
// anything other than `<col> = <single value>`, it returns FullTable so
// the caller drops the entire table from the cache (correctness over
// efficiency).
func Parse(sql string, params []interface{}) ParseResult {
	norm := normalize(sql)
	if norm == "" {
		return ParseResult{NoOp: true}
	}

	if onDuplicateRe.MatchString(norm) || onConflictRe.MatchString(norm) {
		// Upsert: row may or may not exist. Match the table off the
		// INSERT INTO prefix and drop the whole table.
		if m := simpleInsertRe.FindStringSubmatch(norm); m != nil {
			return ParseResult{FullTable: m[1]}
		}
		return ParseResult{}
	}

	switch {
	case strings.HasPrefix(norm, "update "):
		m := updateRe.FindStringSubmatch(norm)
		if m == nil {
			// UPDATE we can't parse — drop the whole table.
			table := extractFirstIdentAfter(norm, "update ")
			return ParseResult{FullTable: table}
		}
		key, ok := resolveValue(m[4], params, sql, true)
		if !ok {
			return ParseResult{FullTable: m[1]}
		}
		return ParseResult{Entries: []Affected{{Table: m[1], Key: key}}}

	case strings.HasPrefix(norm, "delete "):
		m := deleteRe.FindStringSubmatch(norm)
		if m == nil {
			table := extractFirstIdentAfter(norm, "delete from ")
			return ParseResult{FullTable: table}
		}
		key, ok := resolveValue(m[3], params, sql, true)
		if !ok {
			return ParseResult{FullTable: m[1]}
		}
		return ParseResult{Entries: []Affected{{Table: m[1], Key: key}}}

	case strings.HasPrefix(norm, "insert "):
		// Plain INSERT can't dirty an existing cache entry — the row
		// didn't exist before. NoOp.
		return ParseResult{NoOp: true}

	case strings.HasPrefix(norm, "replace "):
		// REPLACE INTO <table> (...): nuke the whole table.
		return ParseResult{FullTable: extractFirstIdentAfter(norm, "into ")}

	case strings.HasPrefix(norm, "truncate "):
		// "TRUNCATE <table>" or "TRUNCATE TABLE <table>".
		rest := strings.TrimPrefix(norm, "truncate ")
		rest = strings.TrimPrefix(rest, "table ")
		return ParseResult{FullTable: firstIdent(rest)}
	}

	// Unknown write shape — be safe.
	return ParseResult{}
}

func extractFirstIdentAfter(norm, marker string) string {
	idx := strings.Index(norm, marker)
	if idx < 0 {
		return ""
	}
	return firstIdent(norm[idx+len(marker):])
}

func firstIdent(s string) string {
	end := 0
	for end < len(s) {
		c := s[end]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			end++
		} else {
			break
		}
	}
	return s[:end]
}

// resolveValue turns the WHERE-clause RHS into a string key. Placeholders
// (`?` or `$N`) are resolved against params; literals are returned verbatim.
// The trailingPlaceholder hint is used when the SQL has a single `?` and we
// want params[len(params)-1] rather than scanning the SQL for the offset.
func resolveValue(token string, params []interface{}, originalSQL string, trailingPlaceholder bool) (string, bool) {
	if token == "?" {
		// Count `?` occurrences in originalSQL up to and including the
		// WHERE clause. The WHERE `?` is at index N-1 where N is the
		// total. trailingPlaceholder lets us shortcut: if true, use last.
		if trailingPlaceholder && len(params) > 0 {
			return formatParam(params[len(params)-1]), true
		}
		count := strings.Count(originalSQL, "?")
		if count == 0 || count > len(params) {
			return "", false
		}
		return formatParam(params[count-1]), true
	}
	if strings.HasPrefix(token, "$") {
		// Postgres-style $1, $2, ...
		var idx int
		_, err := fmt.Sscanf(token, "$%d", &idx)
		if err != nil || idx < 1 || idx > len(params) {
			return "", false
		}
		return formatParam(params[idx-1]), true
	}
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		return token[1 : len(token)-1], true
	}
	// Numeric literal.
	return token, true
}

func formatParam(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		// JSON numbers come in as float64. Trim trailing .0 for whole values.
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%v", x)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
