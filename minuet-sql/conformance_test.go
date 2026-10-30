package minuetsql

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParserConformanceDocumentedSubset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		sql  string
		want statement
	}{
		{
			name: "create table normalizes identifier",
			sql:  "CREATE TABLE Accounts",
			want: createTableStmt{name: "accounts"},
		},
		{
			name: "insert values accepts lowercase keywords",
			sql:  "insert into accounts values (1, 100)",
			want: insertStmt{table: "accounts", row: Row{ID: 1, Value: 100}},
		},
		{
			name: "select all documented projection",
			sql:  "SELECT id, value FROM accounts",
			want: selectStmt{table: "accounts"},
		},
		{
			name: "select by unsigned id",
			sql:  "SELECT id, value FROM accounts WHERE id = 2",
			want: selectStmt{table: "accounts", byID: true, id: 2},
		},
		{
			name: "delete by unsigned id",
			sql:  "DELETE FROM accounts WHERE id = 1",
			want: deleteStmt{table: "accounts", id: 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Parse(%q) = %#v, want %#v", tc.sql, got, tc.want)
			}
		})
	}
}

func TestParserConformanceRejectsDocumentedUnsupportedSyntax(t *testing.T) {
	t.Parallel()
	unsupported := []string{
		"CREATE TABLE accounts (id INTEGER, value INTEGER)",
		"INSERT INTO accounts (id, value) VALUES (1, 100)",
		"INSERT INTO accounts VALUES (-1, 100)",
		"SELECT * FROM accounts",
		"SELECT id, value FROM accounts ORDER BY id",
		"DELETE FROM accounts",
		"UPDATE accounts SET value = 101 WHERE id = 1",
		"BEGIN",
		"INSERT INTO " + strings.Repeat("ȿ", 16) + " VALUES (1, 2)",
		"SELECT id, value FROM " + strings.Repeat("ȿ", 16) + " WHERE id = 1",
		"DELETE FROM " + strings.Repeat("ȿ", 16) + " WHERE id = 1",
		"ſELECT id, value FROM accounts",
	}

	for _, sql := range unsupported {
		sql := sql
		t.Run(sql, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(sql)
			if !errors.Is(err, ErrUnsupportedSQL) {
				t.Fatalf("Parse(%q) err=%v, want ErrUnsupportedSQL", sql, err)
			}
		})
	}
}

func TestParserConformanceUint64LiteralBounds(t *testing.T) {
	t.Parallel()
	const maxUint64 = uint64(18446744073709551615)
	const overflow = "18446744073709551616"

	valid := []struct {
		sql  string
		want statement
	}{
		{
			sql:  "INSERT INTO accounts VALUES (18446744073709551615, 18446744073709551615)",
			want: insertStmt{table: "accounts", row: Row{ID: maxUint64, Value: maxUint64}},
		},
		{
			sql:  "SELECT id, value FROM accounts WHERE id = 18446744073709551615",
			want: selectStmt{table: "accounts", byID: true, id: maxUint64},
		},
		{
			sql:  "DELETE FROM accounts WHERE id = 18446744073709551615",
			want: deleteStmt{table: "accounts", id: maxUint64},
		},
	}
	for _, tc := range valid {
		got, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("Parse(%q) = %#v, want %#v", tc.sql, got, tc.want)
		}
	}

	for _, sql := range []string{
		"INSERT INTO accounts VALUES (" + overflow + ", 0)",
		"INSERT INTO accounts VALUES (0, " + overflow + ")",
		"SELECT id, value FROM accounts WHERE id = " + overflow,
		"DELETE FROM accounts WHERE id = " + overflow,
	} {
		if _, err := Parse(sql); !errors.Is(err, ErrUnsupportedSQL) {
			t.Fatalf("Parse(%q) err=%v, want ErrUnsupportedSQL", sql, err)
		}
	}
}

func TestParserConformanceIdentifierLength(t *testing.T) {
	valid := strings.Repeat("a", catalogMaxNameLen)
	if _, err := Parse("CREATE TABLE " + valid); err != nil {
		t.Fatalf("maximum-length identifier rejected: %v", err)
	}
	if _, err := Parse("CREATE TABLE " + valid + "a"); !errors.Is(err, ErrUnsupportedSQL) {
		t.Fatalf("overlong identifier error = %v, want ErrUnsupportedSQL", err)
	}
}

func FuzzParseNeverPanics(f *testing.F) {
	f.Add("SELECT id, value FROM accounts")
	f.Add("CREATE TABLE accounts")
	f.Add("")
	f.Fuzz(func(t *testing.T, sql string) {
		_, _ = Parse(sql)
	})
}

func TestPlannerConformanceNormalizesTwoTableHistorySequence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sql      string
		wantPlan string
		wantErr  error
	}{
		{sql: "CREATE TABLE generated_parents", wantPlan: "create"},
		{sql: "CREATE TABLE generated_children", wantPlan: "create"},
		{sql: "INSERT INTO generated_parents VALUES (7, 70)", wantPlan: "insert"},
		{sql: "INSERT INTO generated_children VALUES (1, 7)", wantPlan: "insert"},
		{sql: "SELECT id, value FROM generated_parents", wantPlan: "select"},
		{sql: "DELETE FROM generated_children WHERE id = 1", wantPlan: "delete"},
		{sql: "SELECT id, value FROM generated_parents JOIN generated_children ON generated_parents.id = generated_children.value", wantErr: ErrUnsupportedSQL},
		{sql: "UPDATE generated_parents SET value = 71 WHERE id = 7", wantErr: ErrUnsupportedSQL},
		{sql: "BEGIN", wantErr: ErrUnsupportedSQL},
		{sql: "ROLLBACK", wantErr: ErrUnsupportedSQL},
		{sql: "COMMIT", wantErr: ErrUnsupportedSQL},
	}

	for _, tc := range tests {
		stmt, err := Parse(tc.sql)
		if err == nil {
			var plan planNode
			plan, err = Plan(stmt)
			switch plan.(type) {
			case createTablePlan:
				if tc.wantPlan != "create" {
					t.Fatalf("Plan(%q) normalized as create, want %q", tc.sql, tc.wantPlan)
				}
			case insertPlan:
				if tc.wantPlan != "insert" {
					t.Fatalf("Plan(%q) normalized as insert, want %q", tc.sql, tc.wantPlan)
				}
			case selectPlan:
				if tc.wantPlan != "select" {
					t.Fatalf("Plan(%q) normalized as select, want %q", tc.sql, tc.wantPlan)
				}
			case deletePlan:
				if tc.wantPlan != "delete" {
					t.Fatalf("Plan(%q) normalized as delete, want %q", tc.sql, tc.wantPlan)
				}
			case nil:
			default:
				t.Fatalf("Plan(%q) returned unexpected plan %T", tc.sql, plan)
			}
		}
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("Parse/Plan(%q) err=%v, want %v", tc.sql, err, tc.wantErr)
		}
	}
}

func TestSemanticConformanceRejectedTransactionBoundariesDoNotGroupStatements(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)
	for _, sql := range []string{
		"CREATE TABLE generated_parents",
		"CREATE TABLE generated_children",
		"INSERT INTO generated_parents VALUES (7, 70)",
		"INSERT INTO generated_children VALUES (1, 7)",
	} {
		if _, err := e.Exec(ctx, sql); err != nil {
			t.Fatalf("Exec(%q): %v", sql, err)
		}
	}

	for _, sql := range []string{
		"SELECT id, value FROM generated_parents JOIN generated_children ON generated_parents.id = generated_children.value",
		"UPDATE generated_parents SET value = 71 WHERE id = 7",
		"BEGIN",
	} {
		if _, err := e.Exec(ctx, sql); !errors.Is(err, ErrUnsupportedSQL) {
			t.Fatalf("Exec(%q) err=%v, want ErrUnsupportedSQL", sql, err)
		}
	}
	if _, err := e.Exec(ctx, "DELETE FROM generated_children WHERE id = 1"); err != nil {
		t.Fatalf("delete child: %v", err)
	}
	for _, boundary := range []string{"ROLLBACK", "COMMIT"} {
		if _, err := e.Exec(ctx, boundary); !errors.Is(err, ErrUnsupportedSQL) {
			t.Fatalf("Exec(%q) err=%v, want ErrUnsupportedSQL", boundary, err)
		}
	}

	parents, err := e.Exec(ctx, "SELECT id, value FROM generated_parents")
	if err != nil || !reflect.DeepEqual(parents.Rows, []Row{{ID: 7, Value: 70}}) {
		t.Fatalf("parent rows=%#v err=%v", parents.Rows, err)
	}
	children, err := e.Exec(ctx, "SELECT id, value FROM generated_children")
	if err != nil || len(children.Rows) != 0 {
		t.Fatalf("child rows=%#v err=%v", children.Rows, err)
	}
}

func TestSemanticConformanceDocumentedSubset(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(t)

	statements := []string{
		"CREATE TABLE accounts",
		"insert into accounts values (1, 100)",
		"INSERT INTO accounts VALUES (2, 250)",
	}
	for _, sql := range statements {
		if _, err := e.Exec(ctx, sql); err != nil {
			t.Fatalf("Exec(%q): %v", sql, err)
		}
	}

	all, err := e.Exec(ctx, "SELECT id, value FROM accounts")
	if err != nil {
		t.Fatalf("select all: %v", err)
	}
	if !reflect.DeepEqual(all.Rows, []Row{{ID: 1, Value: 100}, {ID: 2, Value: 250}}) {
		t.Fatalf("select all rows=%#v", all.Rows)
	}

	one, err := e.Exec(ctx, "SELECT id, value FROM accounts WHERE id = 2")
	if err != nil {
		t.Fatalf("select by id: %v", err)
	}
	if !reflect.DeepEqual(one.Rows, []Row{{ID: 2, Value: 250}}) {
		t.Fatalf("select by id rows=%#v", one.Rows)
	}

	missing, err := e.Exec(ctx, "SELECT id, value FROM accounts WHERE id = 99")
	if err != nil {
		t.Fatalf("select missing id: %v", err)
	}
	if len(missing.Rows) != 0 {
		t.Fatalf("select missing rows=%#v, want empty", missing.Rows)
	}

	if _, err := e.Exec(ctx, "DELETE FROM accounts WHERE id = 1"); err != nil {
		t.Fatalf("delete existing row: %v", err)
	}
	if _, err := e.Exec(ctx, "DELETE FROM accounts WHERE id = 99"); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("delete missing err=%v, want ErrRowNotFound", err)
	}
}
