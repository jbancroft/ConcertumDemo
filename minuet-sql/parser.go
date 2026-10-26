package minuetsql

import (
	"fmt"
	"strconv"
	"strings"
)

type statement interface{ isStatement() }

type createTableStmt struct{ name string }
type insertStmt struct {
	table string
	row   Row
}
type selectStmt struct {
	table string
	byID  bool
	id    uint64
}
type deleteStmt struct {
	table string
	id    uint64
}

func (createTableStmt) isStatement() {}
func (insertStmt) isStatement()      {}
func (selectStmt) isStatement()      {}
func (deleteStmt) isStatement()      {}

func Parse(sql string) (statement, error) {
	s := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	upper := strings.ToUpper(s)
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE "):
		name, err := parseIdentifier(strings.TrimSpace(s[len("CREATE TABLE "):]))
		if err != nil {
			return nil, fmt.Errorf("%w: table name %q", ErrUnsupportedSQL, strings.TrimSpace(s[len("CREATE TABLE "):]))
		}
		return createTableStmt{name: name}, nil
	case strings.HasPrefix(upper, "INSERT INTO "):
		rest := strings.TrimSpace(s[len("INSERT INTO "):])
		upRest := strings.ToUpper(rest)
		idx := strings.Index(upRest, " VALUES ")
		if idx < 0 {
			return nil, fmt.Errorf("%w: insert syntax", ErrUnsupportedSQL)
		}
		table, err := parseIdentifier(rest[:idx])
		if err != nil {
			return nil, fmt.Errorf("%w: table name %q", ErrUnsupportedSQL, strings.TrimSpace(rest[:idx]))
		}
		vals := strings.TrimSpace(rest[idx+len(" VALUES "):])
		if !strings.HasPrefix(vals, "(") || !strings.HasSuffix(vals, ")") {
			return nil, fmt.Errorf("%w: insert values", ErrUnsupportedSQL)
		}
		vals = strings.TrimPrefix(vals, "(")
		vals = strings.TrimSuffix(vals, ")")
		cols := strings.Split(vals, ",")
		if len(cols) != 2 {
			return nil, fmt.Errorf("%w: insert expects two values", ErrUnsupportedSQL)
		}
		id, err := parseUint(cols[0])
		if err != nil {
			return nil, err
		}
		value, err := parseUint(cols[1])
		if err != nil {
			return nil, err
		}
		return insertStmt{table: table, row: Row{ID: id, Value: value}}, nil
	case strings.HasPrefix(upper, "SELECT "):
		if !strings.HasPrefix(upper, "SELECT ID, VALUE FROM ") {
			return nil, fmt.Errorf("%w: select projection", ErrUnsupportedSQL)
		}
		rest := s[len("SELECT ID, VALUE FROM "):]
		upRest := upper[len("SELECT ID, VALUE FROM "):]
		if idx := strings.Index(upRest, " WHERE ID = "); idx >= 0 {
			table, err := parseIdentifier(rest[:idx])
			if err != nil {
				return nil, fmt.Errorf("%w: table name %q", ErrUnsupportedSQL, strings.TrimSpace(rest[:idx]))
			}
			id, err := parseUint(rest[idx+len(" WHERE ID = "):])
			if err != nil {
				return nil, err
			}
			return selectStmt{table: table, byID: true, id: id}, nil
		}
		table, err := parseIdentifier(rest)
		if err != nil {
			return nil, fmt.Errorf("%w: table name %q", ErrUnsupportedSQL, strings.TrimSpace(rest))
		}
		return selectStmt{table: table}, nil
	case strings.HasPrefix(upper, "DELETE FROM "):
		rest := s[len("DELETE FROM "):]
		upRest := upper[len("DELETE FROM "):]
		idx := strings.Index(upRest, " WHERE ID = ")
		if idx < 0 {
			return nil, fmt.Errorf("%w: delete syntax", ErrUnsupportedSQL)
		}
		table, err := parseIdentifier(rest[:idx])
		if err != nil {
			return nil, fmt.Errorf("%w: table name %q", ErrUnsupportedSQL, strings.TrimSpace(rest[:idx]))
		}
		id, err := parseUint(rest[idx+len(" WHERE ID = "):])
		if err != nil {
			return nil, err
		}
		return deleteStmt{table: table, id: id}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedSQL, sql)
	}
}

func parseUint(v string) (uint64, error) {
	n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: integer literal %q", ErrUnsupportedSQL, strings.TrimSpace(v))
	}
	return n, nil
}

func parseIdentifier(v string) (string, error) {
	name := normalizeName(v)
	if name == "" {
		return "", fmt.Errorf("%w: missing identifier", ErrUnsupportedSQL)
	}
	if len(name) > catalogMaxNameLen {
		return "", fmt.Errorf("%w: identifier too long", ErrUnsupportedSQL)
	}
	for i, r := range name {
		if r == '_' || (r >= 'a' && r <= 'z') || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return "", fmt.Errorf("%w: invalid identifier", ErrUnsupportedSQL)
	}
	return name, nil
}
