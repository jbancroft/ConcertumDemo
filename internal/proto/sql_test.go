package proto

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func TestSQLExecGobRoundTrip(t *testing.T) {
	in := SQLExecRequest{
		Version:       SQLRPCVersion1,
		CorrelationID: "sql-123",
		Statement:     "SELECT id, value FROM t",
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatalf("encode: %v", err)
	}

	var out SQLExecRequest
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, in)
	}
}

func TestSQLDescribeCapabilitiesGobRoundTrip(t *testing.T) {
	in := SQLDescribeCapabilitiesResponse{
		Version: SQLRPCVersion1,
		Capabilities: SQLCapabilities{
			DialectTarget:      "sqlite-core-subset",
			StatementAtomicity: true,
			MultiStmtTxns:      false,
			SupportedStatements: []string{
				"CREATE TABLE <table_name>",
				"INSERT INTO <table_name> VALUES (<id_uint>, <value_uint>)",
				"SELECT id, value FROM <table_name>",
				"SELECT id, value FROM <table_name> WHERE id = <id_uint>",
				"DELETE FROM <table_name> WHERE id = <id_uint>",
			},
			UnsupportedFeatures: []string{
				"UPDATE",
				"JOIN",
				"BEGIN/COMMIT/ROLLBACK",
			},
		},
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatalf("encode: %v", err)
	}

	var out SQLDescribeCapabilitiesResponse
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Version != in.Version {
		t.Fatalf("version mismatch: got %d want %d", out.Version, in.Version)
	}
	if out.Capabilities.DialectTarget != in.Capabilities.DialectTarget {
		t.Fatalf("dialect mismatch: got %q want %q", out.Capabilities.DialectTarget, in.Capabilities.DialectTarget)
	}
	if len(out.Capabilities.SupportedStatements) != len(in.Capabilities.SupportedStatements) {
		t.Fatalf("supported length mismatch: got %d want %d", len(out.Capabilities.SupportedStatements), len(in.Capabilities.SupportedStatements))
	}
	if len(out.Capabilities.UnsupportedFeatures) != len(in.Capabilities.UnsupportedFeatures) {
		t.Fatalf("unsupported length mismatch: got %d want %d", len(out.Capabilities.UnsupportedFeatures), len(in.Capabilities.UnsupportedFeatures))
	}
}
