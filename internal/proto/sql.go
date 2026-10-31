package proto

import "encoding/gob"

const SQLRPCVersion1 uint16 = 1

const (
	MethodSQLDescribeCapabilities = "sqld.describe_capabilities"
	MethodSQLExec                 = "sqld.exec"
	MethodSQLPing                 = "sqld.ping"
)

type SQLExecRequest struct {
	Version       uint16
	CorrelationID string
	Statement     string
}

type SQLExecResponse struct {
	Version       uint16
	CorrelationID string
	ErrorCode     string
	ErrorMessage  string
	Rows          []SQLRow
}

type SQLPingRequest struct {
	Version uint16
}

type SQLPingResponse struct {
	Version uint16
	OK      bool
}

type SQLDescribeCapabilitiesRequest struct {
	Version uint16
}

type SQLDescribeCapabilitiesResponse struct {
	Version      uint16
	Capabilities SQLCapabilities
}

type SQLCapabilities struct {
	DialectTarget       string
	StatementAtomicity  bool
	MultiStmtTxns       bool
	SupportedStatements []string
	UnsupportedFeatures []string
}

type SQLRow struct {
	ID    uint64
	Value uint64
}

func init() {
	for _, body := range []any{
		SQLExecRequest{},
		SQLExecResponse{},
		SQLPingRequest{},
		SQLPingResponse{},
		SQLDescribeCapabilitiesRequest{},
		SQLDescribeCapabilitiesResponse{},
		SQLCapabilities{},
		SQLRow{},
	} {
		gob.Register(body)
	}
}
