package rpcwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jbancroft/concertum2/internal/proto"
)

func TestFrameRoundTripForAllRegisteredRPCBodies(t *testing.T) {
	bodies := []any{
		proto.ReadRequest{Addr: 1, Len: 2},
		proto.ReadResponse{Data: []byte("ok")},
		proto.WriteRequest{Addr: 3, Data: []byte("x")},
		proto.WriteResponse{Ack: true},
		proto.DescribeTransactionCapabilitiesRequest{},
		proto.DescribeTransactionCapabilitiesResponse{DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion},
		proto.ExecPrepareRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 3}, DecisionDeadlineNanos: 123, DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion},
		proto.ExecPrepareResponse{Vote: proto.VoteOk, ReadData: map[uint32][]byte{0: []byte("r")}, CurrentEpoch: 3},
		proto.ExecCommit1PCRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 4, Epoch: 3}},
		proto.ExecCommit1PCResponse{Vote: proto.VoteOk, ReadData: map[uint32][]byte{0: []byte("r")}, CurrentEpoch: 3},
		proto.CommitRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 5, Epoch: 3}, ActionCommit: true},
		proto.CommitResponse{Ack: true},
		proto.CommitBatchRequest{Decisions: []proto.CommitRequest{{TID: proto.TransactionID{CoordinatorID: 1, Counter: 6, Epoch: 3}, ActionCommit: true}}},
		proto.CommitBatchResponse{Acks: []bool{true}},
		proto.QueryInDoubtRequest{},
		proto.QueryInDoubtResponse{Entries: []proto.InDoubtEntry{{TID: proto.TransactionID{CoordinatorID: 1, Counter: 7, Epoch: 3}, PreparedAt: 10}}},
		proto.RecoverForceAbortRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 8, Epoch: 3}, Epoch: 3},
		proto.RecoverForceAbortResponse{Vote: proto.VoteOk, KnownDecision: true, DecisionCommit: true},
		proto.RecoverDecideRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 9, Epoch: 3}, ActionCommit: false},
		proto.RecoverDecideResponse{Ack: true},
		proto.BackupLockAllRequest{},
		proto.BackupLockAllResponse{Ack: true},
		proto.BackupUnlockAllRequest{},
		proto.BackupUnlockAllResponse{Ack: true},
		proto.BeginBackupPhaseRequest{BackupID: "b1"},
		proto.BeginBackupPhaseResponse{Ack: true, BoundaryTID: proto.TransactionID{CoordinatorID: 1, Counter: 10, Epoch: 3}},
		proto.CompleteBackupPhaseRequest{BackupID: "b1"},
		proto.CompleteBackupPhaseResponse{Ack: true, SnapshotPath: "/tmp/snap", SnapshotSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		proto.ReplicateRedoEntryRequest{Entry: proto.RedoEntryPayload{TID: proto.TransactionID{CoordinatorID: 1, Counter: 11, Epoch: 3}, Participants: []uint32{1, 2}, DecisionDeadlineNanos: 123}},
		proto.ReplicateRedoEntryResponse{Ack: true},
		proto.ReplicateCommitDecisionRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 12, Epoch: 3}, ActionCommit: true},
		proto.ReplicateCommitDecisionResponse{Ack: true},
		proto.ListCommittedTIDsRequest{},
		proto.ListCommittedTIDsResponse{TIDs: []proto.TransactionID{{CoordinatorID: 1, Counter: 13, Epoch: 3}}},
		proto.ListRedoEntriesRequest{},
		proto.ListRedoEntriesResponse{Entries: []proto.RedoEntryPayload{{TID: proto.TransactionID{CoordinatorID: 1, Counter: 14, Epoch: 3}}}},
		proto.NotifyAppliedRequest{FromMemID: 1, TIDs: []proto.TransactionID{{CoordinatorID: 1, Counter: 15, Epoch: 3}}},
		proto.NotifyAppliedResponse{Ack: true},
		proto.QueryTidStatusRequest{TID: proto.TransactionID{CoordinatorID: 1, Counter: 16, Epoch: 3}},
		proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeCommit, VotedOK: true},
		proto.DirectoryRequest{},
		proto.DirectoryResponse{Mappings: map[uint32]proto.DirectoryEntry{1: {Primary: proto.Endpoint{RPC: "127.0.0.1:4001", HTTP: "http://127.0.0.1:7001"}}}},
		proto.ResolveRequest{MemID: 1},
		proto.ResolveResponse{MemID: 1, Primary: proto.Endpoint{RPC: "127.0.0.1:4001", HTTP: "http://127.0.0.1:7001"}},
	}

	for _, body := range bodies {
		t.Run(reflect.TypeOf(body).String(), func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeFrame(&buf, Request{Method: "test", Body: body}); err != nil {
				t.Fatalf("write request: %v", err)
			}
			var req Request
			if err := readFrame(&buf, &req); err != nil {
				t.Fatalf("read request: %v", err)
			}
			if reflect.TypeOf(req.Body) != reflect.TypeOf(body) {
				t.Fatalf("request body type mismatch: got %T want %T", req.Body, body)
			}

			buf.Reset()
			if err := writeFrame(&buf, Response{Body: body}); err != nil {
				t.Fatalf("write response: %v", err)
			}
			var resp Response
			if err := readFrame(&buf, &resp); err != nil {
				t.Fatalf("read response: %v", err)
			}
			if reflect.TypeOf(resp.Body) != reflect.TypeOf(body) {
				t.Fatalf("response body type mismatch: got %T want %T", resp.Body, body)
			}
		})
	}
}

func TestReadFrameRejectsMalformedInput(t *testing.T) {
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, maxFrameSize+1)

	tests := []struct {
		name     string
		input    []byte
		wantIs   error
		wantText string
	}{
		{name: "missing header", wantIs: io.EOF},
		{name: "truncated header", input: []byte{0, 0}, wantIs: io.ErrUnexpectedEOF},
		{name: "zero length", input: []byte{0, 0, 0, 0}, wantIs: io.EOF},
		{name: "over size limit", input: oversized, wantText: "rpc frame too large: 67108865"},
		{name: "truncated payload", input: testFrame([]byte{0xff, 0xff})[:5], wantIs: io.ErrUnexpectedEOF},
		{name: "undecodable payload", input: testFrame([]byte{0xff})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var response Response
			err := readFrame(bytes.NewReader(tt.input), &response)
			if err == nil {
				t.Fatal("readFrame accepted malformed input")
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("readFrame error = %v, want %v", err, tt.wantIs)
			}
			if tt.wantText != "" && err.Error() != tt.wantText {
				t.Fatalf("readFrame error = %q, want %q", err, tt.wantText)
			}
		})
	}
}

func TestWriteAllRejectsInvalidWriterCounts(t *testing.T) {
	for _, n := range []int{0, -1, 4} {
		if err := writeAll(rpcTestWriter{n: n}, []byte("abc")); !errors.Is(err, io.ErrShortWrite) {
			t.Errorf("n=%d: err=%v", n, err)
		}
	}
}

type rpcTestWriter struct{ n int }

func (w rpcTestWriter) Write([]byte) (int, error) { return w.n, nil }

func FuzzReadFrameNeverPanics(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0})
	f.Add(testFrame([]byte{0xff, 0x00, 0x01}))
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, maxFrameSize+1)
	f.Add(oversized)

	f.Fuzz(func(t *testing.T, input []byte) {
		var response Response
		_ = readFrame(bytes.NewReader(input), &response)
	})
}

func testFrame(payload []byte) []byte {
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}
