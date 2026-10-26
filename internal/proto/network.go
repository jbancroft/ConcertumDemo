package proto

import "encoding/gob"

type ReadRequest struct {
	Addr       uint64
	Len        uint32
	Generation uint64
}

type ReadResponse struct {
	Data []byte
}

type WriteRequest struct {
	Addr       uint64
	Data       []byte
	Generation uint64
}

type WriteResponse struct {
	Ack bool
}

type Endpoint struct {
	RPC  string `json:"rpc"`
	HTTP string `json:"http,omitempty"`
}

type DirectoryEntry struct {
	Primary    Endpoint  `json:"primary"`
	Replica    *Endpoint `json:"replica,omitempty"`
	Generation uint64    `json:"generation,omitempty"`
}

type DirectoryRequest struct{}

type DirectoryResponse struct {
	Mappings map[uint32]DirectoryEntry
	// Revision is the store-wide immutable directory snapshot revision. A zero
	// value is the bootstrap revision and is also what legacy servers report.
	Revision uint64
}

type ResolveRequest struct {
	MemID uint32
}

type ResolveResponse struct {
	MemID             uint32
	Primary           Endpoint
	Replica           *Endpoint
	Generation        uint64
	DirectoryRevision uint64
}

func init() {
	registerGobTypes()
}

func registerGobTypes() {
	gob.Register(ReadRequest{})
	gob.Register(ReadResponse{})
	gob.Register(WriteRequest{})
	gob.Register(WriteResponse{})
	gob.Register(DescribeTransactionCapabilitiesRequest{})
	gob.Register(DescribeTransactionCapabilitiesResponse{})
	gob.Register(ExecPrepareRequest{})
	gob.Register(ExecPrepareResponse{})
	gob.Register(CommitRequest{})
	gob.Register(CommitResponse{})
	gob.Register(ExecCommit1PCRequest{})
	gob.Register(ExecCommit1PCResponse{})
	gob.Register(CommitBatchRequest{})
	gob.Register(CommitBatchResponse{})
	gob.Register(ReplicateRedoEntryRequest{})
	gob.Register(ReplicateRedoEntryResponse{})
	gob.Register(ReplicateCommitDecisionRequest{})
	gob.Register(ReplicateCommitDecisionResponse{})
	gob.Register(ReplicateReplayRecordRequest{})
	gob.Register(ReplicateReplayRecordResponse{})
	gob.Register(ConfigureReplicationRequest{})
	gob.Register(ConfigureReplicationResponse{})
	gob.Register(ReplicationStatusRequest{})
	gob.Register(ReplicationStatusResponse{})
	gob.Register(ListCommittedTIDsRequest{})
	gob.Register(ListCommittedTIDsResponse{})
	gob.Register(ListRedoEntriesRequest{})
	gob.Register(ListRedoEntriesResponse{})
	gob.Register(ExportReplicationBaselineRequest{})
	gob.Register(ExportReplicationBaselineResponse{})
	gob.Register(InstallReplicationBaselineRequest{})
	gob.Register(InstallReplicationBaselineResponse{})
	gob.Register(ExportReplicationBaselineChunkRequest{})
	gob.Register(ExportReplicationBaselineChunkResponse{})
	gob.Register(BeginReplicationBaselineRequest{})
	gob.Register(BeginReplicationBaselineResponse{})
	gob.Register(WriteReplicationBaselineChunkRequest{})
	gob.Register(WriteReplicationBaselineChunkResponse{})
	gob.Register(CommitReplicationBaselineRequest{})
	gob.Register(CommitReplicationBaselineResponse{})
	gob.Register(AbortReplicationBaselineRequest{})
	gob.Register(AbortReplicationBaselineResponse{})
	gob.Register(RecoverForceAbortRequest{})
	gob.Register(RecoverForceAbortResponse{})
	gob.Register(RecoverDecideRequest{})
	gob.Register(RecoverDecideResponse{})
	gob.Register(QueryInDoubtRequest{})
	gob.Register(QueryInDoubtResponse{})
	gob.Register(QueryTidStatusRequest{})
	gob.Register(QueryTidStatusResponse{})
	gob.Register(NotifyAppliedRequest{})
	gob.Register(NotifyAppliedResponse{})
	gob.Register(RunRedoLogGCRequest{})
	gob.Register(RunRedoLogGCResponse{})
	gob.Register(RunForcedAbortGCRequest{})
	gob.Register(RunForcedAbortGCResponse{})
	gob.Register(BackupLockAllRequest{})
	gob.Register(BackupLockAllResponse{})
	gob.Register(BackupRenewLockRequest{})
	gob.Register(BackupRenewLockResponse{})
	gob.Register(BackupUnlockAllRequest{})
	gob.Register(BackupUnlockAllResponse{})
	gob.Register(BeginBackupPhaseRequest{})
	gob.Register(BeginBackupPhaseResponse{})
	gob.Register(CompleteBackupPhaseRequest{})
	gob.Register(CompleteBackupPhaseResponse{})
	gob.Register(Endpoint{})
	gob.Register(DirectoryEntry{})
	gob.Register(DirectoryRequest{})
	gob.Register(DirectoryResponse{})
	gob.Register(ResolveRequest{})
	gob.Register(ResolveResponse{})
	gob.Register(UpdateEntryRequest{})
	gob.Register(UpdateEntryResponse{})
	gob.Register(SQLExecRequest{})
	gob.Register(SQLExecResponse{})
	gob.Register(SQLPingRequest{})
	gob.Register(SQLPingResponse{})
	gob.Register(SQLDescribeCapabilitiesRequest{})
	gob.Register(SQLDescribeCapabilitiesResponse{})
	gob.Register(SQLCapabilities{})
	gob.Register(SQLRow{})
}
