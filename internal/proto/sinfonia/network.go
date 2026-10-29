package sinfonia

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
	// Revision identifies this directory snapshot. Zero means a new directory.
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
	for _, body := range []any{
		ReadRequest{},
		ReadResponse{},
		WriteRequest{},
		WriteResponse{},
		ExecPrepareRequest{},
		ExecPrepareResponse{},
		CommitRequest{},
		CommitResponse{},
		ExecCommit1PCRequest{},
		ExecCommit1PCResponse{},
		CommitBatchRequest{},
		CommitBatchResponse{},
		ReplicateRedoEntryRequest{},
		ReplicateRedoEntryResponse{},
		ReplicateCommitDecisionRequest{},
		ReplicateCommitDecisionResponse{},
		ReplicateReplayRecordRequest{},
		ReplicateReplayRecordResponse{},
		ConfigureReplicationRequest{},
		ConfigureReplicationResponse{},
		ReplicationStatusRequest{},
		ReplicationStatusResponse{},
		ListCommittedTIDsRequest{},
		ListCommittedTIDsResponse{},
		ListRedoEntriesRequest{},
		ListRedoEntriesResponse{},
		ExportReplicationBaselineRequest{},
		ExportReplicationBaselineResponse{},
		InstallReplicationBaselineRequest{},
		InstallReplicationBaselineResponse{},
		ExportReplicationBaselineChunkRequest{},
		ExportReplicationBaselineChunkResponse{},
		BeginReplicationBaselineRequest{},
		BeginReplicationBaselineResponse{},
		WriteReplicationBaselineChunkRequest{},
		WriteReplicationBaselineChunkResponse{},
		CommitReplicationBaselineRequest{},
		CommitReplicationBaselineResponse{},
		AbortReplicationBaselineRequest{},
		AbortReplicationBaselineResponse{},
		RecoverForceAbortRequest{},
		RecoverForceAbortResponse{},
		RecoverDecideRequest{},
		RecoverDecideResponse{},
		QueryInDoubtRequest{},
		QueryInDoubtResponse{},
		QueryTidStatusRequest{},
		QueryTidStatusResponse{},
		NotifyAppliedRequest{},
		NotifyAppliedResponse{},
		RunRedoLogGCRequest{},
		RunRedoLogGCResponse{},
		RunForcedAbortGCRequest{},
		RunForcedAbortGCResponse{},
		BackupLockAllRequest{},
		BackupLockAllResponse{},
		BackupRenewLockRequest{},
		BackupRenewLockResponse{},
		BackupUnlockAllRequest{},
		BackupUnlockAllResponse{},
		BeginBackupPhaseRequest{},
		BeginBackupPhaseResponse{},
		CompleteBackupPhaseRequest{},
		CompleteBackupPhaseResponse{},
		Endpoint{},
		DirectoryEntry{},
		DirectoryRequest{},
		DirectoryResponse{},
		ResolveRequest{},
		ResolveResponse{},
		UpdateEntryRequest{},
		UpdateEntryResponse{},
	} {
		gob.Register(body)
	}
}
