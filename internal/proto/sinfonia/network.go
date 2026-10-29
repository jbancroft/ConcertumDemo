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
	} {
		gob.Register(body)
	}
}
