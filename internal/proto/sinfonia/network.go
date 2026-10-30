package sinfonia

import "encoding/gob"

func init() {
	registerGobTypes()
}

func registerGobTypes() {
	for _, body := range []any{
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
