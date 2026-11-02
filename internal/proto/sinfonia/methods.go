package sinfonia

const (
	MethodBackupLockAll       = "memnode.backup_lock_all"
	MethodBackupRenewLock     = "memnode.backup_renew_lock"
	MethodBackupUnlockAll     = "memnode.backup_unlock_all"
	MethodBeginBackupPhase    = "memnode.begin_backup_phase"
	MethodCommit              = "memnode.commit"
	MethodCommitBatch         = "memnode.commit_batch"
	MethodCompleteBackupPhase = "memnode.complete_backup_phase"
	MethodExecCommit1PC       = "memnode.exec_commit_1pc"
	MethodExecPrepare         = "memnode.exec_prepare"
	MethodNotifyApplied       = "memnode.notify_applied"
	MethodQueryInDoubt        = "memnode.query_in_doubt"
	MethodQueryTidStatus      = "memnode.query_tid_status"
	MethodRecoveryDecide      = "memnode.recovery_decide"
	MethodRecoveryForceAbort  = "memnode.recovery_force_abort"
	MethodRunForcedAbortGC    = "memnode.run_forced_abort_gc"
	MethodRunRedoLogGC        = "memnode.run_redo_log_gc"
)
