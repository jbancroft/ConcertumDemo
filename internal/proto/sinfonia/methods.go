package sinfonia

const (
	MethodCommit             = "memnode.commit"
	MethodCommitBatch        = "memnode.commit_batch"
	MethodExecCommit1PC      = "memnode.exec_commit_1pc"
	MethodExecPrepare        = "memnode.exec_prepare"
	MethodNotifyApplied      = "memnode.notify_applied"
	MethodQueryInDoubt       = "memnode.query_in_doubt"
	MethodQueryTidStatus     = "memnode.query_tid_status"
	MethodRecoveryDecide     = "memnode.recovery_decide"
	MethodRecoveryForceAbort = "memnode.recovery_force_abort"
	MethodRunForcedAbortGC   = "memnode.run_forced_abort_gc"
	MethodRunRedoLogGC       = "memnode.run_redo_log_gc"
)
