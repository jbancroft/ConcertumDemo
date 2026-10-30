package memnode

import (
	"errors"
	"fmt"
)

var ErrRestartRequired = errors.New("participant faulted; restart recovery required")

// failUncertainLocked stops this instance from serving or voting from incomplete
// evidence. A failed append may exist on disk but not in the log's memory index;
// a failed replication call may already have applied on the replica. Retain
// pending writes and locks, and reopen storage before trying recovery again.
func (n *Participant) failUncertainLocked(operation string, cause error) error {
	if n.restartRequired == nil {
		n.restartRequired = fmt.Errorf("%w: %s: %w", ErrRestartRequired, operation, cause)
	}
	n.recoveryComplete = false
	n.replicaSynced = false
	n.cond.Broadcast()
	return n.restartRequired
}
