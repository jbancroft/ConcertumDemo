package memnode

import (
	"errors"
	"fmt"
)

var ErrRestartRequired = errors.New("participant faulted; restart recovery required")

// failUncertainLocked stops this instance from serving or voting from incomplete
// evidence. A failed append may exist on disk but not in the log's memory index.
// Retain pending writes and locks, and reopen storage before recovery.
func (n *Participant) failUncertainLocked(operation string, cause error) error {
	if n.restartRequired == nil {
		n.restartRequired = fmt.Errorf("%w: %s: %w", ErrRestartRequired, operation, cause)
	}
	n.recoveryComplete = false
	return n.restartRequired
}
