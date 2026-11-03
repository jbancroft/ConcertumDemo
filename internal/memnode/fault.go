package memnode

import (
	"errors"
	"fmt"

	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrRestartRequired = errors.New("participant faulted; restart recovery required")

// failUncertainLocked stops this instance from serving or voting from evidence
// whose durability or in-memory index is uncertain. Retain pending writes and
// locks, and reopen storage before recovery.
func (n *Participant) failUncertainLocked(operation string, cause error) error {
	if n.restartRequired == nil {
		n.restartRequired = fmt.Errorf("%w: %s: %w", ErrRestartRequired, operation, cause)
	}
	n.recoveryComplete = false
	return n.restartRequired
}

// Failed temporary writes leave the old snapshot intact. Only a published
// snapshot whose directory sync failed makes the in-memory decision uncertain.
func (n *Participant) decisionWriteErrorLocked(err error) error {
	if errors.Is(err, storage.ErrDecisionUncertain) {
		return n.failUncertainLocked("save decision snapshot", err)
	}
	return err
}
