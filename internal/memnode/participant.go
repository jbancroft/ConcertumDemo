package memnode

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var (
	ErrOutOfBounds       = errors.New("out of bounds")
	ErrResourceExhausted = errors.New("RESOURCE_EXHAUSTED")
)

const maxConcurrentExecutions = 256

type Participant struct {
	mu           sync.Mutex
	hostId       uint32
	memory       []byte
	storageDir   string
	diskImage    *storage.DiskImage
	redoLog      *storage.RedoLog
	decided      *storage.DecidedTransactionLog
	forcedAborts *storage.ForcedAbortLog
	inDoubt      map[sinfonia.TransactionID]*preparedTxn
	locks        LockManager
	epoch        uint64
	epochFloor   uint64
	allLogTxIds  map[sinfonia.TransactionID]struct{}
	participants map[uint32]RecoveryPeer
	nowFn        func() time.Time

	appliedLocal    map[sinfonia.TransactionID]bool
	appliedRemote   map[sinfonia.TransactionID]map[uint32]bool
	pendingNotify   map[uint32]map[sinfonia.TransactionID]struct{}
	prepareOutcomes map[sinfonia.TransactionID]prepareOutcome
	onePCOutcomes   map[sinfonia.TransactionID]onePCOutcome
	commitOutcomes  map[sinfonia.TransactionID]commitOutcome

	executionSlots chan struct{}

	recoveryComplete bool
	restartRequired  error

	recoverBoundaryHook  func(phase string, txId sinfonia.TransactionID) error
	responseBoundaryHook func(phase string, txId sinfonia.TransactionID) error
	writeApplyFaultHook  func(addr uint64, data []byte) error
}

// NewParticipant opens durable storage. Configure recovery peers and call Recover
// before serving execution requests; every participant requires its own directory.
func NewParticipant(hostId uint32, size int, dir string) (_ *Participant, err error) {
	if hostId == 0 || size <= 0 || dir == "" {
		return nil, errors.New("participant requires a nonzero node ID, positive size, and data directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	epochFloor, err := storage.LoadEpochFloor(dir)
	if err != nil {
		return nil, err
	}

	// Reject malformed decisions before opening the image.
	decided, err := storage.OpenDecidedTransactionLog(dir)
	if err != nil {
		return nil, err
	}
	disk, mem, err := storage.OpenDiskImage(dir, size)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, disk.Close())
		}
	}()
	redoLog, err := storage.OpenRedoLog(dir)
	if err != nil {
		return nil, err
	}
	forcedAbort, err := storage.OpenForcedAbortLog(dir)
	if err != nil {
		return nil, err
	}
	appliedRemote, err := storage.LoadAppliedReceipts(dir)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	n := &Participant{
		hostId:           hostId,
		memory:           mem,
		storageDir:       dir,
		diskImage:        disk,
		redoLog:          redoLog,
		decided:          decided,
		forcedAborts:     forcedAbort,
		inDoubt:          make(map[sinfonia.TransactionID]*preparedTxn),
		epoch:            epochFloor,
		epochFloor:       epochFloor,
		allLogTxIds:      map[sinfonia.TransactionID]struct{}{},
		participants:     map[uint32]RecoveryPeer{},
		nowFn:            time.Now,
		appliedLocal:     map[sinfonia.TransactionID]bool{},
		appliedRemote:    appliedRemote,
		pendingNotify:    map[uint32]map[sinfonia.TransactionID]struct{}{},
		prepareOutcomes:  map[sinfonia.TransactionID]prepareOutcome{},
		onePCOutcomes:    map[sinfonia.TransactionID]onePCOutcome{},
		commitOutcomes:   map[sinfonia.TransactionID]commitOutcome{},
		recoveryComplete: false,
	}
	n.executionSlots = make(chan struct{}, maxConcurrentExecutions)
	for _, e := range redoLog.Entries() {
		n.allLogTxIds[e.TransactionId] = struct{}{}
		if committed, ok := decided.Get(e.TransactionId); ok {
			n.commitOutcomes[e.TransactionId] = commitOutcome{resp: sinfonia.CommitResponse{Ack: true}, actionCommit: committed}
			if committed {
				n.appliedLocal[e.TransactionId] = true
				n.enqueueAppliedNotificationsLocked(e.TransactionId, e.HostIds)
			}
		} else {
			n.inDoubt[e.TransactionId] = &preparedTxn{
				writes:        cloneWrites(e.WriteItems),
				participants:  append([]uint32(nil), e.HostIds...),
				redoLoggedTid: true,
				preparedAt:    now,
			}
		}
	}
	return n, nil
}

// Check readiness under the execution mutex so Recover cannot take its redo
// snapshot between admitting a request and executing it.
func (n *Participant) requireRecoveryCompleteLocked() error {
	if n.restartRequired != nil {
		return n.restartRequired
	}
	if !n.recoveryComplete {
		return ErrRecoveryPending
	}
	return nil
}

// Close releases the disk image. Callers must first stop
// serving requests and join any externally started recovery workers.
func (n *Participant) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.diskImage.Close()
}

func (n *Participant) read(addr uint64, length uint32) ([]byte, error) {
	end := addr + uint64(length)
	if end < addr || end > uint64(len(n.memory)) {
		return nil, ErrOutOfBounds
	}
	return bytes.Clone(n.memory[addr:end]), nil
}

func (n *Participant) applyWrites(writes []sinfonia.WriteItem) error {
	if err := n.validateWriteRangesLocked(writes); err != nil {
		return err
	}
	for _, item := range writes {
		copy(n.memory[item.Addr:], item.Data)
		if n.writeApplyFaultHook != nil {
			if err := n.writeApplyFaultHook(item.Addr, item.Data); err != nil {
				return err
			}
		}
	}
	return n.diskImage.ApplyWrites(writes)
}
