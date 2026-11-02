package memnode

import (
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

	peerTimeout time.Duration

	executionSlots chan struct{}

	recoveryComplete bool
	restartRequired  error

	recoverBoundaryHook  func(phase string, txId sinfonia.TransactionID) error
	responseBoundaryHook func(phase string, txId sinfonia.TransactionID) error
	writeApplyFaultHook  func(addr uint64, data []byte) error
}

func NewParticipant(size int) *Participant {
	n := &Participant{
		memory:          make([]byte, size),
		inDoubt:         make(map[sinfonia.TransactionID]*preparedTxn),
		allLogTxIds:     make(map[sinfonia.TransactionID]struct{}),
		nowFn:           time.Now,
		participants:    make(map[uint32]RecoveryPeer),
		appliedLocal:    make(map[sinfonia.TransactionID]bool),
		appliedRemote:   make(map[sinfonia.TransactionID]map[uint32]bool),
		pendingNotify:   make(map[uint32]map[sinfonia.TransactionID]struct{}),
		prepareOutcomes: make(map[sinfonia.TransactionID]prepareOutcome),
		onePCOutcomes:   make(map[sinfonia.TransactionID]onePCOutcome),
		commitOutcomes:  make(map[sinfonia.TransactionID]commitOutcome),
	}
	n.recoveryComplete = true
	n.peerTimeout = defaultPeerTimeout
	n.executionSlots = make(chan struct{}, maxConcurrentExecutions)
	return n
}

func NewParticipantWithId(hostId uint32, size int) *Participant {
	n := NewParticipant(size)
	n.hostId = hostId
	return n
}

func NewDurableParticipant(hostId uint32, size int, dir string) (_ *Participant, err error) {
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
		appliedRemote:    map[sinfonia.TransactionID]map[uint32]bool{},
		pendingNotify:    map[uint32]map[sinfonia.TransactionID]struct{}{},
		prepareOutcomes:  map[sinfonia.TransactionID]prepareOutcome{},
		onePCOutcomes:    map[sinfonia.TransactionID]onePCOutcome{},
		commitOutcomes:   map[sinfonia.TransactionID]commitOutcome{},
		recoveryComplete: false,
		peerTimeout:      defaultPeerTimeout,
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

// Close releases the disk image. Callers must first stop
// serving requests and join any externally started recovery workers.
func (n *Participant) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.diskImage == nil {
		return nil
	}
	err := n.diskImage.Close()
	n.diskImage = nil
	return err
}

func (n *Participant) read(addr uint64, length uint32) ([]byte, error) {
	end := addr + uint64(length)
	if end < addr || end > uint64(len(n.memory)) {
		return nil, ErrOutOfBounds
	}
	data := make([]byte, length)
	copy(data, n.memory[addr:end])
	return data, nil
}

func (n *Participant) write(addr uint64, data []byte) error {
	end := addr + uint64(len(data))
	if end < addr || end > uint64(len(n.memory)) {
		return ErrOutOfBounds
	}
	copy(n.memory[addr:end], data)
	if n.writeApplyFaultHook != nil {
		if err := n.writeApplyFaultHook(addr, data); err != nil {
			return err
		}
	}
	if n.diskImage != nil {
		if err := n.diskImage.ApplyWrite(addr, data); err != nil {
			return err
		}
	}
	return nil
}

// SetWriteApplyFaultForTest lets recovery tests fail a write after memory changes
// but before the disk image is updated.
func (n *Participant) SetWriteApplyFaultForTest(fn func(addr uint64, data []byte) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.writeApplyFaultHook = fn
}
