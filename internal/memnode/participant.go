package memnode

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/ratelimit"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

var ErrOutOfBounds = errors.New("out of bounds")

const (
	baselineChunkSize            = 1 << 20
	defaultGlobalBarrierLeaseTTL = 30 * time.Second
	maxBaselineBytes             = uint64(1) << 40
)

type Participant struct {
	mu           sync.Mutex
	cond         *sync.Cond
	hostId       uint32
	memory       []byte
	storageDir   string
	durable      bool
	diskImage    *storage.DiskImage
	redoLog      *storage.RedoLog
	decided      *storage.DecidedTransactionLog
	forcedAborts *storage.ForcedAbortLog
	replay       *storage.ReplayStore
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
	logParticipants map[sinfonia.TransactionID][]uint32
	prepareOutcomes map[sinfonia.TransactionID]prepareOutcome
	onePCOutcomes   map[sinfonia.TransactionID]onePCOutcome
	commitOutcomes  map[sinfonia.TransactionID]commitOutcome

	backupMode         bool
	prepareFlights     map[sinfonia.TransactionID]*prepareFlight
	onePCFlights       map[sinfonia.TransactionID]*onePCFlight
	pendingDiskWrites  []sinfonia.WriteItem
	lastBackupBoundary sinfonia.TransactionID

	mode                     ReplicationMode
	replica                  ReplicaPeer
	replicaID                uint32
	primaryID                uint32
	heartbeat                time.Time
	partnerRPC               string
	lastReplicationError     string
	generation               uint64
	peerTLS                  *tls.Config
	peerTimeout              time.Duration
	localBarrierOwner        string
	globalBarrierLeaseTTL    time.Duration
	baselineTransfer         *baselineTransfer
	baselineRecoveryRequired bool

	recoveryComplete bool
	restartRequired  error
	replicaSynced    bool

	maxInFlightPrepare          int
	maxInDoubtSoft              int
	maxInDoubtHard              int
	maxRequestBytes             int
	maxItemsPerTxn              int
	maxReadBytesPerTxn          int64
	maxWriteBytesPerTxn         int64
	prepareSlots                chan struct{}
	prepareLimiter              *ratelimit.KeyedLimiter
	commitLimiter               *ratelimit.KeyedLimiter
	recoveryLimiter             *ratelimit.KeyedLimiter
	recoverBoundaryHook         func(phase string, txId sinfonia.TransactionID) error
	outcomeReplayBoundaryHook   func(phase string, txId sinfonia.TransactionID) error
	baselineInstallBoundaryHook func(phase string) error
	fencingPublicationHook      func(point string) error
	writeApplyFaultHook         func(addr uint64, data []byte) error
}

func (n *Participant) initSafetyDefaults() {
	n.maxInFlightPrepare = defaultMaxInFlightPrepare
	n.maxInDoubtSoft = 0
	n.maxInDoubtHard = 0
	n.maxRequestBytes = 0
	n.maxItemsPerTxn = 0
	n.maxReadBytesPerTxn = 0
	n.maxWriteBytesPerTxn = 0
	n.prepareSlots = make(chan struct{}, n.maxInFlightPrepare)
}

func NewParticipant(size int) *Participant {
	n := &Participant{memory: make([]byte, size), inDoubt: make(map[sinfonia.TransactionID]*preparedTxn), allLogTxIds: map[sinfonia.TransactionID]struct{}{}, nowFn: time.Now, participants: map[uint32]RecoveryPeer{}, appliedLocal: map[sinfonia.TransactionID]bool{}, appliedRemote: map[sinfonia.TransactionID]map[uint32]bool{}, pendingNotify: map[uint32]map[sinfonia.TransactionID]struct{}{}, logParticipants: map[sinfonia.TransactionID][]uint32{}, prepareOutcomes: map[sinfonia.TransactionID]prepareOutcome{}, onePCOutcomes: map[sinfonia.TransactionID]onePCOutcome{}, commitOutcomes: map[sinfonia.TransactionID]commitOutcome{}, prepareFlights: map[sinfonia.TransactionID]*prepareFlight{}, onePCFlights: map[sinfonia.TransactionID]*onePCFlight{}}
	n.mode = ModeSingle
	n.recoveryComplete = true
	n.replicaSynced = true
	n.peerTimeout = defaultPeerTimeout
	n.initSafetyDefaults()
	n.cond = sync.NewCond(&n.mu)
	n.localBarrierOwner = fmt.Sprintf("node/%p", n)
	n.globalBarrierLeaseTTL = defaultGlobalBarrierLeaseTTL
	return n
}

func NewParticipantWithId(hostId uint32, size int) *Participant {
	n := NewParticipant(size)
	n.hostId = hostId
	return n
}

func NewDurableParticipant(hostId uint32, size int, dir string) (*Participant, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := storage.RecoverBaselineInstall(dir); err != nil {
		return nil, err
	}
	epochFloor, err := storage.LoadEpochFloor(dir)
	if err != nil {
		return nil, err
	}
	generation, err := loadFencingGeneration(dir)
	if err != nil {
		return nil, err
	}
	if err := retainFencingGenerations(dir, nil); err != nil {
		return nil, fmt.Errorf("retain fencing generations during startup: %w", err)
	}
	// Reject malformed decisions before opening the image or starting the redo appender.
	decided, err := storage.OpenDecidedTransactionLog(dir)
	if err != nil {
		return nil, err
	}
	disk, mem, err := storage.OpenDiskImage(dir, size)
	if err != nil {
		return nil, err
	}
	redoLog, err := storage.OpenRedoLog(dir)
	if err != nil {
		return nil, err
	}
	forcedAbort, err := storage.OpenForcedAbortLog(dir)
	if err != nil {
		return nil, err
	}
	replay, err := storage.OpenReplayStore(dir)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	n := &Participant{
		hostId:           hostId,
		memory:           mem,
		storageDir:       dir,
		durable:          true,
		diskImage:        disk,
		redoLog:          redoLog,
		decided:          decided,
		forcedAborts:     forcedAbort,
		inDoubt:          make(map[sinfonia.TransactionID]*preparedTxn),
		epoch:            epochFloor,
		epochFloor:       epochFloor,
		allLogTxIds:      map[sinfonia.TransactionID]struct{}{},
		participants:     map[uint32]RecoveryPeer{},
		replay:           replay,
		nowFn:            time.Now,
		appliedLocal:     map[sinfonia.TransactionID]bool{},
		appliedRemote:    map[sinfonia.TransactionID]map[uint32]bool{},
		pendingNotify:    map[uint32]map[sinfonia.TransactionID]struct{}{},
		logParticipants:  map[sinfonia.TransactionID][]uint32{},
		prepareOutcomes:  map[sinfonia.TransactionID]prepareOutcome{},
		onePCOutcomes:    map[sinfonia.TransactionID]onePCOutcome{},
		commitOutcomes:   map[sinfonia.TransactionID]commitOutcome{},
		prepareFlights:   map[sinfonia.TransactionID]*prepareFlight{},
		onePCFlights:     map[sinfonia.TransactionID]*onePCFlight{},
		generation:       generation,
		mode:             ModeSingle,
		recoveryComplete: false,
		replicaSynced:    true,
		peerTimeout:      defaultPeerTimeout,
	}
	n.initSafetyDefaults()
	n.cond = sync.NewCond(&n.mu)
	n.localBarrierOwner = fmt.Sprintf("node/%p", n)
	n.globalBarrierLeaseTTL = defaultGlobalBarrierLeaseTTL
	for _, e := range redoLog.Entries() {
		n.allLogTxIds[e.TransactionId] = struct{}{}
		n.logParticipants[e.TransactionId] = append([]uint32(nil), e.HostIds...)
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

func (n *Participant) Close() error {
	n.mu.Lock()
	disk := n.diskImage
	transfer := n.baselineTransfer
	n.baselineTransfer = nil
	n.diskImage = nil
	n.mu.Unlock()
	if transfer != nil {
		if transfer.file != nil {
			_ = transfer.file.Close()
		}
		_ = os.Remove(transfer.path)
	}
	if disk == nil {
		return nil
	}
	return disk.Close()
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
		if n.backupMode {
			n.pendingDiskWrites = append(n.pendingDiskWrites, sinfonia.WriteItem{MemID: n.hostId, Addr: addr, Data: append([]byte(nil), data...)})
			return nil
		}
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

func (n *Participant) isReadyLocked() bool {
	if n.restartRequired != nil || !n.recoveryComplete || n.backupMode {
		return false
	}
	if n.mode == ModeReplReplica && !n.replicaSynced {
		return false
	}
	return true
}
