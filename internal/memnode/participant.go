package memnode

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/auth"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
	"github.com/jbancroft/ConcertumDemo/internal/ratelimit"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

var ErrOutOfBounds = errors.New("out of bounds")
var ErrRecoveryPending = errors.New("participant recovery pending peer evidence")
var ErrReplayUnavailable = errors.New("durable transaction exists without exact replay payload")
var ErrDeadlineDecisionProtocolUpgradeRequired = errors.New("deadline decision protocol upgrade required")

type ReplicationMode int

const (
	ModeSingle ReplicationMode = iota
	ModeReplPrimary
	ModeReplReplica
)

var ErrReplicaRejectsCoordinator = errors.New("replica rejects direct coordinator requests")
var ErrReplicationRequiresDurability = errors.New("replication requires durable storage")
var ErrStaleGeneration = errors.New("stale or missing fencing generation")
var ErrReplicationFencingProtocolUpgradeRequired = errors.New("replication fencing protocol upgrade required")
var ErrDirectWriteRequiresMiniTransaction = errors.New("durable direct write requires a minitransaction")
var ErrReplicationBaselineRequiresGlobalLock = errors.New("replication baseline requires the global write lock")
var ErrReplicationBaselineLease = errors.New("replication baseline global lock lease is invalid")
var ErrReplicationBaselineInProgress = errors.New("replication baseline installation in progress")

const (
	defaultGlobalBarrierLeaseTTL = 30 * time.Second
	baselineChunkSize            = 1 << 20
	maxBaselineBytes             = uint64(1) << 40
)

type RangeLock struct {
	start uint64
	end   uint64
	write bool
}

// OverlapsWith checks [start, end) ranges: touching endpoints don't overlap.
// This follows the range comparison from gofonia.
func (rl RangeLock) OverlapsWith(other RangeLock) bool {
	if rl.start >= rl.end || other.start >= other.end {
		return false
	}
	minEnd, maxOffset := rl.end, other.start
	if rl.start > other.start {
		minEnd, maxOffset = other.end, rl.start
	}
	return minEnd > maxOffset
}

type prepareOutcome struct {
	resp proto.ExecPrepareResponse
}

type onePCOutcome struct {
	resp proto.ExecCommit1PCResponse
}

type prepareFlight struct {
	done chan struct{}
	resp proto.ExecPrepareResponse
	err  error
}

type onePCFlight struct {
	done chan struct{}
	resp proto.ExecCommit1PCResponse
	err  error
}

type commitOutcome struct {
	resp         proto.CommitResponse
	actionCommit bool
}

// preparedTxn keeps the writes and locks until a commit or abort decision arrives.
// Durable nodes also log prepared writes so they can recover after a restart.
type preparedTxn struct {
	writes                []proto.WriteItem
	readData              map[uint32][]byte
	heldLocks             []RangeLock
	preparedAt            time.Time
	participants          []uint32
	decisionDeadlineNanos int64
	redoLoggedTid         bool
	class                 string
	correlationID         string
}

type LockManager struct {
	locks                 []RangeLock
	globalWriteLock       bool
	globalLockedAt        time.Time
	globalOwner           string
	globalLeaseUntil      time.Time
	globalLeaseGeneration uint64
}

type baselineTransfer struct {
	sessionID        string
	owner            string
	path             string
	file             *os.File
	totalSize        uint64
	nextOffset       uint64
	checksum         []byte
	replayPath       string
	replayFile       *os.File
	replayTotalSize  uint64
	replayNextOffset uint64
	replayChecksum   []byte
	replayPresent    bool
	boundary         proto.TransactionID
}

func (m *LockManager) tryLock(start, end uint64, write bool) bool {
	ok, _ := m.tryLockWithConflict(start, end, write)
	return ok
}

func (m *LockManager) tryLockWithConflict(start, end uint64, write bool) (bool, *proto.LockConflict) {
	if m.globalWriteLock {
		return false, &proto.LockConflict{Reason: "global_write_lock", RequestedStart: start, RequestedEnd: end, RequestedWrite: write}
	}
	idx := lowerBoundLock(m.locks, start)
	for i := idx; i < len(m.locks); i++ {
		l := m.locks[i]
		if l.start >= end {
			break
		}
		if !rangesOverlap(start, end, l.start, l.end) {
			continue
		}
		if write || l.write {
			return false, &proto.LockConflict{
				Reason:         "range_overlap",
				RequestedStart: start, RequestedEnd: end, RequestedWrite: write,
				HeldStart: l.start, HeldEnd: l.end, HeldWrite: l.write,
			}
		}
	}
	for i := idx - 1; i >= 0; i-- {
		l := m.locks[i]
		if l.end <= start {
			continue
		}
		if rangesOverlap(start, end, l.start, l.end) && (write || l.write) {
			return false, &proto.LockConflict{
				Reason:         "range_overlap",
				RequestedStart: start, RequestedEnd: end, RequestedWrite: write,
				HeldStart: l.start, HeldEnd: l.end, HeldWrite: l.write,
			}
		}
	}
	newLock := RangeLock{start: start, end: end, write: write}
	m.locks = append(m.locks, RangeLock{})
	copy(m.locks[idx+1:], m.locks[idx:])
	m.locks[idx] = newLock
	return true, nil
}

func lowerBoundLock(locks []RangeLock, start uint64) int {
	lo, hi := 0, len(locks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if locks[mid].start < start {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (m *LockManager) unlock(target RangeLock) {
	for i := range m.locks {
		l := m.locks[i]
		if l.start == target.start && l.end == target.end && l.write == target.write {
			m.locks = append(m.locks[:i], m.locks[i+1:]...)
			return
		}
	}
}

func rangesOverlap(aStart, aEnd, bStart, bEnd uint64) bool {
	return (RangeLock{start: aStart, end: aEnd}).OverlapsWith(RangeLock{start: bStart, end: bEnd})
}

func overlapsAnyWrite(item minitxn.RangeItem, writes []proto.WriteItem) bool {
	start, end := item.Offset(), item.Offset()+item.Size()
	for _, w := range writes {
		wStart, wEnd := w.Offset(), w.Offset()+w.Size()
		if rangesOverlap(start, end, wStart, wEnd) {
			return true
		}
	}
	return false
}

func requestHasDecisionDeadline(deadlineNanos int64, items []proto.CmpItem) bool {
	if deadlineNanos != 0 {
		return true
	}
	for _, item := range items {
		if !item.ValidUntil.IsZero() {
			return true
		}
	}
	return false
}

type RecoveryPeer interface {
	QueryTidStatus(ctx context.Context, req proto.QueryTidStatusRequest) (proto.QueryTidStatusResponse, error)
	NotifyApplied(ctx context.Context, req proto.NotifyAppliedRequest) (proto.NotifyAppliedResponse, error)
}

type ReplicaPeer interface {
	ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error)
	ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error)
	ReplicateReplayRecord(ctx context.Context, req proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error)
}

type deadlineDecisionCapabilityPeer interface {
	DescribeTransactionCapabilities(context.Context, proto.DescribeTransactionCapabilitiesRequest) (proto.DescribeTransactionCapabilitiesResponse, error)
}

type PrimarySyncPeer interface {
	LockAllBlockingOwned(ctx context.Context, owner string) error
	UnlockAllOwned(owner string)
	ExportReplicationBaselineChunk(ctx context.Context, req proto.ExportReplicationBaselineChunkRequest) (proto.ExportReplicationBaselineChunkResponse, error)
	ListCommittedTIDs(ctx context.Context, req proto.ListCommittedTIDsRequest) (proto.ListCommittedTIDsResponse, error)
	ListRedoEntries(ctx context.Context, req proto.ListRedoEntriesRequest) (proto.ListRedoEntriesResponse, error)
}

type Participant struct {
	mu            sync.Mutex
	cond          *sync.Cond
	hostId        uint32
	memory        []byte
	storageDir    string
	durable       bool
	diskImage     *storage.DiskImage
	redoLog       *storage.RedoLog
	decided       *storage.DecidedTransactionLog
	forcedAborts  *storage.ForcedAbortLog
	replay        *storage.ReplayStore
	inDoubt       map[proto.TransactionID]*preparedTxn
	locks         LockManager
	epoch         uint64
	allLogTxIds   map[proto.TransactionID]struct{}
	participants  map[uint32]RecoveryPeer
	epochDuration time.Duration
	nowFn         func() time.Time
	epochBase     time.Time

	appliedLocal    map[proto.TransactionID]bool
	appliedRemote   map[proto.TransactionID]map[uint32]bool
	pendingNotify   map[uint32]map[proto.TransactionID]struct{}
	logParticipants map[proto.TransactionID][]uint32
	timelines       map[proto.TransactionID][]proto.TxnTimelineEvent
	prepareOutcomes map[proto.TransactionID]prepareOutcome
	onePCOutcomes   map[proto.TransactionID]onePCOutcome
	commitOutcomes  map[proto.TransactionID]commitOutcome

	backupMode         bool
	prepareFlights     map[proto.TransactionID]*prepareFlight
	onePCFlights       map[proto.TransactionID]*onePCFlight
	pendingDiskWrites  []proto.WriteItem
	lastBackupBoundary proto.TransactionID

	mode                     ReplicationMode
	replica                  ReplicaPeer
	replicaID                uint32
	primaryID                uint32
	heartbeat                time.Time
	partnerRPC               string
	partnerHTTP              string
	lastReplicationError     string
	generation               uint64
	peerTLS                  *tls.Config
	peerTimeout              time.Duration
	localBarrierOwner        string
	globalBarrierLeaseTTL    time.Duration
	baselineTransfer         *baselineTransfer
	baselineRecoveryRequired bool

	logger           *slog.Logger
	metrics          *nodeMetrics
	recoveryComplete bool
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
	debugLimiter                *ratelimit.KeyedLimiter
	authorizer                  *auth.Authorizer
	enableDebugEndpoints        bool
	recoverBoundaryHook         func(phase string, txId proto.TransactionID) error
	outcomeReplayBoundaryHook   func(phase string, txId proto.TransactionID) error
	baselineInstallBoundaryHook func(phase string) error
	fencingPublicationHook      func(point string) error
	writeApplyFaultHook         func(addr uint64, data []byte) error
}

func (n *Participant) DescribeTransactionCapabilities(ctx context.Context, _ proto.DescribeTransactionCapabilitiesRequest) (proto.DescribeTransactionCapabilitiesResponse, error) {
	if err := n.authorize(ctx, auth.RoleClient, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.DescribeTransactionCapabilitiesResponse{}, err
	}
	n.mu.Lock()
	mode := n.mode
	replica := n.replica
	n.mu.Unlock()
	if mode == ModeReplPrimary && replica != nil {
		capable, ok := replica.(deadlineDecisionCapabilityPeer)
		if !ok {
			return proto.DescribeTransactionCapabilitiesResponse{}, nil
		}
		resp, err := capable.DescribeTransactionCapabilities(ctx, proto.DescribeTransactionCapabilitiesRequest{})
		if err != nil || resp.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
			return proto.DescribeTransactionCapabilitiesResponse{}, err
		}
	}
	return proto.DescribeTransactionCapabilitiesResponse{DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion}, nil
}

func (n *Participant) initSafetyDefaults() {
	n.maxInFlightPrepare = 256
	n.maxInDoubtSoft = 0
	n.maxInDoubtHard = 0
	n.maxRequestBytes = 0
	n.maxItemsPerTxn = 0
	n.maxReadBytesPerTxn = 0
	n.maxWriteBytesPerTxn = 0
	n.prepareSlots = make(chan struct{}, n.maxInFlightPrepare)
}

func NewParticipant(size int) *Participant {
	now := time.Now()
	n := &Participant{memory: make([]byte, size), inDoubt: make(map[proto.TransactionID]*preparedTxn), epoch: 1, allLogTxIds: map[proto.TransactionID]struct{}{}, epochDuration: time.Minute, nowFn: time.Now, epochBase: now, participants: map[uint32]RecoveryPeer{}, appliedLocal: map[proto.TransactionID]bool{}, appliedRemote: map[proto.TransactionID]map[uint32]bool{}, pendingNotify: map[uint32]map[proto.TransactionID]struct{}{}, logParticipants: map[proto.TransactionID][]uint32{}, timelines: map[proto.TransactionID][]proto.TxnTimelineEvent{}, prepareOutcomes: map[proto.TransactionID]prepareOutcome{}, onePCOutcomes: map[proto.TransactionID]onePCOutcome{}, commitOutcomes: map[proto.TransactionID]commitOutcome{}, prepareFlights: map[proto.TransactionID]*prepareFlight{}, onePCFlights: map[proto.TransactionID]*onePCFlight{}}
	n.mode = ModeSingle
	n.metrics = newNodeMetrics()
	n.recoveryComplete = true
	n.replicaSynced = true
	n.peerTimeout = 5 * time.Second
	n.maxInFlightPrepare = 256
	n.maxInDoubtSoft = 0
	n.maxInDoubtHard = 0
	n.maxRequestBytes = 0
	n.maxItemsPerTxn = 0
	n.maxReadBytesPerTxn = 0
	n.maxWriteBytesPerTxn = 0
	n.prepareSlots = make(chan struct{}, n.maxInFlightPrepare)
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
	generation, err := loadFencingGeneration(dir)
	if err != nil {
		return nil, err
	}
	if err := retainFencingGenerations(dir, nil); err != nil {
		return nil, fmt.Errorf("retain fencing generations during startup: %w", err)
	}
	disk, mem, err := storage.OpenDiskImage(dir, size)
	if err != nil {
		return nil, err
	}
	redoLog, err := storage.OpenRedoLog(dir)
	if err != nil {
		return nil, err
	}
	decided, err := storage.OpenDecidedTransactionLog(dir)
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
		inDoubt:          make(map[proto.TransactionID]*preparedTxn),
		epoch:            1,
		allLogTxIds:      map[proto.TransactionID]struct{}{},
		participants:     map[uint32]RecoveryPeer{},
		replay:           replay,
		epochDuration:    time.Minute,
		nowFn:            time.Now,
		epochBase:        now,
		appliedLocal:     map[proto.TransactionID]bool{},
		appliedRemote:    map[proto.TransactionID]map[uint32]bool{},
		pendingNotify:    map[uint32]map[proto.TransactionID]struct{}{},
		logParticipants:  map[proto.TransactionID][]uint32{},
		timelines:        map[proto.TransactionID][]proto.TxnTimelineEvent{},
		prepareOutcomes:  map[proto.TransactionID]prepareOutcome{},
		onePCOutcomes:    map[proto.TransactionID]onePCOutcome{},
		commitOutcomes:   map[proto.TransactionID]commitOutcome{},
		prepareFlights:   map[proto.TransactionID]*prepareFlight{},
		onePCFlights:     map[proto.TransactionID]*onePCFlight{},
		generation:       generation,
		mode:             ModeSingle,
		metrics:          newNodeMetrics(),
		recoveryComplete: false,
		replicaSynced:    true,
		peerTimeout:      5 * time.Second,
	}
	n.initSafetyDefaults()
	n.cond = sync.NewCond(&n.mu)
	n.localBarrierOwner = fmt.Sprintf("node/%p", n)
	n.globalBarrierLeaseTTL = defaultGlobalBarrierLeaseTTL
	redoLog.SetObservers(func(entryBytes int) {
		n.metrics.redoEntriesWritten.Inc()
		n.metrics.redoLogSizeBytes.Set(int64(len(redoLog.Entries())))
		n.metrics.bytesEvents.Add(time.Now(), float64(entryBytes))
	}, func(latency time.Duration) {
		n.metrics.redoFsyncs.Inc()
		n.metrics.fsyncLatency.Observe(latency)
	})
	n.metrics.diskImageSizeBytes.Set(int64(size))
	for _, e := range redoLog.Entries() {
		n.allLogTxIds[e.TransactionId] = struct{}{}
		n.logParticipants[e.TransactionId] = append([]uint32(nil), e.HostIds...)
		if committed, ok := decided.Get(e.TransactionId); ok {
			n.commitOutcomes[e.TransactionId] = commitOutcome{resp: proto.CommitResponse{Ack: true}, actionCommit: committed}
			if committed {
				n.appliedLocal[e.TransactionId] = true
				n.enqueueAppliedNotificationsLocked(e.TransactionId, e.HostIds)
			}
		} else {
			n.inDoubt[e.TransactionId] = &preparedTxn{
				writes:                cloneWrites(e.WriteItems),
				participants:          append([]uint32(nil), e.HostIds...),
				decisionDeadlineNanos: e.DecisionDeadlineNanos,
				redoLoggedTid:         true,
				preparedAt:            now,
				correlationID:         e.TransactionId.CorrelationID(),
			}
		}
	}
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
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

func (n *Participant) SetEpochSourceForTest(epochDuration time.Duration, nowFn func() time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if epochDuration > 0 {
		n.epochDuration = epochDuration
	}
	if nowFn != nil {
		n.nowFn = nowFn
		n.epochBase = nowFn()
	}
}

func (n *Participant) SetRedoBatchingForTest(window time.Duration, max int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog != nil {
		n.redoLog.ConfigureBatchingForTest(window, max)
	}
}

func (n *Participant) SetRedoFsyncHookForTest(fn func(*os.File) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog != nil {
		n.redoLog.SetFsyncHookForTest(fn)
	}
}

func (n *Participant) SetRecoverBoundaryHookForTest(fn func(phase string, txId proto.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recoverBoundaryHook = fn
}

func (n *Participant) SetOutcomeReplayBoundaryHookForTest(fn func(phase string, txId proto.TransactionID) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.outcomeReplayBoundaryHook = fn
}

// SetWriteApplyFaultForTest lets recovery tests fail a write after memory changes
// but before the disk image is updated.
func (n *Participant) SetWriteApplyFaultForTest(fn func(addr uint64, data []byte) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.writeApplyFaultHook = fn
}

func (n *Participant) RedoFsyncCountForTest() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog == nil {
		return 0
	}
	return n.redoLog.FsyncCallCount()
}

func (n *Participant) currentEpochLocked() uint64 {
	if n.epochDuration <= 0 {
		n.epochDuration = time.Minute
	}
	now := time.Now()
	if n.nowFn != nil {
		now = n.nowFn()
	}
	if n.epochBase.IsZero() {
		n.epochBase = now
	}
	elapsed := now.Sub(n.epochBase)
	if elapsed < 0 {
		elapsed = 0
	}
	current := uint64(elapsed/n.epochDuration) + 1
	if current == 0 {
		current = 1
	}
	if current > n.epoch {
		n.epoch = current
	}
	return n.epoch
}

func (n *Participant) SetRecoveryPeers(peers map[uint32]RecoveryPeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.participants = make(map[uint32]RecoveryPeer, len(peers))
	for k, v := range peers {
		n.participants[k] = v
	}
}

func (n *Participant) SetPeerDialer(tlsConfig *tls.Config, timeout time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerTLS = tlsConfig
	if timeout > 0 {
		n.peerTimeout = timeout
	}
}

func (n *Participant) Generation() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.generation
}

func (n *Participant) SetGenerationForTest(generation uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.generation = generation
}

func (n *Participant) checkGenerationLocked(generation uint64) error {
	if n.generation == 0 {
		return nil
	}
	if generation == 0 || generation != n.generation {
		return ErrStaleGeneration
	}
	return nil
}

func generationFromContext(ctx context.Context) uint64 {
	if generation, ok := proto.GenerationFromContext(ctx); ok {
		return generation
	}
	return 0
}

func (n *Participant) replicationRoleLocked() proto.ReplicationRole {
	switch n.mode {
	case ModeReplPrimary:
		return proto.ReplicationRolePrimary
	case ModeReplReplica:
		return proto.ReplicationRoleReplica
	default:
		return proto.ReplicationRoleSingle
	}
}

func (n *Participant) setReplicationErrorLocked(err error) {
	if err == nil {
		n.lastReplicationError = ""
		return
	}
	n.lastReplicationError = err.Error()
}

func (n *Participant) newRPCPeerLocked(addr string) *RPCPeer {
	return NewRPCPeer(addr, n.peerTLS, n.peerTimeout)
}

func (n *Participant) configureAsPrimaryLocked(replicaID uint32, replica ReplicaPeer) {
	n.mode = ModeReplPrimary
	n.replicaID = replicaID
	n.replica = replica
	n.primaryID = 0
	n.replicaSynced = true
	n.metrics.replicaLagEntries.Set(0)
}

func (n *Participant) ConfigureAsPrimary(replicaID uint32, replica ReplicaPeer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configureAsPrimaryLocked(replicaID, replica)
}

func (n *Participant) configureAsReplicaLocked(primaryID uint32) {
	n.mode = ModeReplReplica
	n.primaryID = primaryID
	n.replica = nil
	n.replicaID = 0
	n.replicaSynced = false
	n.metrics.replicaLagEntries.Set(1)
}

func (n *Participant) ConfigureAsReplica(primaryID uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.configureAsReplicaLocked(primaryID)
}

func (n *Participant) PromoteToPrimary(replicaID uint32, replica ReplicaPeer) {
	n.ConfigureAsPrimary(replicaID, replica)
	n.mu.Lock()
	n.replicaSynced = true
	n.mu.Unlock()
}

func (n *Participant) RecordPrimaryHeartbeat() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.heartbeat = time.Now()
}

func (n *Participant) PrimaryLikelyDown(timeout time.Duration) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica || timeout <= 0 {
		return false
	}
	if n.heartbeat.IsZero() {
		return false
	}
	return time.Since(n.heartbeat) > timeout
}

func (n *Participant) ConfigureReplication(ctx context.Context, req proto.ConfigureReplicationRequest) (proto.ConfigureReplicationResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ConfigureReplicationResponse{Ack: false}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	switch req.Role {
	case proto.ReplicationRoleSingle:
	case proto.ReplicationRolePrimary:
		if n.redoLog == nil {
			return proto.ConfigureReplicationResponse{Ack: false}, ErrReplicationRequiresDurability
		}
		if req.PartnerRPC == "" {
			return proto.ConfigureReplicationResponse{Ack: false}, errors.New("primary replication requires partner_rpc")
		}
	case proto.ReplicationRoleReplica:
		if n.redoLog == nil {
			return proto.ConfigureReplicationResponse{Ack: false}, ErrReplicationRequiresDurability
		}
		if req.PartnerRPC == "" {
			return proto.ConfigureReplicationResponse{Ack: false}, errors.New("replica replication requires partner_rpc")
		}
	default:
		return proto.ConfigureReplicationResponse{Ack: false}, errors.New("unknown replication role")
	}
	if n.generation != 0 && req.Generation < n.generation {
		return proto.ConfigureReplicationResponse{Ack: false}, ErrStaleGeneration
	}
	if n.storageDir != "" && req.Generation > n.generation && req.FencingProtocolVersion < proto.ReplicationFencingProtocolVersion {
		return proto.ConfigureReplicationResponse{Ack: false}, ErrReplicationFencingProtocolUpgradeRequired
	}
	if req.Generation > n.generation {
		if n.storageDir != "" {
			if err := persistFencingGenerationWithHook(n.storageDir, req.Generation, n.fencingPublicationHook); err != nil {
				if isFencingPublicationOutcomeUnknown(err) {
					n.generation = req.Generation
					return proto.ConfigureReplicationResponse{Ack: false, OutcomeUnknown: true, Generation: n.generation}, nil
				}
				return proto.ConfigureReplicationResponse{Ack: false}, err
			}
		}
		n.generation = req.Generation
	}

	switch req.Role {
	case proto.ReplicationRoleSingle:
		n.mode = ModeSingle
		n.replica = nil
		n.replicaID = 0
		n.primaryID = 0
		n.partnerRPC = ""
		n.partnerHTTP = ""
		n.replicaSynced = true
		n.metrics.replicaLagEntries.Set(0)
	case proto.ReplicationRolePrimary:
		n.configureAsPrimaryLocked(0, n.newRPCPeerLocked(req.PartnerRPC))
		n.partnerRPC = req.PartnerRPC
		n.partnerHTTP = req.PartnerHTTP
	case proto.ReplicationRoleReplica:
		n.configureAsReplicaLocked(0)
		n.partnerRPC = req.PartnerRPC
		n.partnerHTTP = req.PartnerHTTP
		n.replicaSynced = req.MarkSynced
		if req.MarkSynced {
			n.metrics.replicaLagEntries.Set(0)
		}
	}
	n.setReplicationErrorLocked(nil)
	return proto.ConfigureReplicationResponse{Ack: true, Generation: n.generation}, nil
}

func (n *Participant) ReplicationStatus(ctx context.Context, _ proto.ReplicationStatusRequest) (proto.ReplicationStatusResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ReplicationStatusResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return proto.ReplicationStatusResponse{
		Role:          n.replicationRoleLocked(),
		Ready:         n.isReadyLocked(),
		ReplicaSynced: n.replicaSynced,
		PartnerRPC:    n.partnerRPC,
		PartnerHTTP:   n.partnerHTTP,
		LastError:     n.lastReplicationError,
		Generation:    n.generation,
	}, nil
}

func (n *Participant) Read(ctx context.Context, addr uint64, length uint32) ([]byte, error) {
	if err := n.authorize(ctx, auth.RoleClient, auth.RoleManager, auth.RoleAdmin); err != nil {
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.checkGenerationLocked(generationFromContext(ctx)); err != nil {
		return nil, err
	}
	return n.read(addr, length)
}

func (n *Participant) read(addr uint64, length uint32) ([]byte, error) {
	end := addr + uint64(length)
	if end > uint64(len(n.memory)) {
		return nil, ErrOutOfBounds
	}
	data := make([]byte, length)
	copy(data, n.memory[addr:end])
	return data, nil
}

func (n *Participant) Write(ctx context.Context, addr uint64, data []byte) error {
	if err := n.authorize(ctx, auth.RoleClient, auth.RoleManager, auth.RoleAdmin); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.checkGenerationLocked(generationFromContext(ctx)); err != nil {
		return err
	}
	if n.durable {
		return ErrDirectWriteRequiresMiniTransaction
	}
	return n.write(addr, data)
}

func (n *Participant) write(addr uint64, data []byte) error {
	end := addr + uint64(len(data))
	if end > uint64(len(n.memory)) {
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
			n.pendingDiskWrites = append(n.pendingDiskWrites, proto.WriteItem{MemID: n.hostId, Addr: addr, Data: append([]byte(nil), data...)})
			return nil
		}
		if err := n.diskImage.ApplyWrite(addr, data); err != nil {
			return err
		}
	}
	return nil
}

func cloneReadData(in map[uint32][]byte) map[uint32][]byte {
	if in == nil {
		return nil
	}
	out := make(map[uint32][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func clonePrepareResponse(resp proto.ExecPrepareResponse) proto.ExecPrepareResponse {
	resp.ReadData = cloneReadData(resp.ReadData)
	if resp.LockConflict != nil {
		conflict := *resp.LockConflict
		resp.LockConflict = &conflict
	}
	return resp
}

func clone1PCResponse(resp proto.ExecCommit1PCResponse) proto.ExecCommit1PCResponse {
	resp.ReadData = cloneReadData(resp.ReadData)
	if resp.LockConflict != nil {
		conflict := *resp.LockConflict
		resp.LockConflict = &conflict
	}
	return resp
}

func (n *Participant) hasDurableTransactionLocked(txId proto.TransactionID) bool {
	if _, ok := n.allLogTxIds[txId]; ok {
		return true
	}
	if n.decided != nil {
		_, ok := n.decided.Get(txId)
		return ok
	}
	return false
}

func (n *Participant) callOutcomeReplayBoundaryHookLocked(phase string, txId proto.TransactionID) error {
	if n.outcomeReplayBoundaryHook == nil {
		return nil
	}
	return n.outcomeReplayBoundaryHook(phase, txId)
}

// replicateReplayRecordLocked saves a reply on the replica before returning it
// to the caller. Release the primary's mutex during the RPC so the replica can
// take its own lock without deadlocking.
func (n *Participant) replicateReplayRecordLocked(txId proto.TransactionID, kind proto.ReplayRecordKind, prepare *proto.ExecPrepareResponse, onePC *proto.ExecCommit1PCResponse) error {
	if n.mode != ModeReplPrimary || n.replica == nil {
		return nil
	}
	req := proto.ReplicateReplayRecordRequest{TID: txId, Kind: kind}
	if prepare != nil {
		cloned := clonePrepareResponse(*prepare)
		req.Prepare = &cloned
	}
	if onePC != nil {
		cloned := clone1PCResponse(*onePC)
		req.OnePC = &cloned
	}
	peer := n.replica
	n.mu.Unlock()
	start := time.Now()
	replResp, replErr := peer.ReplicateReplayRecord(context.Background(), req)
	n.metrics.replicationRoundtripLatency.Observe(time.Since(start))
	n.mu.Lock()
	if replErr != nil || !replResp.Ack {
		n.metrics.replicationFailures.Inc()
		if replErr != nil {
			n.setReplicationErrorLocked(replErr)
			return replErr
		}
		err := errors.New("replication replay record not acknowledged")
		n.setReplicationErrorLocked(err)
		return err
	}
	n.setReplicationErrorLocked(nil)
	return nil
}

func (n *Participant) rememberPrepareOutcomeLocked(txId proto.TransactionID, resp proto.ExecPrepareResponse) (proto.ExecPrepareResponse, error) {
	if n.replay != nil {
		if err := n.replay.AppendPrepare(txId, resp); err != nil {
			return proto.ExecPrepareResponse{RequestID: resp.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
		}
	}
	if err := n.replicateReplayRecordLocked(txId, proto.ReplayRecordPrepare, &resp, nil); err != nil {
		return proto.ExecPrepareResponse{RequestID: resp.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
	}
	n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(resp)}
	return resp, nil
}

func (n *Participant) replayPrepareOutcomeLocked(txId proto.TransactionID, requestID proto.RequestID, currentEpoch uint64) (proto.ExecPrepareResponse, bool, error) {
	var original proto.ExecPrepareResponse
	if out, ok := n.prepareOutcomes[txId]; ok {
		original = clonePrepareResponse(out.resp)
	} else {
		if n.replay == nil {
			return proto.ExecPrepareResponse{}, false, nil
		}
		var ok bool
		original, ok = n.replay.GetPrepare(txId)
		if !ok {
			return proto.ExecPrepareResponse{}, false, nil
		}
		n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(original)}
	}
	if err := n.replicateReplayRecordLocked(txId, proto.ReplayRecordPrepare, &original, nil); err != nil {
		return proto.ExecPrepareResponse{RequestID: requestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, false, err
	}
	resp := original
	resp.RequestID = requestID
	resp.CurrentEpoch = currentEpoch
	return resp, true, nil
}

func (n *Participant) prepareVotedOKLocked(txId proto.TransactionID) bool {
	if out, ok := n.prepareOutcomes[txId]; ok {
		return out.resp.Vote == proto.VoteOk
	}
	if n.replay == nil {
		return false
	}
	resp, ok := n.replay.GetPrepare(txId)
	if !ok {
		return false
	}
	n.prepareOutcomes[txId] = prepareOutcome{resp: clonePrepareResponse(resp)}
	return resp.Vote == proto.VoteOk
}

func (n *Participant) rememberOnePCOutcomeLocked(txId proto.TransactionID, resp proto.ExecCommit1PCResponse) (proto.ExecCommit1PCResponse, error) {
	if n.replay != nil {
		if err := n.replay.AppendOnePC(txId, resp); err != nil {
			return proto.ExecCommit1PCResponse{RequestID: resp.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
		}
	}
	if err := n.replicateReplayRecordLocked(txId, proto.ReplayRecordOnePC, nil, &resp); err != nil {
		return proto.ExecCommit1PCResponse{RequestID: resp.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: resp.CurrentEpoch}, err
	}
	n.onePCOutcomes[txId] = onePCOutcome{resp: clone1PCResponse(resp)}
	return resp, nil
}

func (n *Participant) ExecAndPrepare(ctx context.Context, req proto.ExecPrepareRequest) (resp proto.ExecPrepareResponse, err error) {
	if !n.allowRate(n.prepareLimiter, "prepare") {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrResourceExhausted
	}
	select {
	case n.prepareSlots <- struct{}{}:
		defer func() { <-n.prepareSlots }()
	default:
		n.metrics.rejectedRequestsTotal.Inc()
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrResourceExhausted
	}
	if err := n.authorize(ctx, auth.RoleClient, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
	}
	if requestHasDecisionDeadline(req.DecisionDeadlineNanos, req.CmpItems) && req.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrDeadlineDecisionProtocolUpgradeRequired
	}
	if n.maxItemsPerTxn > 0 && len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems) > n.maxItemsPerTxn {
		n.metrics.rejectedRequestsTotal.Inc()
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrResourceExhausted
	}
	var reqBytes int64
	for _, i := range req.ReadItems {
		reqBytes += int64(i.Len)
	}
	for _, i := range req.WriteItems {
		reqBytes += int64(len(i.Data))
	}
	if (n.maxRequestBytes > 0 && reqBytes > int64(n.maxRequestBytes)) || ((n.maxReadBytesPerTxn > 0 || n.maxWriteBytesPerTxn > 0) && reqBytes > n.maxReadBytesPerTxn+n.maxWriteBytesPerTxn) {
		n.metrics.requestSizeRejectedTotal.Inc()
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrResourceExhausted
	}

	start := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if resp, ok, replayErr := n.replayPrepareOutcomeLocked(req.TID, req.RequestID, currentEpoch); ok || replayErr != nil {
		if replayErr != nil {
			return resp, replayErr
		}
		return resp, nil
	}
	if flight, ok := n.prepareFlights[req.TID]; ok {
		n.mu.Unlock()
		select {
		case <-flight.done:
			n.mu.Lock()
			resp := clonePrepareResponse(flight.resp)
			resp.RequestID = req.RequestID
			resp.CurrentEpoch = currentEpoch
			return resp, flight.err
		case <-ctx.Done():
			n.mu.Lock()
			return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, ctx.Err()
		}
	}
	flight := &prepareFlight{done: make(chan struct{})}
	n.prepareFlights[req.TID] = flight
	defer func() {
		flight.resp = clonePrepareResponse(resp)
		flight.err = err
		delete(n.prepareFlights, req.TID)
		close(flight.done)
	}()

	if n.hasDurableTransactionLocked(req.TID) {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: prepare transaction %v", ErrReplayUnavailable, req.TID)
	}
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadStaleEpoch, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if out, ok := n.commitOutcomes[req.TID]; ok && !out.actionCommit {
		return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadForced, CurrentEpoch: currentEpoch})
	}
	if txn, ok := n.inDoubt[req.TID]; ok {
		resp := proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteOk, ReadData: cloneReadData(txn.readData), CurrentEpoch: currentEpoch}
		return resp, nil
	}

	if err := n.validateWriteRangesLocked(req.WriteItems); err != nil {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	if n.maxInDoubtHard > 0 && len(n.inDoubt) >= n.maxInDoubtHard {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, ErrResourceExhausted
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.TID.CorrelationID()
	}
	decisionDeadlineNanos := req.DecisionDeadlineNanos
	for _, item := range req.CmpItems {
		deadline := item.ValidUntil.UnixNano()
		if item.ValidUntil.IsZero() || (decisionDeadlineNanos != 0 && deadline >= decisionDeadlineNanos) {
			continue
		}
		decisionDeadlineNanos = deadline
	}
	n.inDoubt[req.TID] = &preparedTxn{preparedAt: n.nowFn(), participants: append([]uint32(nil), req.Participants...), decisionDeadlineNanos: decisionDeadlineNanos, class: req.Class, correlationID: correlationID}
	txn := n.inDoubt[req.TID]
	n.recordTimelineLocked(req.TID, correlationID, "prepare_received", "")
	acquired := make([]RangeLock, 0, len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems))

	lockRead := func(item minitxn.RangeItem) (bool, *proto.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: false}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, false)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}
	lockWrite := func(item minitxn.RangeItem) (bool, *proto.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: true}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, true)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}

	for _, item := range req.WriteItems {
		if ok, conflict := lockWrite(item); !ok {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "write-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.CmpItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "cmp-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.ReadItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_bad_lock", "read-lock conflict")
			return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}

	for _, item := range req.CmpItems {
		if !item.ValidUntil.IsZero() && !n.nowFn().Before(item.ValidUntil) {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return n.rememberPrepareOutcomeLocked(req.TID, proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
	}

	readData := make(map[uint32][]byte, len(req.ReadItems))
	for idx, item := range req.ReadItems {
		data, err := n.read(item.Addr, item.Len)
		if err != nil {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		readData[uint32(idx)] = data
	}

	// Save our yes vote before replying. Even with no local writes, we need an
	// empty entry if the transaction writes elsewhere: recovery needs every vote.
	if n.redoLog != nil && (req.HasWrites || len(req.WriteItems) > 0) {
		participants := append([]uint32(nil), req.Participants...)
		clonedWrites := cloneWrites(req.WriteItems)
		if err := n.redoLog.AddItem(storage.RedoLogItem{TransactionId: req.TID, HostIds: participants, DecisionDeadlineNanos: decisionDeadlineNanos, WriteItems: clonedWrites}); err != nil {
			n.releaseLocks(acquired)
			delete(n.inDoubt, req.TID)
			return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		if n.mode == ModeReplPrimary && n.replica != nil {
			n.mu.Unlock()
			replStart := time.Now()
			replResp, replErr := n.replica.ReplicateRedoEntry(context.Background(), proto.ReplicateRedoEntryRequest{Entry: proto.RedoEntryPayload{TID: req.TID, Participants: participants, DecisionDeadlineNanos: decisionDeadlineNanos, WriteItems: clonedWrites}})
			n.metrics.replicationRoundtripLatency.Observe(time.Since(replStart))
			n.metrics.replicationEntriesSent.Inc()
			n.mu.Lock()
			if replErr != nil || !replResp.Ack {
				n.metrics.replicationFailures.Inc()
				if replErr != nil {
					n.setReplicationErrorLocked(replErr)
				} else {
					n.setReplicationErrorLocked(errors.New("replication redo entry not acknowledged"))
				}
				n.releaseLocks(acquired)
				delete(n.inDoubt, req.TID)
				return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = participants
		txn.redoLoggedTid = true
	}

	txn.writes = cloneWrites(req.WriteItems)
	txn.readData = readData
	txn.heldLocks = acquired

	n.metrics.prepareLatency.Observe(time.Since(start))
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
	n.log(slog.LevelInfo, "ExecAndPrepare received", "tid", req.TID, "epoch", req.TID.Epoch)
	n.log(slog.LevelInfo, "Vote decision", "tid", req.TID, "epoch", req.TID.Epoch, "vote", proto.VoteOk)
	n.recordTimelineLocked(req.TID, correlationID, "prepare_vote_ok", "")
	resp = proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	if err := n.callOutcomeReplayBoundaryHookLocked("prepare-before-response", req.TID); err != nil {
		return proto.ExecPrepareResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	return n.rememberPrepareOutcomeLocked(req.TID, resp)
}

func (n *Participant) ExecCommit1PC(ctx context.Context, req proto.ExecCommit1PCRequest) (resp proto.ExecCommit1PCResponse, err error) {
	if err := n.authorize(ctx, auth.RoleClient, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
	}
	if requestHasDecisionDeadline(0, req.CmpItems) && req.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrDeadlineDecisionProtocolUpgradeRequired
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal}, err
	}
	currentEpoch := n.currentEpochLocked()
	if out, ok := n.onePCOutcomes[req.TID]; ok {
		original := clone1PCResponse(out.resp)
		if err := n.replicateReplayRecordLocked(req.TID, proto.ReplayRecordOnePC, nil, &original); err != nil {
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
		resp := original
		resp.RequestID = req.RequestID
		resp.CurrentEpoch = currentEpoch
		return resp, nil
	}
	correlationID := req.CorrelationID
	if n.replay != nil {
		if original, ok := n.replay.GetOnePC(req.TID); ok {
			n.onePCOutcomes[req.TID] = onePCOutcome{resp: clone1PCResponse(original)}
			if err := n.replicateReplayRecordLocked(req.TID, proto.ReplayRecordOnePC, nil, &original); err != nil {
				return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
			}
			original.RequestID = req.RequestID
			original.CurrentEpoch = currentEpoch
			return original, nil
		}
	}
	if flight, ok := n.onePCFlights[req.TID]; ok {
		n.mu.Unlock()
		select {
		case <-flight.done:
			n.mu.Lock()
			resp := clone1PCResponse(flight.resp)
			resp.RequestID = req.RequestID
			resp.CurrentEpoch = currentEpoch
			return resp, flight.err
		case <-ctx.Done():
			n.mu.Lock()
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, ctx.Err()
		}
	}
	flight := &onePCFlight{done: make(chan struct{})}
	n.onePCFlights[req.TID] = flight
	defer func() {
		flight.resp = clone1PCResponse(resp)
		flight.err = err
		delete(n.onePCFlights, req.TID)
		close(flight.done)
	}()

	if n.hasDurableTransactionLocked(req.TID) {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, fmt.Errorf("%w: one-phase transaction %v", ErrReplayUnavailable, req.TID)
	}
	if correlationID == "" {
		correlationID = req.TID.CorrelationID()
	}
	n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_received", "")
	if (storage.ReplayRetention{CurrentEpoch: currentEpoch, RetryHorizon: storage.PublicRetryHorizon}).Expired(req.TID) {
		return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadStaleEpoch, CurrentEpoch: currentEpoch})
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadForced, CurrentEpoch: currentEpoch})
	}

	if err := n.validateWriteRangesLocked(req.WriteItems); err != nil {
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	acquired := make([]RangeLock, 0, len(req.CmpItems)+len(req.ReadItems)+len(req.WriteItems))
	lockRead := func(item minitxn.RangeItem) (bool, *proto.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: false}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, false)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}
	lockWrite := func(item minitxn.RangeItem) (bool, *proto.LockConflict) {
		if item.Size() == 0 {
			return true, nil
		}
		l := RangeLock{start: item.Offset(), end: item.Offset() + item.Size(), write: true}
		ok, conflict := n.locks.tryLockWithConflict(l.start, l.end, true)
		if !ok {
			return false, conflict
		}
		acquired = append(acquired, l)
		return true, nil
	}

	for _, item := range req.WriteItems {
		if ok, conflict := lockWrite(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "write-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.CmpItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "cmp-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}
	for _, item := range req.ReadItems {
		if overlapsAnyWrite(item, req.WriteItems) {
			continue
		}
		if ok, conflict := lockRead(item); !ok {
			n.releaseLocks(acquired)
			n.recordTimelineLocked(req.TID, correlationID, "commit_1pc_bad_lock", "read-lock conflict")
			return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadLock, CurrentEpoch: currentEpoch, LockConflict: conflict})
		}
	}

	for _, item := range req.CmpItems {
		if !item.ValidUntil.IsZero() && !n.nowFn().Before(item.ValidUntil) {
			n.releaseLocks(acquired)
			return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
		actual, err := n.read(item.Addr, uint32(len(item.Data)))
		if err != nil || string(actual) != string(item.Data) {
			n.releaseLocks(acquired)
			return n.rememberOnePCOutcomeLocked(req.TID, proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadCmp, CurrentEpoch: currentEpoch})
		}
	}

	readData := make(map[uint32][]byte, len(req.ReadItems))
	for idx, item := range req.ReadItems {
		data, err := n.read(item.Addr, item.Len)
		if err != nil {
			n.releaseLocks(acquired)
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		readData[uint32(idx)] = data
	}

	if n.redoLog != nil && len(req.WriteItems) > 0 {
		writes := cloneWrites(req.WriteItems)
		entry := storage.RedoLogItem{TransactionId: req.TID, HostIds: []uint32{n.hostId}, WriteItems: writes}
		if err := n.redoLog.AddItem(entry); err != nil {
			n.releaseLocks(acquired)
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
		}
		if n.mode == ModeReplPrimary && n.replica != nil {
			n.mu.Unlock()
			replStart := time.Now()
			replResp, replErr := n.replica.ReplicateRedoEntry(context.Background(), proto.ReplicateRedoEntryRequest{
				Entry: proto.RedoEntryPayload{TID: req.TID, Participants: append([]uint32(nil), entry.HostIds...), WriteItems: writes},
			})
			n.metrics.replicationRoundtripLatency.Observe(time.Since(replStart))
			n.metrics.replicationEntriesSent.Inc()
			n.mu.Lock()
			if replErr != nil || !replResp.Ack {
				n.metrics.replicationFailures.Inc()
				if replErr != nil {
					n.setReplicationErrorLocked(replErr)
				} else {
					n.setReplicationErrorLocked(errors.New("replication redo entry not acknowledged"))
				}
				n.releaseLocks(acquired)
				return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
			n.mu.Unlock()
			replCommitResp, replCommitErr := n.replica.ReplicateCommitDecision(context.Background(), proto.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: true})
			n.mu.Lock()
			if replCommitErr != nil || !replCommitResp.Ack {
				n.metrics.replicationFailures.Inc()
				if replCommitErr != nil {
					n.setReplicationErrorLocked(replCommitErr)
				} else {
					n.setReplicationErrorLocked(errors.New("replication commit decision not acknowledged"))
				}
				n.releaseLocks(acquired)
				return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
			n.setReplicationErrorLocked(nil)
		}
		n.allLogTxIds[req.TID] = struct{}{}
		n.logParticipants[req.TID] = []uint32{n.hostId}
	}
	if n.decided != nil {
		if _, known := n.decided.Get(req.TID); !known {
			if err := n.decided.AddItem(req.TID, true); err != nil {
				n.releaseLocks(acquired)
				return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, nil
			}
		}
	}
	if err := n.callOutcomeReplayBoundaryHookLocked("one-pc-before-response", req.TID); err != nil {
		n.releaseLocks(acquired)
		return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
	}
	resp = proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteOk, ReadData: cloneReadData(readData), CurrentEpoch: currentEpoch}
	for _, item := range req.WriteItems {
		if err := n.write(item.Addr, item.Data); err != nil {
			n.recoveryComplete = false
			n.releaseLocks(acquired)
			return proto.ExecCommit1PCResponse{RequestID: req.RequestID, Vote: proto.VoteBadInternal, CurrentEpoch: currentEpoch}, err
		}
	}
	n.releaseLocks(acquired)
	var replayErr error
	resp, replayErr = n.rememberOnePCOutcomeLocked(req.TID, resp)
	if replayErr != nil {
		return resp, replayErr
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		n.appliedLocal[req.TID] = true
		n.enqueueAppliedNotificationsLocked(req.TID, []uint32{n.hostId})
	}
	n.metrics.minitransactionsTotal.Inc()
	n.metrics.minitransactionsCommitted.Inc()
	n.metrics.ObserveClassDecision(req.Class, true, n.nowFn())
	n.recordTimelineLocked(req.TID, correlationID, "commit_applied", "1pc")
	n.commitOutcomes[req.TID] = commitOutcome{resp: proto.CommitResponse{RequestID: req.RequestID, Ack: true}, actionCommit: true}
	return resp, nil
}

func cloneWrites(items []proto.WriteItem) []proto.WriteItem {
	out := make([]proto.WriteItem, len(items))
	for i, item := range items {
		out[i] = proto.WriteItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...)}
	}
	return out
}

func (n *Participant) validateWriteRangesLocked(items []proto.WriteItem) error {
	for _, item := range items {
		end := item.Addr + uint64(len(item.Data))
		if end < item.Addr || end > uint64(len(n.memory)) {
			return ErrOutOfBounds
		}
	}
	return nil
}

func (n *Participant) releaseLocks(locks []RangeLock) {
	for i := len(locks) - 1; i >= 0; i-- {
		n.locks.unlock(locks[i])
	}
	n.cond.Broadcast()
}

func (n *Participant) Commit(ctx context.Context, req proto.CommitRequest) (proto.CommitResponse, error) {
	return n.commit(ctx, req, false)
}

func (n *Participant) commit(ctx context.Context, req proto.CommitRequest, recoveryDecision bool) (proto.CommitResponse, error) {
	if !n.allowRate(n.commitLimiter, "commit") {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrResourceExhausted
	}
	if err := n.authorize(ctx, auth.RoleClient, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, err
	}

	start := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode == ModeReplReplica {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrReplicaRejectsCoordinator
	}
	if err := n.checkGenerationLocked(req.Generation); err != nil {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, err
	}
	requestedAction := req.ActionCommit
	if out, ok := n.commitOutcomes[req.TID]; ok {
		resp := out.resp
		resp.RequestID = req.RequestID
		resp.Ack = !requestedAction || out.actionCommit
		return resp, nil
	}
	if n.transactionHasDecisionDeadlineLocked(req.TID) && req.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, ErrDeadlineDecisionProtocolUpgradeRequired
	}
	logged := false
	knownDecision := false
	if n.decided != nil {
		_, logged = n.allLogTxIds[req.TID]
		if durableAction, ok := n.decided.Get(req.TID); ok {
			// Keep the saved decision even if log cleanup or recovery removed
			// the other transaction state.
			req.ActionCommit = durableAction
			knownDecision = true
		}
	}
	if !recoveryDecision && !knownDecision && req.ActionCommit && n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, nil
	}
	if logged && !knownDecision {
		if err := n.decided.AddItem(req.TID, req.ActionCommit); err != nil {
			return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, err
		}
	}
	if n.mode == ModeReplPrimary && n.replica != nil {
		n.mu.Unlock()
		replResp, replErr := n.replica.ReplicateCommitDecision(context.Background(), proto.ReplicateCommitDecisionRequest{TID: req.TID, ActionCommit: req.ActionCommit})
		n.mu.Lock()
		if replErr != nil || !replResp.Ack {
			n.metrics.replicationFailures.Inc()
			if replErr != nil {
				n.setReplicationErrorLocked(replErr)
			} else {
				n.setReplicationErrorLocked(errors.New("replication commit decision not acknowledged"))
			}
			return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, errors.New("replication commit decision failed")
		}
		n.setReplicationErrorLocked(nil)
	}
	class := n.classForTIDLocked(req.TID)
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = n.correlationForTIDLocked(req.TID)
	}
	n.recordTimelineLocked(req.TID, correlationID, "commit_decision_received", "")
	resp, err := n.commitLocked(req)
	if err == nil && resp.Ack {
		n.commitOutcomes[req.TID] = commitOutcome{resp: resp, actionCommit: req.ActionCommit}
	}
	n.metrics.commitLatency.Observe(time.Since(start))
	n.metrics.endToEndTxnLatency.Observe(time.Since(start))
	n.metrics.minitransactionsTotal.Inc()
	if req.ActionCommit {
		n.metrics.minitransactionsCommitted.Inc()
	} else {
		n.metrics.minitransactionsAborted.Inc()
	}
	n.metrics.ObserveClassDecision(class, req.ActionCommit, n.nowFn())
	n.log(slog.LevelInfo, "Commit applied", "tid", req.TID, "epoch", req.TID.Epoch, "actionCommit", req.ActionCommit)
	if req.ActionCommit {
		n.recordTimelineLocked(req.TID, correlationID, "commit_applied", "2pc")
	} else {
		n.recordTimelineLocked(req.TID, correlationID, "abort_applied", "2pc")
	}
	if err == nil && requestedAction && !req.ActionCommit {
		resp.Ack = false
	}
	return resp, err
}

func (n *Participant) classForTIDLocked(txId proto.TransactionID) string {
	if txn, ok := n.inDoubt[txId]; ok {
		return txn.class
	}
	return ""
}

func (n *Participant) correlationForTIDLocked(txId proto.TransactionID) string {
	if txn, ok := n.inDoubt[txId]; ok && txn.correlationID != "" {
		return txn.correlationID
	}
	return txId.CorrelationID()
}

func (n *Participant) recordTimelineLocked(txId proto.TransactionID, correlationID, event, details string) {
	if correlationID == "" {
		correlationID = txId.CorrelationID()
	}
	n.timelines[txId] = append(n.timelines[txId], proto.TxnTimelineEvent{
		AtNanos:       n.nowFn().UnixNano(),
		Component:     "memnode",
		Event:         event,
		CorrelationID: correlationID,
		Details:       details,
	})
}

func (n *Participant) CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error) {
	acks := make([]bool, len(req.Decisions))
	for i, d := range req.Decisions {
		resp, err := n.Commit(ctx, d)
		if err != nil {
			return proto.CommitBatchResponse{Acks: acks}, err
		}
		acks[i] = resp.Ack
	}
	return proto.CommitBatchResponse{Acks: acks}, nil
}

// commitLocked saves decisions for logged transactions before applying writes,
// then releases the transaction's locks.
func (n *Participant) commitLocked(req proto.CommitRequest) (proto.CommitResponse, error) {

	if n.decided != nil {
		if _, ok := n.allLogTxIds[req.TID]; ok {
			if durableAction, decided := n.decided.Get(req.TID); decided {
				req.ActionCommit = durableAction
			} else {
				if err := n.decided.AddItem(req.TID, req.ActionCommit); err != nil {
					return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, err
				}
			}
		}
	}

	txn, ok := n.inDoubt[req.TID]
	if !ok {
		return proto.CommitResponse{RequestID: req.RequestID, Ack: true}, nil
	}

	flushNotify := false
	if req.ActionCommit {
		for _, item := range txn.writes {
			if err := n.write(item.Addr, item.Data); err != nil {
				n.recoveryComplete = false
				return proto.CommitResponse{RequestID: req.RequestID, Ack: false}, err
			}
		}
		if txn.redoLoggedTid {
			n.appliedLocal[req.TID] = true
			n.enqueueAppliedNotificationsLocked(req.TID, txn.participants)
			flushNotify = true
		}
	}

	n.releaseLocks(txn.heldLocks)
	delete(n.inDoubt, req.TID)
	n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
	if flushNotify {
		go n.FlushAppliedNotifications(context.Background())
	}
	return proto.CommitResponse{RequestID: req.RequestID, Ack: true}, nil
}

func (n *Participant) ReplicateRedoEntry(ctx context.Context, req proto.ReplicateRedoEntryRequest) (proto.ReplicateRedoEntryResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ReplicateRedoEntryResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return proto.ReplicateRedoEntryResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return proto.ReplicateRedoEntryResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	if n.redoLog == nil {
		return proto.ReplicateRedoEntryResponse{Ack: false}, errors.New("redo log unavailable")
	}
	if err := n.redoLog.AddItem(storage.RedoLogItem{TransactionId: req.Entry.TID, HostIds: append([]uint32(nil), req.Entry.Participants...), DecisionDeadlineNanos: req.Entry.DecisionDeadlineNanos, WriteItems: cloneWrites(req.Entry.WriteItems)}); err != nil {
		return proto.ReplicateRedoEntryResponse{Ack: false}, err
	}
	n.allLogTxIds[req.Entry.TID] = struct{}{}
	n.logParticipants[req.Entry.TID] = append([]uint32(nil), req.Entry.Participants...)
	correlationID := req.Entry.TID.CorrelationID()
	n.inDoubt[req.Entry.TID] = &preparedTxn{writes: cloneWrites(req.Entry.WriteItems), participants: append([]uint32(nil), req.Entry.Participants...), decisionDeadlineNanos: req.Entry.DecisionDeadlineNanos, redoLoggedTid: true, preparedAt: n.nowFn(), correlationID: correlationID}
	n.recordTimelineLocked(req.Entry.TID, correlationID, "replicated_prepare", "")
	return proto.ReplicateRedoEntryResponse{Ack: true}, nil
}

func (n *Participant) ReplicateCommitDecision(ctx context.Context, req proto.ReplicateCommitDecisionRequest) (proto.ReplicateCommitDecisionResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ReplicateCommitDecisionResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return proto.ReplicateCommitDecisionResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return proto.ReplicateCommitDecisionResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	resp, err := n.commitLocked(proto.CommitRequest{TID: req.TID, ActionCommit: req.ActionCommit})
	if err != nil {
		return proto.ReplicateCommitDecisionResponse{Ack: false}, err
	}
	return proto.ReplicateCommitDecisionResponse{Ack: resp.Ack}, nil
}

// ReplicateReplayRecord saves a reply so a promoted replica can answer a retry
// with the same transaction ID. Saving a reply doesn't execute the transaction.
func (n *Participant) ReplicateReplayRecord(ctx context.Context, req proto.ReplicateReplayRecordRequest) (proto.ReplicateReplayRecordResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ReplicateReplayRecordResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return proto.ReplicateReplayRecordResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if n.replicationBaselineIngressBlockedLocked() {
		return proto.ReplicateReplayRecordResponse{Ack: false}, ErrReplicationBaselineInProgress
	}
	if n.replay == nil {
		return proto.ReplicateReplayRecordResponse{Ack: false}, errors.New("replay store unavailable")
	}
	switch req.Kind {
	case proto.ReplayRecordPrepare:
		if req.Prepare == nil || req.OnePC != nil {
			return proto.ReplicateReplayRecordResponse{Ack: false}, errors.New("invalid prepare replay record")
		}
		if err := n.replay.AppendPrepare(req.TID, *req.Prepare); err != nil {
			return proto.ReplicateReplayRecordResponse{Ack: false}, err
		}
		n.prepareOutcomes[req.TID] = prepareOutcome{resp: clonePrepareResponse(*req.Prepare)}
	case proto.ReplayRecordOnePC:
		if req.OnePC == nil || req.Prepare != nil {
			return proto.ReplicateReplayRecordResponse{Ack: false}, errors.New("invalid one-phase replay record")
		}
		if err := n.replay.AppendOnePC(req.TID, *req.OnePC); err != nil {
			return proto.ReplicateReplayRecordResponse{Ack: false}, err
		}
		n.onePCOutcomes[req.TID] = onePCOutcome{resp: clone1PCResponse(*req.OnePC)}
	default:
		return proto.ReplicateReplayRecordResponse{Ack: false}, fmt.Errorf("unknown replay record kind %q", req.Kind)
	}
	return proto.ReplicateReplayRecordResponse{Ack: true}, nil
}

func (n *Participant) ListCommittedTIDs(ctx context.Context, _ proto.ListCommittedTIDsRequest) (proto.ListCommittedTIDsResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ListCommittedTIDsResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := proto.ListCommittedTIDsResponse{}
	if n.decided == nil || n.redoLog == nil {
		return resp, nil
	}
	for _, entry := range n.redoLog.Entries() {
		if c, ok := n.decided.Get(entry.TransactionId); ok && c {
			resp.TIDs = append(resp.TIDs, entry.TransactionId)
		}
	}
	return resp, nil
}

func (n *Participant) ListRedoEntries(ctx context.Context, _ proto.ListRedoEntriesRequest) (proto.ListRedoEntriesResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ListRedoEntriesResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := proto.ListRedoEntriesResponse{}
	if n.redoLog == nil {
		return resp, nil
	}
	for _, entry := range n.redoLog.Entries() {
		resp.Entries = append(resp.Entries, proto.RedoEntryPayload{TID: entry.TransactionId, Participants: append([]uint32(nil), entry.HostIds...), DecisionDeadlineNanos: entry.DecisionDeadlineNanos, WriteItems: cloneWrites(entry.WriteItems)})
	}
	return resp, nil
}

// ExportReplicationBaseline returns the primary's saved memory image.
// The manager must hold the global write lock so updates can't race the copy.
func (n *Participant) ExportReplicationBaseline(ctx context.Context, req proto.ExportReplicationBaselineRequest) (proto.ExportReplicationBaselineResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.ExportReplicationBaselineResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplPrimary {
		return proto.ExportReplicationBaselineResponse{Ack: false}, errors.New("node is not configured as primary")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return proto.ExportReplicationBaselineResponse{Ack: false}, err
	}
	if n.backupMode {
		return proto.ExportReplicationBaselineResponse{Ack: false}, errors.New("replication baseline unavailable during backup phase")
	}
	if n.diskImage == nil {
		return proto.ExportReplicationBaselineResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if err := n.diskImage.Sync(); err != nil {
		return proto.ExportReplicationBaselineResponse{Ack: false}, err
	}
	image, err := n.diskImage.Bytes()
	if err != nil {
		return proto.ExportReplicationBaselineResponse{Ack: false}, err
	}
	checksum := sha256.Sum256(image)
	boundary := proto.TransactionID{}
	if n.redoLog != nil {
		boundary = n.redoLog.LastTID()
	}
	return proto.ExportReplicationBaselineResponse{Ack: true, Image: image, SHA256: checksum[:], BoundaryTID: boundary}, nil
}

// InstallReplicationBaseline replaces a replica's image and old transaction state.
// The manager must hold the replica's global write lock. Saved replies stay local;
// this call doesn't transfer them from the primary.
func (n *Participant) InstallReplicationBaseline(ctx context.Context, req proto.InstallReplicationBaselineRequest) (proto.InstallReplicationBaselineResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.InstallReplicationBaselineResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.mode != ModeReplReplica {
		return proto.InstallReplicationBaselineResponse{Ack: false}, errors.New("node is not configured as replica")
	}
	if err := n.touchGlobalLockLocked(req.BarrierOwner); err != nil {
		return proto.InstallReplicationBaselineResponse{Ack: false}, err
	}
	if n.backupMode {
		return proto.InstallReplicationBaselineResponse{Ack: false}, errors.New("replication baseline unavailable during backup phase")
	}
	if n.diskImage == nil {
		return proto.InstallReplicationBaselineResponse{Ack: false}, ErrReplicationRequiresDurability
	}
	if len(req.Image) != len(n.memory) {
		return proto.InstallReplicationBaselineResponse{Ack: false}, fmt.Errorf("replication baseline size %d does not match memory size %d", len(req.Image), len(n.memory))
	}
	return n.installReplicationBaselineBytesLocked(req.BarrierOwner, req.Image, req.SHA256, req.BoundaryTID)
}

func (n *Participant) SyncFromPrimary(ctx context.Context, primary PrimarySyncPeer) error {
	if primary == nil {
		return errors.New("primary peer required")
	}
	owner := fmt.Sprintf("sync/%p", n)
	if err := primary.LockAllBlockingOwned(ctx, owner); err != nil {
		return err
	}
	primaryLocked := true
	defer func() {
		if primaryLocked {
			primary.UnlockAllOwned(owner)
		}
	}()
	if err := n.LockAllBlockingOwned(ctx, owner); err != nil {
		return err
	}
	replicaLocked := true
	defer func() {
		if replicaLocked {
			n.UnlockAllOwned(owner)
		}
	}()
	first, err := primary.ExportReplicationBaselineChunk(ctx, proto.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "image", Offset: 0, MaxBytes: baselineChunkSize})
	if err != nil || !first.Ack {
		if err != nil {
			return err
		}
		return errors.New("primary did not acknowledge replication baseline chunk")
	}
	if first.TotalSize == 0 || first.TotalSize != uint64(len(n.memory)) || len(first.SHA256) != sha256.Size || len(first.Data) == 0 {
		return errors.New("primary returned an invalid replication baseline manifest")
	}
	replayPresent := len(first.ReplaySHA256) > 0 || first.ReplaySize > 0
	if replayPresent && (first.ReplaySize > maxBaselineBytes || len(first.ReplaySHA256) != sha256.Size) {
		return errors.New("primary returned an invalid replay baseline manifest")
	}
	sessionID := fmt.Sprintf("%s/%d", owner, time.Now().UnixNano())
	begin, err := n.BeginReplicationBaseline(ctx, proto.BeginReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID, TotalSize: first.TotalSize, SHA256: first.SHA256, ReplaySize: first.ReplaySize, ReplaySHA256: first.ReplaySHA256, BoundaryTID: first.BoundaryTID})
	if err != nil || !begin.Ack {
		if err != nil {
			return err
		}
		return errors.New("replica did not acknowledge replication baseline begin")
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = n.AbortReplicationBaseline(context.Background(), proto.AbortReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
		}
	}()
	chunk := first
	offset := uint64(0)
	for {
		if chunk.TotalSize != first.TotalSize || len(chunk.Data) == 0 || len(chunk.Data) > baselineChunkSize || offset+uint64(len(chunk.Data)) > first.TotalSize {
			return errors.New("invalid replication baseline chunk sequence")
		}
		digest := sha256.Sum256(chunk.Data)
		written, err := n.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: sessionID, Artifact: "image", Offset: offset, SHA256: digest[:], Data: chunk.Data})
		if err != nil || !written.Ack {
			if err != nil {
				return err
			}
			return errors.New("replica did not acknowledge replication baseline chunk")
		}
		offset += uint64(len(chunk.Data))
		if offset == first.TotalSize {
			break
		}
		chunk, err = primary.ExportReplicationBaselineChunk(ctx, proto.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "image", Offset: offset, MaxBytes: baselineChunkSize})
		if err != nil || !chunk.Ack {
			if err != nil {
				return err
			}
			return errors.New("primary did not acknowledge replication baseline chunk")
		}
	}
	if replayPresent {
		replayFirst, err := primary.ExportReplicationBaselineChunk(ctx, proto.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: "replay", Offset: 0, MaxBytes: baselineChunkSize})
		if err != nil {
			return err
		}
		if err := syncBaselineArtifact(ctx, primary, n, owner, sessionID, "replay", replayFirst, first.ReplaySize, first.ReplaySHA256); err != nil {
			return err
		}
	}
	installed, err := n.CommitReplicationBaseline(ctx, proto.CommitReplicationBaselineRequest{BarrierOwner: owner, SessionID: sessionID})
	if err != nil || !installed.Ack {
		if err != nil {
			return err
		}
		return errors.New("replica did not acknowledge replication baseline commit")
	}
	committed = true
	n.mu.Lock()
	n.replicaSynced = true
	n.metrics.replicaLagEntries.Set(0)
	n.mu.Unlock()
	return nil
}

func syncBaselineArtifact(ctx context.Context, primary PrimarySyncPeer, replica *Participant, owner, sessionID, artifact string, first proto.ExportReplicationBaselineChunkResponse, total uint64, checksum []byte) error {
	if total == 0 {
		if first.Ack && first.TotalSize == 0 && len(first.Data) == 0 && len(first.SHA256) == sha256.Size && equalBytes(first.SHA256, checksum) {
			return nil
		}
		return errors.New("primary returned an invalid empty baseline artifact")
	}
	if !first.Ack || first.TotalSize != total || len(first.SHA256) != sha256.Size || !equalBytes(first.SHA256, checksum) || len(first.Data) == 0 || len(first.Data) > baselineChunkSize || uint64(len(first.Data)) > total {
		return fmt.Errorf("primary returned an invalid %s baseline artifact", artifact)
	}
	offset := uint64(0)
	chunk := first
	for {
		if chunk.TotalSize != total || len(chunk.Data) == 0 || len(chunk.Data) > baselineChunkSize || offset+uint64(len(chunk.Data)) > total {
			return fmt.Errorf("invalid %s baseline artifact chunk sequence", artifact)
		}
		digest := sha256.Sum256(chunk.Data)
		written, err := replica.WriteReplicationBaselineChunk(ctx, proto.WriteReplicationBaselineChunkRequest{BarrierOwner: owner, SessionID: sessionID, Artifact: artifact, Offset: offset, SHA256: digest[:], Data: chunk.Data})
		if err != nil {
			return err
		}
		if !written.Ack {
			return fmt.Errorf("replica did not acknowledge %s baseline artifact chunk", artifact)
		}
		offset += uint64(len(chunk.Data))
		if offset == total {
			return nil
		}
		chunk, err = primary.ExportReplicationBaselineChunk(ctx, proto.ExportReplicationBaselineChunkRequest{BarrierOwner: owner, Artifact: artifact, Offset: offset, MaxBytes: baselineChunkSize})
		if err != nil {
			return err
		}
		if !chunk.Ack {
			return fmt.Errorf("primary did not acknowledge %s baseline artifact chunk", artifact)
		}
	}
}

func (n *Participant) NotifyApplied(ctx context.Context, req proto.NotifyAppliedRequest) (proto.NotifyAppliedResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.NotifyAppliedResponse{Ack: false}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, txId := range req.TIDs {
		if _, ok := n.allLogTxIds[txId]; !ok {
			continue
		}
		if _, ok := n.appliedRemote[txId]; !ok {
			n.appliedRemote[txId] = map[uint32]bool{}
		}
		n.appliedRemote[txId][req.FromMemID] = true
	}
	return proto.NotifyAppliedResponse{Ack: true}, nil
}

func (n *Participant) enqueueAppliedNotificationsLocked(txId proto.TransactionID, participants []uint32) {
	for _, pid := range participants {
		if pid == n.hostId {
			continue
		}
		if _, ok := n.pendingNotify[pid]; !ok {
			n.pendingNotify[pid] = map[proto.TransactionID]struct{}{}
		}
		n.pendingNotify[pid][txId] = struct{}{}
	}
}

func (n *Participant) FlushAppliedNotifications(ctx context.Context) {
	n.mu.Lock()
	peers := make(map[uint32]RecoveryPeer, len(n.participants))
	for pid, p := range n.participants {
		peers[pid] = p
	}
	pending := make(map[uint32][]proto.TransactionID, len(n.pendingNotify))
	for pid, txIds := range n.pendingNotify {
		list := make([]proto.TransactionID, 0, len(txIds))
		for txId := range txIds {
			list = append(list, txId)
		}
		pending[pid] = list
	}
	n.mu.Unlock()

	for pid, txIds := range pending {
		if len(txIds) == 0 {
			continue
		}
		peer := peers[pid]
		if peer == nil {
			continue
		}
		resp, err := peer.NotifyApplied(ctx, proto.NotifyAppliedRequest{FromMemID: n.hostId, TIDs: txIds})
		if err != nil || !resp.Ack {
			continue
		}
		n.mu.Lock()
		if existing, ok := n.pendingNotify[pid]; ok {
			for _, txId := range txIds {
				delete(existing, txId)
			}
			if len(existing) == 0 {
				delete(n.pendingNotify, pid)
			}
		}
		n.mu.Unlock()
	}
}

func (n *Participant) RunRedoLogGC(ctx context.Context) error {
	n.metrics.logGCRuns.Inc()
	n.log(slog.LevelInfo, "GC truncation", "epoch", n.epoch)
	n.FlushAppliedNotifications(ctx)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog == nil {
		return nil
	}
	entries := n.redoLog.Entries()
	truncateCount := 0
	for _, entry := range entries {
		committed, known := false, false
		if n.decided != nil {
			committed, known = n.decided.Get(entry.TransactionId)
		}
		if !known {
			break
		}
		if !committed {
			truncateCount++
			continue
		}
		if !n.appliedLocal[entry.TransactionId] {
			break
		}
		allApplied := true
		for _, pid := range entry.HostIds {
			if pid == n.hostId {
				continue
			}
			if !n.appliedRemote[entry.TransactionId][pid] {
				allApplied = false
				break
			}
		}
		if !allApplied {
			break
		}
		truncateCount++
	}
	if truncateCount == 0 {
		return nil
	}
	removed := entries[:truncateCount]
	for _, entry := range removed {
		if entry.DecisionDeadlineNanos == 0 {
			continue
		}
		committed, known := n.decided.Get(entry.TransactionId)
		if !known || committed {
			continue
		}
		if n.forcedAborts == nil {
			return errors.New("deadline abort GC requires forced-abort storage")
		}
		// Removing redo loses the deadline protocol field, so save the abort
		// first. Keep it until the protocol's epoch rules allow cleanup.
		if err := n.forcedAborts.AddItem(entry.TransactionId); err != nil {
			return err
		}
	}
	if err := n.redoLog.TruncateHead(truncateCount); err != nil {
		return err
	}
	for _, entry := range removed {
		delete(n.allLogTxIds, entry.TransactionId)
		delete(n.inDoubt, entry.TransactionId)
		delete(n.appliedLocal, entry.TransactionId)
		delete(n.appliedRemote, entry.TransactionId)
		delete(n.logParticipants, entry.TransactionId)
		for pid, txIds := range n.pendingNotify {
			delete(txIds, entry.TransactionId)
			if len(txIds) == 0 {
				delete(n.pendingNotify, pid)
			}
		}
		if n.decided != nil {
			committed, known := n.decided.Get(entry.TransactionId)
			if entry.DecisionDeadlineNanos != 0 && known && !committed {
				continue
			}
			if err := n.decided.Remove(entry.TransactionId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Participant) RunRedoLogGCRPC(ctx context.Context, _ proto.RunRedoLogGCRequest) (proto.RunRedoLogGCResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.RunRedoLogGCResponse{Ack: false}, err
	}
	err := n.RunRedoLogGC(ctx)
	return proto.RunRedoLogGCResponse{Ack: err == nil}, err
}

func (n *Participant) RunForcedAbortGC() error {
	n.log(slog.LevelInfo, "GC truncation", "epoch", n.epoch)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return nil
	}
	current := n.currentEpochLocked()
	removed, err := n.forcedAborts.GCByEpoch(current)
	if err != nil {
		return err
	}
	// Keep a deadline abort until its forced-abort marker has been removed
	// durably under the epoch rules.
	for _, txId := range removed {
		if n.decided == nil {
			continue
		}
		committed, known := n.decided.Get(txId)
		if _, logged := n.allLogTxIds[txId]; !logged && known && !committed {
			if err := n.decided.Remove(txId); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *Participant) RunForcedAbortGCRPC(ctx context.Context, _ proto.RunForcedAbortGCRequest) (proto.RunForcedAbortGCResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.RunForcedAbortGCResponse{Ack: false}, err
	}
	err := n.RunForcedAbortGC()
	return proto.RunForcedAbortGCResponse{Ack: err == nil}, err
}

func (n *Participant) QueryTidStatus(ctx context.Context, req proto.QueryTidStatusRequest) (proto.QueryTidStatusResponse, error) {
	if err := n.authorize(ctx, auth.RolePeer, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.QueryTidStatusResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if out, ok := n.commitOutcomes[req.TID]; ok {
		if out.actionCommit {
			return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeCommit, VotedOK: true}, nil
		}
		return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeAbort, VotedOK: false}, nil
	}
	if n.decided != nil {
		if c, ok := n.decided.Get(req.TID); ok {
			if c {
				return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeCommit, VotedOK: true}, nil
			}
			return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeAbort, VotedOK: false}, nil
		}
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return proto.QueryTidStatusResponse{KnownDecision: false, Outcome: proto.TidOutcomeAbort, VotedOK: false}, nil
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		return proto.QueryTidStatusResponse{KnownDecision: false, Outcome: proto.TidOutcomeUnknown, VotedOK: true}, nil
	}
	if n.prepareVotedOKLocked(req.TID) {
		return proto.QueryTidStatusResponse{KnownDecision: false, Outcome: proto.TidOutcomeUnknown, VotedOK: true}, nil
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.AddItem(req.TID); err != nil {
			return proto.QueryTidStatusResponse{}, err
		}
	}
	n.metrics.forcedAbortCount.Inc()
	n.metrics.forcedAbortCountCurrent.Set(int64(n.metrics.forcedAbortCountCurrent.Value() + 1))
	return proto.QueryTidStatusResponse{KnownDecision: true, Outcome: proto.TidOutcomeAbort, VotedOK: false}, nil
}

func (n *Participant) QueryInDoubt(ctx context.Context, _ proto.QueryInDoubtRequest) (proto.QueryInDoubtResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.QueryInDoubtResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := proto.QueryInDoubtResponse{Entries: make([]proto.InDoubtEntry, 0, len(n.inDoubt))}
	for txId, tx := range n.inDoubt {
		correlationID := tx.correlationID
		if correlationID == "" {
			correlationID = txId.CorrelationID()
		}
		resp.Entries = append(resp.Entries, proto.InDoubtEntry{
			TID:           txId,
			PreparedAt:    tx.preparedAt.UnixNano(),
			CorrelationID: correlationID,
			Participants:  append([]uint32(nil), tx.participants...),
		})
	}
	return resp, nil
}

func (n *Participant) TimelineForTID(txId proto.TransactionID) []proto.TxnTimelineEvent {
	n.mu.Lock()
	defer n.mu.Unlock()
	src := n.timelines[txId]
	out := make([]proto.TxnTimelineEvent, len(src))
	copy(out, src)
	return out
}

func (n *Participant) RecoveryForceAbort(ctx context.Context, req proto.RecoverForceAbortRequest) (proto.RecoverForceAbortResponse, error) {
	if !n.allowRate(n.recoveryLimiter, "recovery") {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteBadInternal}, ErrResourceExhausted
	}
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteBadInternal}, err
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.transactionHasDecisionDeadlineLocked(req.TID) && req.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteBadInternal}, ErrDeadlineDecisionProtocolUpgradeRequired
	}
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = n.correlationForTIDLocked(req.TID)
	}
	n.recordTimelineLocked(req.TID, correlationID, "recovery_force_abort", "")
	if out, ok := n.commitOutcomes[req.TID]; ok {
		if out.actionCommit {
			return proto.RecoverForceAbortResponse{Vote: proto.VoteOk, KnownDecision: true, DecisionCommit: true}, nil
		}
		return proto.RecoverForceAbortResponse{Vote: proto.VoteBadForced, KnownDecision: true}, nil
	}
	if n.decided != nil {
		if committed, ok := n.decided.Get(req.TID); ok {
			if committed {
				return proto.RecoverForceAbortResponse{Vote: proto.VoteOk, KnownDecision: true, DecisionCommit: true}, nil
			}
			return proto.RecoverForceAbortResponse{Vote: proto.VoteBadForced, KnownDecision: true}, nil
		}
	}
	if n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(req.TID) {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteBadForced}, nil
	}
	if _, ok := n.allLogTxIds[req.TID]; ok {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteOk}, nil
	}
	if n.prepareVotedOKLocked(req.TID) {
		return proto.RecoverForceAbortResponse{Vote: proto.VoteOk}, nil
	}
	if n.forcedAborts != nil {
		if err := n.forcedAborts.AddItem(req.TID); err != nil {
			return proto.RecoverForceAbortResponse{Vote: proto.VoteBadInternal}, err
		}
	}
	return proto.RecoverForceAbortResponse{Vote: proto.VoteBadForced}, nil
}

func (n *Participant) RecoveryDecide(ctx context.Context, req proto.RecoverDecideRequest) (proto.RecoverDecideResponse, error) {
	if !n.allowRate(n.recoveryLimiter, "recovery") {
		return proto.RecoverDecideResponse{Ack: false}, ErrResourceExhausted
	}
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.RecoverDecideResponse{Ack: false}, err
	}
	n.mu.Lock()
	deadlineTransaction := n.transactionHasDecisionDeadlineLocked(req.TID)
	n.mu.Unlock()
	if deadlineTransaction && req.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
		return proto.RecoverDecideResponse{Ack: false}, ErrDeadlineDecisionProtocolUpgradeRequired
	}

	resp, err := n.commit(ctx, proto.CommitRequest{TID: req.TID, ActionCommit: req.ActionCommit, CorrelationID: req.CorrelationID, DeadlineDecisionProtocolVersion: req.DeadlineDecisionProtocolVersion}, true)
	if err != nil {
		return proto.RecoverDecideResponse{Ack: false}, err
	}
	return proto.RecoverDecideResponse{Ack: resp.Ack}, nil
}

func (n *Participant) transactionHasDecisionDeadlineLocked(txId proto.TransactionID) bool {
	txn := n.inDoubt[txId]
	return txn != nil && txn.decisionDeadlineNanos != 0
}

// Recover resolves logged transactions using saved decisions and peer evidence.
// The node stays unavailable if it can't safely finish recovery.
func (n *Participant) Recover(ctx context.Context) error {
	n.metrics.recoveryRuns.Inc()
	n.log(slog.LevelInfo, "Recovery initiated", "epoch", n.epoch)
	n.mu.Lock()
	n.recoveryComplete = false
	entries := []storage.RedoLogItem{}
	if n.redoLog != nil {
		entries = n.redoLog.Entries()
	}
	ptr := 0
	var err error
	if n.storageDir != "" {
		ptr, err = storage.LoadProcessedPointer(n.storageDir)
		if err != nil {
			n.mu.Unlock()
			return err
		}
	}
	if ptr < 0 {
		ptr = 0
	}
	peers := make(map[uint32]RecoveryPeer, len(n.participants))
	for k, v := range n.participants {
		peers[k] = v
	}
	n.mu.Unlock()

	for i := ptr; i < len(entries); i++ {
		entry := entries[i]
		if err := n.callRecoverBoundaryHook("replay-before", entry.TransactionId); err != nil {
			return err
		}
		outcomeCommit := false
		known := false

		n.mu.Lock()
		if n.decided != nil {
			if c, ok := n.decided.Get(entry.TransactionId); ok {
				outcomeCommit = c
				known = true
			}
		}
		n.mu.Unlock()

		if !known {
			outcomeCommit, err = n.discoverOutcome(ctx, peers, entry)
			if err != nil {
				return err
			}
		}

		n.mu.Lock()
		if n.decided != nil {
			if decidedCommit, decided := n.decided.Get(entry.TransactionId); decided {
				outcomeCommit = decidedCommit
			} else if err := n.decided.AddItem(entry.TransactionId, outcomeCommit); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		flushNotify := false
		if outcomeCommit {
			if err := n.callRecoverBoundaryHookLocked("apply-before", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
			for _, w := range entry.WriteItems {
				if err := n.write(w.Addr, w.Data); err != nil {
					n.mu.Unlock()
					return err
				}
			}
			n.appliedLocal[entry.TransactionId] = true
			n.enqueueAppliedNotificationsLocked(entry.TransactionId, entry.HostIds)
			flushNotify = true
			if err := n.callRecoverBoundaryHookLocked("apply-after", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		if n.storageDir != "" {
			if err := n.callRecoverBoundaryHookLocked("pointer-before", entry.TransactionId); err != nil {
				n.mu.Unlock()
				return err
			}
			if err := storage.StoreProcessedPointer(n.storageDir, i+1); err != nil {
				n.mu.Unlock()
				return err
			}
		}
		n.commitOutcomes[entry.TransactionId] = commitOutcome{
			resp:         proto.CommitResponse{Ack: true},
			actionCommit: outcomeCommit,
		}
		delete(n.inDoubt, entry.TransactionId)
		n.metrics.inDoubtCount.Set(int64(len(n.inDoubt)))
		n.mu.Unlock()
		if flushNotify {
			go n.FlushAppliedNotifications(context.Background())
		}
	}
	n.mu.Lock()
	currentEpoch := n.currentEpochLocked()
	n.mu.Unlock()
	if n.replay != nil {
		if err := n.replay.Compact(storage.ReplayRetention{
			CurrentEpoch: currentEpoch,
			RetryHorizon: storage.PublicRetryHorizon,
		}); err != nil {
			n.mu.Lock()
			n.recoveryComplete = false
			n.mu.Unlock()
			return err
		}
	}
	n.mu.Lock()
	n.recoveryComplete = true
	n.mu.Unlock()
	return nil
}

func (n *Participant) callRecoverBoundaryHook(phase string, txId proto.TransactionID) error {
	n.mu.Lock()
	hook := n.recoverBoundaryHook
	n.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(phase, txId)
}

func (n *Participant) callRecoverBoundaryHookLocked(phase string, txId proto.TransactionID) error {
	if n.recoverBoundaryHook == nil {
		return nil
	}
	return n.recoverBoundaryHook(phase, txId)
}

func (n *Participant) discoverOutcome(ctx context.Context, peers map[uint32]RecoveryPeer, entry storage.RedoLogItem) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	n.mu.Lock()
	peerTimeout := n.peerTimeout
	forcedAbort := n.forcedAborts != nil && n.forcedAborts.IsForcedAbort(entry.TransactionId)
	n.mu.Unlock()

	knownDecision := false
	knownCommit := false
	negativeVote := forcedAbort
	for _, pid := range entry.HostIds {
		if pid == n.hostId {
			continue
		}
		if peers[pid] == nil {
			return false, fmt.Errorf("%w: transaction %v participant %d is not configured", ErrRecoveryPending, entry.TransactionId, pid)
		}
	}
	for _, pid := range entry.HostIds {
		if pid == n.hostId {
			continue
		}
		peer := peers[pid]
		queryCtx, cancel := context.WithTimeout(ctx, peerTimeout)
		resp, err := peer.QueryTidStatus(queryCtx, proto.QueryTidStatusRequest{TID: entry.TransactionId})
		cancel()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, fmt.Errorf("%w: transaction %v participant %d: %v", ErrRecoveryPending, entry.TransactionId, pid, err)
		}
		if resp.KnownDecision {
			switch resp.Outcome {
			case proto.TidOutcomeCommit:
				if knownDecision && !knownCommit {
					return false, fmt.Errorf("%w: transaction %v has conflicting durable decisions", ErrRecoveryPending, entry.TransactionId)
				}
				knownDecision = true
				knownCommit = true
			case proto.TidOutcomeAbort:
				if knownDecision && knownCommit {
					return false, fmt.Errorf("%w: transaction %v has conflicting durable decisions", ErrRecoveryPending, entry.TransactionId)
				}
				knownDecision = true
				knownCommit = false
			default:
				return false, fmt.Errorf("%w: transaction %v participant %d reported an unknown decision", ErrRecoveryPending, entry.TransactionId, pid)
			}
			continue
		}
		if !resp.VotedOK || resp.Outcome == proto.TidOutcomeAbort {
			negativeVote = true
		}
	}
	if knownDecision {
		return knownCommit, nil
	}
	if negativeVote {
		return false, nil
	}
	return true, nil
}

func (n *Participant) LockAllBlocking(ctx context.Context) error {
	return n.LockAllBlockingOwned(ctx, n.localBarrierOwner)
}

func (n *Participant) LockAllBlockingOwned(ctx context.Context, owner string) error {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return err
	}
	if owner == "" {
		return ErrReplicationBaselineLease
	}
	// sync.Cond doesn't understand contexts. Wake it on cancellation; the timer
	// below also wakes it when an abandoned owner's lease expires.
	wakeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			n.mu.Lock()
			n.cond.Broadcast()
			n.mu.Unlock()
		case <-wakeDone:
		}
	}()
	defer close(wakeDone)

	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n.expireGlobalLockLocked()
		if !n.locks.globalWriteLock && len(n.locks.locks) == 0 {
			n.locks.globalWriteLock = true
			n.locks.globalLockedAt = time.Now()
			n.locks.globalOwner = owner
			n.locks.globalLeaseGeneration++
			n.renewGlobalLeaseLocked()
			return nil
		}
		n.cond.Wait()
	}
}

func (n *Participant) UnlockAll() {
	n.UnlockAllOwned(n.localBarrierOwner)
}

func (n *Participant) UnlockAllOwned(owner string) {
	if owner == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.expireGlobalLockLocked()
	if n.locks.globalOwner != owner {
		return
	}
	n.clearGlobalLockLocked()
	n.cond.Broadcast()
}

func (n *Participant) RenewAllOwned(ctx context.Context, owner string) error {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.touchGlobalLockLocked(owner); err != nil {
		return err
	}
	return nil
}

func (n *Participant) expireGlobalLockLocked() {
	if !n.locks.globalWriteLock || n.locks.globalLeaseUntil.IsZero() {
		return
	}
	if time.Now().Before(n.locks.globalLeaseUntil) {
		return
	}
	n.clearGlobalLockLocked()
	n.cond.Broadcast()
}

func (n *Participant) clearGlobalLockLocked() {
	if n.baselineTransfer != nil {
		n.abortBaselineTransferLocked()
	}
	n.locks.globalWriteLock = false
	n.locks.globalLockedAt = time.Time{}
	n.locks.globalOwner = ""
	n.locks.globalLeaseUntil = time.Time{}
}

func (n *Participant) touchGlobalLockLocked(owner string) error {
	n.expireGlobalLockLocked()
	if owner == "" || !n.locks.globalWriteLock || n.locks.globalOwner != owner {
		return ErrReplicationBaselineLease
	}
	n.renewGlobalLeaseLocked()
	return nil
}

func (n *Participant) renewGlobalLeaseLocked() {
	ttl := n.globalBarrierLeaseTTL
	if ttl <= 0 {
		ttl = defaultGlobalBarrierLeaseTTL
	}
	n.locks.globalLeaseUntil = time.Now().Add(ttl)
	generation := n.locks.globalLeaseGeneration
	owner := n.locks.globalOwner
	time.AfterFunc(ttl, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.locks.globalWriteLock && n.locks.globalOwner == owner && n.locks.globalLeaseGeneration == generation {
			n.expireGlobalLockLocked()
		}
	})
}

func (n *Participant) SetGlobalBarrierLeaseTTLForTest(ttl time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ttl > 0 {
		n.globalBarrierLeaseTTL = ttl
	}
}

func (n *Participant) BeginBackupPhase(ctx context.Context, req proto.BeginBackupPhaseRequest) (proto.BeginBackupPhaseResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.BeginBackupPhaseResponse{Ack: false}, err
	}
	n.metrics.backupRuns.Inc()
	n.log(slog.LevelInfo, "Backup phase transitions", "phase", "begin", "tid", req.BackupID)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.diskImage == nil {
		return proto.BeginBackupPhaseResponse{Ack: true}, nil
	}
	boundary := proto.TransactionID{}
	if n.redoLog != nil {
		boundary = n.redoLog.LastTID()
	}
	n.lastBackupBoundary = boundary
	n.backupMode = true
	if err := n.diskImage.Sync(); err != nil {
		return proto.BeginBackupPhaseResponse{Ack: false}, err
	}
	return proto.BeginBackupPhaseResponse{Ack: true, BoundaryTID: boundary}, nil
}

func (n *Participant) CompleteBackupPhase(ctx context.Context, req proto.CompleteBackupPhaseRequest) (proto.CompleteBackupPhaseResponse, error) {
	if err := n.authorize(ctx, auth.RoleManager, auth.RoleAdmin); err != nil {
		return proto.CompleteBackupPhaseResponse{Ack: false}, err
	}
	n.log(slog.LevelInfo, "Backup phase transitions", "phase", "complete", "tid", req.BackupID)
	n.mu.Lock()
	if n.diskImage == nil {
		n.backupMode = false
		n.pendingDiskWrites = nil
		n.mu.Unlock()
		return proto.CompleteBackupPhaseResponse{Ack: true}, nil
	}
	snapshotPath := ""
	if n.storageDir != "" {
		snapshotPath = n.storageDir + "/backup-" + req.BackupID + ".img"
	}
	n.mu.Unlock()

	if snapshotPath != "" {
		if err := n.diskImage.Snapshot(snapshotPath); err != nil {
			return proto.CompleteBackupPhaseResponse{Ack: false}, err
		}
	}
	n.mu.Lock()
	pending := append([]proto.WriteItem(nil), n.pendingDiskWrites...)
	n.pendingDiskWrites = nil
	n.backupMode = false
	n.mu.Unlock()

	for _, w := range pending {
		if err := n.diskImage.ApplyWrite(w.Addr, w.Data); err != nil {
			return proto.CompleteBackupPhaseResponse{Ack: false}, err
		}
	}
	snapshotSHA256 := ""
	if snapshotPath != "" {
		snapshot, err := os.Open(snapshotPath)
		if err != nil {
			return proto.CompleteBackupPhaseResponse{Ack: false}, err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, snapshot)
		closeErr := snapshot.Close()
		if copyErr != nil {
			return proto.CompleteBackupPhaseResponse{Ack: false}, copyErr
		}
		if closeErr != nil {
			return proto.CompleteBackupPhaseResponse{Ack: false}, closeErr
		}
		snapshotSHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	return proto.CompleteBackupPhaseResponse{Ack: true, SnapshotPath: snapshotPath, SnapshotSHA256: snapshotSHA256}, nil
}

func (n *Participant) GlobalLockHeldDurationForTest() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.locks.globalLockedAt.IsZero() {
		return 0
	}
	return time.Since(n.locks.globalLockedAt)
}

func (n *Participant) HoldWriteLockForTest(addr uint64, length uint32) func() {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := RangeLock{start: addr, end: addr + uint64(length), write: true}
	if !n.locks.tryLock(l.start, l.end, true) {
		return func() {}
	}
	return func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.locks.unlock(l)
	}
}

func (n *Participant) RedoEntryCountForTest() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.redoLog == nil {
		return 0
	}
	return len(n.redoLog.Entries())
}

func (n *Participant) HasDecidedForTest(txId proto.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.decided == nil {
		return false
	}
	_, ok := n.decided.Get(txId)
	return ok
}

func (n *Participant) HasAppliedTrackingForTest(txId proto.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.appliedLocal[txId] {
		return true
	}
	if rem := n.appliedRemote[txId]; len(rem) > 0 {
		return true
	}
	for _, txIds := range n.pendingNotify {
		if _, ok := txIds[txId]; ok {
			return true
		}
	}
	return false
}

func (n *Participant) HasForcedAbortForTest(txId proto.TransactionID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return false
	}
	return n.forcedAborts.IsForcedAbort(txId)
}

func (n *Participant) ForceAbortForTest(txId proto.TransactionID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.forcedAborts == nil {
		return errors.New("forced-abort store unavailable")
	}
	return n.forcedAborts.AddItem(txId)
}
