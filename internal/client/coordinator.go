package client

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto"
	"github.com/jbancroft/ConcertumDemo/internal/rpcwire"
)

var coordinatorIDFallback atomic.Uint64

var ErrDeadlineDecisionProtocolUpgradeRequired = errors.New("deadline decision protocol upgrade required")

// Participant is the node interface used by the coordinator.
type Participant interface {
	Read(ctx context.Context, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, addr uint64, data []byte) error
	ExecAndPrepare(ctx context.Context, req proto.ExecPrepareRequest) (proto.ExecPrepareResponse, error)
	ExecCommit1PC(ctx context.Context, req proto.ExecCommit1PCRequest) (proto.ExecCommit1PCResponse, error)
	Commit(ctx context.Context, req proto.CommitRequest) (proto.CommitResponse, error)
	CommitBatch(ctx context.Context, req proto.CommitBatchRequest) (proto.CommitBatchResponse, error)
}

type deadlineDecisionCapable interface {
	DescribeTransactionCapabilities(context.Context, proto.DescribeTransactionCapabilitiesRequest) (proto.DescribeTransactionCapabilitiesResponse, error)
}

type StatsReader interface {
	GetStats() (memnode.StatsResponse, error)
}

type BackoffStrategy interface {
	Wait(ctx context.Context, attempt int)
}

type RandomBackoff struct {
	Base time.Duration
	Max  time.Duration
	Rng  *rand.Rand
}

func (b RandomBackoff) Wait(ctx context.Context, attempt int) {
	if b.Base <= 0 {
		b.Base = 5 * time.Millisecond
	}
	if b.Max <= 0 {
		b.Max = 100 * time.Millisecond
	}
	r := b.Rng
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	delay := b.Base << attempt
	if delay > b.Max {
		delay = b.Max
	}
	jitter := time.Duration(r.Int63n(int64(delay) + 1))
	t := time.NewTimer(jitter)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
	}
}

// Coordinator gathers participant votes and sends commit or abort decisions.
type Coordinator struct {
	nodes       map[uint32]Participant
	primaryFor  map[uint32]uint32
	generations map[uint32]uint64
	genMu       sync.Mutex
	tidCounter  uint64
	coordID     uint64
	epoch       uint64
	backoff     BackoffStrategy
	maxRetries  int
	workersMu   sync.Mutex
	workers     map[uint32]*rpcWorker
	stats       map[uint32]StatsReader
}

func NewCoordinator(nodes map[uint32]Participant) *Coordinator {
	cpy := make(map[uint32]Participant, len(nodes))
	for id, node := range nodes {
		cpy[id] = node
	}
	c := &Coordinator{nodes: cpy, primaryFor: map[uint32]uint32{}, generations: map[uint32]uint64{}, coordID: newCoordinatorID(), epoch: 1, backoff: RandomBackoff{}, maxRetries: 5, workers: map[uint32]*rpcWorker{}, stats: map[uint32]StatsReader{}}
	for id, node := range cpy {
		c.workers[id] = newRPCWorker(id, node)
	}
	return c
}

func newCoordinatorID() uint64 {
	var raw [8]byte
	if _, err := cryptorand.Read(raw[:]); err == nil {
		if id := binary.LittleEndian.Uint64(raw[:]); id != 0 {
			return id
		}
	}
	id := uint64(time.Now().UnixNano()) ^ coordinatorIDFallback.Add(1)
	if id == 0 {
		return 1
	}
	return id
}

// SetCoordinatorIDForTest gives tests a predictable coordinator ID.
// Call it before running any transactions.
func (c *Coordinator) SetCoordinatorIDForTest(id uint64) {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	c.coordID = id
}

func (c *Coordinator) SetPrimaryMapping(logicalToPrimary map[uint32]uint32) {
	c.primaryFor = map[uint32]uint32{}
	for logical, primary := range logicalToPrimary {
		c.primaryFor[logical] = primary
	}
}

func (c *Coordinator) SetGenerations(generations map[uint32]uint64) {
	c.generations = map[uint32]uint64{}
	for hostId, generation := range generations {
		c.generations[hostId] = generation
	}
}

func (c *Coordinator) SetStatsReaders(readers map[uint32]StatsReader) {
	c.stats = map[uint32]StatsReader{}
	for hostId, reader := range readers {
		c.stats[hostId] = reader
	}
}

func (c *Coordinator) resolveHostId(hostId uint32) uint32 {
	if primary, ok := c.primaryFor[hostId]; ok {
		return primary
	}
	return hostId
}

func (c *Coordinator) generationFor(hostId uint32) uint64 {
	return c.generations[hostId]
}

func (c *Coordinator) SetBackoffForTest(backoff BackoffStrategy) {
	c.backoff = backoff
}

func (c *Coordinator) SetMaxRetriesForTest(max int) {
	c.maxRetries = max
}

func (c *Coordinator) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	node, ok := c.nodes[c.resolveHostId(hostId)]
	if !ok {
		return nil, ErrUnknownMemNode
	}
	return node.Read(proto.WithGeneration(ctx, c.generationFor(c.resolveHostId(hostId))), addr, length)
}

func (c *Coordinator) GetStats(hostId uint32) (memnode.StatsResponse, error) {
	physical := c.resolveHostId(hostId)
	if reader, ok := c.stats[physical]; ok && reader != nil {
		return reader.GetStats()
	}
	if node, ok := c.nodes[physical]; ok {
		if statsNode, ok := node.(interface{ StatsSnapshot() memnode.StatsResponse }); ok {
			return statsNode.StatsSnapshot(), nil
		}
		return memnode.StatsResponse{}, ErrNoAdminEndpoint
	}
	return memnode.StatsResponse{}, ErrUnknownMemNode
}

func (c *Coordinator) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	node, ok := c.nodes[c.resolveHostId(hostId)]
	if !ok {
		return ErrUnknownMemNode
	}
	return node.Write(proto.WithGeneration(ctx, c.generationFor(c.resolveHostId(hostId))), addr, data)
}

func (c *Coordinator) nextTID() proto.TransactionID {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	c.tidCounter++
	return proto.TransactionID{CoordinatorID: c.coordID, Counter: c.tidCounter, Epoch: c.epoch}
}

func (c *Coordinator) observeEpoch(epoch uint64) {
	if epoch == 0 {
		return
	}
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if epoch > c.epoch {
		c.epoch = epoch
	}
}

type ExecResult struct {
	Committed        bool
	ReadResults      map[proto.ReadItem][]byte
	Status           proto.Vote
	ParticipantVotes map[uint32]proto.Vote
	TransportErr     error
	UnknownOutcome   bool
}

func (c *Coordinator) SetRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) error {
	if maxRetries < 0 {
		return errors.New("max retries must be >= 0")
	}
	if baseBackoff <= 0 || maxBackoff <= 0 {
		return errors.New("retry backoff durations must be > 0")
	}
	if baseBackoff > maxBackoff {
		return errors.New("retry backoff base must be <= max")
	}
	c.maxRetries = maxRetries
	c.backoff = RandomBackoff{Base: baseBackoff, Max: maxBackoff}
	return nil
}

func (c *Coordinator) ExecuteAndCommit(ctx context.Context, class string, compares []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) ExecResult {
	seen := make(map[uint32]struct{})
	for _, i := range compares {
		seen[c.resolveHostId(i.MemID)] = struct{}{}
	}
	for _, i := range reads {
		seen[c.resolveHostId(i.MemID)] = struct{}{}
	}
	for _, i := range writes {
		seen[c.resolveHostId(i.MemID)] = struct{}{}
	}
	hostIds := make([]uint32, 0, len(seen))
	for hostId := range seen {
		hostIds = append(hostIds, hostId)
	}
	sort.Slice(hostIds, func(i, j int) bool { return hostIds[i] < hostIds[j] })

	attempt := 0
	for {
		txId := c.nextTID()
		res, badLock := c.execAttempt(ctx, txId, hostIds, class, compares, reads, writes)
		if !badLock {
			return res
		}
		if attempt >= c.maxRetries {
			return res
		}
		c.backoff.Wait(ctx, attempt)
		for _, hostId := range hostIds {
			if node, ok := c.nodes[hostId]; ok {
				if mn, ok2 := node.(*memnode.Participant); ok2 {
					mn.RecordRetryForClass(class)
				}
			}
		}
		attempt++
	}
}

type prepareResult struct {
	hostId   uint32
	resp     proto.ExecPrepareResponse
	err      error
	readData map[proto.ReadItem][]byte
}

type commitDispatch struct {
	tid                             proto.TransactionID
	requestID                       proto.RequestID
	correlationID                   string
	actionCommit                    bool
	generation                      uint64
	deadlineDecisionProtocolVersion uint16
	barrier                         bool
	respC                           chan error
}

type rpcWorker struct {
	hostId     uint32
	node       Participant
	prepareCh  chan prepareCall
	commitCh   chan commitDispatch
	batchDelay time.Duration
	batchMax   int
}

// FlushPendingCommits waits for queued commit/abort messages and reports errors.
// ExecuteAndCommit returns once the decision is recoverable from participant
// state. For deadline-bearing writes, it also saves the decision on a participant
// before returning. Use this method when you need to wait for delivery too.
func (c *Coordinator) FlushPendingCommits(ctx context.Context) error {
	c.workersMu.Lock()
	workers := make([]*rpcWorker, 0, len(c.workers))
	for _, worker := range c.workers {
		workers = append(workers, worker)
	}
	c.workersMu.Unlock()

	results := make(chan error, len(workers))
	for _, worker := range workers {
		go func(worker *rpcWorker) {
			results <- worker.flushPending(ctx)
		}(worker)
	}
	var firstErr error
	for range workers {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type prepareCall struct {
	ctx   context.Context
	req   proto.ExecPrepareRequest
	respC chan prepareResult
}

func newRPCWorker(hostId uint32, node Participant) *rpcWorker {
	w := &rpcWorker{hostId: hostId, node: node, prepareCh: make(chan prepareCall, 256), commitCh: make(chan commitDispatch, 1024), batchDelay: 2 * time.Millisecond, batchMax: 64}
	go w.runPrepare()
	go w.runCommitBatcher()
	return w
}

func (w *rpcWorker) runPrepare() {
	for req := range w.prepareCh {
		resp, err := w.node.ExecAndPrepare(req.ctx, req.req)
		mappedReads := make(map[proto.ReadItem][]byte, len(req.req.ReadItems))
		for idx, item := range req.req.ReadItems {
			if data, ok := resp.ReadData[uint32(idx)]; ok {
				mappedReads[item] = data
			}
		}
		req.respC <- prepareResult{hostId: w.hostId, resp: resp, err: err, readData: mappedReads}
	}
}

func (w *rpcWorker) runCommitBatcher() {
	t := time.NewTimer(time.Hour)
	if !t.Stop() {
		<-t.C
	}
	defer t.Stop()

	batch := make([]commitDispatch, 0, w.batchMax)
	var deliveryErr error
	rememberDeliveryErr := func(err error) {
		if err != nil && deliveryErr == nil {
			deliveryErr = err
		}
	}
	var timerC <-chan time.Time
	stopTimer := func() {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		timerC = nil
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		decisions := make([]proto.CommitRequest, 0, len(batch))
		for _, d := range batch {
			decisions = append(decisions, proto.CommitRequest{TID: d.tid, RequestID: d.requestID, CorrelationID: d.correlationID, ActionCommit: d.actionCommit, Generation: d.generation, DeadlineDecisionProtocolVersion: d.deadlineDecisionProtocolVersion})
		}
		resp, err := w.node.CommitBatch(context.Background(), proto.CommitBatchRequest{Decisions: decisions})
		var firstErr error
		if err != nil {
			for _, d := range batch {
				commitResp, err2 := w.node.Commit(context.Background(), proto.CommitRequest{TID: d.tid, RequestID: d.requestID, CorrelationID: d.correlationID, ActionCommit: d.actionCommit, Generation: d.generation, DeadlineDecisionProtocolVersion: d.deadlineDecisionProtocolVersion})
				if err2 != nil {
					log.Printf("commit rpc failed for memnode %d tid=%+v: %v", w.hostId, d.tid, err2)
					if firstErr == nil {
						firstErr = err2
					}
				} else if !commitResp.Ack {
					log.Printf("commit not acknowledged for memnode %d tid=%+v", w.hostId, d.tid)
					if firstErr == nil {
						firstErr = errors.New("commit not acknowledged")
					}
				}
			}
		} else {
			if len(resp.Acks) != len(batch) {
				firstErr = errors.New("batched commit acknowledgement count mismatch")
			}
			for i, ack := range resp.Acks {
				if !ack && i < len(batch) {
					log.Printf("batched commit not acknowledged for memnode %d tid=%+v", w.hostId, batch[i].tid)
					if firstErr == nil {
						firstErr = errors.New("batched commit not acknowledged")
					}
				}
			}
		}
		for _, d := range batch {
			if d.respC != nil {
				d.respC <- firstErr
			}
		}
		batch = batch[:0]
		return firstErr
	}

	for {
		select {
		case d := <-w.commitCh:
			if d.barrier {
				rememberDeliveryErr(flush())
				d.respC <- deliveryErr
				deliveryErr = nil
				continue
			}
			batch = append(batch, d)
			if len(batch) == 1 {
				t.Reset(w.batchDelay)
				timerC = t.C
			}
			if len(batch) >= w.batchMax {
				rememberDeliveryErr(flush())
			}
		case <-timerC:
			rememberDeliveryErr(flush())
		}
	}
}

func (w *rpcWorker) flushPending(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case w.commitCh <- commitDispatch{barrier: true, respC: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Coordinator) execAttempt(ctx context.Context, txId proto.TransactionID, hostIds []uint32, class string, compares []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) (ExecResult, bool) {
	decisionDeadlineNanos := earliestDecisionDeadlineNanos(compares)
	if decisionDeadlineNanos != 0 {
		if err := c.requireDeadlineDecisionProtocol(ctx, hostIds); err != nil {
			return ExecResult{Committed: false, ReadResults: map[proto.ReadItem][]byte{}, Status: proto.VoteBadInternal, TransportErr: err}, false
		}
	}
	if len(hostIds) == 1 {
		return c.execSingleParticipant1PC(ctx, txId, hostIds[0], class, compares, reads, writes), false
	}

	results := make(chan prepareResult, len(hostIds))
	for _, hostId := range hostIds {
		go func(hostId uint32) {
			c.workersMu.Lock()
			worker, ok := c.workers[hostId]
			c.workersMu.Unlock()
			if !ok {
				results <- prepareResult{hostId: hostId, err: ErrUnknownMemNode}
				return
			}
			reqReads := readItemsForHost(hostId, reads, c.resolveHostId)
			worker.prepareCh <- prepareCall{ctx: ctx, req: proto.ExecPrepareRequest{
				TID:                             txId,
				RequestID:                       proto.RequestID(txId.CorrelationID() + ":prepare:" + strconv.FormatUint(uint64(hostId), 10)),
				CorrelationID:                   txId.CorrelationID(),
				Participants:                    append([]uint32(nil), hostIds...),
				HasWrites:                       len(writes) > 0,
				DecisionDeadlineNanos:           decisionDeadlineNanos,
				DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
				CmpItems:                        compareItemsForHost(hostId, compares, c.resolveHostId),
				ReadItems:                       reqReads,
				WriteItems:                      writeItemsForHost(hostId, writes, c.resolveHostId),
				Epoch:                           txId.Epoch,
				Class:                           class,
				Generation:                      c.generationFor(hostId),
			}, respC: results}
		}(hostId)
	}

	decideCommit := true
	status := proto.VoteOk
	badLock := false
	aggregatedReads := map[proto.ReadItem][]byte{}
	participantVotes := map[uint32]proto.Vote{}
	for range hostIds {
		r := <-results
		if r.err != nil {
			decideCommit = false
			status = proto.VoteBadInternal
			participantVotes[r.hostId] = proto.VoteBadInternal
			var remoteErr *rpcwire.RemoteError
			if !errors.As(r.err, &remoteErr) {
				return ExecResult{Committed: false, UnknownOutcome: true, ReadResults: map[proto.ReadItem][]byte{}, Status: status, ParticipantVotes: participantVotes, TransportErr: r.err}, false
			}
			continue
		}
		participantVotes[r.hostId] = r.resp.Vote
		if r.resp.Vote != proto.VoteOk {
			c.observeEpoch(r.resp.CurrentEpoch)
			decideCommit = false
			status = r.resp.Vote
			if r.resp.Vote == proto.VoteBadLock {
				badLock = true
			}
			continue
		}
		for k, v := range r.readData {
			aggregatedReads[k] = append([]byte(nil), v...)
		}
		c.observeEpoch(r.resp.CurrentEpoch)
	}
	if decideCommit && !comparisonsValidAt(compares, time.Now()) {
		decideCommit = false
		status = proto.VoteBadCmp
	}
	decisionWitness := uint32(0)
	if decisionDeadlineNanos != 0 && len(writes) > 0 && allVotesOK(participantVotes, hostIds) {
		var err error
		decisionWitness, err = c.persistConditionalDecision(ctx, txId, hostIds, decideCommit)
		if err != nil {
			return ExecResult{Committed: false, UnknownOutcome: true, ReadResults: map[proto.ReadItem][]byte{}, Status: proto.VoteBadInternal, ParticipantVotes: participantVotes, TransportErr: err}, false
		}
	}
	for _, hostId := range hostIds {
		if hostId == decisionWitness {
			continue
		}
		c.workersMu.Lock()
		worker := c.workers[hostId]
		c.workersMu.Unlock()
		if worker != nil {
			worker.commitCh <- commitDispatch{tid: txId, requestID: proto.RequestID(txId.CorrelationID() + ":commit:" + strconv.FormatUint(uint64(hostId), 10)), correlationID: txId.CorrelationID(), actionCommit: decideCommit, generation: c.generationFor(hostId), deadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion}
		}
	}

	if !decideCommit {
		return ExecResult{Committed: false, ReadResults: map[proto.ReadItem][]byte{}, Status: status, ParticipantVotes: participantVotes}, badLock
	}
	return ExecResult{Committed: true, ReadResults: aggregatedReads, Status: proto.VoteOk, ParticipantVotes: participantVotes}, false
}

func (c *Coordinator) persistConditionalDecision(ctx context.Context, txId proto.TransactionID, hostIds []uint32, actionCommit bool) (uint32, error) {
	ordered := append([]uint32(nil), hostIds...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered) == 0 {
		return 0, ErrUnknownMemNode
	}
	witness := ordered[0]
	c.workersMu.Lock()
	worker := c.workers[witness]
	c.workersMu.Unlock()
	if worker == nil {
		return 0, ErrUnknownMemNode
	}
	resp, err := worker.node.Commit(ctx, proto.CommitRequest{
		TID: txId, RequestID: proto.RequestID(txId.CorrelationID() + ":commit:" + strconv.FormatUint(uint64(witness), 10)),
		CorrelationID: txId.CorrelationID(), ActionCommit: actionCommit, Generation: c.generationFor(witness),
		DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
	})
	if err != nil {
		return 0, err
	}
	if !resp.Ack {
		return 0, errors.New("conditional decision was not acknowledged")
	}
	return witness, nil
}

func allVotesOK(votes map[uint32]proto.Vote, hostIds []uint32) bool {
	for _, participant := range hostIds {
		vote, ok := votes[participant]
		if !ok || vote != proto.VoteOk {
			return false
		}
	}
	return true
}

func (c *Coordinator) requireDeadlineDecisionProtocol(ctx context.Context, hostIds []uint32) error {
	ordered := append([]uint32(nil), hostIds...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, hostId := range ordered {
		node := c.nodes[hostId]
		capable, ok := node.(deadlineDecisionCapable)
		if !ok {
			return fmt.Errorf("%w: memnode %d does not report capabilities", ErrDeadlineDecisionProtocolUpgradeRequired, hostId)
		}
		resp, err := capable.DescribeTransactionCapabilities(ctx, proto.DescribeTransactionCapabilitiesRequest{})
		if err != nil {
			return fmt.Errorf("%w: memnode %d: %v", ErrDeadlineDecisionProtocolUpgradeRequired, hostId, err)
		}
		if resp.DeadlineDecisionProtocolVersion < proto.DeadlineDecisionProtocolVersion {
			return fmt.Errorf("%w: memnode %d reports version %d", ErrDeadlineDecisionProtocolUpgradeRequired, hostId, resp.DeadlineDecisionProtocolVersion)
		}
	}
	return nil
}

func earliestDecisionDeadlineNanos(items []proto.CmpItem) int64 {
	var earliest time.Time
	for _, item := range items {
		if item.ValidUntil.IsZero() || (!earliest.IsZero() && !item.ValidUntil.Before(earliest)) {
			continue
		}
		earliest = item.ValidUntil
	}
	if earliest.IsZero() {
		return 0
	}
	return earliest.UnixNano()
}

func comparisonsValidAt(items []proto.CmpItem, now time.Time) bool {
	for _, item := range items {
		if !item.ValidUntil.IsZero() && !now.Before(item.ValidUntil) {
			return false
		}
	}
	return true
}

func (c *Coordinator) execSingleParticipant1PC(ctx context.Context, txId proto.TransactionID, hostId uint32, class string, compares []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) ExecResult {
	node, ok := c.nodes[hostId]
	if !ok {
		return ExecResult{Committed: false, Status: proto.VoteBadInternal, ReadResults: map[proto.ReadItem][]byte{}, ParticipantVotes: map[uint32]proto.Vote{hostId: proto.VoteBadInternal}}
	}
	reqReads := readItemsForHost(hostId, reads, c.resolveHostId)
	resp, err := node.ExecCommit1PC(ctx, proto.ExecCommit1PCRequest{
		TID:                             txId,
		RequestID:                       proto.RequestID(txId.CorrelationID() + ":1pc:" + strconv.FormatUint(uint64(hostId), 10)),
		CorrelationID:                   txId.CorrelationID(),
		CmpItems:                        compareItemsForHost(hostId, compares, c.resolveHostId),
		ReadItems:                       reqReads,
		WriteItems:                      writeItemsForHost(hostId, writes, c.resolveHostId),
		Epoch:                           txId.Epoch,
		Class:                           class,
		Generation:                      c.generationFor(hostId),
		DeadlineDecisionProtocolVersion: proto.DeadlineDecisionProtocolVersion,
	})
	if err != nil {
		var remoteErr *rpcwire.RemoteError
		res := ExecResult{Committed: false, Status: proto.VoteBadInternal, ReadResults: map[proto.ReadItem][]byte{}, ParticipantVotes: map[uint32]proto.Vote{hostId: proto.VoteBadInternal}}
		if !errors.As(err, &remoteErr) {
			res.TransportErr = err
			res.UnknownOutcome = true
		}
		return res
	}
	c.observeEpoch(resp.CurrentEpoch)
	if resp.Vote != proto.VoteOk {
		return ExecResult{Committed: false, Status: resp.Vote, ReadResults: map[proto.ReadItem][]byte{}, ParticipantVotes: map[uint32]proto.Vote{hostId: resp.Vote}}
	}
	mappedReads := make(map[proto.ReadItem][]byte, len(reqReads))
	for idx, item := range reqReads {
		if data, ok := resp.ReadData[uint32(idx)]; ok {
			mappedReads[item] = append([]byte(nil), data...)
		}
	}
	return ExecResult{Committed: true, Status: proto.VoteOk, ReadResults: mappedReads, ParticipantVotes: map[uint32]proto.Vote{hostId: proto.VoteOk}}
}

func compareItemsForHost(hostId uint32, items []proto.CmpItem, resolve func(uint32) uint32) []proto.CmpItem {
	out := make([]proto.CmpItem, 0)
	for _, i := range items {
		if resolve(i.MemID) == hostId {
			out = append(out, i)
		}
	}
	return out
}

func readItemsForHost(hostId uint32, items []proto.ReadItem, resolve func(uint32) uint32) []proto.ReadItem {
	out := make([]proto.ReadItem, 0)
	for _, i := range items {
		if resolve(i.MemID) == hostId {
			out = append(out, i)
		}
	}
	return out
}

func writeItemsForHost(hostId uint32, items []proto.WriteItem, resolve func(uint32) uint32) []proto.WriteItem {
	out := make([]proto.WriteItem, 0)
	for _, i := range items {
		if resolve(i.MemID) == hostId {
			out = append(out, i)
		}
	}
	return out
}
