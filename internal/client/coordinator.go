package client

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
	"github.com/jbancroft/ConcertumDemo/minitxn"
)

var coordinatorIDFallback atomic.Uint64

const (
	commitBatchDelay   = 2 * time.Millisecond
	commitBatchSize    = 64
	commitQueueSize    = 1024
	defaultBaseBackoff = 5 * time.Millisecond
	defaultMaxBackoff  = 100 * time.Millisecond
	defaultMaxRetries  = 5
	prepareQueueSize   = 256
)

// Participant is the node interface used by the coordinator.
type Participant interface {
	recovery.Participant
	ExecAndPrepare(ctx context.Context, req sinfonia.ExecPrepareRequest) (sinfonia.ExecPrepareResponse, error)
	ExecCommit1PC(ctx context.Context, req sinfonia.ExecCommit1PCRequest) (sinfonia.ExecCommit1PCResponse, error)
	Commit(ctx context.Context, req sinfonia.CommitRequest) (sinfonia.CommitResponse, error)
	CommitBatch(ctx context.Context, req sinfonia.CommitBatchRequest) (sinfonia.CommitBatchResponse, error)
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
		b.Base = defaultBaseBackoff
	}
	if b.Max <= 0 {
		b.Max = defaultMaxBackoff
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
	closeOnce  sync.Once
	closeErr   error
	nodes      map[uint32]Participant
	genMu      sync.Mutex
	tidCounter uint64
	coordID    uint64
	epoch      uint64
	backoff    BackoffStrategy
	maxRetries int
	workers    map[uint32]*rpcWorker
}

func NewCoordinator(nodes map[uint32]Participant) *Coordinator {
	cpy := make(map[uint32]Participant, len(nodes))
	for id, node := range nodes {
		cpy[id] = node
	}
	c := &Coordinator{nodes: cpy, coordID: newCoordinatorID(), epoch: sinfonia.EpochAt(time.Now()), backoff: RandomBackoff{}, maxRetries: defaultMaxRetries, workers: map[uint32]*rpcWorker{}}
	for id, node := range cpy {
		c.workers[id] = newRPCWorker(id, node)
	}
	return c
}

// Close drains queued decisions and joins the workers. Call only after all API
// calls have returned; the coordinator must not be used afterward except to Close.
// Participants are borrowed; their owner must close them after the coordinator.
func (c *Coordinator) Close() error {
	c.closeOnce.Do(func() {
		for _, worker := range c.workers {
			close(worker.prepareCh)
			close(worker.commitCh)
		}
		for _, worker := range c.workers {
			worker.done.Wait()
			c.closeErr = errors.Join(c.closeErr, worker.deliveryErr)
		}
	})
	return c.closeErr
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

func (c *Coordinator) SetBackoffForTest(backoff BackoffStrategy) {
	c.backoff = backoff
}

func (c *Coordinator) SetMaxRetriesForTest(max int) {
	c.maxRetries = max
}

func (c *Coordinator) nextTID() sinfonia.TransactionID {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if epoch := sinfonia.EpochAt(time.Now()); epoch > c.epoch {
		c.epoch = epoch
	}
	c.tidCounter++
	return sinfonia.TransactionID{CoordinatorID: c.coordID, Counter: c.tidCounter, Epoch: c.epoch}
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
	Recovery         *minitxn.RecoveryHandle
	Committed        bool
	ReadResults      map[sinfonia.ReadItem][]byte
	Status           sinfonia.Vote
	ParticipantVotes map[uint32]sinfonia.Vote
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

func (c *Coordinator) ExecuteAndCommit(ctx context.Context, compares []sinfonia.CmpItem, reads []sinfonia.ReadItem, writes []sinfonia.WriteItem) ExecResult {
	ctx, cancel := context.WithTimeout(ctx, defaultExecuteTimeout)
	defer cancel()
	seen := make(map[uint32]struct{})
	for _, i := range compares {
		seen[i.MemID] = struct{}{}
	}
	for _, i := range reads {
		seen[i.MemID] = struct{}{}
	}
	for _, i := range writes {
		seen[i.MemID] = struct{}{}
	}
	hostIds := make([]uint32, 0, len(seen))
	for hostId := range seen {
		hostIds = append(hostIds, hostId)
	}
	sort.Slice(hostIds, func(i, j int) bool { return hostIds[i] < hostIds[j] })

	attempt := 0
	for {
		txId := c.nextTID()
		res, retry := c.execAttempt(ctx, txId, hostIds, compares, reads, writes)
		if res.UnknownOutcome {
			handle := &minitxn.RecoveryHandle{TID: txId, Participants: append([]uint32(nil), hostIds...)}
			for _, item := range reads {
				handle.Reads = append(handle.Reads, minitxn.ReadItem{HostId: item.MemID, Address: item.Addr, Length: item.Len})
			}
			resolved := c.Resolve(ctx, *handle)
			if resolved.UnknownOutcome {
				resolved.TransportErr = errors.Join(res.TransportErr, resolved.TransportErr)
			}
			return resolved
		}
		if !retry || ctx.Err() != nil {
			return res
		}
		if attempt >= c.maxRetries {
			return res
		}
		c.backoff.Wait(ctx, attempt)
		attempt++
	}
}

type prepareResult struct {
	hostId   uint32
	resp     sinfonia.ExecPrepareResponse
	err      error
	readData map[sinfonia.ReadItem][]byte
}

type commitDispatch struct {
	tid          sinfonia.TransactionID
	requestID    sinfonia.RequestID
	actionCommit bool
	barrier      bool
	respC        chan error
}

type rpcWorker struct {
	done        sync.WaitGroup
	deliveryErr error // Written by the commit worker; read after joining it.
	hostId      uint32
	node        Participant
	prepareCh   chan prepareCall
	commitCh    chan commitDispatch
	batchDelay  time.Duration
	batchMax    int
}

// FlushPendingCommits waits for queued commit/abort messages and reports errors.
func (c *Coordinator) FlushPendingCommits(ctx context.Context) error {
	results := make(chan error, len(c.workers))
	for _, worker := range c.workers {
		go func(worker *rpcWorker) {
			results <- worker.flushPending(ctx)
		}(worker)
	}
	var firstErr error
	for range c.workers {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type prepareCall struct {
	ctx   context.Context
	req   sinfonia.ExecPrepareRequest
	respC chan prepareResult
}

func newRPCWorker(hostId uint32, node Participant) *rpcWorker {
	w := &rpcWorker{hostId: hostId, node: node, prepareCh: make(chan prepareCall, prepareQueueSize), commitCh: make(chan commitDispatch, commitQueueSize), batchDelay: commitBatchDelay, batchMax: commitBatchSize}
	w.done.Add(2)
	go w.runPrepare()
	go w.runCommitBatcher()
	return w
}

func (w *rpcWorker) runPrepare() {
	defer w.done.Done()
	for req := range w.prepareCh {
		if err := req.ctx.Err(); err != nil {
			req.respC <- prepareResult{hostId: w.hostId, err: err}
			continue
		}
		resp, err := w.node.ExecAndPrepare(req.ctx, req.req)
		mappedReads := make(map[sinfonia.ReadItem][]byte, len(req.req.ReadItems))
		for idx, item := range req.req.ReadItems {
			if data, ok := resp.ReadData[uint32(idx)]; ok {
				mappedReads[item] = data
			}
		}
		req.respC <- prepareResult{hostId: w.hostId, resp: resp, err: err, readData: mappedReads}
	}
}

func (w *rpcWorker) runCommitBatcher() {
	defer w.done.Done()
	t := time.NewTimer(time.Hour)
	if !t.Stop() {
		<-t.C
	}
	defer t.Stop()

	batch := make([]commitDispatch, 0, w.batchMax)
	rememberDeliveryErr := func(err error) {
		if err != nil && w.deliveryErr == nil {
			w.deliveryErr = err
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
		decisions := make([]sinfonia.CommitRequest, 0, len(batch))
		for _, d := range batch {
			decisions = append(decisions, sinfonia.CommitRequest{TID: d.tid, RequestID: d.requestID, ActionCommit: d.actionCommit})
		}
		resp, err := w.node.CommitBatch(context.Background(), sinfonia.CommitBatchRequest{Decisions: decisions})
		var firstErr error
		if err != nil {
			for _, d := range batch {
				commitResp, err2 := w.node.Commit(context.Background(), sinfonia.CommitRequest{TID: d.tid, RequestID: d.requestID, ActionCommit: d.actionCommit})
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
		case d, ok := <-w.commitCh:
			if !ok {
				rememberDeliveryErr(flush())
				return
			}
			if d.barrier {
				rememberDeliveryErr(flush())
				d.respC <- w.deliveryErr
				w.deliveryErr = nil
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

func (c *Coordinator) execAttempt(ctx context.Context, txId sinfonia.TransactionID, hostIds []uint32, compares []sinfonia.CmpItem, reads []sinfonia.ReadItem, writes []sinfonia.WriteItem) (ExecResult, bool) {
	if len(hostIds) == 1 {
		res := c.execSingleParticipant1PC(ctx, txId, hostIds[0], compares, reads, writes)
		return res, res.Status == sinfonia.VoteBadLock || res.Status == sinfonia.VoteBadStaleEpoch
	}

	results := make(chan prepareResult, len(hostIds))
	// A cancelled attempt may return before replies arrive, but its producers
	// must finish enqueueing before Close can close the worker channels.
	var dispatches sync.WaitGroup
	defer dispatches.Wait()
	for _, hostId := range hostIds {
		dispatches.Add(1)
		go func(hostId uint32) {
			defer dispatches.Done()
			worker, ok := c.workers[hostId]
			if !ok {
				results <- prepareResult{hostId: hostId, err: ErrUnknownMemNode}
				return
			}
			reqReads := readItemsForHost(hostId, reads)
			call := prepareCall{ctx: ctx, req: sinfonia.ExecPrepareRequest{
				TID:          txId,
				RequestID:    sinfonia.RequestID(txId.CorrelationID() + ":prepare:" + strconv.FormatUint(uint64(hostId), 10)),
				Participants: append([]uint32(nil), hostIds...),
				HasWrites:    len(writes) > 0,
				CmpItems:     compareItemsForHost(hostId, compares),
				ReadItems:    reqReads,
				WriteItems:   writeItemsForHost(hostId, writes),
				Epoch:        txId.Epoch,
			}, respC: results}
			select {
			case worker.prepareCh <- call:
			case <-ctx.Done():
				results <- prepareResult{hostId: hostId, err: ctx.Err()}
			}
		}(hostId)
	}

	decideCommit := true
	status := sinfonia.VoteOk
	retry := false
	aggregatedReads := map[sinfonia.ReadItem][]byte{}
	participantVotes := map[uint32]sinfonia.Vote{}
	var prepareErr error
	for range hostIds {
		var r prepareResult
		select {
		case r = <-results:
		case <-ctx.Done():
			return ExecResult{UnknownOutcome: true, Status: sinfonia.VoteBadInternal, TransportErr: ctx.Err()}, false
		}
		if r.err != nil {
			participantVotes[r.hostId] = sinfonia.VoteBadInternal
			// An error may follow a saved yes vote; only a returned vote
			// establishes that this participant rejected the transaction.
			prepareErr = errors.Join(prepareErr, r.err)
			continue
		}
		participantVotes[r.hostId] = r.resp.Vote
		if r.resp.Vote != sinfonia.VoteOk {
			c.observeEpoch(r.resp.CurrentEpoch)
			decideCommit = false
			status = r.resp.Vote
			if r.resp.Vote == sinfonia.VoteBadLock || r.resp.Vote == sinfonia.VoteBadStaleEpoch {
				retry = true
			}
			continue
		}
		for k, v := range r.readData {
			aggregatedReads[k] = append([]byte(nil), v...)
		}
		c.observeEpoch(r.resp.CurrentEpoch)
	}
	if prepareErr != nil {
		return ExecResult{UnknownOutcome: true, Status: sinfonia.VoteBadInternal, ParticipantVotes: participantVotes, TransportErr: prepareErr}, false
	}
	for _, hostId := range hostIds {
		worker := c.workers[hostId]
		if worker != nil {
			decision := commitDispatch{tid: txId, requestID: sinfonia.RequestID(txId.CorrelationID() + ":commit:" + strconv.FormatUint(uint64(hostId), 10)), actionCommit: decideCommit}
			select {
			case worker.commitCh <- decision:
			case <-ctx.Done():
				// The votes already establish the decision. Participant recovery finishes
				// delivery if the caller cannot wait for room in the dispatch queue.
				return ExecResult{Committed: decideCommit, ReadResults: aggregatedReads, Status: status, ParticipantVotes: participantVotes, TransportErr: ctx.Err()}, false
			}
		}
	}

	if !decideCommit {
		return ExecResult{Committed: false, ReadResults: map[sinfonia.ReadItem][]byte{}, Status: status, ParticipantVotes: participantVotes}, retry
	}
	return ExecResult{Committed: true, ReadResults: aggregatedReads, Status: sinfonia.VoteOk, ParticipantVotes: participantVotes}, false
}

func (c *Coordinator) execSingleParticipant1PC(ctx context.Context, txId sinfonia.TransactionID, hostId uint32, compares []sinfonia.CmpItem, reads []sinfonia.ReadItem, writes []sinfonia.WriteItem) ExecResult {
	node, ok := c.nodes[hostId]
	if !ok {
		return ExecResult{Committed: false, Status: sinfonia.VoteBadInternal, ReadResults: map[sinfonia.ReadItem][]byte{}, ParticipantVotes: map[uint32]sinfonia.Vote{hostId: sinfonia.VoteBadInternal}}
	}
	reqReads := readItemsForHost(hostId, reads)
	resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
		TID:        txId,
		RequestID:  sinfonia.RequestID(txId.CorrelationID() + ":1pc:" + strconv.FormatUint(uint64(hostId), 10)),
		CmpItems:   compareItemsForHost(hostId, compares),
		ReadItems:  reqReads,
		WriteItems: writeItemsForHost(hostId, writes),
		Epoch:      txId.Epoch,
	})
	if err != nil {
		// Even a server error can occur after the durable commit decision.
		return ExecResult{UnknownOutcome: true, Status: sinfonia.VoteBadInternal, ParticipantVotes: map[uint32]sinfonia.Vote{hostId: sinfonia.VoteBadInternal}, TransportErr: err}
	}
	c.observeEpoch(resp.CurrentEpoch)
	if resp.Vote != sinfonia.VoteOk {
		return ExecResult{Committed: false, Status: resp.Vote, ReadResults: map[sinfonia.ReadItem][]byte{}, ParticipantVotes: map[uint32]sinfonia.Vote{hostId: resp.Vote}}
	}
	mappedReads := make(map[sinfonia.ReadItem][]byte, len(reqReads))
	for idx, item := range reqReads {
		if data, ok := resp.ReadData[uint32(idx)]; ok {
			mappedReads[item] = append([]byte(nil), data...)
		}
	}
	return ExecResult{Committed: true, Status: sinfonia.VoteOk, ReadResults: mappedReads, ParticipantVotes: map[uint32]sinfonia.Vote{hostId: sinfonia.VoteOk}}
}

func compareItemsForHost(hostId uint32, items []sinfonia.CmpItem) []sinfonia.CmpItem {
	out := make([]sinfonia.CmpItem, 0)
	for _, i := range items {
		if i.MemID == hostId {
			out = append(out, i)
		}
	}
	return out
}

func readItemsForHost(hostId uint32, items []sinfonia.ReadItem) []sinfonia.ReadItem {
	out := make([]sinfonia.ReadItem, 0)
	for _, i := range items {
		if i.MemID == hostId {
			out = append(out, i)
		}
	}
	return out
}

func writeItemsForHost(hostId uint32, items []sinfonia.WriteItem) []sinfonia.WriteItem {
	out := make([]sinfonia.WriteItem, 0)
	for _, i := range items {
		if i.MemID == hostId {
			out = append(out, i)
		}
	}
	return out
}
