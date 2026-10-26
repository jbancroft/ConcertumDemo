package concertum_test

import (
	"context"

	"github.com/jbancroft/concertum2/internal/client"
	"github.com/jbancroft/concertum2/internal/memnode"
	"github.com/jbancroft/concertum2/internal/proto"
	"github.com/jbancroft/concertum2/pkg/concertum"
)

type coordinatorBackend struct {
	coord interface {
		Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error)
		Write(ctx context.Context, memID uint32, addr uint64, data []byte) error
		GetStats(memID uint32) (memnode.StatsResponse, error)
		ExecAndCommit(ctx context.Context, class string, cmp []proto.CmpItem, reads []proto.ReadItem, writes []proto.WriteItem) client.ExecResult
	}
}

func newCoordinatorBackend(coord *client.Coordinator) concertum.Backend {
	return &coordinatorBackend{coord: coord}
}

func (b *coordinatorBackend) Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error) {
	return b.coord.Read(ctx, memID, addr, length)
}

func (b *coordinatorBackend) Write(ctx context.Context, memID uint32, addr uint64, data []byte) error {
	return b.coord.Write(ctx, memID, addr, data)
}

func (b *coordinatorBackend) GetStats(memID uint32) (concertum.Stats, error) {
	stats, err := b.coord.GetStats(memID)
	if err != nil {
		return concertum.Stats{}, err
	}
	out := concertum.Stats{NodeID: stats.NodeID, Role: stats.Role, ByClass: map[string]concertum.TxnClassStats{}}
	out.Txns.Total = stats.Txns.Total
	out.Txns.Committed = stats.Txns.Committed
	out.Txns.Aborted = stats.Txns.Aborted
	out.Txns.Retried = stats.Txns.Retried
	out.Rates.Txns = concertum.TxnRateGroup{ExecutedPerSec: concertum.RateWindows(stats.Rates.Txns.ExecutedPerSec), RetriedPerSec: concertum.RateWindows(stats.Rates.Txns.RetriedPerSec), AbortedPerSec: concertum.RateWindows(stats.Rates.Txns.AbortedPerSec)}
	out.Rates.BytesWrittenPerSec = concertum.RateWindows(stats.Rates.BytesWrittenPerSec)
	for class, classStats := range stats.ByClass {
		out.ByClass[class] = concertum.TxnClassStats{Total: classStats.Total, Committed: classStats.Committed, Aborted: classStats.Aborted, Retried: classStats.Retried, Rates: concertum.TxnRateGroup{ExecutedPerSec: concertum.RateWindows(classStats.Rates.ExecutedPerSec), RetriedPerSec: concertum.RateWindows(classStats.Rates.RetriedPerSec), AbortedPerSec: concertum.RateWindows(classStats.Rates.AbortedPerSec)}}
	}
	if len(out.ByClass) == 0 {
		out.ByClass = nil
	}
	return out, nil
}

func (b *coordinatorBackend) ExecAndCommit(ctx context.Context, class string, cmp []concertum.CmpItem, reads []concertum.ReadItem, writes []concertum.WriteItem) concertum.ExecResult {
	internalCmp := make([]proto.CmpItem, 0, len(cmp))
	for _, item := range cmp {
		internalCmp = append(internalCmp, proto.CmpItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...), ValidUntil: item.ValidUntil})
	}
	internalReads := make([]proto.ReadItem, 0, len(reads))
	for _, item := range reads {
		internalReads = append(internalReads, proto.ReadItem{MemID: item.MemID, Addr: item.Addr, Len: item.Len})
	}
	internalWrites := make([]proto.WriteItem, 0, len(writes))
	for _, item := range writes {
		internalWrites = append(internalWrites, proto.WriteItem{MemID: item.MemID, Addr: item.Addr, Data: append([]byte(nil), item.Data...)})
	}
	res := b.coord.ExecAndCommit(ctx, class, internalCmp, internalReads, internalWrites)
	readsOut := make(map[concertum.ReadKey][]byte, len(res.ReadResults))
	for k, v := range res.ReadResults {
		readsOut[concertum.ReadKey{MemID: k.MemID, Addr: k.Addr, Len: k.Len}] = append([]byte(nil), v...)
	}
	if res.UnknownOutcome {
		return concertum.ExecResult{Committed: false, UnknownOutcome: true, ReadResults: map[concertum.ReadKey][]byte{}, Status: concertum.StatusUnknown}
	}
	if !res.Committed {
		return concertum.ExecResult{Committed: false, ReadResults: map[concertum.ReadKey][]byte{}, Status: concertum.StatusAborted}
	}
	return concertum.ExecResult{Committed: true, ReadResults: readsOut, Status: concertum.StatusCommitted}
}
