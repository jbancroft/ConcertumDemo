package memnode_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestDurableLargeWritesSurviveRestart(t *testing.T) {
	const (
		firstWriteSize  = 64 << 10
		secondWriteSize = 256 << 10
		memorySize      = firstWriteSize + secondWriteSize
	)
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "applied", true: "committed before apply"}[interrupted], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			node, err := memnode.NewDurableParticipant(1, memorySize, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer node.Close()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			want := make([]byte, memorySize)
			for i := range want {
				// Exercise every byte value, including invalid UTF-8 and newlines.
				want[i] = byte(i)
			}
			fault := errors.New("interrupted after saving decision")
			for i, write := range []sinfonia.WriteItem{
				{MemID: 1, Addr: 0, Data: want[:firstWriteSize]},
				{MemID: 1, Addr: firstWriteSize, Data: want[firstWriteSize:]},
			} {
				if interrupted && i == 1 {
					node.SetOutcomeReplayBoundaryHookForTest(func(phase string, _ sinfonia.TransactionID) error {
						if phase == "one-pc-before-response" {
							return fault
						}
						return nil
					})
				}
				resp, err := node.ExecCommit1PC(ctx, sinfonia.ExecCommit1PCRequest{
					TID:        sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: sinfonia.EpochAt(time.Now())},
					WriteItems: []sinfonia.WriteItem{write},
				})
				if interrupted && i == 1 {
					if !errors.Is(err, fault) {
						t.Fatalf("commit interruption: %v", err)
					}
					got, err := node.ReadBytesForTest(firstWriteSize, secondWriteSize)
					if err != nil || !bytes.Equal(got, make([]byte, secondWriteSize)) {
						t.Fatalf("interrupted write applied before restart: %v", err)
					}
				} else if err != nil || resp.Vote != sinfonia.VoteOk {
					t.Fatalf("large write: %+v, %v", resp, err)
				}
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			node, err = memnode.NewDurableParticipant(1, memorySize, dir)
			if err != nil {
				t.Fatalf("reopen node after large writes: %v", err)
			}
			defer node.Close()
			if err := node.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			got, err := node.ReadBytesForTest(0, memorySize)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("restart did not preserve all binary write data: %v", err)
			}
		})
	}
}
