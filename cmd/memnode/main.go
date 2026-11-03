// Command memnode serves a durable Sinfonia participant over TCP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
	"github.com/jbancroft/ConcertumDemo/internal/recovery"
)

const (
	defaultMemorySize = 2 << 20 // 2 MiB
	peerTimeout       = 5 * time.Second
	recoveryRetry     = 100 * time.Millisecond
	recoveryTimeout   = 30 * time.Second
	shutdownTimeout   = 5 * time.Second
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	cancel()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.Reader, out, errOut io.Writer) (err error) {
	flags := flag.NewFlagSet("memnode", flag.ContinueOnError)
	flags.SetOutput(errOut)
	id := flags.Uint64("id", 1, "unique nonzero memory node ID")
	address := flags.String("listen", "127.0.0.1:7001", "RPC listen address (port 0 chooses a free port)")
	size := flags.Int("size", defaultMemorySize, "memory image size in bytes")
	dir := flags.String("data-dir", "", "required durable storage directory")
	parent := flags.Bool("exit-on-stdin-close", false, "exit when the demo parent closes stdin")
	peers := make(map[uint32]string)
	flags.Func("peer", "recovery peer as ID=RPC_ADDRESS; repeat for each peer", func(value string) error {
		idText, addr, ok := strings.Cut(value, "=")
		peerID, parseErr := strconv.ParseUint(idText, 10, 32)
		if !ok || parseErr != nil || peerID == 0 {
			return fmt.Errorf("invalid peer %q; want nonzero ID=HOST:PORT", value)
		}
		if _, _, splitErr := net.SplitHostPort(addr); splitErr != nil {
			return fmt.Errorf("invalid peer address: %w", splitErr)
		}
		if _, exists := peers[uint32(peerID)]; exists {
			return fmt.Errorf("duplicate peer ID %d", peerID)
		}
		peers[uint32(peerID)] = addr
		return nil
	})
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == 0 || *id > math.MaxUint32 || *size <= 0 {
		return errors.New("expected flags only, a nonzero uint32 node ID, and a positive size")
	}
	if *dir == "" {
		return errors.New("-data-dir is required")
	}
	if _, exists := peers[uint32(*id)]; exists {
		return errors.New("a node cannot be its own recovery peer")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if *parent {
		go func() {
			_, _ = io.Copy(io.Discard, input)
			cancel()
		}()
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	defer listener.Close()
	node, err := memnode.NewParticipant(uint32(*id), *size, *dir)
	if err != nil {
		return err
	}
	recoveryWorkers := make(map[uint32]recovery.Participant)
	recoveryPeers := make(map[uint32]memnode.RecoveryPeer)
	for peerID, addr := range peers {
		peer := memnode.NewRPCPeer(addr, peerTimeout)
		recoveryPeers[peerID] = peer
		recoveryWorkers[peerID] = peer
	}
	node.SetRecoveryPeers(recoveryPeers)
	finished := make(chan error, 1)
	go func() { finished <- node.RPCServer().Serve(listener) }()
	defer func() {
		_ = listener.Close()
		err = errors.Join(err, <-finished)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cleanupCancel()
		node.FlushAppliedNotifications(cleanupCtx)
		err = errors.Join(err, node.Close())
	}()
	// Recovery RPCs are available before readiness so nodes can restart together.
	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, recoveryTimeout)
	defer recoveryCancel()
	for {
		recoveryErr := node.Recover(recoveryCtx)
		if recoveryErr == nil {
			break
		}
		if !errors.Is(recoveryErr, memnode.ErrRecoveryPending) {
			return recoveryErr
		}
		select {
		case <-recoveryCtx.Done():
			return fmt.Errorf("recovery did not finish: %w: %v", recoveryCtx.Err(), recoveryErr)
		case <-time.After(recoveryRetry):
		}
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	go func() { defer close(maintenanceDone); node.RunRecovery(maintenanceCtx, recoveryWorkers) }()
	defer func() { stopMaintenance(); <-maintenanceDone }()
	// One JSON line is the startup handshake used by the demo launcher.
	if err := json.NewEncoder(out).Encode(struct {
		ID         uint32 `json:"id"`
		RPCAddress string `json:"rpc_address"`
	}{uint32(*id), listener.Addr().String()}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case serveErr := <-finished:
		finished <- serveErr // The deferred shutdown joins this result.
		return serveErr
	}
}
