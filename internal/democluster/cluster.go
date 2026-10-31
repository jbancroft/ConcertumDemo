// Package democluster starts standalone memnode processes for the local demos.
package democluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	sinfoniaclient "github.com/jbancroft/ConcertumDemo/sinfonia-client"
)

const shutdownTimeout = 10 * time.Second

type process struct {
	command *exec.Cmd
	input   io.WriteCloser
	done    chan struct{}
	ready   chan error
	err     error // Written before done is closed.
}

type Cluster struct {
	Client *sinfoniaclient.Client
	config map[uint32]sinfoniaclient.NodeConfig
	nodes  []*process
	once   sync.Once
	err    error
}

// Start launches two nodes. An empty data directory selects in-memory storage.
func Start(ctx context.Context, binary, dataDir string, memorySize int) (_ *Cluster, err error) {
	if binary == "" {
		return nil, errors.New("memnode executable required; use the Makefile demo or test target")
	}
	c := &Cluster{}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.Close())
		}
	}()
	// Reserve both addresses before launching either process. Keep the sockets
	// until immediately before Start; a bind conflict is reported as startup failure.
	var reservations []net.Listener
	defer func() {
		for _, listener := range reservations {
			_ = listener.Close()
		}
	}()
	config := make(map[uint32]sinfoniaclient.NodeConfig)
	c.config = config
	for id := uint32(1); id <= 2; id++ {
		listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			return nil, listenErr
		}
		reservations = append(reservations, listener)
		config[id] = sinfoniaclient.NodeConfig{RPCAddress: listener.Addr().String()}
	}
	for id := uint32(1); id <= 2; id++ {
		args := []string{"-id", fmt.Sprint(id), "-listen", config[id].RPCAddress,
			"-size", strconv.Itoa(memorySize), "-exit-on-stdin-close"}
		if dataDir != "" {
			args = append(args, "-data-dir", filepath.Join(dataDir, fmt.Sprint(id)))
		}
		for peerID := uint32(1); peerID <= 2; peerID++ {
			if peerID != id {
				args = append(args, "-peer", fmt.Sprintf("%d=%s", peerID, config[peerID].RPCAddress))
			}
		}
		command := exec.Command(binary, args...)
		var stderr bytes.Buffer
		command.Stderr = &stderr
		input, pipeErr := command.StdinPipe()
		if pipeErr != nil {
			return nil, pipeErr
		}
		output, pipeErr := command.StdoutPipe()
		if pipeErr != nil {
			_ = input.Close()
			return nil, pipeErr
		}
		_ = reservations[id-1].Close()
		if startErr := command.Start(); startErr != nil {
			_ = input.Close()
			_ = output.Close()
			return nil, startErr
		}
		node := &process{command: command, input: input, done: make(chan struct{}), ready: make(chan error, 1)}
		c.nodes = append(c.nodes, node)
		go func() {
			var ready struct {
				ID         uint32 `json:"id"`
				RPCAddress string `json:"rpc_address"`
			}
			decodeErr := json.NewDecoder(output).Decode(&ready)
			if decodeErr == nil && (ready.ID != id || ready.RPCAddress != config[id].RPCAddress) {
				decodeErr = fmt.Errorf("unexpected memnode readiness: %+v", ready)
			}
			node.ready <- decodeErr
			// Drain stdout before Wait closes its pipe.
			_, _ = io.Copy(io.Discard, output)
			waitErr := command.Wait()
			if waitErr != nil {
				waitErr = fmt.Errorf("memnode %d: %w\n%s", id, waitErr, stderr.String())
			}
			node.err = waitErr
			close(node.done)
		}()
		go func() {
			// A parent crash closes stdin too. A stuck child is forcibly reaped.
			select {
			case <-ctx.Done():
				_ = input.Close()
			case <-node.done:
				return
			}
			select {
			case <-node.done:
			case <-time.After(shutdownTimeout):
				_ = command.Process.Kill()
			}
		}()
	}
	for i, node := range c.nodes {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case readyErr := <-node.ready:
			if readyErr != nil {
				return nil, fmt.Errorf("memnode %d startup: %w", i+1, readyErr)
			}
		}
	}
	c.Client = sinfoniaclient.NewClient(config)
	return c, nil
}

func (c *Cluster) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		if c.Client != nil {
			c.err = errors.Join(c.err, c.Client.Close())
		}
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		for _, node := range c.nodes {
			_ = node.input.Close()
		}
		for _, node := range c.nodes {
			select {
			case <-node.done:
				c.err = errors.Join(c.err, node.err)
			case <-ctx.Done():
				_ = node.command.Process.Kill()
				<-node.done
				c.err = errors.Join(c.err, ctx.Err(), node.err)
			}
		}
	})
	return c.err
}
