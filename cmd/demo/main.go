// Command demo runs transactions, an index, and SQL on two local nodes.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jbancroft/concertum2/internal/client"
	"github.com/jbancroft/concertum2/internal/memnode"
	minuetindex "github.com/jbancroft/concertum2/minuet-index"
	minuetsql "github.com/jbancroft/concertum2/minuet-sql"
	"github.com/jbancroft/concertum2/pkg/concertum"
	sinfoniaclient "github.com/jbancroft/concertum2/sinfonia-client"
)

const balanceAddr = 768 // Separate from the SQL catalog and index pages.

func main() {
	dataRoot := flag.String("data-dir", "tmp/demo", "parent directory for a fresh demo run")
	flag.Parse()
	if err := os.MkdirAll(*dataRoot, 0o755); err != nil {
		fail(err)
	}
	dir, err := os.MkdirTemp(*dataRoot, "run-")
	if err != nil {
		fail(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx, dir, os.Stdout); err != nil {
		fail(err)
	}
	fmt.Printf("Demo data retained in %s\n", dir)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func run(ctx context.Context, dir string, out io.Writer) (err error) {
	c, err := openCluster(ctx, dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, c.close()) }()
	fmt.Fprintln(out, "Two durable participants listening on loopback TCP")

	if err := commit(ctx, c.api, c.api.NewMiniTransaction().
		Write(1, balanceAddr, number(100)).Write(2, balanceAddr, number(100))); err != nil {
		return err
	}
	if err := commit(ctx, c.api, c.api.NewMiniTransaction().
		Compare(1, balanceAddr, number(100)).Compare(2, balanceAddr, number(100)).
		Write(1, balanceAddr, number(75)).Write(2, balanceAddr, number(125))); err != nil {
		return err
	}
	// The balance is now 75, so comparing it with 100 must reject both writes.
	ok, _, status := c.api.NewMiniTransaction().Compare(1, balanceAddr, number(100)).
		Write(1, balanceAddr, number(0)).Write(2, balanceAddr, number(0)).ExecuteAndCommit(ctx)
	if ok || status != concertum.StatusAborted {
		return fmt.Errorf("stale compare: committed=%t status=%d", ok, status)
	}
	if err := c.api.FlushPendingCommits(ctx); err != nil {
		return err
	}
	if err := checkBalances(ctx, c.api); err != nil {
		return err
	}
	fmt.Fprintln(out, "Sinfonia: transfer committed; stale compare aborted; balances = 75, 125")

	index := newIndex(c.api)
	for key := uint64(1); key <= 40; key++ {
		if err := index.Insert(ctx, key, key*10); err != nil {
			return err
		}
	}
	for key := uint64(1); key <= 32; key++ {
		if err := index.Delete(ctx, key); err != nil {
			return err
		}
	}
	if err := checkIndex(ctx, index); err != nil {
		return err
	}
	fmt.Fprintln(out, "Minuet: inserted 40 keys; deleted 32; ordered range = 33..40")

	sql, err := newSQL(c.api)
	if err != nil {
		return err
	}
	for _, statement := range []string{
		"CREATE TABLE scores", "INSERT INTO scores VALUES (1, 100)",
		"INSERT INTO scores VALUES (2, 250)",
	} {
		if _, err := sql.Exec(ctx, statement); err != nil {
			return err
		}
	}
	if err := checkSQL(ctx, sql); err != nil {
		return err
	}
	fmt.Fprintln(out, "SQL: SELECT id, value FROM scores -> [{1 100} {2 250}]")

	if err := c.close(); err != nil {
		return err
	}
	c, err = openCluster(ctx, dir)
	if err != nil {
		return err
	}
	if err := checkBalances(ctx, c.api); err != nil {
		return err
	}
	if err := checkIndex(ctx, newIndex(c.api)); err != nil {
		return err
	}
	sql, err = newSQL(c.api)
	if err != nil {
		return err
	}
	if err := checkSQL(ctx, sql); err != nil {
		return err
	}
	fmt.Fprintln(out, "Recovery: reopened both nodes; balances, index, and SQL catalog/data verified")
	return nil
}

func number(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

func commit(ctx context.Context, api *concertum.Client, tx *concertum.MiniTransaction) error {
	ok, _, status := tx.ExecuteAndCommit(ctx)
	if !ok {
		return fmt.Errorf("transaction did not commit: status=%d", status)
	}
	// Wait for the background commit messages before checking the saved values.
	return api.FlushPendingCommits(ctx)
}

func checkBalances(ctx context.Context, api *concertum.Client) error {
	for i, want := range []uint64{75, 125} {
		data, err := api.Read(ctx, uint32(i+1), balanceAddr, 8)
		if err != nil {
			return err
		}
		if len(data) != 8 || binary.BigEndian.Uint64(data) != want {
			return fmt.Errorf("balance on participant %d: got %x, want %d", i+1, data, want)
		}
	}
	return nil
}

func newIndex(api *concertum.Client) *minuetindex.BTree {
	return minuetindex.NewBTree(sinfoniaclient.NewFromConcertumClient(api),
		minuetindex.Config{HostId: 1, BaseAddress: 64 * 1024})
}

func checkIndex(ctx context.Context, index *minuetindex.BTree) error {
	it, err := index.RangeScan(ctx, 0, 100)
	if err != nil {
		return err
	}
	for key := uint64(33); key <= 40; key++ {
		entry, ok := it.Next()
		if !ok || entry.Key != key || entry.Value != key*10 {
			return fmt.Errorf("range entry: got %+v present=%t, want key %d", entry, ok, key)
		}
	}
	if _, ok := it.Next(); ok {
		return errors.New("unexpected extra range entry")
	}
	value, found, err := index.Search(ctx, 40)
	if err != nil || !found || value != 400 {
		return fmt.Errorf("search: value=%d found=%t err=%v", value, found, err)
	}
	return nil
}

func newSQL(api *concertum.Client) (*minuetsql.Engine, error) {
	sdk := sinfoniaclient.NewFromConcertumClient(api)
	return minuetsql.NewEngine(minuetsql.Config{HostId: 2, TxnClient: sdk,
		BTreeFactory: func(id uint32, base uint64) *minuetindex.BTree {
			return minuetindex.NewBTree(sdk, minuetindex.Config{HostId: id, BaseAddress: base})
		}})
}

func checkSQL(ctx context.Context, engine *minuetsql.Engine) error {
	result, err := engine.Exec(ctx, "SELECT id, value FROM scores")
	if err != nil {
		return err
	}
	if len(result.Rows) != 2 || result.Rows[0] != (minuetsql.Row{ID: 1, Value: 100}) ||
		result.Rows[1] != (minuetsql.Row{ID: 2, Value: 250}) {
		return fmt.Errorf("unexpected SQL rows: %+v", result.Rows)
	}
	return nil
}

type cluster struct {
	nodes     []*memnode.Participant
	listeners []*closingListener
	servers   []chan error
	api       *concertum.Client
}

func openCluster(ctx context.Context, dir string) (_ *cluster, err error) {
	c := &cluster{}
	defer func() {
		if err != nil {
			err = errors.Join(err, c.close())
		}
	}()
	participants := map[uint32]client.Participant{}
	for id := uint32(1); id <= 2; id++ {
		node, err := memnode.NewDurableParticipant(id, 2<<20, filepath.Join(dir, fmt.Sprint(id)))
		if err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, node)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		tracked := &closingListener{Listener: listener}
		c.listeners = append(c.listeners, tracked)
		finished := make(chan error, 1)
		c.servers = append(c.servers, finished)
		go func() { finished <- node.RPCServer().Serve(tracked) }()
		participants[id] = client.NewRPCParticipant(listener.Addr().String(), nil, 5*time.Second)
	}
	// Recovery peers call each other directly; client requests go over local TCP.
	c.nodes[0].SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{2: c.nodes[1]})
	c.nodes[1].SetRecoveryPeers(map[uint32]memnode.RecoveryPeer{1: c.nodes[0]})
	for _, node := range c.nodes {
		if err := node.Recover(ctx); err != nil {
			return nil, err
		}
	}
	c.api = concertum.NewClient(client.NewCoordinator(participants))
	return c, nil
}

func (c *cluster) close() error {
	if c == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result error
	if c.api != nil {
		result = errors.Join(result, c.api.FlushPendingCommits(ctx))
		c.api = nil
	}
	for _, node := range c.nodes {
		node.FlushAppliedNotifications(ctx)
	}
	for _, listener := range c.listeners {
		result = errors.Join(result, listener.Close())
	}
	for _, finished := range c.servers {
		result = errors.Join(result, <-finished)
	}
	for _, node := range c.nodes {
		result = errors.Join(result, node.Close())
	}
	c.nodes, c.listeners, c.servers = nil, nil, nil
	return result
}

// closingListener closes accepted connections as well as the listening socket,
// so restarting the demo never leaves pooled connections attached to old nodes.
type closingListener struct {
	net.Listener
	mu     sync.Mutex
	closed bool
	conns  []net.Conn
}

func (l *closingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	l.conns = append(l.conns, conn)
	return conn, nil
}

func (l *closingListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	err := l.Listener.Close()
	for _, conn := range l.conns {
		_ = conn.Close()
	}
	return err
}
