package minuetindex

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"time"

	sinfoniaclient "github.com/jbancroft/concertum2/sinfonia-client"
)

const (
	superblockSize  = 64
	pageSize        = 512
	superMagic      = uint32(0x4d4e5432) // ASCII "MNT2": marks the tree metadata.
	pageMagic       = uint32(0x50414745) // ASCII "PAGE": marks a node's page.
	maxLeafKeys     = 8
	maxInternalKeys = 8
)

var (
	ErrNotFound = errors.New("minuet-index: key not found")
	ErrBusy     = errors.New("minuet-index: retry budget exhausted")
)

type Config struct {
	HostId       uint32
	BaseAddress  uint64
	MaxRetries   int
	RetryBackoff time.Duration
}

type Entry struct {
	Key   uint64
	Value uint64
}

// BTree stores one tree at a fixed address on one memory node.
type BTree struct {
	cfg    Config
	client sinfoniaclient.TxnClient
}

// superblock is the tree's starting point: which page is the root and where to
// allocate next. Version is an update counter, not a storage-format version.
type superblock struct {
	Version  uint64
	RootPage uint32
	NextPage uint32
}

// BTreeNode holds either leaf key/value pairs or internal keys and child IDs.
type BTreeNode struct {
	ID       uint32
	isLeaf   bool
	keys     []uint64
	values   []uint64
	children []uint32
	NextLeaf uint32
}

// snapshot is a working copy. raw and rawSB keep the original bytes for the
// commit comparisons; the reads themselves aren't a consistent snapshot.
type snapshot struct {
	super superblock
	nodes map[uint32]*BTreeNode
	raw   map[uint32][]byte
	rawSB []byte
}

type Iterator struct {
	entries []Entry
	idx     int
}

func (it *Iterator) Next() (Entry, bool) {
	if it.idx >= len(it.entries) {
		return Entry{}, false
	}
	e := it.entries[it.idx]
	it.idx++
	return e, true
}

func NewBTree(client sinfoniaclient.TxnClient, cfg Config) *BTree {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 8
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Millisecond
	}
	return &BTree{cfg: cfg, client: client}
}

func (idx *BTree) Search(ctx context.Context, key uint64) (uint64, bool, error) {
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		return 0, false, err
	}
	if s.super.RootPage == 0 {
		return 0, false, nil
	}
	n := s.nodes[s.super.RootPage]
	for !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
		n = s.nodes[n.children[i]]
	}
	at := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
	if at < len(n.keys) && n.keys[at] == key {
		return n.values[at], true, nil
	}
	return 0, false, nil
}

func (idx *BTree) Insert(ctx context.Context, key, value uint64) error {
	return idx.mutate(ctx, "insert", func(s *snapshot) error {
		if s.super.RootPage == 0 {
			root := idx.allocNode(s, true)
			root.keys = []uint64{key}
			root.values = []uint64{value}
			s.super.RootPage = root.ID
			return nil
		}
		splitKey, splitChild, split, err := idx.insertRec(s, s.super.RootPage, key, value)
		if err != nil {
			return err
		}
		if split {
			newRoot := idx.allocNode(s, false)
			newRoot.keys = []uint64{splitKey}
			newRoot.children = []uint32{s.super.RootPage, splitChild}
			s.super.RootPage = newRoot.ID
		}
		return nil
	})
}

func (idx *BTree) Delete(ctx context.Context, key uint64) error {
	return idx.mutate(ctx, "delete", func(s *snapshot) error {
		if s.super.RootPage == 0 {
			return nil
		}
		_, underflow, err := idx.deleteRec(s, s.super.RootPage, key, true)
		if err != nil {
			return err
		}
		if underflow {
			root := s.nodes[s.super.RootPage]
			if !root.isLeaf && len(root.keys) == 0 {
				s.super.RootPage = root.children[0]
			}
			if root.isLeaf && len(root.keys) == 0 {
				s.super.RootPage = 0
			}
		}
		return nil
	})
}

func (idx *BTree) RangeScan(ctx context.Context, start, end uint64) (*Iterator, error) {
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0)
	if s.super.RootPage == 0 {
		return &Iterator{entries: out}, nil
	}
	n := s.nodes[s.super.RootPage]
	for !n.isLeaf {
		i := sort.Search(len(n.keys), func(i int) bool { return start < n.keys[i] })
		n = s.nodes[n.children[i]]
	}
	for {
		for i := range n.keys {
			if n.keys[i] < start {
				continue
			}
			if n.keys[i] > end {
				return &Iterator{entries: out}, nil
			}
			out = append(out, Entry{Key: n.keys[i], Value: n.values[i]})
		}
		if n.NextLeaf == 0 {
			break
		}
		n = s.nodes[n.NextLeaf]
	}
	return &Iterator{entries: out}, nil
}

// mutate reloads the tree and retries if it can't commit the changed copy.
func (idx *BTree) mutate(ctx context.Context, class string, fn func(*snapshot) error) error {
	for attempt := 0; attempt < idx.cfg.MaxRetries; attempt++ {
		s, err := idx.loadSnapshot(ctx)
		if err != nil {
			return err
		}
		if err := fn(s); err != nil {
			return err
		}
		if idx.commitSnapshot(ctx, class, s) {
			return nil
		}
		timer := time.NewTimer(idx.cfg.RetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrBusy
}

func (idx *BTree) loadSnapshot(ctx context.Context) (*snapshot, error) {
	rawSB, err := idx.client.Read(ctx, idx.cfg.HostId, idx.cfg.BaseAddress, superblockSize)
	if err != nil {
		return nil, err
	}
	if len(rawSB) < superblockSize {
		return nil, errors.New("minuet-index: truncated superblock")
	}
	s := &snapshot{nodes: map[uint32]*BTreeNode{}, raw: map[uint32][]byte{}, rawSB: rawSB}
	if binary.BigEndian.Uint32(rawSB[0:4]) != superMagic {
		// This also treats damaged metadata as a new tree. A later write can
		// overwrite old data; a safer version would separate create from open.
		s.super = superblock{Version: 0, RootPage: 0, NextPage: 1}
		return s, nil
	}
	s.super = decodeSuperblock(rawSB)
	if s.super.RootPage == 0 {
		return s, nil
	}
	if err := idx.readNodeRecursive(ctx, s, s.super.RootPage); err != nil {
		return nil, err
	}
	return s, nil
}

func (idx *BTree) readNodeRecursive(ctx context.Context, s *snapshot, pageID uint32) error {
	if _, ok := s.nodes[pageID]; ok {
		return nil
	}
	addr := idx.pageAddr(pageID)
	raw, err := idx.client.Read(ctx, idx.cfg.HostId, addr, pageSize)
	if err != nil {
		return err
	}
	n, err := decodeNode(raw)
	if err != nil {
		return err
	}
	s.nodes[pageID] = n
	s.raw[pageID] = raw
	if !n.isLeaf {
		for _, child := range n.children {
			if err := idx.readNodeRecursive(ctx, s, child); err != nil {
				return err
			}
		}
	}
	return nil
}

// commitSnapshot checks the original bytes and writes the changed pages together.
func (idx *BTree) commitSnapshot(ctx context.Context, class string, s *snapshot) bool {
	compares := []sinfoniaclient.CompareItem{{HostId: idx.cfg.HostId, Address: idx.cfg.BaseAddress, Data: s.rawSB}}
	writes := []sinfoniaclient.WriteItem{{HostId: idx.cfg.HostId, Address: idx.cfg.BaseAddress, Data: encodeSuperblock(s.super)}}
	for id, n := range s.nodes {
		addr := idx.pageAddr(id)
		old := s.raw[id]
		if old == nil {
			old = make([]byte, pageSize)
		}
		compares = append(compares, sinfoniaclient.CompareItem{HostId: idx.cfg.HostId, Address: addr, Data: old})
		writes = append(writes, sinfoniaclient.WriteItem{HostId: idx.cfg.HostId, Address: addr, Data: encodeNode(n)})
	}
	res := idx.client.ExecuteAndCommit(ctx, "minuet-index:"+class, compares, nil, writes)
	return res.Committed
}

func (idx *BTree) allocNode(s *snapshot, leaf bool) *BTreeNode {
	id := s.super.NextPage
	s.super.NextPage++
	n := &BTreeNode{ID: id, isLeaf: leaf}
	s.nodes[id] = n
	return n
}

func (idx *BTree) insertRec(s *snapshot, pageID uint32, key, value uint64) (uint64, uint32, bool, error) {
	n := s.nodes[pageID]
	if n.isLeaf {
		pos := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
		if pos < len(n.keys) && n.keys[pos] == key {
			n.values[pos] = value
			return 0, 0, false, nil
		}
		n.keys = append(n.keys, 0)
		n.values = append(n.values, 0)
		copy(n.keys[pos+1:], n.keys[pos:])
		copy(n.values[pos+1:], n.values[pos:])
		n.keys[pos], n.values[pos] = key, value
		if len(n.keys) <= maxLeafKeys {
			return 0, 0, false, nil
		}
		right := idx.allocNode(s, true)
		mid := len(n.keys) / 2
		right.keys = append(right.keys, n.keys[mid:]...)
		right.values = append(right.values, n.values[mid:]...)
		n.keys = append([]uint64(nil), n.keys[:mid]...)
		n.values = append([]uint64(nil), n.values[:mid]...)
		right.NextLeaf = n.NextLeaf
		n.NextLeaf = right.ID
		return right.keys[0], right.ID, true, nil
	}
	childPos := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
	splitKey, splitChild, split, err := idx.insertRec(s, n.children[childPos], key, value)
	if err != nil || !split {
		return 0, 0, false, err
	}
	n.keys = append(n.keys, 0)
	copy(n.keys[childPos+1:], n.keys[childPos:])
	n.keys[childPos] = splitKey
	n.children = append(n.children, 0)
	copy(n.children[childPos+2:], n.children[childPos+1:])
	n.children[childPos+1] = splitChild
	if len(n.keys) <= maxInternalKeys {
		return 0, 0, false, nil
	}
	right := idx.allocNode(s, false)
	mid := len(n.keys) / 2
	promote := n.keys[mid]
	right.keys = append(right.keys, n.keys[mid+1:]...)
	right.children = append(right.children, n.children[mid+1:]...)
	n.keys = append([]uint64(nil), n.keys[:mid]...)
	n.children = append([]uint32(nil), n.children[:mid+1]...)
	return promote, right.ID, true, nil
}

func (idx *BTree) deleteRec(s *snapshot, pageID uint32, key uint64, isRoot bool) (bool, bool, error) {
	n := s.nodes[pageID]
	if n.isLeaf {
		pos := sort.Search(len(n.keys), func(i int) bool { return n.keys[i] >= key })
		if pos >= len(n.keys) || n.keys[pos] != key {
			return false, false, nil
		}
		n.keys = append(n.keys[:pos], n.keys[pos+1:]...)
		n.values = append(n.values[:pos], n.values[pos+1:]...)
		if isRoot {
			return true, len(n.keys) == 0, nil
		}
		return true, len(n.keys) < minLeafKeys(), nil
	}
	childPos := sort.Search(len(n.keys), func(i int) bool { return key < n.keys[i] })
	deleted, underflow, err := idx.deleteRec(s, n.children[childPos], key, false)
	if err != nil || !deleted {
		return deleted, false, err
	}
	if childPos > 0 {
		left := s.nodes[n.children[childPos-1]]
		cur := s.nodes[n.children[childPos]]
		if cur.isLeaf && len(cur.keys) > 0 {
			n.keys[childPos-1] = cur.keys[0]
		}
		if !cur.isLeaf && len(cur.children) > 0 {
			_ = left
		}
	}
	if underflow {
		idx.rebalanceChild(s, n, childPos)
	}
	if isRoot {
		if len(n.keys) == 0 {
			return true, true, nil
		}
		return true, false, nil
	}
	return true, len(n.keys) < minInternalKeys(), nil
}

func (idx *BTree) rebalanceChild(s *snapshot, parent *BTreeNode, childPos int) {
	child := s.nodes[parent.children[childPos]]
	if child.isLeaf {
		if childPos > 0 {
			left := s.nodes[parent.children[childPos-1]]
			if len(left.keys) > minLeafKeys() {
				k := left.keys[len(left.keys)-1]
				v := left.values[len(left.values)-1]
				left.keys = left.keys[:len(left.keys)-1]
				left.values = left.values[:len(left.values)-1]
				child.keys = append([]uint64{k}, child.keys...)
				child.values = append([]uint64{v}, child.values...)
				parent.keys[childPos-1] = child.keys[0]
				return
			}
		}
		if childPos+1 < len(parent.children) {
			right := s.nodes[parent.children[childPos+1]]
			if len(right.keys) > minLeafKeys() {
				child.keys = append(child.keys, right.keys[0])
				child.values = append(child.values, right.values[0])
				right.keys = right.keys[1:]
				right.values = right.values[1:]
				parent.keys[childPos] = right.keys[0]
				return
			}
		}
		if childPos > 0 {
			left := s.nodes[parent.children[childPos-1]]
			left.keys = append(left.keys, child.keys...)
			left.values = append(left.values, child.values...)
			left.NextLeaf = child.NextLeaf
			parent.keys = append(parent.keys[:childPos-1], parent.keys[childPos:]...)
			parent.children = append(parent.children[:childPos], parent.children[childPos+1:]...)
			return
		}
		if childPos+1 < len(parent.children) {
			right := s.nodes[parent.children[childPos+1]]
			child.keys = append(child.keys, right.keys...)
			child.values = append(child.values, right.values...)
			child.NextLeaf = right.NextLeaf
			parent.keys = append(parent.keys[:childPos], parent.keys[childPos+1:]...)
			parent.children = append(parent.children[:childPos+1], parent.children[childPos+2:]...)
		}
		return
	}
	if childPos > 0 {
		left := s.nodes[parent.children[childPos-1]]
		if len(left.keys) > minInternalKeys() {
			sep := parent.keys[childPos-1]
			movedKey := left.keys[len(left.keys)-1]
			movedChild := left.children[len(left.children)-1]
			left.keys = left.keys[:len(left.keys)-1]
			left.children = left.children[:len(left.children)-1]
			child.keys = append([]uint64{sep}, child.keys...)
			child.children = append([]uint32{movedChild}, child.children...)
			parent.keys[childPos-1] = movedKey
			return
		}
	}
	if childPos+1 < len(parent.children) {
		right := s.nodes[parent.children[childPos+1]]
		if len(right.keys) > minInternalKeys() {
			sep := parent.keys[childPos]
			child.keys = append(child.keys, sep)
			child.children = append(child.children, right.children[0])
			parent.keys[childPos] = right.keys[0]
			right.keys = right.keys[1:]
			right.children = right.children[1:]
			return
		}
	}
	if childPos > 0 {
		left := s.nodes[parent.children[childPos-1]]
		sep := parent.keys[childPos-1]
		left.keys = append(left.keys, sep)
		left.keys = append(left.keys, child.keys...)
		left.children = append(left.children, child.children...)
		parent.keys = append(parent.keys[:childPos-1], parent.keys[childPos:]...)
		parent.children = append(parent.children[:childPos], parent.children[childPos+1:]...)
		return
	}
	if childPos+1 < len(parent.children) {
		right := s.nodes[parent.children[childPos+1]]
		sep := parent.keys[childPos]
		child.keys = append(child.keys, sep)
		child.keys = append(child.keys, right.keys...)
		child.children = append(child.children, right.children...)
		parent.keys = append(parent.keys[:childPos], parent.keys[childPos+1:]...)
		parent.children = append(parent.children[:childPos+1], parent.children[childPos+2:]...)
	}
}

func minLeafKeys() int     { return (maxLeafKeys + 1) / 2 }
func minInternalKeys() int { return (maxInternalKeys+1)/2 - 1 }

func encodeSuperblock(sb superblock) []byte {
	buf := make([]byte, superblockSize)
	binary.BigEndian.PutUint32(buf[0:4], superMagic)
	binary.BigEndian.PutUint64(buf[8:16], sb.Version+1)
	binary.BigEndian.PutUint32(buf[16:20], sb.RootPage)
	binary.BigEndian.PutUint32(buf[20:24], sb.NextPage)
	return buf
}

func decodeSuperblock(buf []byte) superblock {
	return superblock{
		Version:  binary.BigEndian.Uint64(buf[8:16]),
		RootPage: binary.BigEndian.Uint32(buf[16:20]),
		NextPage: binary.BigEndian.Uint32(buf[20:24]),
	}
}

func encodeNode(n *BTreeNode) []byte {
	buf := make([]byte, pageSize)
	binary.BigEndian.PutUint32(buf[0:4], pageMagic)
	binary.BigEndian.PutUint32(buf[4:8], n.ID)
	if n.isLeaf {
		buf[8] = 1
	}
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(n.keys)))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(n.children)))
	binary.BigEndian.PutUint32(buf[16:20], n.NextLeaf)
	off := 24
	for _, k := range n.keys {
		binary.BigEndian.PutUint64(buf[off:off+8], k)
		off += 8
	}
	if n.isLeaf {
		for _, v := range n.values {
			binary.BigEndian.PutUint64(buf[off:off+8], v)
			off += 8
		}
	} else {
		for _, c := range n.children {
			binary.BigEndian.PutUint32(buf[off:off+4], c)
			off += 4
		}
	}
	return buf
}

func decodeNode(buf []byte) (*BTreeNode, error) {
	if len(buf) < 24 {
		return nil, errors.New("minuet-index: truncated page header")
	}
	if binary.BigEndian.Uint32(buf[0:4]) != pageMagic {
		return nil, errors.New("minuet-index: invalid page")
	}
	n := &BTreeNode{ID: binary.BigEndian.Uint32(buf[4:8]), isLeaf: buf[8] == 1, NextLeaf: binary.BigEndian.Uint32(buf[16:20])}
	keyCount := int(binary.BigEndian.Uint16(buf[10:12]))
	childCount := int(binary.BigEndian.Uint16(buf[12:14]))
	off := 24
	itemBytes := keyCount * 8
	if n.isLeaf {
		itemBytes *= 2
	} else {
		itemBytes += childCount * 4
	}
	if itemBytes > len(buf)-off {
		return nil, errors.New("minuet-index: truncated page payload")
	}
	n.keys = make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		n.keys[i] = binary.BigEndian.Uint64(buf[off : off+8])
		off += 8
	}
	if n.isLeaf {
		n.values = make([]uint64, keyCount)
		for i := 0; i < keyCount; i++ {
			n.values[i] = binary.BigEndian.Uint64(buf[off : off+8])
			off += 8
		}
	} else {
		n.children = make([]uint32, childCount)
		for i := 0; i < childCount; i++ {
			n.children[i] = binary.BigEndian.Uint32(buf[off : off+4])
			off += 4
		}
	}
	return n, nil
}

func (idx *BTree) pageAddr(pageID uint32) uint64 {
	return idx.cfg.BaseAddress + superblockSize + uint64(pageID-1)*pageSize
}
