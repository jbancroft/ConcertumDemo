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
	superMagic      = uint32(0x4d4e5432) // MNT2
	pageMagic       = uint32(0x50414745) // PAGE
	maxLeafKeys     = 8
	maxInternalKeys = 8
)

var (
	ErrNotFound = errors.New("minuet-index: key not found")
	ErrBusy     = errors.New("minuet-index: retry budget exhausted")
)

type Config struct {
	MemID        uint32
	BaseAddr     uint64
	MaxRetries   int
	RetryBackoff time.Duration
}

type Entry struct {
	Key   uint64
	Value uint64
}

type Index struct {
	cfg    Config
	client sinfoniaclient.TxnClient
}

type superblock struct {
	Version  uint64
	RootPage uint32
	NextPage uint32
}

type node struct {
	ID       uint32
	Leaf     bool
	Keys     []uint64
	Values   []uint64
	Children []uint32
	NextLeaf uint32
}

type snapshot struct {
	super superblock
	nodes map[uint32]*node
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

func New(client sinfoniaclient.TxnClient, cfg Config) *Index {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 8
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = time.Millisecond
	}
	return &Index{cfg: cfg, client: client}
}

func (idx *Index) Search(ctx context.Context, key uint64) (uint64, bool, error) {
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		return 0, false, err
	}
	if s.super.RootPage == 0 {
		return 0, false, nil
	}
	n := s.nodes[s.super.RootPage]
	for !n.Leaf {
		i := sort.Search(len(n.Keys), func(i int) bool { return key < n.Keys[i] })
		n = s.nodes[n.Children[i]]
	}
	at := sort.Search(len(n.Keys), func(i int) bool { return n.Keys[i] >= key })
	if at < len(n.Keys) && n.Keys[at] == key {
		return n.Values[at], true, nil
	}
	return 0, false, nil
}

func (idx *Index) Insert(ctx context.Context, key, value uint64) error {
	return idx.mutate(ctx, "insert", func(s *snapshot) error {
		if s.super.RootPage == 0 {
			root := idx.allocNode(s, true)
			root.Keys = []uint64{key}
			root.Values = []uint64{value}
			s.super.RootPage = root.ID
			return nil
		}
		splitKey, splitChild, split, err := idx.insertRec(s, s.super.RootPage, key, value)
		if err != nil {
			return err
		}
		if split {
			newRoot := idx.allocNode(s, false)
			newRoot.Keys = []uint64{splitKey}
			newRoot.Children = []uint32{s.super.RootPage, splitChild}
			s.super.RootPage = newRoot.ID
		}
		return nil
	})
}

func (idx *Index) Delete(ctx context.Context, key uint64) error {
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
			if !root.Leaf && len(root.Keys) == 0 {
				s.super.RootPage = root.Children[0]
			}
			if root.Leaf && len(root.Keys) == 0 {
				s.super.RootPage = 0
			}
		}
		return nil
	})
}

func (idx *Index) RangeScan(ctx context.Context, start, end uint64) (*Iterator, error) {
	s, err := idx.loadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0)
	if s.super.RootPage == 0 {
		return &Iterator{entries: out}, nil
	}
	n := s.nodes[s.super.RootPage]
	for !n.Leaf {
		i := sort.Search(len(n.Keys), func(i int) bool { return start < n.Keys[i] })
		n = s.nodes[n.Children[i]]
	}
	for {
		for i := range n.Keys {
			if n.Keys[i] < start {
				continue
			}
			if n.Keys[i] > end {
				return &Iterator{entries: out}, nil
			}
			out = append(out, Entry{Key: n.Keys[i], Value: n.Values[i]})
		}
		if n.NextLeaf == 0 {
			break
		}
		n = s.nodes[n.NextLeaf]
	}
	return &Iterator{entries: out}, nil
}

func (idx *Index) mutate(ctx context.Context, class string, fn func(*snapshot) error) error {
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

func (idx *Index) loadSnapshot(ctx context.Context) (*snapshot, error) {
	rawSB, err := idx.client.Read(ctx, idx.cfg.MemID, idx.cfg.BaseAddr, superblockSize)
	if err != nil {
		return nil, err
	}
	if len(rawSB) < superblockSize {
		return nil, errors.New("minuet-index: truncated superblock")
	}
	s := &snapshot{nodes: map[uint32]*node{}, raw: map[uint32][]byte{}, rawSB: rawSB}
	if binary.BigEndian.Uint32(rawSB[0:4]) != superMagic {
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

func (idx *Index) readNodeRecursive(ctx context.Context, s *snapshot, pageID uint32) error {
	if _, ok := s.nodes[pageID]; ok {
		return nil
	}
	addr := idx.pageAddr(pageID)
	raw, err := idx.client.Read(ctx, idx.cfg.MemID, addr, pageSize)
	if err != nil {
		return err
	}
	n, err := decodeNode(raw)
	if err != nil {
		return err
	}
	s.nodes[pageID] = n
	s.raw[pageID] = raw
	if !n.Leaf {
		for _, child := range n.Children {
			if err := idx.readNodeRecursive(ctx, s, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *Index) commitSnapshot(ctx context.Context, class string, s *snapshot) bool {
	cmp := []sinfoniaclient.CmpItem{{MemID: idx.cfg.MemID, Addr: idx.cfg.BaseAddr, Data: s.rawSB}}
	writes := []sinfoniaclient.WriteItem{{MemID: idx.cfg.MemID, Addr: idx.cfg.BaseAddr, Data: encodeSuperblock(s.super)}}
	for id, n := range s.nodes {
		addr := idx.pageAddr(id)
		old := s.raw[id]
		if old == nil {
			old = make([]byte, pageSize)
		}
		cmp = append(cmp, sinfoniaclient.CmpItem{MemID: idx.cfg.MemID, Addr: addr, Data: old})
		writes = append(writes, sinfoniaclient.WriteItem{MemID: idx.cfg.MemID, Addr: addr, Data: encodeNode(n)})
	}
	res := idx.client.ExecAndCommit(ctx, "minuet-index:"+class, cmp, nil, writes)
	return res.Committed
}

func (idx *Index) allocNode(s *snapshot, leaf bool) *node {
	id := s.super.NextPage
	s.super.NextPage++
	n := &node{ID: id, Leaf: leaf}
	s.nodes[id] = n
	return n
}

func (idx *Index) insertRec(s *snapshot, pageID uint32, key, value uint64) (uint64, uint32, bool, error) {
	n := s.nodes[pageID]
	if n.Leaf {
		pos := sort.Search(len(n.Keys), func(i int) bool { return n.Keys[i] >= key })
		if pos < len(n.Keys) && n.Keys[pos] == key {
			n.Values[pos] = value
			return 0, 0, false, nil
		}
		n.Keys = append(n.Keys, 0)
		n.Values = append(n.Values, 0)
		copy(n.Keys[pos+1:], n.Keys[pos:])
		copy(n.Values[pos+1:], n.Values[pos:])
		n.Keys[pos], n.Values[pos] = key, value
		if len(n.Keys) <= maxLeafKeys {
			return 0, 0, false, nil
		}
		right := idx.allocNode(s, true)
		mid := len(n.Keys) / 2
		right.Keys = append(right.Keys, n.Keys[mid:]...)
		right.Values = append(right.Values, n.Values[mid:]...)
		n.Keys = append([]uint64(nil), n.Keys[:mid]...)
		n.Values = append([]uint64(nil), n.Values[:mid]...)
		right.NextLeaf = n.NextLeaf
		n.NextLeaf = right.ID
		return right.Keys[0], right.ID, true, nil
	}
	childPos := sort.Search(len(n.Keys), func(i int) bool { return key < n.Keys[i] })
	splitKey, splitChild, split, err := idx.insertRec(s, n.Children[childPos], key, value)
	if err != nil || !split {
		return 0, 0, false, err
	}
	n.Keys = append(n.Keys, 0)
	copy(n.Keys[childPos+1:], n.Keys[childPos:])
	n.Keys[childPos] = splitKey
	n.Children = append(n.Children, 0)
	copy(n.Children[childPos+2:], n.Children[childPos+1:])
	n.Children[childPos+1] = splitChild
	if len(n.Keys) <= maxInternalKeys {
		return 0, 0, false, nil
	}
	right := idx.allocNode(s, false)
	mid := len(n.Keys) / 2
	promote := n.Keys[mid]
	right.Keys = append(right.Keys, n.Keys[mid+1:]...)
	right.Children = append(right.Children, n.Children[mid+1:]...)
	n.Keys = append([]uint64(nil), n.Keys[:mid]...)
	n.Children = append([]uint32(nil), n.Children[:mid+1]...)
	return promote, right.ID, true, nil
}

func (idx *Index) deleteRec(s *snapshot, pageID uint32, key uint64, isRoot bool) (bool, bool, error) {
	n := s.nodes[pageID]
	if n.Leaf {
		pos := sort.Search(len(n.Keys), func(i int) bool { return n.Keys[i] >= key })
		if pos >= len(n.Keys) || n.Keys[pos] != key {
			return false, false, nil
		}
		n.Keys = append(n.Keys[:pos], n.Keys[pos+1:]...)
		n.Values = append(n.Values[:pos], n.Values[pos+1:]...)
		if isRoot {
			return true, len(n.Keys) == 0, nil
		}
		return true, len(n.Keys) < minLeafKeys(), nil
	}
	childPos := sort.Search(len(n.Keys), func(i int) bool { return key < n.Keys[i] })
	deleted, underflow, err := idx.deleteRec(s, n.Children[childPos], key, false)
	if err != nil || !deleted {
		return deleted, false, err
	}
	if childPos > 0 {
		left := s.nodes[n.Children[childPos-1]]
		cur := s.nodes[n.Children[childPos]]
		if cur.Leaf && len(cur.Keys) > 0 {
			n.Keys[childPos-1] = cur.Keys[0]
		}
		if !cur.Leaf && len(cur.Children) > 0 {
			_ = left
		}
	}
	if underflow {
		idx.rebalanceChild(s, n, childPos)
	}
	if isRoot {
		if len(n.Keys) == 0 {
			return true, true, nil
		}
		return true, false, nil
	}
	return true, len(n.Keys) < minInternalKeys(), nil
}

func (idx *Index) rebalanceChild(s *snapshot, parent *node, childPos int) {
	child := s.nodes[parent.Children[childPos]]
	if child.Leaf {
		if childPos > 0 {
			left := s.nodes[parent.Children[childPos-1]]
			if len(left.Keys) > minLeafKeys() {
				k := left.Keys[len(left.Keys)-1]
				v := left.Values[len(left.Values)-1]
				left.Keys = left.Keys[:len(left.Keys)-1]
				left.Values = left.Values[:len(left.Values)-1]
				child.Keys = append([]uint64{k}, child.Keys...)
				child.Values = append([]uint64{v}, child.Values...)
				parent.Keys[childPos-1] = child.Keys[0]
				return
			}
		}
		if childPos+1 < len(parent.Children) {
			right := s.nodes[parent.Children[childPos+1]]
			if len(right.Keys) > minLeafKeys() {
				child.Keys = append(child.Keys, right.Keys[0])
				child.Values = append(child.Values, right.Values[0])
				right.Keys = right.Keys[1:]
				right.Values = right.Values[1:]
				parent.Keys[childPos] = right.Keys[0]
				return
			}
		}
		if childPos > 0 {
			left := s.nodes[parent.Children[childPos-1]]
			left.Keys = append(left.Keys, child.Keys...)
			left.Values = append(left.Values, child.Values...)
			left.NextLeaf = child.NextLeaf
			parent.Keys = append(parent.Keys[:childPos-1], parent.Keys[childPos:]...)
			parent.Children = append(parent.Children[:childPos], parent.Children[childPos+1:]...)
			return
		}
		if childPos+1 < len(parent.Children) {
			right := s.nodes[parent.Children[childPos+1]]
			child.Keys = append(child.Keys, right.Keys...)
			child.Values = append(child.Values, right.Values...)
			child.NextLeaf = right.NextLeaf
			parent.Keys = append(parent.Keys[:childPos], parent.Keys[childPos+1:]...)
			parent.Children = append(parent.Children[:childPos+1], parent.Children[childPos+2:]...)
		}
		return
	}
	if childPos > 0 {
		left := s.nodes[parent.Children[childPos-1]]
		if len(left.Keys) > minInternalKeys() {
			sep := parent.Keys[childPos-1]
			movedKey := left.Keys[len(left.Keys)-1]
			movedChild := left.Children[len(left.Children)-1]
			left.Keys = left.Keys[:len(left.Keys)-1]
			left.Children = left.Children[:len(left.Children)-1]
			child.Keys = append([]uint64{sep}, child.Keys...)
			child.Children = append([]uint32{movedChild}, child.Children...)
			parent.Keys[childPos-1] = movedKey
			return
		}
	}
	if childPos+1 < len(parent.Children) {
		right := s.nodes[parent.Children[childPos+1]]
		if len(right.Keys) > minInternalKeys() {
			sep := parent.Keys[childPos]
			child.Keys = append(child.Keys, sep)
			child.Children = append(child.Children, right.Children[0])
			parent.Keys[childPos] = right.Keys[0]
			right.Keys = right.Keys[1:]
			right.Children = right.Children[1:]
			return
		}
	}
	if childPos > 0 {
		left := s.nodes[parent.Children[childPos-1]]
		sep := parent.Keys[childPos-1]
		left.Keys = append(left.Keys, sep)
		left.Keys = append(left.Keys, child.Keys...)
		left.Children = append(left.Children, child.Children...)
		parent.Keys = append(parent.Keys[:childPos-1], parent.Keys[childPos:]...)
		parent.Children = append(parent.Children[:childPos], parent.Children[childPos+1:]...)
		return
	}
	if childPos+1 < len(parent.Children) {
		right := s.nodes[parent.Children[childPos+1]]
		sep := parent.Keys[childPos]
		child.Keys = append(child.Keys, sep)
		child.Keys = append(child.Keys, right.Keys...)
		child.Children = append(child.Children, right.Children...)
		parent.Keys = append(parent.Keys[:childPos], parent.Keys[childPos+1:]...)
		parent.Children = append(parent.Children[:childPos+1], parent.Children[childPos+2:]...)
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

func encodeNode(n *node) []byte {
	buf := make([]byte, pageSize)
	binary.BigEndian.PutUint32(buf[0:4], pageMagic)
	binary.BigEndian.PutUint32(buf[4:8], n.ID)
	if n.Leaf {
		buf[8] = 1
	}
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(n.Keys)))
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(n.Children)))
	binary.BigEndian.PutUint32(buf[16:20], n.NextLeaf)
	off := 24
	for _, k := range n.Keys {
		binary.BigEndian.PutUint64(buf[off:off+8], k)
		off += 8
	}
	if n.Leaf {
		for _, v := range n.Values {
			binary.BigEndian.PutUint64(buf[off:off+8], v)
			off += 8
		}
	} else {
		for _, c := range n.Children {
			binary.BigEndian.PutUint32(buf[off:off+4], c)
			off += 4
		}
	}
	return buf
}

func decodeNode(buf []byte) (*node, error) {
	if len(buf) < 24 {
		return nil, errors.New("minuet-index: truncated page header")
	}
	if binary.BigEndian.Uint32(buf[0:4]) != pageMagic {
		return nil, errors.New("minuet-index: invalid page")
	}
	n := &node{ID: binary.BigEndian.Uint32(buf[4:8]), Leaf: buf[8] == 1, NextLeaf: binary.BigEndian.Uint32(buf[16:20])}
	keyCount := int(binary.BigEndian.Uint16(buf[10:12]))
	childCount := int(binary.BigEndian.Uint16(buf[12:14]))
	off := 24
	itemBytes := keyCount * 8
	if n.Leaf {
		itemBytes *= 2
	} else {
		itemBytes += childCount * 4
	}
	if itemBytes > len(buf)-off {
		return nil, errors.New("minuet-index: truncated page payload")
	}
	n.Keys = make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		n.Keys[i] = binary.BigEndian.Uint64(buf[off : off+8])
		off += 8
	}
	if n.Leaf {
		n.Values = make([]uint64, keyCount)
		for i := 0; i < keyCount; i++ {
			n.Values[i] = binary.BigEndian.Uint64(buf[off : off+8])
			off += 8
		}
	} else {
		n.Children = make([]uint32, childCount)
		for i := 0; i < childCount; i++ {
			n.Children[i] = binary.BigEndian.Uint32(buf[off : off+4])
			off += 4
		}
	}
	return n, nil
}

func (idx *Index) pageAddr(pageID uint32) uint64 {
	return idx.cfg.BaseAddr + superblockSize + uint64(pageID-1)*pageSize
}
