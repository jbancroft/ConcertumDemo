package client

import (
	"sync"

	"github.com/jbancroft/concertum2/internal/proto"
)

type MemEndpoints struct {
	Primary    proto.Endpoint
	Replica    *proto.Endpoint
	Generation uint64
}

type Directory struct {
	mu      sync.RWMutex
	entries map[uint32]MemEndpoints
}

func NewDirectory() *Directory {
	return &Directory{entries: map[uint32]MemEndpoints{}}
}

func (d *Directory) Set(logicalMemID uint32, endpoints MemEndpoints) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries[logicalMemID] = endpoints
}

func (d *Directory) ResolvePrimary(logicalMemID uint32) (proto.Endpoint, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ep, ok := d.entries[logicalMemID]
	if !ok {
		return proto.Endpoint{}, false
	}
	return ep.Primary, true
}

func (d *Directory) ResolveReplica(logicalMemID uint32) (proto.Endpoint, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ep, ok := d.entries[logicalMemID]
	if !ok || ep.Replica == nil {
		return proto.Endpoint{}, false
	}
	return *ep.Replica, true
}
