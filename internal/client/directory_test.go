package client_test

import (
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/client"
	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestDirectoryResolvePrimaryAndReplica(t *testing.T) {
	d := client.NewDirectory()
	d.Set(10, client.MemEndpoints{
		Primary: sinfonia.Endpoint{RPC: "127.0.0.1:4001"},
		Replica: &sinfonia.Endpoint{RPC: "127.0.0.1:4002"},
	})
	primary, ok := d.ResolvePrimary(10)
	if !ok || primary.RPC != "127.0.0.1:4001" {
		t.Fatalf("expected primary endpoint, got %+v found=%v", primary, ok)
	}
	replica, ok := d.ResolveReplica(10)
	if !ok || replica.RPC != "127.0.0.1:4002" {
		t.Fatalf("expected replica endpoint, got %+v found=%v", replica, ok)
	}
}
