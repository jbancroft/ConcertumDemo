package sinfoniaclient

import (
	"context"
	"testing"

	"github.com/jbancroft/ConcertumDemo/pkg/concertum"
)

type domainTestBackend struct{ marker byte }

func (*domainTestBackend) Read(context.Context, uint32, uint64, uint32) ([]byte, error) {
	return nil, nil
}

func (*domainTestBackend) Write(context.Context, uint32, uint64, []byte) error { return nil }

func (*domainTestBackend) GetStats(uint32) (concertum.Stats, error) { return concertum.Stats{}, nil }

func (*domainTestBackend) ExecuteAndCommit(context.Context, string, []concertum.CompareItem, []concertum.ReadItem, []concertum.WriteItem) concertum.ExecResult {
	return concertum.ExecResult{}
}

func TestSameTransactionDomainRecognizesSharedBackend(t *testing.T) {
	backend := &domainTestBackend{}
	first := NewFromBackend(backend)
	second := NewFromBackend(backend)
	if !SameTransactionDomain(first, second) {
		t.Fatal("separate clients for one backend did not share a transaction domain")
	}
	if SameTransactionDomain(first, NewFromBackend(&domainTestBackend{marker: 1})) {
		t.Fatal("clients for distinct backends shared a transaction domain")
	}
}

func TestSameTransactionDomainRejectsTypedNilClients(t *testing.T) {
	var first *Client
	var second *Client
	if SameTransactionDomain(first, second) {
		t.Fatal("typed-nil clients shared a transaction domain")
	}
	var backend *domainTestBackend
	if id := transactionDomainID(backend); id != "" {
		t.Fatalf("typed-nil backend domain ID = %q, want empty", id)
	}
	if SameTransactionDomain(NewFromBackend(backend), NewFromBackend(backend)) {
		t.Fatal("clients wrapping typed-nil backends shared a transaction domain")
	}
}
