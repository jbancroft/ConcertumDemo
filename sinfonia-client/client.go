package sinfoniaclient

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jbancroft/concertum2/mini_transaction"
	"github.com/jbancroft/concertum2/pkg/concertum"
)

type CompareItem = mini_transaction.CompareItem
type ReadItem = mini_transaction.ReadItem
type WriteItem = mini_transaction.WriteItem
type ReadKey = mini_transaction.ReadKey

type ExecResult struct {
	Committed      bool
	ReadResults    map[ReadKey][]byte
	UnknownOutcome bool
}

// TxnClient is the minitransaction contract consumed by higher layers.
type TxnClient interface {
	ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult
	Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error
}

// TransactionDomainProvider identifies clients that can participate in the
// same atomic minitransactions even when they are distinct wrapper instances.
type TransactionDomainProvider interface {
	TransactionDomainID() string
}

// SameTransactionDomain reports whether two clients target the same atomic
// minitransaction domain. Custom clients without a domain ID must be the same
// comparable client instance.
func SameTransactionDomain(a, b TxnClient) bool {
	if a == nil || b == nil {
		return false
	}
	if isNilReference(reflect.ValueOf(a)) || isNilReference(reflect.ValueOf(b)) {
		return false
	}
	t := reflect.TypeOf(a)
	if t == reflect.TypeOf(b) && t.Comparable() && a == b {
		return true
	}
	aProvider, aOK := a.(TransactionDomainProvider)
	bProvider, bOK := b.(TransactionDomainProvider)
	if !aOK || !bOK {
		return false
	}
	aID, bID := aProvider.TransactionDomainID(), bProvider.TransactionDomainID()
	return aID != "" && aID == bID
}

type Client struct {
	inner    *concertum.Client
	domainID string
}

func NewFromBackend(backend any) *Client {
	return &Client{inner: concertum.NewClient(backend), domainID: transactionDomainID(backend)}
}

func NewFromConcertumClient(client *concertum.Client) *Client {
	return &Client{inner: client, domainID: transactionDomainID(client)}
}

func (c *Client) TransactionDomainID() string { return c.domainID }

func transactionDomainID(value any) string {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return ""
	}
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		if rv.IsNil() {
			return ""
		}
		return fmt.Sprintf("%T:%x", value, rv.Pointer())
	default:
		return ""
	}
}

func isNilReference(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (c *Client) Read(ctx context.Context, hostId uint32, addr uint64, length uint32) ([]byte, error) {
	return c.inner.Read(ctx, hostId, addr, length)
}

func (c *Client) Write(ctx context.Context, hostId uint32, addr uint64, data []byte) error {
	return c.inner.Write(ctx, hostId, addr, data)
}

func (c *Client) ExecuteAndCommit(ctx context.Context, class string, compares []CompareItem, reads []ReadItem, writes []WriteItem) ExecResult {
	tx := c.inner.NewMiniTransaction().WithClass(class)
	for _, item := range compares {
		if item.ValidUntil.IsZero() {
			tx.Compare(item.HostId, item.Address, item.Data)
		} else {
			tx.CompareValidUntil(item.HostId, item.Address, item.Data, item.ValidUntil)
		}
	}
	for _, item := range reads {
		tx.Read(item.HostId, item.Address, item.Length)
	}
	for _, item := range writes {
		tx.Write(item.HostId, item.Address, item.Data)
	}
	committed, readResults, status, _ := tx.ExecuteAndCommitDetailed(ctx)
	out := make(map[ReadKey][]byte, len(readResults))
	for k, v := range readResults {
		out[ReadKey{HostId: k.HostId, Address: k.Address, Length: k.Length}] = append([]byte(nil), v...)
	}
	return ExecResult{Committed: committed, ReadResults: out, UnknownOutcome: status == concertum.StatusUnknown}
}
