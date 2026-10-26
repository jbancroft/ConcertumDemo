package sinfoniaclient

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/jbancroft/concertum2/pkg/concertum"
)

type CmpItem struct {
	MemID      uint32
	Addr       uint64
	Data       []byte
	ValidUntil time.Time
}

type ReadItem struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

type WriteItem struct {
	MemID uint32
	Addr  uint64
	Data  []byte
}

type ReadKey struct {
	MemID uint32
	Addr  uint64
	Len   uint32
}

type ExecResult struct {
	Committed      bool
	ReadResults    map[ReadKey][]byte
	UnknownOutcome bool
}

// TxnClient is the minitransaction contract consumed by higher layers.
type TxnClient interface {
	ExecAndCommit(ctx context.Context, class string, cmp []CmpItem, reads []ReadItem, writes []WriteItem) ExecResult
	Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error)
	Write(ctx context.Context, memID uint32, addr uint64, data []byte) error
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

func (c *Client) Read(ctx context.Context, memID uint32, addr uint64, length uint32) ([]byte, error) {
	return c.inner.Read(ctx, memID, addr, length)
}

func (c *Client) Write(ctx context.Context, memID uint32, addr uint64, data []byte) error {
	return c.inner.Write(ctx, memID, addr, data)
}

func (c *Client) ExecAndCommit(ctx context.Context, class string, cmp []CmpItem, reads []ReadItem, writes []WriteItem) ExecResult {
	mtx := c.inner.NewMTx().WithClass(class)
	for _, item := range cmp {
		if item.ValidUntil.IsZero() {
			mtx.Cmp(item.MemID, item.Addr, item.Data)
		} else {
			mtx.CmpValidUntil(item.MemID, item.Addr, item.Data, item.ValidUntil)
		}
	}
	for _, item := range reads {
		mtx.Read(item.MemID, item.Addr, item.Len)
	}
	for _, item := range writes {
		mtx.Write(item.MemID, item.Addr, item.Data)
	}
	committed, readResults, status, _ := mtx.ExecAndCommitDetailed(ctx)
	out := make(map[ReadKey][]byte, len(readResults))
	for k, v := range readResults {
		out[ReadKey{MemID: k.MemID, Addr: k.Addr, Len: k.Len}] = append([]byte(nil), v...)
	}
	return ExecResult{Committed: committed, ReadResults: out, UnknownOutcome: status == concertum.StatusUnknown}
}
