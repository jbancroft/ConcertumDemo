package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

func TestAppliedReceiptsReplaceAndReopen(t *testing.T) {
	dir := t.TempDir()
	if got, err := LoadAppliedReceipts(dir); err != nil || len(got) != 0 {
		t.Fatalf("new receipt store = %v, %v", got, err)
	}
	first := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	second := first
	second.Counter++
	receipts := map[sinfonia.TransactionID]map[uint32]bool{
		first: {2: true, 3: true}, second: {2: true},
	}
	if err := StoreAppliedReceipts(dir, receipts); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadAppliedReceipts(dir); err != nil || !reflect.DeepEqual(got, receipts) {
		t.Fatalf("reopened receipts = %v, %v", got, err)
	}
	unchanged := unchangedOpenLog(t, filepath.Join(dir, appliedReceiptFile))
	delete(receipts, first)
	if err := StoreAppliedReceipts(dir, receipts); err != nil {
		t.Fatal(err)
	}
	unchanged()
	if got, err := LoadAppliedReceipts(dir); err != nil || !reflect.DeepEqual(got, receipts) {
		t.Fatalf("retained receipts = %v, %v", got, err)
	}
	delete(receipts, second)
	if err := StoreAppliedReceipts(dir, receipts); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadAppliedReceipts(dir); err != nil || len(got) != 0 {
		t.Fatalf("collected receipts = %v, %v", got, err)
	}
}

func TestAppliedReceiptsRejectMalformedSnapshot(t *testing.T) {
	record, err := encodeGobRecord(map[sinfonia.TransactionID]map[uint32]bool{})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"empty":         nil,
		"truncated":     record[:len(record)-1],
		"trailing data": append(bytes.Clone(record), 0),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, appliedReceiptFile)
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadAppliedReceipts(dir); err == nil {
				t.Fatal("accepted malformed receipt snapshot")
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
				t.Fatalf("failed load changed snapshot: %v", err)
			}
		})
	}
}
