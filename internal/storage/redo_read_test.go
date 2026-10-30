package storage_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestRedoLogLargeRecordsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	log, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	const (
		largeWriteSize  = 64 << 10
		largerWriteSize = 256 << 10
	)
	var want []storage.RedoLogItem
	for i, size := range []int{largeWriteSize, largerWriteSize, 1} {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j + i)
		}
		entry := storage.RedoLogItem{
			TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1},
			HostIds:       []uint32{1, 2},
			WriteItems:    []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: data}},
		}
		if err := log.AddItem(entry); err != nil {
			t.Fatal(err)
		}
		want = append(want, entry)
	}
	reopened, err := storage.OpenRedoLog(dir)
	if err != nil {
		t.Fatalf("reopen accepted large records: %v", err)
	}
	if !reflect.DeepEqual(reopened.Entries(), want) {
		t.Fatal("reopened redo log changed record order, metadata, or binary payloads")
	}
}

func TestRedoLogReaderEOFAndMalformedRecords(t *testing.T) {
	const first = `{"tid":{"Counter":1},"participants":[1],"write_items":[]}`
	const second = `{"tid":{"Counter":2},"participants":[1],"write_items":[]}`
	for _, tc := range []struct {
		name    string
		data    string
		wantIDs []uint64
		wantErr bool
	}{
		{name: "empty"},
		{name: "whitespace", data: " \t\r\n\n "},
		{name: "multiple records and blank lines", data: first + "\r\n\n \t\n" + second + "\n", wantIDs: []uint64{1, 2}},
		{name: "complete final record without newline", data: first + "\n" + second, wantIDs: []uint64{1, 2}},
		{name: "malformed middle record", data: first + "\nnot JSON\n" + second + "\n", wantErr: true},
		{name: "truncated final record", data: first + "\n" + second[:len(second)-1], wantErr: true},
		{name: "invalid binary encoding", data: `{"write_items":[{"Data":"!!!"}]}` + "\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "redo.log")
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			log, err := storage.OpenRedoLog(dir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("accepted malformed redo log")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var ids []uint64
			for _, entry := range log.Entries() {
				ids = append(ids, entry.TransactionId.Counter)
			}
			if !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Fatalf("record IDs = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}
