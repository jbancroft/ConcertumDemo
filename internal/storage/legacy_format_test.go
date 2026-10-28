package storage

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/jbancroft/concertum2/internal/proto"
)

func TestRedoLogItemBeforeGofoniaRenaming(t *testing.T) {
	// Captured from Concertum2 8611b32 before adopting gofonia's source names.
	data, err := os.ReadFile("testdata/redo_before_gofonia.json")
	if err != nil {
		t.Fatal(err)
	}
	var item RedoLogItem
	if err := json.Unmarshal(data, &item); err != nil {
		t.Fatal(err)
	}
	want := RedoLogItem{
		TransactionId: proto.TransactionID{CoordinatorID: 17, Counter: 23, Epoch: 1},
		HostIds:       []uint32{7, 9},
		WriteItems:    []proto.WriteItem{{MemID: 7, Addr: 32, Data: []byte("write")}},
	}
	if !reflect.DeepEqual(item, want) {
		t.Fatalf("old redo record changed meaning: got %+v, want %+v", item, want)
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var oldJSON, newJSON any
	if err := json.Unmarshal(data, &oldJSON); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &newJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(oldJSON, newJSON) {
		t.Fatalf("persisted schema changed: %s", encoded)
	}
}
