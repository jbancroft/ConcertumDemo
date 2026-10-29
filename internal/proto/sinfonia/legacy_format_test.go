package sinfonia

import (
	"bytes"
	"encoding/gob"
	"os"
	"reflect"
	"testing"
)

func TestPrepareFrameBeforeGofoniaRenaming(t *testing.T) {
	// Captured from Concertum2 8611b32 before adopting gofonia's source names.
	f, err := os.Open("testdata/prepare_before_gofonia.gob")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var message any
	if err := gob.NewDecoder(f).Decode(&message); err != nil {
		t.Fatal(err)
	}
	want := ExecPrepareRequest{
		TID:       TransactionID{CoordinatorID: 17, Counter: 23, Epoch: 1},
		RequestID: "before-gofonia", Participants: []uint32{7, 9}, HasWrites: true,
		CmpItems:   []CmpItem{{MemID: 7, Addr: 8, Data: []byte("compare")}},
		ReadItems:  []ReadItem{{MemID: 9, Addr: 16, Len: 4}},
		WriteItems: []WriteItem{{MemID: 7, Addr: 32, Data: []byte("write")}}, Epoch: 1,
	}
	if !reflect.DeepEqual(message, want) {
		t.Fatalf("old wire frame changed meaning: got %+v, want %+v", message, want)
	}

	// Old peers also need to recognize messages sent by the renamed module.
	var encoded bytes.Buffer
	message = want
	if err := gob.NewEncoder(&encoded).Encode(&message); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded.Bytes(), []byte("github.com/jbancroft/concertum2/internal/proto.ExecPrepareRequest")) {
		t.Fatal("prepare frame lost its original gob type name")
	}
}
