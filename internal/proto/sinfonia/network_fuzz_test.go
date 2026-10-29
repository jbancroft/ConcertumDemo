package sinfonia

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func FuzzRegisteredGobTypesNeverPanic(f *testing.F) {
	var seed bytes.Buffer
	var request any = ExecPrepareRequest{
		TID:          TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 3},
		RequestID:    "request-1",
		Participants: []uint32{1, 2},
		CmpItems:     []CmpItem{{MemID: 1, Addr: 8, Data: []byte("cmp")}},
		ReadItems:    []ReadItem{{MemID: 1, Addr: 16, Len: 4}},
		WriteItems:   []WriteItem{{MemID: 2, Addr: 24, Data: []byte("write")}},
	}
	if err := gob.NewEncoder(&seed).Encode(&request); err != nil {
		f.Fatal(err)
	}
	f.Add([]byte{})
	f.Add([]byte("invalid"))
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		var message any
		_ = gob.NewDecoder(bytes.NewReader(data)).Decode(&message)
	})
}
