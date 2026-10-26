package storage

import (
	"bytes"
	"testing"
)

func FuzzLoadReplayRecordsNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("invalid\n"))
	f.Add([]byte("{\"tid\":{},\"kind\":\"prepare\",\"prepare\":{}}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = loadReplayRecords(bytes.NewReader(data))
	})
}
