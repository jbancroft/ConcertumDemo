package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// Fuzz coverage for internal storage decoders.

func FuzzParseTIDKeyNeverPanics(f *testing.F) {
	f.Add("1:2:3")
	f.Add("18446744073709551615:18446744073709551615:18446744073709551615")
	f.Add("1:2")
	f.Add("-1:2:3")
	f.Add("1:2:3:4")
	f.Fuzz(func(t *testing.T, key string) {
		txId, ok := parseTidKey(key)
		if !ok {
			return
		}
		canonical := tidKey(txId)
		reparsed, ok := parseTidKey(canonical)
		if !ok || reparsed != txId {
			t.Fatalf("canonical key %q did not preserve transaction ID %+v", canonical, txId)
		}
	})
}

func FuzzLoadReplayStoreRecordsNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("invalid\n"))
	f.Add([]byte("{\"tid\":{},\"kind\":\"prepare\",\"prepare\":{}}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "replay.log")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, _ = loadReplayStoreRecords(path)
	})
}
