package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Fuzz coverage for internal storage decoders and recovery journals.

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

func FuzzLoadReplayRecordsNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("invalid\n"))
	f.Add([]byte("{\"tid\":{},\"kind\":\"prepare\",\"prepare\":{}}\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = loadReplayRecords(bytes.NewReader(data))
	})
}

func FuzzRecoverBaselineJournalNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not json"))
	f.Add([]byte(`{"files":[]}`))
	f.Add([]byte(`{"files":[{"name":"../image.bin","exists":true}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		journal := filepath.Join(dir, baselineJournalDirName)
		if err := os.Mkdir(journal, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(journal, "manifest.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		_ = recoverJournal(dir, journal)
	})
}
