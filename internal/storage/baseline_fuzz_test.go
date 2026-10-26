package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzRecoverBaselineJournalNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not json"))
	f.Add([]byte(`{"version":2,"files":[]}`))
	f.Add([]byte(`{"version":2,"files":[{"name":"../image.bin","exists":true}]}`))
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
