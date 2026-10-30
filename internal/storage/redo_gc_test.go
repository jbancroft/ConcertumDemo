package storage_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
	"github.com/jbancroft/ConcertumDemo/internal/storage"
)

func TestRedoLogCompactionReplacesCompleteFile(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(map[int]string{1: "partial", 2: "full"}[count], func(t *testing.T) {
			dir := t.TempDir()
			log, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			for i := uint64(1); i <= 2; i++ {
				if err := log.AddItem(storage.RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: i}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := storage.StoreProcessedPointer(dir, 2); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "redo.log")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			original, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer original.Close()
			if err := log.TruncateHead(count); err != nil {
				t.Fatal(err)
			}
			// Readers holding the old file see its complete contents, never
			// an empty or partly rewritten log while a replacement is built.
			if got, err := io.ReadAll(original); err != nil || !bytes.Equal(got, before) {
				t.Errorf("original log was rewritten in place: %q, %v", got, err)
			}
			if ptr, err := storage.LoadProcessedPointer(dir); err != nil || ptr != 0 {
				t.Errorf("checkpoint after compaction = %d, %v; want 0", ptr, err)
			}
			if err := log.AddItem(storage.RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 3}}); err != nil {
				t.Fatal(err)
			}
			reopened, err := storage.OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			entries := reopened.Entries()
			if len(entries) != 3-count {
				t.Fatalf("reopened entries = %d, want %d", len(entries), 3-count)
			}
			for i, entry := range entries {
				if entry.TransactionId.Counter != uint64(count+i+1) {
					t.Fatalf("entry %d = %+v", i, entry)
				}
			}
		})
	}
}
