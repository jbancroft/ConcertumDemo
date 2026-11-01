package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	logCrashExitCode = 97
	logCrashTimeout  = 10 * time.Second
)

// Durable log reading, batching, compaction, and interrupted replacement.

func TestRedoLogLargeRecordsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	const (
		largerWriteSize = 256 << 10
		largeWriteSize  = 64 << 10
	)
	var want []RedoLogItem
	for i, size := range []int{largeWriteSize, largerWriteSize, 1} {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j + i)
		}
		entry := RedoLogItem{
			TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1},
			HostIds:       []uint32{1, 2},
			WriteItems:    []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: data}},
		}
		if err := log.AddItem(entry); err != nil {
			t.Fatal(err)
		}
		want = append(want, entry)
	}
	reopened, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatalf("reopen accepted large records: %v", err)
	}
	defer reopened.Close()
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
			log, err := OpenRedoLog(dir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("accepted malformed redo log")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
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

func TestRedoLogBatchingReducesFsyncCallsAndPreservesDurability(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	log.ConfigureBatchingForTest(3*time.Millisecond, 64)

	var fsyncCalls atomic.Int64
	log.SetFsyncHookForTest(func(f *os.File) error {
		time.Sleep(2 * time.Millisecond)
		fsyncCalls.Add(1)
		return f.Sync()
	})

	const entries = 200
	var wg sync.WaitGroup
	wg.Add(entries)
	for i := 0; i < entries; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := log.AddItem(RedoLogItem{TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1}, HostIds: []uint32{1}, WriteItems: []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: []byte{byte(i)}}}})
			if err != nil {
				t.Errorf("append failed: %v", err)
			}
		}()
	}
	wg.Wait()
	for range 2 {
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if got := fsyncCalls.Load(); got >= entries {
		t.Fatalf("expected batching to reduce fsync calls; got=%d entries=%d", got, entries)
	}

	reloaded, err := OpenRedoLog(filepath.Clean(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	if got := len(reloaded.Entries()); got != entries {
		t.Fatalf("expected %d durable entries, got %d", entries, got)
	}
}

func TestRedoLogCompactionReplacesCompleteFile(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(map[int]string{1: "partial", 2: "full"}[count], func(t *testing.T) {
			dir := t.TempDir()
			log, err := OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			for i := uint64(1); i <= 2; i++ {
				if err := log.AddItem(RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: i}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := StoreProcessedPointer(dir, 2); err != nil {
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
			if ptr, err := LoadProcessedPointer(dir); err != nil || ptr != 0 {
				t.Errorf("checkpoint after compaction = %d, %v; want 0", ptr, err)
			}
			if err := log.AddItem(RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 3}}); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
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

func TestForcedAbortLogReplacement(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "gc"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log, err := OpenForcedAbortLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			old := sinfonia.TransactionID{Counter: 1, Epoch: 100}
			recent := sinfonia.TransactionID{Counter: 2, Epoch: 103}
			for _, tid := range []sinfonia.TransactionID{old, recent} {
				if err := log.AddItem(tid); err != nil {
					t.Fatal(err)
				}
			}
			checkOriginal := unchangedOpenLog(t, filepath.Join(dir, "forced_abort.log"))
			if reset {
				err = log.Reset()
			} else {
				var removed []sinfonia.TransactionID
				removed, err = log.GCByEpoch(103)
				if len(removed) != 1 || removed[0] != old {
					t.Fatalf("removed = %v, want only %v", removed, old)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			checkOriginal()
			if log.IsForcedAbort(old) || log.IsForcedAbort(recent) == reset {
				t.Fatal("wrong live forced-abort set after replacement")
			}
			// A later append must go to the replacement, not the old inode.
			later := sinfonia.TransactionID{Counter: 3, Epoch: 103}
			if err := log.AddItem(later); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenForcedAbortLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.IsForcedAbort(old) || reopened.IsForcedAbort(recent) == reset || !reopened.IsForcedAbort(later) {
				t.Fatal("wrong durable forced-abort set after replacement and append")
			}
		})
	}
}

func TestDecidedLogReplacement(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "remove"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log, err := OpenDecidedTransactionLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			removed := sinfonia.TransactionID{Counter: 1, Epoch: 100}
			retained := sinfonia.TransactionID{Counter: 2, Epoch: 103}
			if err := log.AddItem(removed, true); err != nil {
				t.Fatal(err)
			}
			if err := log.AddItem(retained, false); err != nil {
				t.Fatal(err)
			}
			checkOriginal := unchangedOpenLog(t, filepath.Join(dir, "decided.log"))
			if reset {
				err = log.Reset()
			} else {
				err = log.Remove(removed)
			}
			if err != nil {
				t.Fatal(err)
			}
			checkOriginal()
			if _, known := log.Get(removed); known {
				t.Fatal("removed decision remains in memory")
			}
			if committed, known := log.Get(retained); committed || known == reset {
				t.Fatalf("retained decision = %t, %t", committed, known)
			}
			later := sinfonia.TransactionID{Counter: 3, Epoch: 103}
			if err := log.AddItem(later, true); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenDecidedTransactionLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, known := reopened.Get(removed); known {
				t.Fatal("removed decision remains on disk")
			}
			if committed, known := reopened.Get(retained); committed || known == reset {
				t.Fatalf("reopened retained decision = %t, %t", committed, known)
			}
			if committed, known := reopened.Get(later); !committed || !known {
				t.Fatal("append after replacement was lost")
			}
		})
	}
}

func TestTransactionLogFailedReplacementPreservesMemory(t *testing.T) {
	for _, kind := range []string{"forced-abort", "decided"} {
		for _, reset := range []bool{false, true} {
			operation := "cleanup"
			if reset {
				operation = "reset"
			}
			t.Run(kind+"/"+operation, func(t *testing.T) {
				dir := t.TempDir()
				old := sinfonia.TransactionID{Counter: 1, Epoch: 100}
				recent := sinfonia.TransactionID{Counter: 2, Epoch: 103}
				var path string
				var rewrite func() error
				var contains func(sinfonia.TransactionID) bool
				if kind == "forced-abort" {
					log, err := OpenForcedAbortLog(dir)
					if err != nil {
						t.Fatal(err)
					}
					for _, tid := range []sinfonia.TransactionID{old, recent} {
						if err := log.AddItem(tid); err != nil {
							t.Fatal(err)
						}
					}
					path = filepath.Join(dir, "forced_abort.log")
					contains = log.IsForcedAbort
					rewrite = func() error { _, err := log.GCByEpoch(103); return err }
					if reset {
						rewrite = log.Reset
					}
				} else {
					log, err := OpenDecidedTransactionLog(dir)
					if err != nil {
						t.Fatal(err)
					}
					for _, tid := range []sinfonia.TransactionID{old, recent} {
						if err := log.AddItem(tid, false); err != nil {
							t.Fatal(err)
						}
					}
					path = filepath.Join(dir, "decided.log")
					contains = func(tid sinfonia.TransactionID) bool { _, ok := log.Get(tid); return ok }
					rewrite = func() error { return log.Remove(old) }
					if reset {
						rewrite = log.Reset
					}
				}
				// A directory at the destination prevents publishing a replacement.
				backup := path + ".saved"
				if err := os.Rename(path, backup); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := rewrite(); err == nil {
					t.Fatal("replacement unexpectedly succeeded")
				}
				if !contains(old) || !contains(recent) {
					t.Error("failed replacement removed in-memory history")
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(backup, path); err != nil {
					t.Fatal(err)
				}
				if err := rewrite(); err != nil {
					t.Fatal(err)
				}
				if contains(old) || contains(recent) == reset {
					t.Fatal("replacement did not succeed after the failure was removed")
				}
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 1 {
					t.Fatalf("replacement leaked temporary files: %v, %v", entries, err)
				}
			})
		}
	}
}

// An open reader must keep seeing the complete original file after publication.
func unchangedOpenLog(t *testing.T, path string) func() {
	t.Helper()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = original.Close() })
	return func() {
		t.Helper()
		if got, err := io.ReadAll(original); err != nil || !bytes.Equal(got, before) {
			t.Errorf("original log was rewritten in place: %q, %v; want %q", got, err, before)
		}
	}
}

func TestPublishLogFileWriteFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forced_abort.log")
	const original = "1:1:100\n1:2:103\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("interrupted replacement write")
	err := publishLogFile(path, forcedAbortTempPrefix, func(w io.Writer) error {
		if _, err := io.WriteString(w, "1:2:"); err != nil {
			t.Fatal(err)
		}
		return writeErr
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("replacement error = %v, want %v", err, writeErr)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != original {
		t.Fatalf("live log after failed write = %q, %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("failed write leaked temporary files: %v, %v", entries, err)
	}
}

func TestPublishLogFileCrashBoundaries(t *testing.T) {
	for _, kind := range []string{"forced-abort", "decided"} {
		for _, phase := range []string{"created", "partial-write", "complete-write", "published", "directory-synced"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				dir := t.TempDir()
				name, _, original, replacement := crashLogContents(kind)
				path := filepath.Join(dir, name)
				if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), logCrashTimeout)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublishLogFileCrashHelper$")
				cmd.Env = append(os.Environ(),
					"CONCERTUM_LOG_CRASH_DIR="+dir,
					"CONCERTUM_LOG_CRASH_KIND="+kind,
					"CONCERTUM_LOG_CRASH_PHASE="+phase,
				)
				output, err := cmd.CombinedOutput()
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != logCrashExitCode {
					t.Fatalf("crash helper = %v, output: %s", err, output)
				}
				want := original
				if phase == "published" || phase == "directory-synced" {
					want = replacement
				}
				if got, err := os.ReadFile(path); err != nil || string(got) != want {
					t.Fatalf("live log after crash = %q, %v; want complete log %q", got, err, want)
				}
				// Reopening ignores any incomplete temporary file and retains
				// the current transaction's negative evidence in every phase.
				tid, _ := parseTidKey("1:2:103")
				if kind == "forced-abort" {
					log, err := OpenForcedAbortLog(dir)
					if err != nil || !log.IsForcedAbort(tid) {
						t.Fatalf("reopen lost current forced abort: %v", err)
					}
				} else {
					log, err := OpenDecidedTransactionLog(dir)
					if err != nil {
						t.Fatal(err)
					}
					if committed, known := log.Get(tid); committed || !known {
						t.Fatalf("reopen lost current abort decision: committed=%t known=%t", committed, known)
					}
				}
			})
		}
	}
}

func TestPublishLogFileCrashHelper(t *testing.T) {
	dir := os.Getenv("CONCERTUM_LOG_CRASH_DIR")
	if dir == "" {
		return
	}
	phase := os.Getenv("CONCERTUM_LOG_CRASH_PHASE")
	name, prefix, _, replacement := crashLogContents(os.Getenv("CONCERTUM_LOG_CRASH_KIND"))
	err := publishLogFile(filepath.Join(dir, name), prefix, func(w io.Writer) error {
		if phase == "created" {
			os.Exit(logCrashExitCode)
		}
		midpoint := len(replacement) / 2
		if _, err := io.WriteString(w, replacement[:midpoint]); err != nil {
			return err
		}
		if phase == "partial-write" {
			os.Exit(logCrashExitCode)
		}
		if _, err := io.WriteString(w, replacement[midpoint:]); err != nil {
			return err
		}
		if phase == "complete-write" {
			os.Exit(logCrashExitCode)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if phase == "published" {
		os.Exit(logCrashExitCode)
	}
	if err := syncDir(dir); err != nil {
		t.Fatal(err)
	}
	if phase == "directory-synced" {
		os.Exit(logCrashExitCode)
	}
	t.Fatalf("unknown crash phase %q", phase)
}

func crashLogContents(kind string) (name, prefix, original, replacement string) {
	if kind == "decided" {
		return "decided.log", decidedTempPrefix, "1:1:100 commit\n1:2:103 abort\n", "1:2:103 abort\n"
	}
	return "forced_abort.log", forcedAbortTempPrefix, "1:1:100\n1:2:103\n", "1:2:103\n"
}
