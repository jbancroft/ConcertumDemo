package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	logCrashExitCode = 97
	logCrashTimeout  = 10 * time.Second
)

// Durable log reading, appends, compaction, and interrupted replacement.

func TestRedoLogLargeRecordsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
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
	if !reflect.DeepEqual(reopened.Entries(), want) {
		t.Fatal("reopened redo log changed record order, metadata, or binary payloads")
	}
}

func TestDecidedLogReaderEOFAndMalformedRecords(t *testing.T) {
	first := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	later := sinfonia.TransactionID{CoordinatorID: 1, Counter: 3, Epoch: 100}
	for _, tc := range []struct {
		name    string
		data    string
		wantErr bool
	}{
		{name: "empty"},
		{name: "complete record", data: "1:1:100 commit\n"},
		{name: "multiple records", data: "1:1:100 commit\n1:2:100 abort\n"},
		{name: "unterminated commit", data: "1:1:100 commit\n1:2:100 commit", wantErr: true},
		{name: "unterminated abort", data: "1:1:100 commit\n1:2:100 abort", wantErr: true},
		{name: "partial outcome", data: "1:1:100 commit\n1:2:100 com\n", wantErr: true},
		{name: "invalid ID", data: "1:1:100 commit\ninvalid abort\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "decided.log")
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			log, err := OpenDecidedTransactionLog(dir)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), path+":2:") {
					t.Fatalf("expected malformed record error identifying file and line: %v", err)
				}
				if got, err := os.ReadFile(path); err != nil || string(got) != tc.data {
					t.Fatalf("failed open modified decision log: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := log.AddItem(later, false); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenDecidedTransactionLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			if committed, known := reopened.Get(later); committed || !known {
				t.Fatal("appended abort was not preserved on reopen")
			}
			if committed, known := reopened.Get(first); known != (tc.data != "") || committed != known {
				t.Fatal("reopening lost or invented the original commit")
			}
		})
	}
}

func TestDecidedRetentionFollowsRedoRegardlessOfEpoch(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDecidedTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Decisions follow redo membership, regardless of their age.
	current := ^uint64(0)
	old := sinfonia.TransactionID{Counter: 1, Epoch: current - TransactionEpochWindow}
	recent := sinfonia.TransactionID{Counter: 2, Epoch: current - 1}
	now := sinfonia.TransactionID{Counter: 3, Epoch: current}
	pinned := sinfonia.TransactionID{Counter: 4, Epoch: 1}
	for _, tid := range []sinfonia.TransactionID{old, recent, now, pinned} {
		if err := log.AddItem(tid, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Retain(map[sinfonia.TransactionID]struct{}{old: {}, pinned: {}}); err != nil {
		t.Fatal(err)
	}
	log, err = OpenDecidedTransactionLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, tid := range []sinfonia.TransactionID{recent, now} {
		if _, known := log.Get(tid); known {
			t.Fatal("decision without redo survived compaction")
		}
	}
	for _, tid := range []sinfonia.TransactionID{old, pinned} {
		if committed, known := log.Get(tid); !committed || !known {
			t.Fatalf("required decision lost after compaction and reopen: %+v", tid)
		}
	}
}

func TestRedoLogReaderEOFAndMalformedRecords(t *testing.T) {
	first, err := encodeGobRecord(RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 1}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeGobRecord(RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 2}})
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(first)
	corrupt[len(corrupt)-1] = 0xff // Break the gob payload, leaving the frame complete.
	hugeLength := bytes.Clone(first)
	binary.BigEndian.PutUint64(hugeLength[len(gobRecordMagic):], ^uint64(0))
	emptyPayload := bytes.Clone(first[:gobRecordHeaderSize])
	binary.BigEndian.PutUint64(emptyPayload[len(gobRecordMagic):], 0)
	for _, tc := range []struct {
		name    string
		data    []byte
		wantIDs []uint64
		wantErr bool
	}{
		{name: "empty"},
		{name: "multiple records", data: append(bytes.Clone(first), second...), wantIDs: []uint64{1, 2}},
		{name: "partial magic", data: first[:2], wantErr: true},
		{name: "partial length", data: first[:gobRecordHeaderSize-1], wantErr: true},
		{name: "partial payload", data: append(bytes.Clone(first), second[:len(second)-1]...), wantErr: true},
		{name: "corrupt payload", data: append(corrupt, second...), wantErr: true},
		{name: "huge length", data: hugeLength, wantErr: true},
		{name: "empty payload", data: emptyPayload, wantErr: true},
		{name: "old JSON", data: []byte("{\"tid\":{}}\n"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "redo.log")
			if err := os.WriteFile(path, tc.data, 0o644); err != nil {
				t.Fatal(err)
			}
			log, err := OpenRedoLog(dir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("accepted malformed redo log")
				}
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, tc.data) {
					t.Fatalf("failed open modified redo log: %v", err)
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
			if err := log.AddItem(RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 3}}); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenRedoLog(dir)
			if err != nil {
				t.Fatalf("reopen after append: %v", err)
			}
			if !reflect.DeepEqual(reopened.Entries(), log.Entries()) {
				t.Fatal("append after reopening changed saved records")
			}
		})
	}
}

func TestRedoLogConcurrentAppendsAndCompactionPreserveDurability(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Only this oldest record is eligible for the concurrent compaction.
	if err := log.AddItem(RedoLogItem{}); err != nil {
		t.Fatal(err)
	}
	const entries = 200
	var wg sync.WaitGroup
	wg.Add(entries + 1)
	for i := 0; i < entries; i++ {
		go func(i int) {
			defer wg.Done()
			entry := RedoLogItem{
				TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: uint64(i + 1), Epoch: 1},
				HostIds:       []uint32{1},
				WriteItems:    []sinfonia.WriteItem{{MemID: 1, Addr: uint64(i), Data: []byte{byte(i)}}},
			}
			if err := log.AddItem(entry); err != nil {
				t.Errorf("append failed: %v", err)
			}
		}(i)
	}
	go func() {
		defer wg.Done()
		if err := log.TruncateHead(1); err != nil {
			t.Errorf("compaction failed: %v", err)
		}
	}()
	wg.Wait()

	reloaded, err := OpenRedoLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := reloaded.Entries()
	if len(got) != entries || !reflect.DeepEqual(got, log.Entries()) {
		t.Fatalf("durable entries do not match the acknowledged appends: got %d, want %d", len(got), entries)
	}
	seen := make(map[uint64]bool, entries)
	for _, entry := range got {
		id := entry.TransactionId.Counter
		if id < 1 || id > entries || seen[id] {
			t.Fatalf("unexpected or duplicate transaction: %+v", entry.TransactionId)
		}
		want := RedoLogItem{
			TransactionId: sinfonia.TransactionID{CoordinatorID: 1, Counter: id, Epoch: 1},
			HostIds:       []uint32{1},
			WriteItems:    []sinfonia.WriteItem{{MemID: 1, Addr: id - 1, Data: []byte{byte(id - 1)}}},
		}
		if !reflect.DeepEqual(entry, want) {
			t.Fatalf("durable record changed: got %+v, want %+v", entry, want)
		}
		seen[id] = true
	}
}

func TestRedoLogAppendWaitsForSync(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "sync failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log, err := OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			injected := errors.New("sync failed")
			log.SetFsyncHookForTest(func(f *os.File) error {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				if fail {
					return injected
				}
				return f.Sync()
			})
			entry := RedoLogItem{TransactionId: sinfonia.TransactionID{Counter: 1}, HostIds: []uint32{1}}
			result := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				result <- log.AddItem(entry)
			}()
			defer func() { unblock(); <-finished }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err := <-result:
				t.Fatalf("append returned before sync finished: %v", err)
			default:
			}
			unblock()
			err = <-result
			if fail {
				if !errors.Is(err, injected) || len(log.Entries()) != 0 {
					t.Fatalf("failed sync published an entry or lost its error: entries=%v, err=%v", log.Entries(), err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			reloaded, err := OpenRedoLog(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(reloaded.Entries(), []RedoLogItem{entry}) {
				t.Fatal("successful append was not preserved on reopen")
			}
		})
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

func TestForcedAbortLogReaderEOFAndMalformedRecords(t *testing.T) {
	first := sinfonia.TransactionID{CoordinatorID: 1, Counter: 1, Epoch: 100}
	later := sinfonia.TransactionID{CoordinatorID: 1, Counter: 2, Epoch: 100}
	for _, tc := range []struct {
		name    string
		data    string
		wantErr bool
	}{
		{name: "empty"},
		{name: "complete record", data: "1:1:100\n"},
		{name: "partial ID", data: "1:1:100\n1:2:", wantErr: true},
		{name: "missing newline", data: "1:1:100\n1:2:100", wantErr: true},
		{name: "invalid ID", data: "1:1:100\n1:two:100\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "forced_abort.log")
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			log, err := OpenForcedAbortLog(dir)
			if tc.wantErr {
				if err == nil {
					t.Fatal("accepted malformed forced-abort log")
				}
				if !strings.Contains(err.Error(), path+":2:") {
					t.Fatalf("error does not identify the file and line: %v", err)
				}
				if got, err := os.ReadFile(path); err != nil || string(got) != tc.data {
					t.Fatalf("failed open modified forced-abort log: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := log.AddItem(later); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenForcedAbortLog(dir)
			if err != nil {
				t.Fatalf("reopen after append: %v", err)
			}
			if !reopened.IsForcedAbort(later) || reopened.IsForcedAbort(first) != (tc.data != "") {
				t.Fatal("reopening lost or invented a forced-abort record")
			}
		})
	}
}

func TestForcedAbortLogReplacement(t *testing.T) {

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

	var removed []sinfonia.TransactionID
	removed, err = log.GCByEpoch(103)
	if len(removed) != 1 || removed[0] != old {
		t.Fatalf("removed = %v, want only %v", removed, old)
	}

	if err != nil {
		t.Fatal(err)
	}
	checkOriginal()
	if log.IsForcedAbort(old) || !log.IsForcedAbort(recent) {
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
	if reopened.IsForcedAbort(old) || !reopened.IsForcedAbort(recent) || !reopened.IsForcedAbort(later) {
		t.Fatal("wrong durable forced-abort set after replacement and append")
	}

}

func TestDecidedLogReplacement(t *testing.T) {

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

	err = log.Retain(map[sinfonia.TransactionID]struct{}{retained: {}})

	if err != nil {
		t.Fatal(err)
	}
	checkOriginal()
	if _, known := log.Get(removed); known {
		t.Fatal("removed decision remains in memory")
	}
	if committed, known := log.Get(retained); committed || !known {
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
	if committed, known := reopened.Get(retained); committed || !known {
		t.Fatalf("reopened retained decision = %t, %t", committed, known)
	}
	if committed, known := reopened.Get(later); !committed || !known {
		t.Fatal("append after replacement was lost")
	}

}

func TestTransactionLogFailedReplacementPreservesMemory(t *testing.T) {
	for _, kind := range []string{"forced-abort", "decided"} {

		t.Run(kind, func(t *testing.T) {
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
				rewrite = func() error { return log.Retain(map[sinfonia.TransactionID]struct{}{recent: {}}) }

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
			if contains(old) || contains(recent) == false {
				t.Fatal("replacement did not succeed after the failure was removed")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("replacement leaked temporary files: %v, %v", entries, err)
			}
		})

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
