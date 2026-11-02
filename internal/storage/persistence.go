package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	decidedTempPrefix     = ".decided.log.tmp-"
	forcedAbortTempPrefix = ".forced_abort.log.tmp-"
	redoTempPrefix        = ".redo.log.tmp-"
)

type RedoLogItem struct {
	TransactionId sinfonia.TransactionID
	HostIds       []uint32
	WriteItems    []sinfonia.WriteItem
}

type DiskImage struct {
	mu   sync.Mutex
	file *os.File
	size int
}

// OpenDiskImage initializes a new image or opens an existing image of exactly
// the requested size. Existing images are never resized.
func OpenDiskImage(dir string, size int) (*DiskImage, []byte, error) {
	if size <= 0 {
		return nil, nil, fmt.Errorf("disk image size must be positive, got %d", size)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, "image.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		f, err = os.OpenFile(path, os.O_RDWR, 0)
	}
	if err != nil {
		return nil, nil, err
	}
	if created {
		if err := f.Truncate(int64(size)); err != nil {
			f.Close()
			return nil, nil, err
		}
	} else {
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, nil, err
		}
		if info.Size() != int64(size) {
			f.Close()
			return nil, nil, fmt.Errorf("disk image %s has size %d bytes, configured size is %d bytes", path, info.Size(), size)
		}
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &DiskImage{file: f, size: size}, buf, nil
}

func (d *DiskImage) ApplyWrite(addr uint64, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	end := addr + uint64(len(data))
	if end > uint64(d.size) {
		return fmt.Errorf("disk image out of bounds")
	}
	if _, err := d.file.WriteAt(data, int64(addr)); err != nil {
		return err
	}
	return d.file.Sync()
}

func (d *DiskImage) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}

type RedoLog struct {
	mu      sync.Mutex
	path    string
	entries []RedoLogItem
	fsyncFn func(*os.File) error
}

func OpenRedoLog(dir string) (*RedoLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, "redo.log")
	entries := make([]RedoLogItem, 0)
	f, err := os.OpenFile(p, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader := io.NewSectionReader(f, 0, info.Size())
	for {
		var entry RedoLogItem
		if err := readGobRecord(reader, &entry); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		entries = append(entries, entry)
	}
	return &RedoLog{path: p, entries: entries, fsyncFn: (*os.File).Sync}, nil
}

func (r *RedoLog) Entries() []RedoLogItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RedoLogItem, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *RedoLog) SetFsyncHookForTest(fn func(*os.File) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fn == nil {
		r.fsyncFn = (*os.File).Sync
		return
	}
	r.fsyncFn = fn
}

// AddItem persists one record before publishing it in memory. The mutex also
// serializes appends with compaction so writes always reach the current log.
func (r *RedoLog) AddItem(entry RedoLogItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	f, err := os.OpenFile(r.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeGobRecord(f, entry); err != nil {
		return err
	}
	if err := r.fsyncFn(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	r.entries = append(r.entries, entry)
	return nil
}

func (r *RedoLog) TruncateHead(count int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if count <= 0 || len(r.entries) == 0 {
		return nil
	}
	if count > len(r.entries) {
		count = len(r.entries)
	}
	remaining := append([]RedoLogItem(nil), r.entries[count:]...)
	// Compaction changes entry indexes. Persist a conservative checkpoint
	// first: either the old or replacement log can be replayed from zero.
	if err := StoreProcessedPointer(filepath.Dir(r.path), 0); err != nil {
		return err
	}
	if err := rewriteRedoLogFile(r.path, remaining); err != nil {
		return err
	}
	// Rename has published the replacement even if the directory sync fails.
	// Keep the in-memory entries consistent with the currently visible file.
	r.entries = remaining
	return syncDir(filepath.Dir(r.path))
}

// publishLogFile replaces a log with a complete, synced file. The caller must
// serialize appends, update its in-memory state after publication, and sync the
// directory before acknowledging success. A later directory-sync error does not
// undo the rename, so memory must already match the visible replacement.
func publishLogFile(path, tempPrefix string, write func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), tempPrefix)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	if err := f.Chmod(0o644); err != nil {
		return err
	}
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// rewriteRedoLogFile publishes a complete, synced replacement. Callers update
// their in-memory entries and sync the directory before acknowledging success.
func rewriteRedoLogFile(path string, entries []RedoLogItem) error {
	return publishLogFile(path, redoTempPrefix, func(w io.Writer) error {
		for _, e := range entries {
			if err := writeGobRecord(w, e); err != nil {
				return err
			}
		}
		return nil
	})
}

type DecidedTransactionLog struct {
	mu       sync.Mutex
	path     string
	outcomes map[string]bool
}

func OpenDecidedTransactionLog(dir string) (*DecidedTransactionLog, error) {
	return openOutcomeStore(filepath.Join(dir, "decided.log"))
}

type ForcedAbortLog struct {
	mu      sync.Mutex
	path    string
	set     map[string]struct{}
	fsyncFn func(*os.File) error
}

func OpenForcedAbortLog(dir string) (*ForcedAbortLog, error) {
	p := filepath.Join(dir, "forced_abort.log")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]struct{}{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		k := strings.TrimSpace(s.Text())
		if k != "" {
			out[k] = struct{}{}
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return &ForcedAbortLog{path: p, set: out, fsyncFn: (*os.File).Sync}, nil
}

func (s *ForcedAbortLog) SetFsyncHookForTest(fn func(*os.File) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fn == nil {
		fn = (*os.File).Sync
	}
	s.fsyncFn = fn
}

func (s *ForcedAbortLog) IsForcedAbort(txId sinfonia.TransactionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.set[tidKey(txId)]
	return ok
}

func (s *ForcedAbortLog) AddItem(txId sinfonia.TransactionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tidKey(txId)
	if _, ok := s.set[k]; ok {
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(k + "\n"); err != nil {
		return err
	}
	if err := s.fsyncFn(f); err != nil {
		return err
	}
	s.set[k] = struct{}{}
	return nil
}

func (s *ForcedAbortLog) GCByEpoch(currentEpoch uint64) ([]sinfonia.TransactionID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if currentEpoch <= PublicRetryHorizon {
		return nil, nil
	}
	threshold := currentEpoch - PublicRetryHorizon
	retained := make(map[string]struct{}, len(s.set))
	removed := make([]sinfonia.TransactionID, 0)
	for key := range s.set {
		txId, ok := parseTidKey(key)
		if ok && txId.Epoch < threshold {
			removed = append(removed, txId)
		} else {
			retained[key] = struct{}{}
		}
	}
	if len(removed) == 0 {
		return nil, nil
	}
	if err := s.rewriteLocked(retained); err != nil {
		return nil, err
	}
	return removed, nil
}

func (s *ForcedAbortLog) rewriteLocked(retained map[string]struct{}) error {
	keys := make([]string, 0, len(retained))
	for k := range retained {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if err := publishLogFile(s.path, forcedAbortTempPrefix, func(w io.Writer) error {
		for _, k := range keys {
			if _, err := io.WriteString(w, k+"\n"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	s.set = retained
	return syncDir(filepath.Dir(s.path))
}

func openOutcomeStore(path string) (*DecidedTransactionLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	m := map[string]bool{}
	s := bufio.NewScanner(f)
	lineNumber := 0
	for s.Scan() {
		lineNumber++
		parts := strings.Fields(s.Text())
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s:%d: invalid decision record: expected transaction ID and outcome", path, lineNumber)
		}
		tid, ok := parseTidKey(parts[0])
		if !ok {
			return nil, fmt.Errorf("%s:%d: invalid transaction ID %q", path, lineNumber, parts[0])
		}
		switch parts[1] {
		case "commit":
			m[tidKey(tid)] = true
		case "abort":
			m[tidKey(tid)] = false
		default:
			return nil, fmt.Errorf("%s:%d: invalid transaction decision %q", path, lineNumber, parts[1])
		}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("%s:%d: read decision record: %w", path, lineNumber+1, err)
	}
	return &DecidedTransactionLog{path: path, outcomes: m}, nil
}

func (s *DecidedTransactionLog) Get(txId sinfonia.TransactionID) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.outcomes[tidKey(txId)]
	return v, ok
}

func (s *DecidedTransactionLog) AddItem(txId sinfonia.TransactionID, committed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tidKey(txId)
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	state := "abort"
	if committed {
		state = "commit"
	}
	if _, err := f.WriteString(k + " " + state + "\n"); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	s.outcomes[k] = committed
	return nil
}

func (s *DecidedTransactionLog) Remove(txId sinfonia.TransactionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tidKey(txId)
	if _, ok := s.outcomes[k]; !ok {
		return nil
	}
	retained := make(map[string]bool, len(s.outcomes)-1)
	for key, committed := range s.outcomes {
		if key != k {
			retained[key] = committed
		}
	}
	return s.rewriteLocked(retained)
}

// Compact removes expired decisions unless the caller still needs them for redo.
func (s *DecidedTransactionLog) Compact(retention ReplayRetention) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	retained := make(map[string]bool, len(s.outcomes))
	for key, committed := range s.outcomes {
		tid, ok := parseTidKey(key)
		if !ok || !retention.Expired(tid) {
			retained[key] = committed
		}
	}
	if len(retained) == len(s.outcomes) {
		return nil
	}
	return s.rewriteLocked(retained)
}

func (s *DecidedTransactionLog) rewriteLocked(retained map[string]bool) error {
	keys := make([]string, 0, len(retained))
	for key := range retained {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if err := publishLogFile(s.path, decidedTempPrefix, func(w io.Writer) error {
		for _, key := range keys {
			state := "abort"
			if retained[key] {
				state = "commit"
			}
			if _, err := io.WriteString(w, key+" "+state+"\n"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	s.outcomes = retained
	return syncDir(filepath.Dir(s.path))
}

func LoadProcessedPointer(dir string) (int, error) {
	p := filepath.Join(dir, "processed.ptr")
	b, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func StoreProcessedPointer(dir string, idx int) error {
	p := filepath.Join(dir, "processed.ptr")
	tmp, err := os.CreateTemp(dir, ".processed.ptr.tmp-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := tmp.WriteString(strconv.Itoa(idx)); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, p); err != nil {
		return err
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = dirFile.Sync()
	closeErr := dirFile.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func tidKey(t sinfonia.TransactionID) string {
	return fmt.Sprintf("%d:%d:%d", t.CoordinatorID, t.Counter, t.Epoch)
}

func parseTidKey(k string) (sinfonia.TransactionID, bool) {
	parts := strings.Split(k, ":")
	if len(parts) != 3 {
		return sinfonia.TransactionID{}, false
	}
	coord, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return sinfonia.TransactionID{}, false
	}
	counter, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return sinfonia.TransactionID{}, false
	}
	epoch, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return sinfonia.TransactionID{}, false
	}
	return sinfonia.TransactionID{CoordinatorID: coord, Counter: counter, Epoch: epoch}, true
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
