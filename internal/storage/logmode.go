package storage

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	fileCopyBufferSize     = 1 << 20
	defaultRedoBatchWindow = 2 * time.Millisecond
	defaultRedoBatchSize   = 32
	redoAppendQueueSize    = 4096
	redoBatchCapacity      = 64
	redoTempPrefix         = ".redo.log.tmp-"
)

type RedoLogItem struct {
	TransactionId sinfonia.TransactionID `json:"tid"`
	HostIds       []uint32               `json:"participants"`
	WriteItems    []sinfonia.WriteItem   `json:"write_items"`
}

type DiskImage struct {
	mu   sync.Mutex
	file *os.File
	size int
}

func OpenDiskImage(dir string, size int) (*DiskImage, []byte, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, "image.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, nil, err
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, os.ErrNotExist) {
		if !errors.Is(err, io.EOF) {
			f.Close()
			return nil, nil, err
		}
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

func (d *DiskImage) Snapshot(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data := make([]byte, d.size)
	if _, err := io.ReadFull(d.file, data); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	return nil
}

func (d *DiskImage) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Sync()
}

func (d *DiskImage) Bytes() ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, d.size)
	if _, err := io.ReadFull(d.file, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadAt reads part of the saved memory image. To get a consistent copy across
// several reads, the caller must hold the global write lock for the whole copy.
func (d *DiskImage) ReadAt(buf []byte, offset int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if offset < 0 || offset+int64(len(buf)) > int64(d.size) {
		return fmt.Errorf("disk image read out of bounds")
	}
	_, err := io.ReadFull(io.NewSectionReader(d.file, offset, int64(len(buf))), buf)
	return err
}

func (d *DiskImage) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.size
}

// SHA256 hashes the image without loading the whole file into memory.
func (d *DiskImage) SHA256() ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := sha256.New()
	buf := make([]byte, fileCopyBufferSize)
	for offset := 0; offset < d.size; {
		want := len(buf)
		if remaining := d.size - offset; remaining < want {
			want = remaining
		}
		if _, err := io.ReadFull(io.NewSectionReader(d.file, int64(offset), int64(want)), buf[:want]); err != nil {
			return nil, err
		}
		if _, err := h.Write(buf[:want]); err != nil {
			return nil, err
		}
		offset += want
	}
	sum := h.Sum(nil)
	return sum, nil
}

// Replace writes and syncs a complete memory image. The size must match exactly,
// and the caller must prevent competing state changes during replacement.
func (d *DiskImage) Replace(data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(data) != d.size {
		return fmt.Errorf("disk image size %d, replacement size %d", d.size, len(data))
	}
	n, err := d.file.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return d.file.Sync()
}

// ReplaceFrom copies a staged image without loading it all into memory. The size
// must match, and the caller must prevent competing state changes.
func (d *DiskImage) ReplaceFrom(path string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if info.Size() != int64(d.size) {
		return fmt.Errorf("disk image size %d, staged replacement size %d", d.size, info.Size())
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	buf := make([]byte, fileCopyBufferSize)
	for offset := int64(0); offset < int64(d.size); {
		want := len(buf)
		if remaining := int64(d.size) - offset; remaining < int64(want) {
			want = int(remaining)
		}
		if _, err := io.ReadFull(in, buf[:want]); err != nil {
			return err
		}
		n, err := d.file.WriteAt(buf[:want], offset)
		if err != nil {
			return err
		}
		if n != want {
			return io.ErrShortWrite
		}
		offset += int64(want)
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

	batchWindow time.Duration
	batchMax    int
	appendReqCh chan appendReq
	fsyncFn     func(*os.File) error
}

type appendReq struct {
	entry RedoLogItem
	ack   chan error
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
	// Redo records contain arbitrary write payloads and can exceed Scanner's
	// token limit. Read one whole JSON line without imposing that size ceiling.
	reader := bufio.NewReader(f)
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, readErr
		}
		if len(bytes.TrimSpace(line)) != 0 {
			var e RedoLogItem
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, err
			}
			entries = append(entries, e)
		}
		// ReadBytes can return a final record together with EOF; decode it above
		// so an unterminated record is neither skipped nor exempt from validation.
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	r := &RedoLog{path: p, entries: entries, batchWindow: defaultRedoBatchWindow, batchMax: defaultRedoBatchSize, fsyncFn: func(f *os.File) error { return f.Sync() }}
	r.appendReqCh = make(chan appendReq, redoAppendQueueSize)
	go r.runAppender()
	return r, nil
}

func (r *RedoLog) Entries() []RedoLogItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RedoLogItem, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *RedoLog) Append(entry RedoLogItem) error {
	return r.AddItem(entry)
}

func (r *RedoLog) ConfigureBatchingForTest(window time.Duration, max int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if window > 0 {
		r.batchWindow = window
	}
	if max > 0 {
		r.batchMax = max
	}
}

func (r *RedoLog) SetFsyncHookForTest(fn func(*os.File) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fn == nil {
		r.fsyncFn = func(f *os.File) error { return f.Sync() }
		return
	}
	r.fsyncFn = fn
}

func (r *RedoLog) AddItem(entry RedoLogItem) error {
	ack := make(chan error, 1)
	r.appendReqCh <- appendReq{entry: entry, ack: ack}
	return <-ack
}

func (r *RedoLog) runAppender() {
	t := time.NewTimer(time.Hour)
	if !t.Stop() {
		<-t.C
	}
	defer t.Stop()

	batch := make([]appendReq, 0, redoBatchCapacity)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := r.flushBatch(batch)
		for _, req := range batch {
			req.ack <- err
		}
		batch = batch[:0]
	}

	for {
		if len(batch) == 0 {
			req := <-r.appendReqCh
			batch = append(batch, req)
			r.mu.Lock()
			window := r.batchWindow
			r.mu.Unlock()
			if window <= 0 {
				window = time.Millisecond
			}
			t.Reset(window)
		}

		r.mu.Lock()
		max := r.batchMax
		r.mu.Unlock()
		if max <= 0 {
			max = 1
		}

		if len(batch) >= max {
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			flush()
			continue
		}

		select {
		case req := <-r.appendReqCh:
			batch = append(batch, req)
		case <-t.C:
			flush()
		}
	}
}

func (r *RedoLog) flushBatch(batch []appendReq) error {
	f, err := os.OpenFile(r.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	entries := make([]RedoLogItem, 0, len(batch))
	for _, req := range batch {
		b, err := json.Marshal(req.entry)
		if err != nil {
			return err
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			return err
		}
		entries = append(entries, req.entry)
	}
	r.mu.Lock()
	fsyncFn := r.fsyncFn
	r.mu.Unlock()
	if fsyncFn == nil {
		fsyncFn = func(f *os.File) error { return f.Sync() }
	}
	if err := fsyncFn(f); err != nil {
		return err
	}

	r.mu.Lock()
	r.entries = append(r.entries, entries...)
	r.mu.Unlock()
	return nil
}
func (r *RedoLog) LastTID() sinfonia.TransactionID {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return sinfonia.TransactionID{}
	}
	return r.entries[len(r.entries)-1].TransactionId
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

// Reset clears and syncs the redo log when a replica installs a replacement image.
func (r *RedoLog) Reset() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := rewriteRedoLogFile(r.path, nil); err != nil {
		return err
	}
	r.entries = nil
	return syncDir(filepath.Dir(r.path))
}

// rewriteRedoLogFile publishes a complete, synced replacement. Callers update
// their in-memory entries and sync the directory before acknowledging success.
func rewriteRedoLogFile(path string, entries []RedoLogItem) error {
	f, err := os.CreateTemp(filepath.Dir(path), redoTempPrefix)
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
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
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
	mu   sync.Mutex
	path string
	set  map[string]struct{}
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
	return &ForcedAbortLog{path: p, set: out}, nil
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
	if err := f.Sync(); err != nil {
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
	changed := false
	removed := make([]sinfonia.TransactionID, 0)
	for key := range s.set {
		txId, ok := parseTidKey(key)
		if !ok {
			continue
		}
		if txId.Epoch < threshold {
			delete(s.set, key)
			removed = append(removed, txId)
			changed = true
		}
	}
	if !changed {
		return nil, nil
	}
	if err := s.rewriteLocked(); err != nil {
		return nil, err
	}
	return removed, nil
}

// Reset clears and syncs the forced-abort log when replacing a replica's history.
// Keeping an old marker could incorrectly reject a later transaction.
func (s *ForcedAbortLog) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.set
	s.set = map[string]struct{}{}
	if err := s.rewriteLocked(); err != nil {
		s.set = previous
		return err
	}
	return nil
}

func (s *ForcedAbortLog) rewriteLocked() error {
	keys := make([]string, 0, len(s.set))
	for k := range s.set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, k := range keys {
		if _, err := f.WriteString(k + "\n"); err != nil {
			return err
		}
	}
	return f.Sync()
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
	for s.Scan() {
		parts := strings.Split(strings.TrimSpace(s.Text()), " ")
		if len(parts) != 2 {
			continue
		}
		m[parts[0]] = parts[1] == "commit"
	}
	if err := s.Err(); err != nil {
		return nil, err
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
	delete(s.outcomes, k)
	keys := make([]string, 0, len(s.outcomes))
	for key := range s.outcomes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, key := range keys {
		state := "abort"
		if s.outcomes[key] {
			state = "commit"
		}
		if _, err := f.WriteString(key + " " + state + "\n"); err != nil {
			return err
		}
	}
	return f.Sync()
}

// Reset clears the saved decisions. The caller must prevent participant
// operations from running during the reset.
func (s *DecidedTransactionLog) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}
	s.outcomes = map[string]bool{}
	return nil
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
