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
	"reflect"
	"sort"
	"sync"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	replayKindPrepare = "prepare"
	replayKindOnePC   = "one_pc"
	replayTempPrefix  = ".replay.log.tmp-"
)

var ErrReplayConflict = errors.New("conflicting replay record for transaction")

type replayRecord struct {
	TID     sinfonia.TransactionID          `json:"tid"`
	Kind    string                          `json:"kind"`
	Prepare *sinfonia.ExecPrepareResponse   `json:"prepare,omitempty"`
	OnePC   *sinfonia.ExecCommit1PCResponse `json:"one_pc,omitempty"`
}

// ReplayStore saves participant replies by transaction ID. A retry gets the
// original vote and data, with its own RequestID filled in by the caller.
type ReplayStore struct {
	mu      sync.Mutex
	path    string
	records map[string]replayRecord
}

// PublicRetryHorizon limits a transaction's age in epochs when a client retries it.
const PublicRetryHorizon uint64 = 2

func OpenReplayStore(dir string) (*ReplayStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "replay.log")
	records, err := loadReplayStoreRecords(path)
	if err != nil {
		return nil, err
	}
	return &ReplayStore{path: path, records: records}, nil
}

// ReplayRetention decides which saved replies are too old to need anymore.
// A reply can be dropped when Epoch + RetryHorizon <= CurrentEpoch, matching
// the node's rule for rejecting old retries.
// A zero RetryHorizon keeps everything. The caller supplies an epoch that only
// moves forward; the store doesn't guess it from the clock.
type ReplayRetention struct {
	// CurrentEpoch is the caller's view of the current retry epoch.
	CurrentEpoch uint64
	// RetryHorizon is the retry window in epochs. Zero keeps all replies.
	RetryHorizon uint64
}

// Expired says whether a transaction is too old to retry.
// A zero RetryHorizon disables expiration.
func (r ReplayRetention) Expired(txId sinfonia.TransactionID) bool {
	if r.RetryHorizon == 0 {
		return false
	}
	// Subtract instead of adding to Epoch so uint64 can't overflow.
	// If the current epoch hasn't reached the retry window, keep everything.
	if r.CurrentEpoch < r.RetryHorizon {
		return false
	}
	return txId.Epoch <= r.CurrentEpoch-r.RetryHorizon
}

// Bytes reads the saved reply log while holding the store's lock.
func (s *ReplayStore) Bytes() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.ReadFile(s.path)
}

// Size returns the log size without reading its contents.
func (s *ReplayStore) Size() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Stat(s.path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ReadAt reads part of the reply log. To get a consistent copy across several
// reads, the caller must hold the replication barrier for the whole sequence.
func (s *ReplayStore) ReadAt(buf []byte, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < 0 {
		return fmt.Errorf("replay read offset %d is negative", offset)
	}
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.ReadFull(io.NewSectionReader(f, offset, int64(len(buf))), buf); err != nil {
		return err
	}
	return nil
}

// SHA256 hashes the log without loading the whole file into memory.
func (s *ReplayStore) SHA256() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, 1<<20)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (s *ReplayStore) GetPrepare(txId sinfonia.TransactionID) (sinfonia.ExecPrepareResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[replayKey(txId, replayKindPrepare)]
	if !ok || record.Prepare == nil {
		return sinfonia.ExecPrepareResponse{}, false
	}
	return clonePrepareReplay(*record.Prepare), true
}

func (s *ReplayStore) GetOnePC(txId sinfonia.TransactionID) (sinfonia.ExecCommit1PCResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[replayKey(txId, replayKindOnePC)]
	if !ok || record.OnePC == nil {
		return sinfonia.ExecCommit1PCResponse{}, false
	}
	return cloneOnePCReplay(*record.OnePC), true
}

func (s *ReplayStore) AppendPrepare(txId sinfonia.TransactionID, response sinfonia.ExecPrepareResponse) error {
	record := replayRecord{TID: txId, Kind: replayKindPrepare, Prepare: prepareReplayPtr(response)}
	return s.append(record)
}

func (s *ReplayStore) AppendOnePC(txId sinfonia.TransactionID, response sinfonia.ExecCommit1PCResponse) error {
	record := replayRecord{TID: txId, Kind: replayKindOnePC, OnePC: onePCReplayPtr(response)}
	return s.append(record)
}

// ReplaceFrom checks and installs a staged reply log, then updates the in-memory
// index once the file is durable. The caller must prevent competing state changes.
func (s *ReplayStore) ReplaceFrom(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	staged, err := os.Open(path)
	if err != nil {
		return err
	}
	defer staged.Close()
	info, err := staged.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("staged replay artifact %q is not a regular file", path)
	}
	records, err := loadReplayRecords(staged)
	if err != nil {
		return err
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), replayTempPrefix)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := copyReaderDurable(tmp, staged, 0o644); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	s.records = records
	return nil
}

// Compact drops replies that are too old to retry. It writes a temporary file
// and renames it, so a crash leaves either the old log or the new one.
// Keep replies still inside the retry window. A zero retry window, or a current
// epoch smaller than the window, removes nothing.
func (s *ReplayStore) Compact(retention ReplayRetention) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := 0
	for _, record := range s.records {
		if !retention.Expired(record.TID) {
			kept++
		}
	}
	// Nothing expired, or cleanup is disabled.
	if kept == len(s.records) || retention.RetryHorizon == 0 {
		return nil
	}

	// Sort by key so the same records produce the same file each time.
	keys := make([]string, 0, kept)
	for key, record := range s.records {
		if !retention.Expired(record.TID) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, key := range keys {
		record := s.records[key]
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		buf.Write(encoded)
		buf.WriteByte('\n')
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), replayTempPrefix)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := io.Copy(tmp, bytes.NewReader(buf.Bytes())); err != nil {
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
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	// The file is durable. Keep the old records before replacing the map so
	// we can rebuild the index from them.
	snapshot := make(map[string]replayRecord, kept)
	for _, key := range keys {
		snapshot[key] = s.records[key]
	}
	s.records = snapshot
	return nil
}

func (s *ReplayStore) append(record replayRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateReplayRecord(record); err != nil {
		return err
	}
	key := replayKey(record.TID, record.Kind)
	if existing, exists := s.records[key]; exists {
		if !reflect.DeepEqual(existing, record) {
			return fmt.Errorf("%w: %s", ErrReplayConflict, key)
		}
		return nil
	}
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		_ = f.Close()
		return err
	}
	line := append(encoded, '\n')
	if written, err := f.Write(line); err != nil {
		_ = f.Close()
		return err
	} else if written != len(line) {
		_ = f.Close()
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.records[key] = cloneReplayRecord(record)
	return nil
}

func loadReplayStoreRecords(path string) (map[string]replayRecord, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	records := make(map[string]replayRecord)
	reader := bufio.NewReader(f)
	var (
		validLen  int64
		lastLine  []byte
		lastStart int64
	)
	for {
		line, readErr := reader.ReadBytes('\n')
		lastStart = validLen
		lastLine = line
		validLen += int64(len(line))
		trimmed := bytes.TrimSpace(line)
		// A record is complete only when its newline was written. Drop a final
		// partial line even if it happens to contain valid JSON.
		complete := len(line) > 0 && line[len(line)-1] == '\n'
		if complete && len(trimmed) != 0 {
			var record replayRecord
			if e := json.Unmarshal(trimmed, &record); e != nil {
				return nil, e
			} else {
				if e := validateReplayRecord(record); e != nil {
					return nil, e
				}
				key := replayKey(record.TID, record.Kind)
				if existing, exists := records[key]; exists {
					if !reflect.DeepEqual(existing, record) {
						return nil, fmt.Errorf("%w: %s", ErrReplayConflict, key)
					}
				} else {
					records[key] = cloneReplayRecord(record)
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	// Remove a final partial record and sync the truncation. Recovery must use
	// only complete records, never guess an outcome from a half-written reply.
	if len(lastLine) > 0 && lastLine[len(lastLine)-1] != '\n' {
		if err := truncateDurableReplay(path, lastStart); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// truncateDurableReplay removes a partial tail, then syncs the file and its
// parent directory. length must end at the last complete record's newline.
func truncateDurableReplay(path string, length int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := f.Truncate(length); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func loadReplayRecords(r io.Reader) (map[string]replayRecord, error) {
	records := make(map[string]replayRecord)
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) != 0 {
			var record replayRecord
			if err := json.Unmarshal(trimmed, &record); err != nil {
				return nil, err
			}
			if err := validateReplayRecord(record); err != nil {
				return nil, err
			}
			key := replayKey(record.TID, record.Kind)
			if existing, exists := records[key]; exists {
				if !reflect.DeepEqual(existing, record) {
					return nil, fmt.Errorf("%w: %s", ErrReplayConflict, key)
				}
			} else {
				records[key] = cloneReplayRecord(record)
			}
		}
		// Every record needs a newline. A staged log must already be complete;
		// only the live log loader repairs a partial tail.
		if len(line) > 0 && line[len(line)-1] != '\n' {
			return nil, errors.New("incomplete replay JSONL record")
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return records, nil
}

func validateReplayRecord(record replayRecord) error {
	switch record.Kind {
	case replayKindPrepare:
		if record.Prepare == nil || record.OnePC != nil {
			return errors.New("invalid prepare replay record payload")
		}
	case replayKindOnePC:
		if record.OnePC == nil || record.Prepare != nil {
			return errors.New("invalid one-phase replay record payload")
		}
	default:
		return fmt.Errorf("unknown replay record kind %q", record.Kind)
	}
	return nil
}

func replayKey(txId sinfonia.TransactionID, kind string) string {
	return kind + ":" + tidKey(txId)
}

func cloneReplayRecord(record replayRecord) replayRecord {
	cloned := record
	if record.Prepare != nil {
		cloned.Prepare = prepareReplayPtr(*record.Prepare)
	}
	if record.OnePC != nil {
		cloned.OnePC = onePCReplayPtr(*record.OnePC)
	}
	return cloned
}

func prepareReplayPtr(response sinfonia.ExecPrepareResponse) *sinfonia.ExecPrepareResponse {
	cloned := clonePrepareReplay(response)
	return &cloned
}

func onePCReplayPtr(response sinfonia.ExecCommit1PCResponse) *sinfonia.ExecCommit1PCResponse {
	cloned := cloneOnePCReplay(response)
	return &cloned
}

func clonePrepareReplay(response sinfonia.ExecPrepareResponse) sinfonia.ExecPrepareResponse {
	response.ReadData = cloneReplayReadData(response.ReadData)
	response.LockConflict = cloneLockConflict(response.LockConflict)
	return response
}

func cloneOnePCReplay(response sinfonia.ExecCommit1PCResponse) sinfonia.ExecCommit1PCResponse {
	response.ReadData = cloneReplayReadData(response.ReadData)
	response.LockConflict = cloneLockConflict(response.LockConflict)
	return response
}

func cloneReplayReadData(input map[uint32][]byte) map[uint32][]byte {
	if input == nil {
		return nil
	}
	output := make(map[uint32][]byte, len(input))
	for index, data := range input {
		output[index] = append([]byte(nil), data...)
	}
	return output
}

func cloneLockConflict(conflict *sinfonia.LockConflict) *sinfonia.LockConflict {
	if conflict == nil {
		return nil
	}
	cloned := *conflict
	return &cloned
}
