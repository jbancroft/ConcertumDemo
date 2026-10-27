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

	"github.com/jbancroft/concertum2/internal/proto"
)

const (
	replayKindPrepare = "prepare"
	replayKindOnePC   = "one_pc"
	replayTempPrefix  = ".replay.log.tmp-"
)

var ErrReplayConflict = errors.New("conflicting replay record for transaction")

type replayRecord struct {
	TID     proto.TransactionID          `json:"tid"`
	Kind    string                       `json:"kind"`
	Prepare *proto.ExecPrepareResponse   `json:"prepare,omitempty"`
	OnePC   *proto.ExecCommit1PCResponse `json:"one_pc,omitempty"`
}

// ReplayStore durably retains terminal participant responses by transaction
// ID. RequestID is delivery-attempt metadata and is replaced by the caller on
// replay; the vote and response payload remain the original result.
type ReplayStore struct {
	mu      sync.Mutex
	path    string
	records map[string]replayRecord
}

// PublicRetryHorizon is the number of epochs for which a transaction ID may
// still be retried by the memnode public request path.
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

// ReplayRetention is a point-in-time snapshot of the public transaction-ID retry
// horizon used to decide which terminal replay records may be reclaimed.
//
// A record whose TID.Epoch > 0 is expired (and eligible for reclamation) when
// TID.Epoch + RetryHorizon <= CurrentEpoch. This matches the memnode stale-
// epoch reject rule (TID.Epoch > 0 && TID.Epoch + horizon <= currentEpoch): a
// TID that has scrolled past the retry horizon can no longer be the subject of a
// public retry, so its replay record is safe to drop. Records with Epoch == 0
// are legacy TIDs that are never subject to the epoch horizon and are always
// retained. A RetryHorizon of 0 disables reclamation entirely (unbounded
// retention). Retention is only ever evaluated against a caller-supplied,
// monotonic CurrentEpoch; the store never infers the current epoch, so a stale
// or zero CurrentEpoch cannot cause reclamation to over-reclaim.
type ReplayRetention struct {
	// CurrentEpoch is the caller's view of the current retry epoch.
	CurrentEpoch uint64
	// RetryHorizon is the number of epochs for which a TID remains retryable
	// after its own epoch. 0 disables reclamation (retain all).
	RetryHorizon uint64
}

// Expired reports whether a record with the given TID is outside r's retry horizon.
// It is the inverse of "still retryable"; it never returns true for Epoch 0 or
// when reclamation is disabled.
func (r ReplayRetention) Expired(txId proto.TransactionID) bool {
	if txId.Epoch == 0 || r.RetryHorizon == 0 {
		return false
	}
	// Compare without forming Epoch+RetryHorizon to avoid uint64 overflow:
	// Epoch + Horizon <= Current  <=>  Epoch <= Current - Horizon (when
	// Current >= Horizon). If Current < Horizon no in-horizon TID can be
	// expired, so nothing is reclaimed.
	if r.CurrentEpoch <= r.RetryHorizon {
		return false
	}
	return txId.Epoch <= r.CurrentEpoch-r.RetryHorizon
}

// Bytes snapshots the durable replay artifact under the store mutex.
func (s *ReplayStore) Bytes() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.ReadFile(s.path)
}

// Size returns the current durable replay artifact size without materializing
// the log in memory.
func (s *ReplayStore) Size() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := os.Stat(s.path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ReadAt reads one bounded portion of the durable replay artifact. Callers
// that need a consistent sequence must hold the higher-level replication
// barrier while issuing a sequence of reads.
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

// SHA256 computes the replay artifact digest in bounded chunks.
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

func (s *ReplayStore) GetPrepare(txId proto.TransactionID) (proto.ExecPrepareResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[replayKey(txId, replayKindPrepare)]
	if !ok || record.Prepare == nil {
		return proto.ExecPrepareResponse{}, false
	}
	return clonePrepareReplay(*record.Prepare), true
}

func (s *ReplayStore) GetOnePC(txId proto.TransactionID) (proto.ExecCommit1PCResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[replayKey(txId, replayKindOnePC)]
	if !ok || record.OnePC == nil {
		return proto.ExecCommit1PCResponse{}, false
	}
	return cloneOnePCReplay(*record.OnePC), true
}

func (s *ReplayStore) AppendPrepare(txId proto.TransactionID, response proto.ExecPrepareResponse) error {
	record := replayRecord{TID: txId, Kind: replayKindPrepare, Prepare: prepareReplayPtr(response)}
	return s.append(record)
}

func (s *ReplayStore) AppendOnePC(txId proto.TransactionID, response proto.ExecCommit1PCResponse) error {
	record := replayRecord{TID: txId, Kind: replayKindOnePC, OnePC: onePCReplayPtr(response)}
	return s.append(record)
}

// ReplaceFrom validates a staged replay artifact, atomically publishes it at
// the store path, and refreshes the in-memory index only after the new file is
// durable. The caller must hold any higher-level state transition barrier.
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

// Compact reclaims replay records whose TIDs have scrolled past the public
// retry horizon described by retention. The rewrite is safe at the storage
// boundary: only records that can no longer be the subject of a public retry are removed, and
// the rewrite is atomic (staged to a temp file and renamed) so a crash during
// compaction leaves either the old or the new artifact, never a mixed state.
// Records with TID.Epoch == 0 and records the retention rules deem still
// retryable are preserved. A retention with RetryHorizon == 0 or CurrentEpoch
// <= RetryHorizon reclaims nothing.
func (s *ReplayStore) Compact(retention ReplayRetention) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := 0
	for _, record := range s.records {
		if !retention.Expired(record.TID) {
			kept++
		}
	}
	// No reclaimable records or reclamation disabled: nothing to do.
	if kept == len(s.records) || retention.RetryHorizon == 0 {
		return nil
	}

	// Rebuild the artifact deterministically from the retained records. Order by
	// key so the rewritten file is stable across compactions.
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
	// The artifact is now durable; refresh the in-memory index from the
	// retained records. Snapshot before reassigning so we do not read from the
	// new (empty) map.
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
		// Only newline-terminated records are complete. A final line without
		// its delimiter is a torn write and must be discarded, even when the
		// bytes happen to contain valid JSON.
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
	// A non-empty final line that is not newline-terminated is a torn tail: a
	// record whose terminating newline was never written. Truncate it durably so
	// the artifact contains only completed records and no outcome is ever
	// synthesized.
	if len(lastLine) > 0 && lastLine[len(lastLine)-1] != '\n' {
		if err := truncateDurableReplay(path, lastStart); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// truncateDurableReplay physically truncates the replay artifact to length bytes
// and makes the truncation durable by syncing the file and its parent
// directory. It is only used to remove a torn tail, so length is always the
// boundary of the last completed (newline-terminated) record.
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
		// JSONL requires every non-empty record to end in a newline. Unlike the
		// live log loader, staged artifacts cannot be repaired in place.
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

func replayKey(txId proto.TransactionID, kind string) string {
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

func prepareReplayPtr(response proto.ExecPrepareResponse) *proto.ExecPrepareResponse {
	cloned := clonePrepareReplay(response)
	return &cloned
}

func onePCReplayPtr(response proto.ExecCommit1PCResponse) *proto.ExecCommit1PCResponse {
	cloned := cloneOnePCReplay(response)
	return &cloned
}

func clonePrepareReplay(response proto.ExecPrepareResponse) proto.ExecPrepareResponse {
	response.ReadData = cloneReplayReadData(response.ReadData)
	response.LockConflict = cloneLockConflict(response.LockConflict)
	return response
}

func cloneOnePCReplay(response proto.ExecCommit1PCResponse) proto.ExecCommit1PCResponse {
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

func cloneLockConflict(conflict *proto.LockConflict) *proto.LockConflict {
	if conflict == nil {
		return nil
	}
	cloned := *conflict
	return &cloned
}
