package storage

import (
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
	replayKindOnePC   = "one_pc"
	replayKindPrepare = "prepare"
	replayTempPrefix  = ".replay.log.tmp-"
)

var ErrReplayConflict = errors.New("conflicting replay record for transaction")

type replayRecord struct {
	TID     sinfonia.TransactionID
	Kind    string
	Prepare *sinfonia.ExecPrepareResponse
	OnePC   *sinfonia.ExecCommit1PCResponse
}

// ReplayStore saves participant replies by transaction ID. A retry gets the
// original vote and data, with its own RequestID filled in by the caller.
type ReplayStore struct {
	mu      sync.Mutex
	path    string
	records map[string]replayRecord
}

// PublicRetryHorizon rejects transaction IDs two UTC-hour epochs behind the node.
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
// An unpinned reply can be dropped when Epoch + RetryHorizon <= CurrentEpoch,
// matching the node's rule for rejecting old retries.
// A zero RetryHorizon keeps everything. The caller supplies an epoch that only
// moves forward; the store doesn't guess it from the clock.
type ReplayRetention struct {
	// CurrentEpoch is the caller's view of the current retry epoch.
	CurrentEpoch uint64
	// RetryHorizon is the retry window in epochs. Zero keeps all replies.
	RetryHorizon uint64
	// Keep pins records still needed by in-flight transactions or recovery.
	Keep map[sinfonia.TransactionID]struct{}
}

// Expired says whether a transaction is outside the retry window and not pinned.
// A zero RetryHorizon disables expiration.
func (r ReplayRetention) Expired(txId sinfonia.TransactionID) bool {
	if _, keep := r.Keep[txId]; keep {
		return false
	}
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

// Compact drops replies that are too old to retry. It writes a temporary file
// and renames it, so a crash leaves either the old log or the new one.
// Keep replies inside the retry window or explicitly pinned by the caller.
// A zero retry window, or a current epoch smaller than the window, removes nothing.
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
	if err := publishLogFile(s.path, replayTempPrefix, func(w io.Writer) error {
		for _, key := range keys {
			if err := writeGobRecord(w, s.records[key]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Match the published file even if the directory sync fails.
	snapshot := make(map[string]replayRecord, kept)
	for _, key := range keys {
		snapshot[key] = s.records[key]
	}
	s.records = snapshot
	return syncDir(filepath.Dir(s.path))
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
	if err := writeGobRecord(f, record); err != nil {
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
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader := io.NewSectionReader(f, 0, info.Size())
	for {
		start, err := reader.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		var record replayRecord
		err = readGobRecord(reader, &record)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errIncompleteGobRecord) {
			if err := truncateDurableReplay(path, start); err != nil {
				return nil, err
			}
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s at byte %d: %w", path, start, err)
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
	return records, nil
}

// truncateDurableReplay removes a partial tail, then syncs the file and its
// parent directory. length must end at the last complete gob record.
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
