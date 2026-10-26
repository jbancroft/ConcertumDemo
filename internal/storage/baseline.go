package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	baselineJournalDirName   = ".baseline-install"
	baselineTempPrefix       = ".baseline-install.tmp-"
	baselineRestorePrefix    = ".baseline-restore.tmp-"
	baselineTransferPrefix   = ".baseline-transfer-"
	baselineTombstonePrefix  = ".baseline-install.done-"
	baselineJournalVersion   = 2
	baselineJournalVersionV1 = 1
)

var ErrBaselineInstallInProgress = errors.New("replication baseline install recovery is required")

var baselineInstallFiles = []string{
	"image.bin",
	"redo.log",
	"decided.log",
	"forced_abort.log",
	"processed.ptr",
	"replay.log",
}

var baselineInstallFilesV1 = []string{
	"image.bin",
	"redo.log",
	"decided.log",
	"forced_abort.log",
	"processed.ptr",
}

type baselineJournal struct {
	Version int                   `json:"version"`
	Files   []baselineJournalFile `json:"files"`
}

type baselineJournalFile struct {
	Name   string `json:"name"`
	Exists bool   `json:"exists"`
	Mode   uint32 `json:"mode,omitempty"`
	Size   int64  `json:"size,omitempty"`
}

// BaselineInstallJournal records the old durable replica files before an
// install mutates any of them. A journal left behind by a crashed process is
// rolled back on the next durable-node open; a journal removed by Commit means
// every new file was already synchronized and is therefore the new complete
// baseline.
type BaselineInstallJournal struct {
	dir     string
	journal string
}

// BeginBaselineInstall creates and durably publishes a rollback journal. The
// caller must keep the journal until all new durable files have been written
// and synchronized.
func BeginBaselineInstall(dir string) (*BaselineInstallJournal, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("baseline install directory is required")
	}
	journalPath := filepath.Join(dir, baselineJournalDirName)
	if _, err := os.Stat(journalPath); err == nil {
		return nil, ErrBaselineInstallInProgress
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	tmp, err := os.MkdirTemp(dir, baselineTempPrefix)
	if err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(tmp)
		}
	}()

	manifest := baselineJournal{Version: baselineJournalVersion, Files: make([]baselineJournalFile, 0, len(baselineInstallFiles))}
	for _, name := range baselineInstallFiles {
		path := filepath.Join(dir, name)
		info, statErr := os.Stat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			manifest.Files = append(manifest.Files, baselineJournalFile{Name: name})
			continue
		}
		if statErr != nil {
			return nil, statErr
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("baseline journal source %s is not a regular file", name)
		}
		if err := copyFileDurable(path, filepath.Join(tmp, name), info.Mode().Perm()); err != nil {
			return nil, err
		}
		manifest.Files = append(manifest.Files, baselineJournalFile{Name: name, Exists: true, Mode: uint32(info.Mode().Perm()), Size: info.Size()})
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if err := writeDurable(filepath.Join(tmp, "manifest.json"), manifestBytes, 0o644); err != nil {
		return nil, err
	}
	if err := syncDir(tmp); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, journalPath); err != nil {
		return nil, err
	}
	published = true
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return &BaselineInstallJournal{dir: dir, journal: journalPath}, nil
}

// Commit atomically retires the rollback journal only after the caller has
// synchronized the complete new baseline. If publication or its directory
// sync fails, startup can still select the active journal and recover the old
// state; cleanup of the retired tombstone is best effort.
func (j *BaselineInstallJournal) Commit() error {
	if j == nil || j.journal == "" {
		return errors.New("baseline install journal is nil")
	}
	tombstone, err := os.MkdirTemp(j.dir, baselineTombstonePrefix)
	if err != nil {
		return err
	}
	if err := os.Remove(tombstone); err != nil {
		return err
	}
	if err := os.Rename(j.journal, tombstone); err != nil {
		return err
	}
	if err := syncDir(j.dir); err != nil {
		return err
	}
	// Tombstone cleanup is deliberately best effort. Its presence cannot make
	// the active journal reappear, so a crash here still selects the new state.
	_ = os.RemoveAll(tombstone)
	_ = syncDir(j.dir)
	return nil
}

// RecoverBaselineInstall rolls back a published but uncommitted baseline
// install. It is called before opening durable storage files, so no live file
// descriptor can observe the partially restored state.
func RecoverBaselineInstall(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	journalPath := filepath.Join(dir, baselineJournalDirName)
	if _, err := os.Stat(journalPath); err == nil {
		if err := recoverJournal(dir, journalPath); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return removeBaselineTemps(dir)
}

func recoverJournal(dir, journalPath string) error {
	manifestBytes, err := os.ReadFile(filepath.Join(journalPath, "manifest.json"))
	if err != nil {
		return err
	}
	var manifest baselineJournal
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	var installFiles []string
	switch manifest.Version {
	case baselineJournalVersionV1:
		installFiles = baselineInstallFilesV1
	case baselineJournalVersion:
		installFiles = baselineInstallFiles
	default:
		return fmt.Errorf("unsupported baseline journal version or file set")
	}
	if len(manifest.Files) != len(installFiles) {
		return fmt.Errorf("unsupported baseline journal version or file set")
	}
	known := make(map[string]struct{}, len(installFiles))
	for _, name := range installFiles {
		known[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, entry := range manifest.Files {
		if _, ok := known[entry.Name]; !ok {
			return fmt.Errorf("baseline journal contains unexpected file %q", entry.Name)
		}
		if _, ok := seen[entry.Name]; ok {
			return fmt.Errorf("baseline journal repeats file %q", entry.Name)
		}
		seen[entry.Name] = struct{}{}
	}
	if len(seen) != len(known) {
		return errors.New("baseline journal is missing a durable state file")
	}
	// Validate the entire rollback set before restoring any live file. A
	// missing or truncated backup must fail closed, never expose a mixed set.
	for _, entry := range manifest.Files {
		if !entry.Exists {
			continue
		}
		backupPath := filepath.Join(journalPath, entry.Name)
		info, err := os.Stat(backupPath)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != entry.Size {
			return fmt.Errorf("baseline journal backup %q is incomplete", entry.Name)
		}
	}
	for _, entry := range manifest.Files {
		livePath := filepath.Join(dir, entry.Name)
		if !entry.Exists {
			if err := os.Remove(livePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		mode := os.FileMode(entry.Mode)
		if mode == 0 {
			mode = 0o644
		}
		if err := restoreFile(filepath.Join(journalPath, entry.Name), livePath, mode); err != nil {
			return err
		}
	}
	tombstone, err := os.MkdirTemp(dir, baselineTombstonePrefix)
	if err != nil {
		return err
	}
	if err := os.Remove(tombstone); err != nil {
		return err
	}
	if err := os.Rename(journalPath, tombstone); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	_ = os.RemoveAll(tombstone)
	_ = syncDir(dir)
	return nil
}

func removeBaselineTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, baselineTempPrefix) || strings.HasPrefix(name, baselineRestorePrefix) || strings.HasPrefix(name, baselineTransferPrefix) || strings.HasPrefix(name, baselineTombstonePrefix) || strings.HasPrefix(name, ".processed.ptr.tmp-") || strings.HasPrefix(name, replayTempPrefix) {
			if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), baselineRestorePrefix)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := copyReaderDurable(tmp, in, mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dst))
}

func copyFileDurable(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if err := copyReaderDurable(out, in, mode); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func copyReaderDurable(out *os.File, src io.Reader, mode os.FileMode) error {
	if _, err := io.Copy(out, src); err != nil {
		return err
	}
	if err := out.Chmod(mode); err != nil {
		return err
	}
	return out.Sync()
}

func writeDurable(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
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
