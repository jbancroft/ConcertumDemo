package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LoadEpochFloor reads the minimum epoch that must survive a restart after GC.
func LoadEpochFloor(dir string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(dir, "epoch.floor"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
}

// StoreEpochFloor must complete before transaction history is discarded.
// The participant serializes calls and only advances the floor.
func StoreEpochFloor(dir string, epoch uint64) error {
	file, err := os.CreateTemp(dir, ".epoch.floor.tmp-")
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()
	if _, err := file.WriteString(strconv.FormatUint(epoch, 10)); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(dir, "epoch.floor")); err != nil {
		return err
	}
	return syncDir(dir)
}
