package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jbancroft/ConcertumDemo/internal/proto/sinfonia"
)

const (
	appliedReceiptFile       = "applied.gob"
	appliedReceiptTempPrefix = ".applied.gob.tmp-"
)

// LoadAppliedReceipts restores the peer application evidence retained for redo GC.
// A missing file means no notifications have been acknowledged yet.
func LoadAppliedReceipts(dir string) (map[sinfonia.TransactionID]map[uint32]bool, error) {
	path := filepath.Join(dir, appliedReceiptFile)
	receipts := map[sinfonia.TransactionID]map[uint32]bool{}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return receipts, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	reader := io.NewSectionReader(f, 0, info.Size())
	if err := readGobRecord(reader, &receipts); err != nil {
		return nil, fmt.Errorf("%s: read applied receipts: %w", path, err)
	}
	if offset, err := reader.Seek(0, io.SeekCurrent); err != nil || offset != info.Size() {
		return nil, fmt.Errorf("%s: trailing data after applied receipts", path)
	}
	return receipts, nil
}

// StoreAppliedReceipts publishes a complete snapshot before the caller acknowledges
// notifications. Callers serialize access and remove receipts only after durable
// removal of the corresponding redo entries.
func StoreAppliedReceipts(dir string, receipts map[sinfonia.TransactionID]map[uint32]bool) error {
	if err := publishLogFile(filepath.Join(dir, appliedReceiptFile), appliedReceiptTempPrefix, func(w io.Writer) error {
		return writeGobRecord(w, receipts)
	}); err != nil {
		return err
	}
	return syncDir(dir)
}
