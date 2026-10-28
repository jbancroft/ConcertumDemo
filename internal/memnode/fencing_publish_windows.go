//go:build windows

package memnode

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

const moveFileWriteThrough = 0x8

var (
	kernel32MoveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")
)

func publishFencingGeneration(temp, final string) (bool, error) {
	from, err := syscall.UTF16PtrFromString(temp)
	if err != nil {
		return false, err
	}
	to, err := syscall.UTF16PtrFromString(final)
	if err != nil {
		return false, err
	}
	r1, _, callErr := kernel32MoveFileEx.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), moveFileWriteThrough)
	if r1 == 0 {
		return false, callErr
	}
	return true, nil
}

func syncFencingNamespace(_ string) error {
	return nil
}

func removeFencingGeneration(path string) error {
	tombstone := filepath.Join(filepath.Dir(path), ".fencing-generation.retired")
	if err := os.Remove(tombstone); err != nil && !os.IsNotExist(err) {
		return err
	}
	from, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(tombstone)
	if err != nil {
		return err
	}
	r1, _, callErr := kernel32MoveFileEx.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), moveFileWriteThrough)
	if r1 == 0 {
		if os.IsNotExist(callErr) {
			return os.ErrNotExist
		}
		return callErr
	}
	// The write-through rename durably removes the active generation name.
	// A leftover retired file is harmless; cleanup can wait.
	return os.Remove(tombstone)
}
