package memnode

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func FuzzLoadFencingGenerationNeverPanics(f *testing.F) {
	f.Add([]byte("1\n"))
	f.Add([]byte(" 1 "))
	f.Add([]byte("0"))
	f.Add([]byte("2"))
	f.Add([]byte("18446744073709551615"))
	f.Add([]byte("18446744073709551616"))
	f.Add([]byte("invalid"))
	dir := f.TempDir()
	path := fencingGenerationPath(dir, 1)
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := loadFencingGeneration(dir)
		actual, parseErr := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		wantSuccess := parseErr == nil && actual == 1
		if (err == nil) != wantSuccess {
			t.Fatalf("generation = %d, error = %v; parsed body = %d, %v", got, err, actual, parseErr)
		}
		if err == nil && got != 1 {
			t.Fatalf("generation = %d, want 1", got)
		}
	})
}
