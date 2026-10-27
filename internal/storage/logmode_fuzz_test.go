package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func FuzzLoadProcessedPointerNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("0\n"))
	f.Add([]byte("-1"))
	f.Add([]byte("9223372036854775807"))
	f.Add([]byte("18446744073709551615"))
	f.Add([]byte("invalid"))
	dir := f.TempDir()
	path := filepath.Join(dir, "processed.ptr")
	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := LoadProcessedPointer(dir)
		trimmed := strings.TrimSpace(string(data))
		if trimmed == "" {
			if err != nil || got != 0 {
				t.Fatalf("blank pointer = %d, %v; want 0, nil", got, err)
			}
			return
		}
		want, parseErr := strconv.Atoi(trimmed)
		if (err == nil) != (parseErr == nil) {
			t.Fatalf("decode error = %v, strconv error = %v", err, parseErr)
		}
		if err == nil && got != want {
			t.Fatalf("decoded pointer = %d, want %d", got, want)
		}
	})
}

func FuzzParseTIDKeyNeverPanics(f *testing.F) {
	f.Add("1:2:3")
	f.Add("18446744073709551615:18446744073709551615:18446744073709551615")
	f.Add("1:2")
	f.Add("-1:2:3")
	f.Add("1:2:3:4")
	f.Fuzz(func(t *testing.T, key string) {
		txId, ok := parseTidKey(key)
		if !ok {
			return
		}
		canonical := tidKey(txId)
		reparsed, ok := parseTidKey(canonical)
		if !ok || reparsed != txId {
			t.Fatalf("canonical key %q did not preserve transaction ID %+v", canonical, txId)
		}
	})
}
