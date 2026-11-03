package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInvalidConfigurationDoesNotCreateStorage(t *testing.T) {
	for _, args := range [][]string{
		{"-id", "0"}, {"-id", "4294967296"}, {"-size", "0"},
		{"-peer", "1=127.0.0.1:7001"},
		{"-peer", "0=127.0.0.1:7002"},
		{"-peer", "2=127.0.0.1:7002", "-peer", "2=127.0.0.1:7003"},
		{"-peer", "2=invalid"}, {"unexpected-argument"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			dir := filepath.Join(t.TempDir(), "node")
			flags := append([]string{"-data-dir", dir, "-listen", "127.0.0.1:0"}, args...)
			if err := run(ctx, flags, nil, io.Discard, io.Discard); err == nil {
				t.Fatal("invalid node configuration was accepted")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("invalid configuration created storage: %v", err)
			}
		})
	}
}

func TestDataDirectoryRequiredBeforeListening(t *testing.T) {
	for _, args := range [][]string{nil, {"-data-dir", ""}} {
		flags := append([]string{"-listen", "invalid"}, args...)
		err := run(t.Context(), flags, nil, io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "-data-dir is required") {
			t.Fatalf("missing data directory: %v", err)
		}
	}
}
