package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestDemoOverTCPAndDurableReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), demoTimeout)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, os.Getenv("MEMNODE_BIN"), t.TempDir(), &output); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"balances = 75, 125", "ordered range = 33..40",
		"[{1 100} {2 250}]", "Recovery: reopened both nodes",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in output:\n%s", want, output.String())
		}
	}
}
