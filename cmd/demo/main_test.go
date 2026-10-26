package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestDemoOverTCPAndDurableReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, t.TempDir(), &output); err != nil {
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
