package client

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"
)

type statsResponseTransport []byte

func (body statsResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}

func FuzzAdminHTTPStatsResponseNeverPanics(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not json"))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"minitransactions_committed":1} {}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		client := NewAdminHTTPClient("http://memnode.test", time.Second)
		client.hc.Transport = statsResponseTransport(data)
		_, _ = client.GetStats()
	})
}
