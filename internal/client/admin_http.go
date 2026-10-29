package client

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jbancroft/ConcertumDemo/internal/memnode"
)

const defaultStatsTimeout = 5 * time.Second

type AdminHTTPClient struct {
	baseURL string
	hc      *http.Client
}

func NewAdminHTTPClient(baseURL string, timeout time.Duration) *AdminHTTPClient {
	if timeout <= 0 {
		timeout = defaultStatsTimeout
	}
	return &AdminHTTPClient{baseURL: strings.TrimRight(baseURL, "/"), hc: &http.Client{Timeout: timeout}}
}

func (c *AdminHTTPClient) GetStats() (memnode.StatsResponse, error) {
	var out memnode.StatsResponse
	resp, err := c.hc.Get(c.baseURL + "/stats")
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("stats status %d", resp.StatusCode)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}
