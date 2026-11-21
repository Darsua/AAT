// Problem 5 (IF4031 M2): 10,000 concurrent Virtual Users (VUs) priority QoS stress test.
//
// Spawns 10,000 concurrent goroutine VUs (default: 9,500 "media" VUs + 500 critical "field" VUs)
// with automatic JWT renewal and per-role HTTP status and latency percentile reporting.
//
// Usage:
//   go run ./scripts/load_test_10k_qos.go -targets http://10.5.27.240,http://10.5.27.241,http://10.5.27.242 -host aat.darsu.dev
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type tokenResp struct {
	AccessToken string `json:"access_token"`
}

func fetchToken(client *http.Client, target, host, cid, secret, scope string) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"client_id":       cid,
		"client_secret":   secret,
		"requested_scope": scope,
	})
	req, err := http.NewRequest("POST", strings.TrimRight(target, "/")+"/auth/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	if host != "" {
		req.Host = host
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}
	var tr tokenResp
	if err := json.Unmarshal(raw, &tr); err != nil {
		return "", err
	}
	return tr.AccessToken, nil
}

type roleStats struct {
	mu        sync.Mutex
	statuses  map[string]int64
	latencies []float64
	activeVUs atomic.Int64
	peakVUs   atomic.Int64
}

func newRoleStats() *roleStats {
	return &roleStats{
		statuses:  make(map[string]int64),
		latencies: make([]float64, 0, 65536),
	}
}

func (s *roleStats) record(code string, latMs float64) {
	s.mu.Lock()
	s.statuses[code]++
	s.latencies = append(s.latencies, latMs)
	s.mu.Unlock()
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func (s *roleStats) summarize(name string, vus int, durSec float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sort.Float64s(s.latencies)
	var total int64
	for _, c := range s.statuses {
		total += c
	}
	rps := float64(total) / durSec
	fmt.Printf("  [%s] configured_vus=%d peak_active_vus=%d total_requests=%d rps=%.2f statuses=%v\n",
		name, vus, s.peakVUs.Load(), total, rps, s.statuses)
	if len(s.latencies) > 0 {
		fmt.Printf("  [%s] latency_ms: min=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f\n",
			name,
			s.latencies[0],
			percentile(s.latencies, 0.50),
			percentile(s.latencies, 0.95),
			percentile(s.latencies, 0.99),
			s.latencies[len(s.latencies)-1],
		)
	}
}

func main() {
	defaultTargets := os.Getenv("TARGETS")
	if defaultTargets == "" {
		defaultTargets = os.Getenv("TARGET_URL")
	}
	if defaultTargets == "" {
		defaultTargets = "http://10.5.27.240,http://10.5.27.241,http://10.5.27.242"
	}

	var (
		mediaVUs   = flag.Int("media-vus", 9500, "number of concurrent media VUs")
		fieldVUs   = flag.Int("field-vus", 500, "number of concurrent field VUs")
		durSec     = flag.Int("duration", 30, "test duration in seconds")
		targetsCSV = flag.String("targets", defaultTargets, "comma-separated ingress target base URLs")
		authURL    = flag.String("auth-url", os.Getenv("AUTH_URL"), "optional separate auth service base URL (defaults to first target)")
		hostHeader = flag.String("host", "aat.darsu.dev", "Host header sent on requests")
	)
	flag.Parse()

	var nodes []string
	for _, raw := range strings.Split(*targetsCSV, ",") {
		trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
		if trimmed != "" {
			nodes = append(nodes, trimmed)
		}
	}
	if len(nodes) == 0 {
		fmt.Fprintln(os.Stderr, "at least one target URL is required")
		os.Exit(1)
	}
	authBase := strings.TrimRight(strings.TrimSpace(*authURL), "/")
	if authBase == "" {
		authBase = nodes[0]
	}

	tr := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   4 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          12000,
		MaxIdleConnsPerHost:   4000,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       60 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		DisableCompression:    true,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   6 * time.Second,
	}

	mediaID := os.Getenv("AUTH_MEDIA_CLIENT_ID")
	if mediaID == "" {
		mediaID = "test-media-client"
	}
	fieldID := os.Getenv("AUTH_FIELD_CLIENT_ID")
	if fieldID == "" {
		fieldID = "test-field-client"
	}
	mediaSecret := os.Getenv("AUTH_MEDIA_CLIENT_SECRET")
	fieldSecret := os.Getenv("AUTH_FIELD_CLIENT_SECRET")
	if mediaSecret == "" || fieldSecret == "" {
		fmt.Fprintln(os.Stderr, "AUTH_MEDIA_CLIENT_SECRET and AUTH_FIELD_CLIENT_SECRET environment variables are required")
		os.Exit(1)
	}

	mediaTok, err := fetchToken(client, authBase, *hostHeader, mediaID, mediaSecret, "media")
	if err != nil {
		fmt.Fprintf(os.Stderr, "media token error: %v\n", err)
		os.Exit(1)
	}
	fieldTok, err := fetchToken(client, authBase, *hostHeader, fieldID, fieldSecret, "field")
	if err != nil {
		fmt.Fprintf(os.Stderr, "field token error: %v\n", err)
		os.Exit(1)
	}

	var mediaTokenVal, fieldTokenVal atomic.Value
	mediaTokenVal.Store(mediaTok)
	fieldTokenVal.Store(fieldTok)

	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if t, err := fetchToken(client, authBase, *hostHeader, mediaID, mediaSecret, "media"); err == nil {
					mediaTokenVal.Store(t)
				}
				if t, err := fetchToken(client, authBase, *hostHeader, fieldID, fieldSecret, "field"); err == nil {
					fieldTokenVal.Store(t)
				}
			}
		}
	}()

	mediaStats := newRoleStats()
	fieldStats := newRoleStats()
	var globalActiveVUs, globalPeakVUs atomic.Int64

	startBarrier := make(chan struct{})
	var readyWG sync.WaitGroup
	var doneWG sync.WaitGroup

	totalVUs := *mediaVUs + *fieldVUs
	readyWG.Add(totalVUs)
	doneWG.Add(totalVUs)

	deadline := time.Now().Add(time.Duration(*durSec)*time.Second + 2*time.Second)

	spawnVU := func(vuID int, isMedia bool, st *roleStats, path string) {
		defer doneWG.Done()
		cur := st.activeVUs.Add(1)
		for {
			old := st.peakVUs.Load()
			if cur <= old || st.peakVUs.CompareAndSwap(old, cur) {
				break
			}
		}
		gcur := globalActiveVUs.Add(1)
		for {
			old := globalPeakVUs.Load()
			if gcur <= old || globalPeakVUs.CompareAndSwap(old, gcur) {
				break
			}
		}
		defer st.activeVUs.Add(-1)
		defer globalActiveVUs.Add(-1)

		rng := rand.New(rand.NewSource(int64(vuID)*1000003 + time.Now().UnixNano()))
		readyWG.Done()
		<-startBarrier

		time.Sleep(time.Duration(rng.Intn(500)) * time.Millisecond)

		iter := 0
		for time.Now().Before(deadline) {
			iter++
			baseURL := nodes[(vuID+iter)%len(nodes)]
			var tok string
			if isMedia {
				tok = mediaTokenVal.Load().(string)
			} else {
				tok = fieldTokenVal.Load().(string)
			}
			req, err := http.NewRequest("GET", baseURL+path, nil)
			if err != nil {
				st.record("err:NewRequest", 0)
				continue
			}
			if *hostHeader != "" {
				req.Host = *hostHeader
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("Accept", "application/json")

			t0 := time.Now()
			resp, err := client.Do(req)
			latMs := float64(time.Since(t0).Microseconds()) / 1000.0
			if err != nil {
				st.record("err:Transport", latMs)
			} else {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				st.record(fmt.Sprintf("%d", resp.StatusCode), latMs)
			}

			sleepDur := time.Duration(250+rng.Intn(300)) * time.Millisecond
			if time.Now().Add(sleepDur).After(deadline) {
				break
			}
			time.Sleep(sleepDur)
		}
	}

	for i := 0; i < *fieldVUs; i++ {
		go spawnVU(i, false, fieldStats, "/api/v1/hazards?fields=summary&limit=20&type=SEISMIC")
	}
	for i := 0; i < *mediaVUs; i++ {
		go spawnVU(*fieldVUs+i, true, mediaStats, "/api/v1/hazards?fields=summary&limit=20&type=VOLCANIC")
	}

	readyWG.Wait()
	actualStart := time.Now()
	deadline = actualStart.Add(time.Duration(*durSec) * time.Second)
	fmt.Printf("=== All %d Virtual Users Spawned & Active (peak_concurrent_vus=%d: %d media + %d field) ===\n",
		totalVUs, globalPeakVUs.Load(), *mediaVUs, *fieldVUs)
	close(startBarrier)

	doneWG.Wait()
	close(stopCh)
	actualDur := time.Since(actualStart).Seconds()

	fmt.Printf("=== 10,000-VU Concurrency Test Completed (duration=%.2fs, peak_concurrent_vus=%d) ===\n",
		actualDur, globalPeakVUs.Load())
	fieldStats.summarize("field (critical)", *fieldVUs, actualDur)
	mediaStats.summarize("media (public)", *mediaVUs, actualDur)
}
