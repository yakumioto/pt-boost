// Package statusapi implements the GET /status HTTP endpoint described in
// docs/design-overview.md §7 and issue #7: a read-only, JSON view of every
// configured site's current hot-torrent pool, its most recent scan time,
// and each pool member's cold-detection verdict and sample window.
//
// This package never calls torrentfs-go or any PT site itself — it only
// reads whatever a Scheduler (via the PoolReporter seam) already has in
// memory. Prometheus /metrics is explicitly out of scope for v1 (issue
// #7's acceptance criteria rule it out).
package statusapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// PoolReporter is the read-only view one site's Scheduler exposes to this
// endpoint. *scheduler.Scheduler satisfies it implicitly via its existing
// exported Pool()/LastScanAt() methods (both already mutex-protected
// against the concurrent scan/cold-detection loops, see scheduler.go) —
// this package adds no locking of its own and does not need to.
type PoolReporter interface {
	Pool() []scheduler.PoolMember
	LastScanAt() time.Time
}

// Site pairs one configured site's name with its Scheduler (as a
// PoolReporter), for NewHandler.
type Site struct {
	Name     string
	Reporter PoolReporter
}

// NewHandler returns the GET /status http.Handler describing every given
// site's current pool. It never fails on a site whose pool hasn't been
// rebuilt yet since startup: PoolReporter naturally reports an empty pool
// and a zero LastScanAt in that case, which this handler renders as an
// empty "pool" array and a null "last_scan_at" rather than an error
// (issue #7: "/status 端点在服务刚启动、池还未重建完成时也能正常响应").
func NewHandler(sites []Site) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := response{Sites: make(map[string]siteStatus, len(sites))}
		for _, s := range sites {
			resp.Sites[s.Name] = buildSiteStatus(s.Reporter)
		}

		w.Header().Set("Content-Type", "application/json")
		// Encoding a plain map/struct of strings, bools, and numbers
		// cannot fail; any error here would mean a bug in this package,
		// not a caller-facing condition worth a 5xx status for.
		_ = json.NewEncoder(w).Encode(resp)
	})
}

type response struct {
	Sites map[string]siteStatus `json:"sites"`
}

type siteStatus struct {
	LastScanAt *string      `json:"last_scan_at"`
	Pool       []poolMember `json:"pool"`
}

type poolMember struct {
	InfoHash               string  `json:"info_hash"`
	TorrentFSID            string  `json:"torrentfs_id"`
	JoinedAt               string  `json:"joined_at"`
	Cold                   bool    `json:"cold"`
	LastRateBytesPerSecond int64   `json:"last_rate_bytes_per_second"`
	SampleCount            int     `json:"sample_count"`
	OldestSampleAt         *string `json:"oldest_sample_at"`
	NewestSampleAt         *string `json:"newest_sample_at"`
	WindowSpanSeconds      float64 `json:"window_span_seconds"`
}

func buildSiteStatus(r PoolReporter) siteStatus {
	st := siteStatus{
		LastScanAt: formatTimeOrNil(r.LastScanAt()),
		Pool:       make([]poolMember, 0),
	}
	for _, m := range r.Pool() {
		st.Pool = append(st.Pool, poolMember{
			InfoHash:               m.InfoHash,
			TorrentFSID:            m.TorrentFSID,
			JoinedAt:               m.JoinedAt.UTC().Format(time.RFC3339),
			Cold:                   m.Cold,
			LastRateBytesPerSecond: m.LastRateBytesPerSecond,
			SampleCount:            m.SampleCount,
			OldestSampleAt:         formatTimeOrNil(m.OldestSampleAt),
			NewestSampleAt:         formatTimeOrNil(m.NewestSampleAt),
			WindowSpanSeconds:      m.WindowSpan.Seconds(),
		})
	}
	// Defensive: Scheduler.Pool() already sorts by InfoHash, but this
	// package's own contract (PoolReporter) doesn't require that of every
	// implementation, so re-sort for a deterministic response either way.
	sort.Slice(st.Pool, func(i, j int) bool { return st.Pool[i].InfoHash < st.Pool[j].InfoHash })
	return st
}

// formatTimeOrNil renders t as RFC3339 UTC, or nil for the zero time.Time
// (meaning "never happened yet" — not scanned, or no samples taken).
func formatTimeOrNil(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}
