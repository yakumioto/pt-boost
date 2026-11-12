package statusapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// fakeReporter is an in-memory PoolReporter double so these tests exercise
// only the HTTP/JSON wiring, not a real Scheduler (see docs/to-spec seam
// convention: Scheduler's own exported methods, Pool()/LastScanAt(), are
// the seam; this package only needs something that returns the same
// shapes).
type fakeReporter struct {
	pool       []scheduler.PoolMember
	lastScanAt time.Time
}

func (f fakeReporter) Pool() []scheduler.PoolMember { return f.pool }
func (f fakeReporter) LastScanAt() time.Time        { return f.lastScanAt }

func doGetStatus(t *testing.T, sites []Site) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	NewHandler(sites).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding response body: %v; body=%s", err, rec.Body.String())
	}
	return out
}

func TestHandler_NotYetRebuilt_ReturnsEmptyPoolAndNullLastScanAt(t *testing.T) {
	// A Scheduler that hasn't run RebuildFromTorrentFS/Scan yet (service
	// just started) reports an empty pool and the zero time.Time from
	// LastScanAt(). The endpoint must still respond 200, not error or
	// hang (issue #7 acceptance criterion).
	sites := []Site{{Name: "mteam", Reporter: fakeReporter{}}}

	out := doGetStatus(t, sites)

	siteOut, ok := out["sites"].(map[string]any)["mteam"].(map[string]any)
	if !ok {
		t.Fatalf("response missing sites.mteam: %v", out)
	}
	if siteOut["last_scan_at"] != nil {
		t.Errorf("last_scan_at = %v, want nil (never scanned)", siteOut["last_scan_at"])
	}
	pool, ok := siteOut["pool"].([]any)
	if !ok {
		t.Fatalf("pool field missing or wrong type: %v", siteOut["pool"])
	}
	if len(pool) != 0 {
		t.Errorf("pool = %v, want empty", pool)
	}
}

func TestHandler_ReportsPoolMembersWithColdVerdictAndSampleWindow(t *testing.T) {
	joined := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	oldest := time.Date(2026, 11, 12, 10, 5, 0, 0, time.UTC)
	newest := time.Date(2026, 11, 12, 10, 25, 0, 0, time.UTC)
	scanAt := time.Date(2026, 11, 12, 10, 30, 0, 0, time.UTC)

	sites := []Site{{
		Name: "mteam",
		Reporter: fakeReporter{
			lastScanAt: scanAt,
			pool: []scheduler.PoolMember{
				{
					InfoHash:               "hash-cold",
					TorrentFSID:            "tfs-1",
					JoinedAt:               joined,
					Cold:                   true,
					LastRateBytesPerSecond: 42,
					SampleCount:            3,
					OldestSampleAt:         oldest,
					NewestSampleAt:         newest,
					WindowSpan:             newest.Sub(oldest),
				},
				{
					InfoHash: "hash-new",
					// SampleCount 0: just rebuilt/landed, never judged yet.
				},
			},
		},
	}}

	out := doGetStatus(t, sites)

	siteOut := out["sites"].(map[string]any)["mteam"].(map[string]any)
	if got := siteOut["last_scan_at"]; got != "2026-11-12T10:30:00Z" {
		t.Errorf("last_scan_at = %v, want 2026-11-12T10:30:00Z", got)
	}

	pool := siteOut["pool"].([]any)
	if len(pool) != 2 {
		t.Fatalf("pool length = %d, want 2", len(pool))
	}

	// Pool() already sorts by InfoHash; "hash-cold" < "hash-new".
	cold := pool[0].(map[string]any)
	if cold["info_hash"] != "hash-cold" || cold["torrentfs_id"] != "tfs-1" {
		t.Errorf("cold member identity wrong: %v", cold)
	}
	if cold["cold"] != true {
		t.Errorf("cold[\"cold\"] = %v, want true", cold["cold"])
	}
	if cold["last_rate_bytes_per_second"].(float64) != 42 {
		t.Errorf("cold rate = %v, want 42", cold["last_rate_bytes_per_second"])
	}
	if cold["sample_count"].(float64) != 3 {
		t.Errorf("cold sample_count = %v, want 3", cold["sample_count"])
	}
	if cold["oldest_sample_at"] != "2026-11-12T10:05:00Z" {
		t.Errorf("oldest_sample_at = %v, want 2026-11-12T10:05:00Z", cold["oldest_sample_at"])
	}
	if cold["newest_sample_at"] != "2026-11-12T10:25:00Z" {
		t.Errorf("newest_sample_at = %v, want 2026-11-12T10:25:00Z", cold["newest_sample_at"])
	}
	if cold["window_span_seconds"].(float64) != 1200 {
		t.Errorf("window_span_seconds = %v, want 1200", cold["window_span_seconds"])
	}

	newMember := pool[1].(map[string]any)
	if newMember["info_hash"] != "hash-new" {
		t.Errorf("second member info_hash = %v, want hash-new", newMember["info_hash"])
	}
	if newMember["cold"] != false {
		t.Errorf("newMember[\"cold\"] = %v, want false", newMember["cold"])
	}
	if newMember["oldest_sample_at"] != nil || newMember["newest_sample_at"] != nil {
		t.Errorf("newMember sample timestamps = %v / %v, want both nil",
			newMember["oldest_sample_at"], newMember["newest_sample_at"])
	}
	if newMember["sample_count"].(float64) != 0 {
		t.Errorf("newMember sample_count = %v, want 0", newMember["sample_count"])
	}
}

func TestHandler_MultipleSites_EachReportedIndependently(t *testing.T) {
	sites := []Site{
		{Name: "mteam", Reporter: fakeReporter{pool: []scheduler.PoolMember{{InfoHash: "h1"}}}},
		{Name: "other-site", Reporter: fakeReporter{pool: []scheduler.PoolMember{{InfoHash: "h2"}, {InfoHash: "h3"}}}},
	}

	out := doGetStatus(t, sites)
	sitesOut := out["sites"].(map[string]any)

	if len(sitesOut) != 2 {
		t.Fatalf("sites count = %d, want 2: %v", len(sitesOut), sitesOut)
	}
	mteamPool := sitesOut["mteam"].(map[string]any)["pool"].([]any)
	if len(mteamPool) != 1 {
		t.Errorf("mteam pool length = %d, want 1", len(mteamPool))
	}
	otherPool := sitesOut["other-site"].(map[string]any)["pool"].([]any)
	if len(otherPool) != 2 {
		t.Errorf("other-site pool length = %d, want 2", len(otherPool))
	}
}

func TestHandler_RejectsNonGetMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/status", nil)
	rec := httptest.NewRecorder()
	NewHandler([]Site{{Name: "mteam", Reporter: fakeReporter{}}}).ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status code = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
