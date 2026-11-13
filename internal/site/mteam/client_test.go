package mteam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// fakeServer simulates this adapter's assumed understanding of M-Team's
// /api/torrent/search and /api/torrent/genDlToken contract (see package
// doc: unverified against an official schema). /api/torrent/detail is
// registered only to count calls: the adapter must never hit it.
type fakeServer struct {
	mu sync.Mutex

	searchPages       [][]searchResultItem // page N of each searched section
	searchPagesByMode map[string][][]searchResultItem
	searchRequests    []searchRequest

	detailCallCount int

	tokenRequests []string // raw "id" form values seen by genDlToken

	torrentBytes map[string][]byte // served by the fetch URL

	rateLimitSearchCalls int // first N search calls return 429
	searchCallCount      int
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		torrentBytes: map[string][]byte{},
	}
}

func (f *fakeServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/torrent/search", f.handleSearch)
	mux.HandleFunc("/api/torrent/genDlToken", f.handleGenDlToken)
	mux.HandleFunc("/api/torrent/detail", f.handleDetail)
	mux.HandleFunc("/fetch/", f.handleFetch)
	return mux
}

func (f *fakeServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.searchCallCount++
	callNum := f.searchCallCount
	f.mu.Unlock()

	if callNum <= f.rateLimitSearchCalls {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.searchRequests = append(f.searchRequests, req)
	pages := f.searchPages
	if f.searchPagesByMode != nil {
		pages = f.searchPagesByMode[req.Mode]
	}
	var page []searchResultItem
	if req.PageNumber-1 < len(pages) {
		page = pages[req.PageNumber-1]
	}
	f.mu.Unlock()

	writeEnvelope(w, searchResponseData{Data: page})
}

func (f *fakeServer) handleGenDlToken(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
		http.Error(w, fmt.Sprintf("want form-urlencoded, got %s", ct), http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := r.FormValue("id")

	f.mu.Lock()
	f.tokenRequests = append(f.tokenRequests, id)
	f.mu.Unlock()

	writeEnvelope(w, "/fetch/"+id)
}

func (f *fakeServer) handleDetail(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.detailCallCount++
	f.mu.Unlock()
	http.Error(w, "detail must not be called", http.StatusGone)
}

func (f *fakeServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/fetch/"):]
	f.mu.Lock()
	body, ok := f.torrentBytes[id]
	f.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	_, _ = w.Write(body)
}

func writeEnvelope(w http.ResponseWriter, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(envelope{
		Code:    "0",
		Message: "SUCCESS",
		Data:    raw,
	})
}

func newTestClient(t *testing.T, srv *httptest.Server, opts ...Option) *Client {
	t.Helper()
	baseOpts := append([]Option{withSleep(func(time.Duration) {})}, opts...)
	return New(Config{
		BaseURL: srv.URL,
		APIKey:  "test-key",
	}, baseOpts...)
}

func item(id string, seeders, leechers int) searchResultItem {
	return searchResultItem{
		ID: flexString(id),
		Status: torrentStatus{
			Seeders:  flexInt(seeders),
			Leechers: flexInt(leechers),
			Discount: flexString("FREE"),
		},
	}
}

func discounted(id, discount, endTime string) searchResultItem {
	it := item(id, 20, 100)
	it.Status.Discount = flexString(discount)
	it.Status.DiscountEndTime = flexString(endTime)
	return it
}

func TestSearchHotTorrents_PaginatesAndStopsOnShortPage(t *testing.T) {
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{
		"adult": {
			{item("1", 20, 100), item("2", 20, 100)}, // full page (pageSize=2)
			{item("3", 20, 100)},                     // short page: stop here
		},
		"normal": {{item("4", 20, 100)}},
	}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	candidates, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 5, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}
	if len(candidates) != 4 {
		t.Fatalf("got %d candidates, want 4", len(candidates))
	}
	if len(fs.searchRequests) != 3 {
		t.Fatalf("got %d search requests, want 3 (each section stops after its short page)", len(fs.searchRequests))
	}
}

func TestSearchHotTorrents_SearchesR18SectionFirstWithLeechersDescSort(t *testing.T) {
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{"adult": {{}}}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}

	if len(fs.searchRequests) != 2 {
		t.Fatalf("got %d search requests, want 2 (R18 section plus normal section)", len(fs.searchRequests))
	}
	if fs.searchRequests[0].Mode != "adult" || fs.searchRequests[1].Mode != "normal" {
		t.Errorf("modes = %q/%q, want adult/normal (mode \"all\" excludes R18 in production)",
			fs.searchRequests[0].Mode, fs.searchRequests[1].Mode)
	}
	for _, got := range fs.searchRequests {
		if got.SortField != "LEECHERS" || got.SortDirection != "DESC" {
			t.Errorf("sort = %s/%s, want LEECHERS/DESC", got.SortField, got.SortDirection)
		}
	}
}

// TestSearchHotTorrents_ReturnsRawCandidatesWithoutFilteringByThreshold is
// a regression test for the spec realignment in issue #5: this adapter
// must return the raw candidate list, with no local seeders/leechers
// threshold filtering of its own — pool-membership thresholds are
// scheduler.Scheduler.Scan's sole responsibility (it re-applies them
// independently). Even a candidate with very low seeders/leechers must
// still be returned.
func TestSearchHotTorrents_ReturnsRawCandidatesWithoutFilteringByThreshold(t *testing.T) {
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{"adult": {{
		item("barely-alive", 1, 1), // would fail any sane threshold
	}}}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	candidates, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates, want 1 (no local threshold filtering)", len(candidates))
	}
	got := candidates[0]
	if got.TorrentID != "barely-alive" || got.Seeders != 1 || got.Leechers != 1 {
		t.Fatalf("got %+v, want the low-seeders/leechers candidate returned as-is", got)
	}
}

// TestSearchHotTorrents_OnlyCallsSearch guards ADR 0003: candidates are
// identified by M-Team's torrent id alone, so searching must cost exactly
// the search pages — no per-candidate /api/torrent/detail lookup (its
// infoHash is null in production) and no .torrent download.
func TestSearchHotTorrents_OnlyCallsSearch(t *testing.T) {
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{"adult": {{item("42", 20, 100), item("43", 30, 90)}}}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	candidates, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}
	want := []scheduler.Candidate{
		{TorrentID: "42", Seeders: 20, Leechers: 100, Free: true, Preferred: true},
		{TorrentID: "43", Seeders: 30, Leechers: 90, Free: true, Preferred: true},
	}
	if len(candidates) != len(want) {
		t.Fatalf("got %d candidates, want %d", len(candidates), len(want))
	}
	for i := range want {
		if candidates[i] != want[i] {
			t.Errorf("candidate[%d] = %+v, want %+v", i, candidates[i], want[i])
		}
	}
	if fs.detailCallCount != 0 {
		t.Errorf("detail called %d times, want 0", fs.detailCallCount)
	}
	if len(fs.tokenRequests) != 0 {
		t.Errorf("genDlToken called for %v during search, want no calls", fs.tokenRequests)
	}
}

func TestSearchHotTorrents_RetriesOn429(t *testing.T) {
	fs := newFakeServer()
	fs.rateLimitSearchCalls = 2 // first two calls 429, third succeeds
	fs.searchPagesByMode = map[string][][]searchResultItem{"adult": {{}}}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	var sleeps []time.Duration
	c := New(Config{BaseURL: srv.URL, APIKey: "k"},
		withSleep(func(d time.Duration) { sleeps = append(sleeps, d) }))

	_, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}
	if fs.searchCallCount != 4 {
		t.Fatalf("server saw %d search calls, want 4 (2 failed + 1 success in the R18 section, 1 normal section)", fs.searchCallCount)
	}
	// Two 429-triggered backoff sleeps (1s, 2s) plus the first pacing
	// sleep (0, first call never waits) -- just check the two retry
	// backoffs are present among the recorded sleeps.
	foundOneSecond, foundTwoSecond := false, false
	for _, d := range sleeps {
		if d == 1*time.Second {
			foundOneSecond = true
		}
		if d == 2*time.Second {
			foundTwoSecond = true
		}
	}
	if !foundOneSecond || !foundTwoSecond {
		t.Errorf("sleeps = %v, want to contain 1s and 2s backoffs", sleeps)
	}
}

func TestSearchHotTorrents_PacesBetweenSearchPages(t *testing.T) {
	fs := newFakeServer()
	fs.searchPages = [][]searchResultItem{
		{item("1", 20, 100)},
		{item("2", 20, 100)},
	}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	var sleeps []time.Duration
	c := New(Config{BaseURL: srv.URL, APIKey: "k"},
		withSleep(func(d time.Duration) { sleeps = append(sleeps, d) }))

	_, err := c.SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 2, PageSize: 1})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}

	foundSearchPacing := false
	for _, d := range sleeps {
		if d > 2*time.Second && d <= defaultSearchInterval {
			foundSearchPacing = true
		}
	}
	if !foundSearchPacing {
		t.Errorf("sleeps = %v, want one close to the %v search interval between pages", sleeps, defaultSearchInterval)
	}
}

func TestGetDownloadToken_SendsFormUrlEncoded(t *testing.T) {
	fs := newFakeServer()
	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	token, err := c.GetDownloadToken(context.Background(), "99")
	if err != nil {
		t.Fatalf("GetDownloadToken: %v", err)
	}
	if token != "/fetch/99" {
		t.Errorf("token = %q, want %q", token, "/fetch/99")
	}
	if len(fs.tokenRequests) != 1 || fs.tokenRequests[0] != "99" {
		t.Errorf("server saw id requests %v, want [\"99\"]", fs.tokenRequests)
	}
}

func TestFetch_DownloadsRawBytesFromToken(t *testing.T) {
	fs := newFakeServer()
	fs.torrentBytes["123"] = []byte("fake-torrent-bytes")
	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	body, err := c.Fetch(context.Background(), srv.URL+"/fetch/123")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != "fake-torrent-bytes" {
		t.Errorf("body = %q, want %q", body, "fake-torrent-bytes")
	}
}

func TestFetch_NotFoundReturnsError(t *testing.T) {
	fs := newFakeServer()
	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	_, err := c.Fetch(context.Background(), srv.URL+"/fetch/missing")
	if err == nil {
		t.Fatal("Fetch: want error for missing torrent, got nil")
	}
}

func TestFetch_RelativeTokenResolvedAgainstBaseURL(t *testing.T) {
	// Regression test: a manual smoke run against a fake M-Team server
	// returning a bare path (e.g. "/fetch/mt-1") from genDlToken instead
	// of an absolute URL surfaced "unsupported protocol scheme" from
	// Fetch, because GetDownloadToken's result was passed to Fetch
	// unmodified (as Scheduler.land really does) with no base URL to
	// resolve against. Fetch must tolerate this even though a real
	// M-Team instance is documented/expected to always return an
	// absolute, pre-signed URL.
	fs := newFakeServer()
	fs.torrentBytes["77"] = []byte("relative-path-bytes")
	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	body, err := c.Fetch(context.Background(), "/fetch/77")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != "relative-path-bytes" {
		t.Errorf("body = %q, want %q", body, "relative-path-bytes")
	}
}

func TestEndToEnd_SearchThenTokenThenFetch(t *testing.T) {
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{"adult": {{item("55", 20, 100)}}}
	fs.torrentBytes["55"] = []byte("torrent-file-contents")

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	c := newTestClient(t, srv)
	ctx := context.Background()

	candidates, err := c.SearchHotTorrents(ctx, scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates, want 1", len(candidates))
	}

	token, err := c.GetDownloadToken(ctx, candidates[0].TorrentID)
	if err != nil {
		t.Fatalf("GetDownloadToken: %v", err)
	}

	// token is used exactly as GetDownloadToken returned it: Scheduler
	// never prefixes it with anything (Site is an opaque black box to
	// Scheduler), so Fetch itself must be able to resolve it.
	body, err := c.Fetch(ctx, token)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != "torrent-file-contents" {
		t.Errorf("body = %q, want %q", body, "torrent-file-contents")
	}
}

// Only FREE costs no download quota, and an expired or unreadable
// promotion must fail closed: reporting Free for a paid torrent would
// silently spend the operator's account quota.
func TestSearchHotTorrents_ReportsFreeAndPreferredSection(t *testing.T) {
	future := time.Now().In(siteTimeZone).Add(time.Hour).Format(discountTimeLayout)
	past := time.Now().In(siteTimeZone).Add(-time.Hour).Format(discountTimeLayout)
	fs := newFakeServer()
	fs.searchPagesByMode = map[string][][]searchResultItem{
		"adult": {{
			discounted("free-forever", "FREE", ""),
			discounted("free-until", "FREE", future),
			discounted("free-expired", "FREE", past),
			discounted("unparsable", "FREE", "not-a-time"),
		}},
		"normal": {{
			discounted("half", "PERCENT_50", ""),
			discounted("paid", "NORMAL", ""),
			discounted("free-normal", "FREE", ""),
		}},
	}

	srv := httptest.NewServer(fs.handler())
	defer srv.Close()

	candidates, err := newTestClient(t, srv).SearchHotTorrents(context.Background(), scheduler.SearchParams{MaxPages: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchHotTorrents: %v", err)
	}

	wantFree := map[string]bool{"free-forever": true, "free-until": true, "free-expired": false,
		"unparsable": false, "half": false, "paid": false, "free-normal": true}
	wantPreferred := map[string]bool{"free-forever": true, "free-until": true, "free-expired": true,
		"unparsable": true, "half": false, "paid": false, "free-normal": false}
	if len(candidates) != len(wantFree) {
		t.Fatalf("got %d candidates, want %d (adapter must not filter)", len(candidates), len(wantFree))
	}
	for _, c := range candidates {
		if c.Free != wantFree[c.TorrentID] {
			t.Errorf("%s: Free = %v, want %v", c.TorrentID, c.Free, wantFree[c.TorrentID])
		}
		if c.Preferred != wantPreferred[c.TorrentID] {
			t.Errorf("%s: Preferred = %v, want %v", c.TorrentID, c.Preferred, wantPreferred[c.TorrentID])
		}
	}
}
