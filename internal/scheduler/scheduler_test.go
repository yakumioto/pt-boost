package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

func baseParams() Params {
	return Params{
		Category:          "boost-mteam",
		PoolSize:          2,
		SeedersMin:        10,
		LeechersMin:       50,
		MaxScanPages:      3,
		PageSize:          100,
		TargetShareBytes:  256 * 1024 * 1024,
		ColdWindow:        20 * time.Minute,
		ColdRateThreshold: 50 * 1000, // 50KB/s
	}
}

// setupLanding wires a fakeSite + fakeFetcher + fakeTorrentFS so that
// candidate c can be fully landed: GetDownloadToken, Fetch, and AddTorrent
// all agree on a consistent token/bytes/infohash chain. The .torrent for
// site torrent id X resolves to infohash "hashX" — an identity only
// torrentfs-go knows; the Candidate itself never carries it.
func setupLanding(site *fakeSite, fetcher *fakeFetcher, tfs *fakeTorrentFS, c Candidate) {
	token := "token-" + c.TorrentID
	fileBytes := []byte("torrent-bytes-" + c.TorrentID)
	if site.downloadTokens == nil {
		site.downloadTokens = map[string]string{}
	}
	site.downloadTokens[c.TorrentID] = token
	if fetcher.files == nil {
		fetcher.files = map[string][]byte{}
	}
	fetcher.files[token] = fileBytes
	if tfs.infoHashByBytes == nil {
		tfs.infoHashByBytes = map[string]string{}
	}
	tfs.infoHashByBytes[string(fileBytes)] = "hash" + c.TorrentID
}

func TestScan_FillsEmptyPoolUpToPoolSize(t *testing.T) {
	candidates := []Candidate{
		{TorrentID: "A", Seeders: 20, Leechers: 100},
		{TorrentID: "B", Seeders: 20, Leechers: 90},
		{TorrentID: "C", Seeders: 20, Leechers: 80},
	}
	site := &fakeSite{candidates: candidates}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	for _, c := range candidates {
		setupLanding(site, fetcher, tfs, c)
	}

	params := baseParams() // PoolSize: 2
	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}

	pool := sched.Pool()
	if len(pool) != 2 {
		t.Fatalf("pool size = %d, want 2 (bounded by PoolSize)", len(pool))
	}
	if tfs.addTorrentCalls != 2 {
		t.Errorf("AddTorrent called %d times, want 2", tfs.addTorrentCalls)
	}
}

func TestScan_SkipsCandidatesBelowThresholds(t *testing.T) {
	candidates := []Candidate{
		{TorrentID: "lowSeeders", Seeders: 5, Leechers: 100},  // seeders <= min
		{TorrentID: "lowLeechers", Seeders: 20, Leechers: 10}, // leechers <= min
	}
	site := &fakeSite{candidates: candidates}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	for _, c := range candidates {
		setupLanding(site, fetcher, tfs, c)
	}

	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), baseParams())
	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}

	if len(sched.Pool()) != 0 {
		t.Fatalf("pool should stay empty, got %d members", len(sched.Pool()))
	}
	if tfs.addTorrentCalls != 0 {
		t.Errorf("AddTorrent should not have been called, got %d calls", tfs.addTorrentCalls)
	}
}

func TestScan_SkipsCandidateAlreadyInPool(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	b := Candidate{TorrentID: "B", Seeders: 20, Leechers: 90}

	site := &fakeSite{candidates: []Candidate{a}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)
	setupLanding(site, fetcher, tfs, b)

	params := baseParams()
	params.PoolSize = 5
	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("first Scan returned error: %v", err)
	}
	if len(sched.Pool()) != 1 {
		t.Fatalf("after first scan, pool size = %d, want 1", len(sched.Pool()))
	}

	// Second scan returns A again (already pooled) plus a new candidate B.
	site.candidates = []Candidate{a, b}
	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("second Scan returned error: %v", err)
	}

	if len(sched.Pool()) != 2 {
		t.Fatalf("after second scan, pool size = %d, want 2 (A skipped, B added)", len(sched.Pool()))
	}
	if tfs.addTorrentCalls != 2 {
		t.Errorf("AddTorrent called %d times, want 2 (A once, B once; A not re-added)", tfs.addTorrentCalls)
	}
}

func TestScan_PoolFullNoColdMember_NoReplacement(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	b := Candidate{TorrentID: "B", Seeders: 20, Leechers: 90}
	c := Candidate{TorrentID: "C", Seeders: 20, Leechers: 80}

	site := &fakeSite{candidates: []Candidate{a, b}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)
	setupLanding(site, fetcher, tfs, b)
	setupLanding(site, fetcher, tfs, c)

	params := baseParams() // PoolSize: 2
	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("first Scan returned error: %v", err)
	}
	if len(sched.Pool()) != 2 {
		t.Fatalf("pool should be full at 2, got %d", len(sched.Pool()))
	}
	callsAfterFirstScan := tfs.addTorrentCalls

	// No cold detection ever ran, so nothing is marked cold. A third,
	// qualifying candidate shows up: it must NOT be added.
	site.candidates = []Candidate{a, b, c}
	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("second Scan returned error: %v", err)
	}

	if len(sched.Pool()) != 2 {
		t.Fatalf("pool size changed to %d, want still 2 (no cold member to replace)", len(sched.Pool()))
	}
	if tfs.addTorrentCalls != callsAfterFirstScan {
		t.Errorf("AddTorrent called again (%d -> %d) despite no cold member", callsAfterFirstScan, tfs.addTorrentCalls)
	}
	if len(tfs.deleteTorrentCalls) != 0 {
		t.Errorf("DeleteTorrent should not have been called, got %v", tfs.deleteTorrentCalls)
	}
}

func TestScan_PoolFullWithColdMember_ReplacesWorst(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100} // will go cold
	b := Candidate{TorrentID: "B", Seeders: 20, Leechers: 90}  // stays active
	c := Candidate{TorrentID: "C", Seeders: 20, Leechers: 80}  // new candidate

	site := &fakeSite{candidates: []Candidate{a, b}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)
	setupLanding(site, fetcher, tfs, b)
	setupLanding(site, fetcher, tfs, c)

	params := baseParams() // PoolSize: 2, ColdWindow: 20m, ColdRateThreshold: 50KB/s
	start := time.Now()
	clock := newFakeClock(start)
	sched := New(site, tfs, fetcher, clock, params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}
	poolAfterFill := sched.Pool()
	if len(poolAfterFill) != 2 {
		t.Fatalf("pool should be full at 2, got %d", len(poolAfterFill))
	}

	idA := findTorrentFSID(tfs, "hashA")
	idB := findTorrentFSID(tfs, "hashB")

	// Drive two cold-detection samples spanning exactly ColdWindow: A barely
	// uploads (cold), B uploads briskly (stays active).
	if err := sched.DetectColdTorrents(ctx); err != nil {
		t.Fatalf("first DetectColdTorrents returned error: %v", err)
	}
	clock.Advance(params.ColdWindow)
	tfs.setUploaded(idA, 1000)                                        // ~1000/1200s, far under 50KB/s
	tfs.setUploaded(idB, int64(100*1000*params.ColdWindow.Seconds())) // well above 50KB/s
	if err := sched.DetectColdTorrents(ctx); err != nil {
		t.Fatalf("second DetectColdTorrents returned error: %v", err)
	}

	pool := sched.Pool()
	coldCount, activeCount := 0, 0
	for _, m := range pool {
		if m.InfoHash == "hashA" && !m.Cold {
			t.Errorf("expected hashA to be marked cold")
		}
		if m.InfoHash == "hashB" && m.Cold {
			t.Errorf("expected hashB to stay active (not cold)")
		}
		if m.Cold {
			coldCount++
		} else {
			activeCount++
		}
	}
	if coldCount != 1 || activeCount != 1 {
		t.Fatalf("expected exactly 1 cold + 1 active member, got cold=%d active=%d", coldCount, activeCount)
	}

	// Now a new qualifying candidate shows up: it should replace the cold
	// member (A), not the active one (B).
	site.candidates = []Candidate{a, b, c}
	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("replacement Scan returned error: %v", err)
	}

	if len(tfs.deleteTorrentCalls) != 1 || tfs.deleteTorrentCalls[0] != idA {
		t.Fatalf("DeleteTorrent calls = %v, want exactly [%s] (the cold member)", tfs.deleteTorrentCalls, idA)
	}

	finalPool := sched.Pool()
	if len(finalPool) != 2 {
		t.Fatalf("pool size after replacement = %d, want 2", len(finalPool))
	}
	hashes := map[string]bool{}
	for _, m := range finalPool {
		hashes[m.InfoHash] = true
	}
	if !hashes["hashB"] {
		t.Errorf("hashB (active member) should still be in the pool")
	}
	if !hashes["hashC"] {
		t.Errorf("hashC (new candidate) should have landed")
	}
	if hashes["hashA"] {
		t.Errorf("hashA (cold member) should have been evicted")
	}
}

func findTorrentFSID(tfs *fakeTorrentFS, infoHash string) string {
	for id, e := range tfs.entries {
		if e.infoHash == infoHash {
			return id
		}
	}
	return ""
}

func TestDetectColdTorrents_NewMemberWithinColdWindow_NotJudged(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	site := &fakeSite{candidates: []Candidate{a}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)

	params := baseParams()
	start := time.Now()
	clock := newFakeClock(start)
	sched := New(site, tfs, fetcher, clock, params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}
	idA := findTorrentFSID(tfs, "hashA")

	// Advance less than ColdWindow and report a very low upload rate. The
	// member must NOT be judged cold yet: there isn't enough history.
	clock.Advance(params.ColdWindow / 2)
	tfs.setUploaded(idA, 1) // effectively 0 B/s
	if err := sched.DetectColdTorrents(ctx); err != nil {
		t.Fatalf("DetectColdTorrents returned error: %v", err)
	}

	pool := sched.Pool()
	if len(pool) != 1 {
		t.Fatalf("expected 1 pool member, got %d", len(pool))
	}
	if pool[0].Cold {
		t.Errorf("member younger than ColdWindow must not be marked cold yet")
	}
}

func TestLand_RunsFixedSequenceWithConfiguredCategoryAndRange(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	var calls []string
	site := &fakeSite{candidates: []Candidate{a}, calls: &calls}
	fetcher := &fakeFetcher{calls: &calls}
	tfs := newFakeTorrentFS()
	tfs.calls = &calls
	setupLanding(site, fetcher, tfs, a)

	params := baseParams()
	params.Category = "boost-mteam"
	params.TargetShareBytes = 256 * 1024 * 1024
	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), params)

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}

	wantOrder := []string{
		"Site.SearchHotTorrents",
		"Site.GetDownloadToken",
		"Fetcher.Fetch",
		"TorrentFS.AddTorrent",
		"TorrentFS.SetCategory",
		"TorrentFS.TriggerPieceDownload",
	}
	if len(calls) != len(wantOrder) {
		t.Fatalf("call order = %v, want %v", calls, wantOrder)
	}
	for i, want := range wantOrder {
		if calls[i] != want {
			t.Errorf("call[%d] = %q, want %q (full sequence: %v)", i, calls[i], want, calls)
		}
	}

	if len(tfs.setCategoryCalls) != 1 || tfs.setCategoryCalls[0].category != "boost-mteam" {
		t.Errorf("SetCategory calls = %+v, want category boost-mteam", tfs.setCategoryCalls)
	}
	if len(tfs.triggerPieceDownloadLog) != 1 {
		t.Fatalf("TriggerPieceDownload called %d times, want 1", len(tfs.triggerPieceDownloadLog))
	}
	gotRange := tfs.triggerPieceDownloadLog[0].Range
	if gotRange.Offset != 0 || gotRange.Length != params.TargetShareBytes {
		t.Errorf("piece-download range = %+v, want {Offset:0 Length:%d}", gotRange, params.TargetShareBytes)
	}
}

func TestRebuildFromTorrentFS_FiltersByCategoryWithEmptySampleWindows(t *testing.T) {
	tfs := newFakeTorrentFS()
	tfs.infoHashByBytes["x"] = "hashMine" // unused helper state, harmless
	tfs.entries["hashMine"] = &fakeTorrentFSEntry{infoHash: "hashMine", category: "boost-mteam", uploadedBytes: 500}
	tfs.entries["hashOther"] = &fakeTorrentFSEntry{infoHash: "hashOther", category: "boost-othersite", uploadedBytes: 500}

	params := baseParams()
	params.Category = "boost-mteam"
	sched := New(&fakeSite{}, tfs, &fakeFetcher{}, newFakeClock(time.Now()), params)

	if err := sched.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatalf("RebuildFromTorrentFS returned error: %v", err)
	}

	pool := sched.Pool()
	if len(pool) != 1 {
		t.Fatalf("pool size = %d, want 1 (only the matching category)", len(pool))
	}
	if pool[0].InfoHash != "hashMine" {
		t.Errorf("rebuilt member InfoHash = %q, want hashMine", pool[0].InfoHash)
	}
	if pool[0].Cold {
		t.Errorf("freshly rebuilt member should not start out marked cold")
	}

	// A single DetectColdTorrents tick right after rebuild must not yet
	// judge the member cold, even with a low upload rate: its sample
	// window is empty, not a full ColdWindow.
	if err := sched.DetectColdTorrents(ctx); err != nil {
		t.Fatalf("DetectColdTorrents returned error: %v", err)
	}
	if sched.Pool()[0].Cold {
		t.Errorf("rebuilt member must not be judged cold from a single post-rebuild sample")
	}
}

// TestScan_AfterRestart_RecognizesRebuiltMemberViaIdempotentAdd covers the
// "no state kept across restarts" design (ADR 0003): a rebuilt pool knows
// its members only by torrentfs id, so the first scan re-adds a still-hot
// candidate once, sees the returned id is already pooled, and leaves it
// alone. From then on the in-process cache skips it without downloading.
func TestScan_AfterRestart_RecognizesRebuiltMemberViaIdempotentAdd(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	site := &fakeSite{candidates: []Candidate{a}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)
	tfs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}

	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), baseParams())
	if err := sched.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatalf("RebuildFromTorrentFS returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := sched.Scan(ctx); err != nil {
			t.Fatalf("Scan #%d returned error: %v", i+1, err)
		}
	}

	if len(sched.Pool()) != 1 {
		t.Fatalf("pool size = %d, want 1 (rebuilt member, not duplicated)", len(sched.Pool()))
	}
	if tfs.addTorrentCalls != 1 {
		t.Errorf("AddTorrent called %d times, want 1 (once to resolve the id, then cached)", tfs.addTorrentCalls)
	}
	if len(tfs.setCategoryCalls) != 0 || len(tfs.triggerPieceDownloadLog) != 0 {
		t.Errorf("an already-pooled member must not be re-landed: setCategory=%v pieceDownload=%v",
			tfs.setCategoryCalls, tfs.triggerPieceDownloadLog)
	}
}

// TestScan_DuplicateCandidateDoesNotEvictColdMember guards the add-before-
// evict ordering: with a full pool and a cold member available, a
// candidate that turns out to already be pooled must not cost that cold
// member its slot.
func TestScan_DuplicateCandidateDoesNotEvictColdMember(t *testing.T) {
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	site := &fakeSite{candidates: []Candidate{a}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, a)
	tfs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}
	tfs.entries["hashCold"] = &fakeTorrentFSEntry{infoHash: "hashCold", category: "boost-mteam"}

	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), baseParams()) // PoolSize: 2
	if err := sched.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatalf("RebuildFromTorrentFS returned error: %v", err)
	}
	sched.pool["hashCold"].cold = true

	if err := sched.Scan(ctx); err != nil {
		t.Fatalf("Scan returned error: %v", err)
	}

	if len(tfs.deleteTorrentCalls) != 0 {
		t.Fatalf("DeleteTorrent calls = %v, want none (candidate was a duplicate)", tfs.deleteTorrentCalls)
	}
	if len(sched.Pool()) != 2 {
		t.Fatalf("pool size = %d, want 2", len(sched.Pool()))
	}
}

// TestScan_EvictFailureRollsBackAddedTorrent: the incoming torrent is added
// before the cold member is evicted, so a failed eviction must delete it
// again rather than leave an uncategorized orphan in torrentfs-go.
func TestScan_EvictFailureRollsBackAddedTorrent(t *testing.T) {
	c := Candidate{TorrentID: "C", Seeders: 20, Leechers: 80}
	site := &fakeSite{candidates: []Candidate{c}}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	setupLanding(site, fetcher, tfs, c)
	tfs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}
	tfs.entries["hashB"] = &fakeTorrentFSEntry{infoHash: "hashB", category: "boost-mteam"}
	tfs.deleteErr = errors.New("torrentfs-go unavailable")

	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), baseParams()) // PoolSize: 2
	if err := sched.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatalf("RebuildFromTorrentFS returned error: %v", err)
	}
	sched.pool["hashA"].cold = true

	if err := sched.Scan(ctx); err == nil {
		t.Fatal("Scan: want error when eviction fails, got nil")
	}

	want := []string{"hashA", "hashC"} // failed eviction, then rollback of the incoming torrent
	if len(tfs.deleteTorrentCalls) != 2 || tfs.deleteTorrentCalls[0] != want[0] || tfs.deleteTorrentCalls[1] != want[1] {
		t.Fatalf("DeleteTorrent calls = %v, want %v", tfs.deleteTorrentCalls, want)
	}
	if len(tfs.setCategoryCalls) != 0 {
		t.Errorf("incoming torrent must not be categorized when eviction failed, got %v", tfs.setCategoryCalls)
	}
	pool := sched.Pool()
	if len(pool) != 2 {
		t.Fatalf("pool size = %d, want 2 (unchanged)", len(pool))
	}
	for _, m := range pool {
		if m.TorrentFSID == "hashC" {
			t.Errorf("incoming torrent must not be in the pool after a failed eviction")
		}
	}
}

// TestScan_PreexistingForeignTorrentIsLeftAlone: AddTorrent being
// idempotent means a candidate can resolve to a torrent torrentfs-go
// already manages for someone else — the user's own library (no category)
// or another site's pool. Scheduler must not re-tag it, pool it (which
// would later get it deleted as a cold member), evict for it, or delete
// it.
func TestScan_PreexistingForeignTorrentIsLeftAlone(t *testing.T) {
	for _, category := range []string{"", "boost-othersite"} {
		t.Run("category="+category, func(t *testing.T) {
			a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
			site := &fakeSite{candidates: []Candidate{a}}
			fetcher := &fakeFetcher{}
			tfs := newFakeTorrentFS()
			setupLanding(site, fetcher, tfs, a)
			tfs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: category}

			sched := New(site, tfs, fetcher, newFakeClock(time.Now()), baseParams())
			if err := sched.Scan(ctx); err != nil {
				t.Fatalf("Scan returned error: %v", err)
			}

			if len(sched.Pool()) != 0 {
				t.Fatalf("pool size = %d, want 0", len(sched.Pool()))
			}
			if got := tfs.entries["hashA"].category; got != category {
				t.Errorf("category = %q, want it left as %q", got, category)
			}
			if len(tfs.triggerPieceDownloadLog) != 0 {
				t.Errorf("TriggerPieceDownload calls = %v, want none", tfs.triggerPieceDownloadLog)
			}
			if len(tfs.deleteTorrentCalls) != 0 {
				t.Errorf("DeleteTorrent calls = %v, want none", tfs.deleteTorrentCalls)
			}
		})
	}
}

// TestConcurrentScanAndDetectColdTorrents_NoDataRace guards the mutex
// added to Scheduler for issue #7: in production, Runner (internal/app)
// drives Scan and DetectColdTorrents for the same Scheduler from two
// independent goroutines (the scan loop and the cold-detection loop never
// share a ticker), and the /status HTTP handler (internal/statusapi) reads
// Pool()/LastScanAt() from a third goroutine. Before that mutex existed,
// `go test -race` did not catch this because every other test in this
// package calls Scheduler from a single goroutine. Run with -race to be
// meaningful; without it, this test only proves the code doesn't
// deadlock/panic, not that it's race-free.
func TestConcurrentScanAndDetectColdTorrents_NoDataRace(t *testing.T) {
	candidates := []Candidate{
		{TorrentID: "A", Seeders: 20, Leechers: 100},
		{TorrentID: "B", Seeders: 20, Leechers: 90},
	}
	site := &fakeSite{candidates: candidates}
	fetcher := &fakeFetcher{}
	tfs := newFakeTorrentFS()
	for _, c := range candidates {
		setupLanding(site, fetcher, tfs, c)
	}

	params := baseParams()
	params.PoolSize = 2
	params.ColdWindow = time.Millisecond // tiny, so DetectColdTorrents does real work fast
	// fakeClock.Now() is only ever read concurrently in this test (no
	// Advance calls while goroutines run below), which is race-safe.
	sched := New(site, tfs, fetcher, newFakeClock(time.Now()), params)

	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = sched.Scan(ctx) // reuses the same two candidates every call; harmless once pooled
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = sched.DetectColdTorrents(ctx)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = sched.Pool()
			_ = sched.LastScanAt()
		}
	}()

	wg.Wait()
}

// TestDetectColdTorrents_JitteredTicksStillReachFullWindow guards against a
// pruning off-by-one found against a real clock: ticks never land exactly
// ColdWindow apart, so the one sample just older than the cutoff must be
// kept as the window's anchor. Dropping it left the span permanently short
// of ColdWindow and no member was ever judged cold.
func TestDetectColdTorrents_JitteredTicksStillReachFullWindow(t *testing.T) {
	tfs := newFakeTorrentFS()
	tfs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}
	clock := newFakeClock(time.Now())
	params := baseParams()
	sched := New(&fakeSite{}, tfs, &fakeFetcher{}, clock, params)
	if err := sched.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatalf("RebuildFromTorrentFS returned error: %v", err)
	}

	tick := params.ColdWindow/10 + 137*time.Millisecond // never divides ColdWindow evenly
	for i := 0; i < 15; i++ {
		if err := sched.DetectColdTorrents(ctx); err != nil {
			t.Fatalf("DetectColdTorrents returned error: %v", err)
		}
		clock.Advance(tick)
	}

	pool := sched.Pool()
	if len(pool) != 1 || !pool[0].Cold {
		t.Fatalf("pool = %+v, want the idle member judged cold after more than a full ColdWindow of samples", pool)
	}
}
