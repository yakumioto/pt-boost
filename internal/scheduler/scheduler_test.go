package scheduler

import (
	"context"
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
// all agree on a consistent token/bytes/infohash chain.
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
	tfs.infoHashByBytes[string(fileBytes)] = c.InfoHash
}

func TestScan_FillsEmptyPoolUpToPoolSize(t *testing.T) {
	candidates := []Candidate{
		{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100},
		{TorrentID: "B", InfoHash: "hashB", Seeders: 20, Leechers: 90},
		{TorrentID: "C", InfoHash: "hashC", Seeders: 20, Leechers: 80},
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
		{TorrentID: "lowSeeders", InfoHash: "h1", Seeders: 5, Leechers: 100},  // seeders <= min
		{TorrentID: "lowLeechers", InfoHash: "h2", Seeders: 20, Leechers: 10}, // leechers <= min
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
	a := Candidate{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100}
	b := Candidate{TorrentID: "B", InfoHash: "hashB", Seeders: 20, Leechers: 90}

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
	a := Candidate{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100}
	b := Candidate{TorrentID: "B", InfoHash: "hashB", Seeders: 20, Leechers: 90}
	c := Candidate{TorrentID: "C", InfoHash: "hashC", Seeders: 20, Leechers: 80}

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
	a := Candidate{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100} // will go cold
	b := Candidate{TorrentID: "B", InfoHash: "hashB", Seeders: 20, Leechers: 90}  // stays active
	c := Candidate{TorrentID: "C", InfoHash: "hashC", Seeders: 20, Leechers: 80}  // new candidate

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
	a := Candidate{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100}
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
	a := Candidate{TorrentID: "A", InfoHash: "hashA", Seeders: 20, Leechers: 100}
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
	tfs.entries["tfs-1"] = &fakeTorrentFSEntry{infoHash: "hashMine", category: "boost-mteam", uploadedBytes: 500}
	tfs.entries["tfs-2"] = &fakeTorrentFSEntry{infoHash: "hashOther", category: "boost-othersite", uploadedBytes: 500}

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
