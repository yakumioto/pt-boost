package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestScan_PermanentRetirementSameRoundAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retired.json")
	history, err := OpenRetired(path)
	if err != nil {
		t.Fatal(err)
	}
	a := Candidate{TorrentID: "A", Seeders: 20, Leechers: 100}
	b := Candidate{TorrentID: "B", Seeders: 20, Leechers: 100}
	site := &fakeSite{candidates: []Candidate{b, a}}
	fetch := &fakeFetcher{}
	fs := newFakeTorrentFS()
	setupLanding(site, fetch, fs, a)
	setupLanding(site, fetch, fs, b)
	p := baseParams()
	p.PoolSize = 2
	s := New(site, fs, fetch, newFakeClock(time.Now()), p, WithRetired(history))
	site.candidates = []Candidate{a}
	if err := s.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	s.pool["hashA"].cold = true
	// A second cold member keeps replacement possible after B evicts A.
	fs.entries["hashX"] = &fakeTorrentFSEntry{infoHash: "hashX", category: p.Category}
	s.pool["hashX"] = &member{torrentFSID: "hashX", cold: true, lastRateBytes: 1}
	site.candidates = []Candidate{b, a}
	if err := s.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if fs.addTorrentCalls != 2 {
		t.Fatalf("same-round resurrection: adds=%d want 2", fs.addTorrentCalls)
	}
	if !fs.entries["hashA"].deleted {
		t.Fatal("retired A re-created")
	}

	history, err = OpenRetired(path)
	if err != nil {
		t.Fatal(err)
	}
	// Much later, after a process restart and with spare capacity.
	p.PoolSize = 3
	restarted := New(site, fs, fetch, newFakeClock(time.Now().Add(365*24*time.Hour)), p, WithRetired(history))
	site.candidates = []Candidate{a}
	if err := restarted.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if fs.addTorrentCalls != 2 {
		t.Fatal("retired A downloaded/added after restart")
	}
	if len(restarted.Pool()) != 0 {
		t.Fatal("retired A pooled after restart")
	}
}

func TestScan_RetiredUnknownAliasCleanedUp(t *testing.T) {
	history, _ := OpenRetired("")
	if err := history.Remember("hashA", []string{"A"}); err != nil {
		t.Fatal(err)
	}
	alias := Candidate{TorrentID: "alias", Seeders: 20, Leechers: 100}
	site := &fakeSite{candidates: []Candidate{alias}}
	fetch := &fakeFetcher{}
	fs := newFakeTorrentFS()
	setupLanding(site, fetch, fs, alias)
	// setupLanding's exact byte encoding is taken from its fetch fixture.
	for _, b := range fetch.files {
		fs.infoHashByBytes[string(b)] = "hashA"
	}
	s := New(site, fs, fetch, newFakeClock(time.Now()), baseParams(), WithRetired(history))
	if err := s.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.Pool()) != 0 || !fs.entries["hashA"].deleted {
		t.Fatal("alias resurrected or left uncategorized")
	}
	if !history.hasSiteID("alias") {
		t.Fatal("alias not remembered")
	}
	if err := s.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	if fs.addTorrentCalls != 1 {
		t.Fatal("known retired alias downloaded again")
	}
}

func TestScan_SeedersUpperBoundExclusive(t *testing.T) {
	for _, tc := range []struct {
		seeders int
		want    int
	}{{10, 0}, {11, 1}, {49, 1}, {50, 0}, {51, 0}} {
		t.Run(fmt.Sprint(tc.seeders), func(t *testing.T) {
			c := Candidate{TorrentID: "A", Seeders: tc.seeders, Leechers: 100}
			site := &fakeSite{candidates: []Candidate{c}}
			fetch := &fakeFetcher{}
			fs := newFakeTorrentFS()
			setupLanding(site, fetch, fs, c)
			p := baseParams()
			p.SeedersMax = 50
			s := New(site, fs, fetch, newFakeClock(time.Now()), p)
			if err := s.Scan(ctx); err != nil {
				t.Fatal(err)
			}
			if fs.addTorrentCalls != tc.want {
				t.Fatalf("seeders=%d adds=%d want %d", tc.seeders, fs.addTorrentCalls, tc.want)
			}
		})
	}
}

func TestRetired_CorruptStateFailsClosed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "retired.json")
	if err := os.WriteFile(p, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenRetired(p); err == nil {
		t.Fatal("corrupt state silently accepted")
	}
}

func TestRetired_WriteFailurePreventsEviction(t *testing.T) {
	p := filepath.Join(t.TempDir(), "parent")
	if err := os.WriteFile(p, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	h := &Retired{path: filepath.Join(p, "retired.json"), Records: map[string][]string{}}
	site := &fakeSite{}
	fs := newFakeTorrentFS()
	fs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}
	s := New(site, fs, &fakeFetcher{}, newFakeClock(time.Now()), baseParams(), WithRetired(h))
	if err := s.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.evict(ctx, s.pool["hashA"]); err == nil {
		t.Fatal("want persistence failure")
	}
	if len(fs.deleteTorrentCalls) != 0 {
		t.Fatal("deleted before permanent history was saved")
	}
}

func TestEvict_DeletingIsNotSuccess(t *testing.T) {
	fs := newFakeTorrentFS()
	fs.operationStatus = &OperationStatus{Done: false}
	fs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: "boost-mteam"}
	h, _ := OpenRetired("")
	s := New(&fakeSite{}, fs, &fakeFetcher{}, newFakeClock(time.Now()), baseParams(), WithRetired(h))
	if err := s.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := s.evict(c, s.pool["hashA"]); err == nil {
		t.Fatal("reported success before operation completed")
	}
	if len(s.Pool()) != 1 {
		t.Fatal("lost pending member")
	}
	if !h.hasID("hashA") {
		t.Fatal("retirement not recorded before delete")
	}
	// Remote deletion completes after the timeout. Reconciliation must allow retry.
	if err := s.evict(ctx, s.pool["hashA"]); err != nil {
		t.Fatal(err)
	}
	if len(s.Pool()) != 0 {
		t.Fatal("completed remote deletion never reconciled")
	}
}

func TestRetired_DeleteFailureThenRestartWithoutCandidates(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(fmt.Sprint(alias), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "retired.json")
			h, _ := OpenRetired(path)
			fs := newFakeTorrentFS()
			site := &fakeSite{}
			fetch := &fakeFetcher{}
			s := New(site, fs, fetch, newFakeClock(time.Now()), baseParams(), WithRetired(h))
			fs.deleteErr = errors.New("injected deletion failure")
			if alias {
				if err := h.Remember("hashA", []string{"original"}); err != nil {
					t.Fatal(err)
				}
				c := Candidate{TorrentID: "alias", Seeders: 20, Leechers: 100}
				setupLanding(site, fetch, fs, c)
				site.candidates = []Candidate{c}
				for _, b := range fetch.files {
					fs.infoHashByBytes[string(b)] = "hashA"
				}
				if err := s.Scan(ctx); err == nil {
					t.Fatal("expected delete failure")
				}
			} else {
				fs.entries["hashA"] = &fakeTorrentFSEntry{infoHash: "hashA", category: baseParams().Category}
				if err := s.RebuildFromTorrentFS(ctx); err != nil {
					t.Fatal(err)
				}
				if err := s.evict(ctx, s.pool["hashA"]); err == nil {
					t.Fatal("expected delete failure")
				}
			}
			h, err := OpenRetired(path)
			if err != nil {
				t.Fatal(err)
			}
			if !h.pending["hashA"] {
				t.Fatal("cleanup intent lost")
			}
			fs.deleteErr = nil
			site.candidates = nil
			restarted := New(site, fs, fetch, newFakeClock(time.Now()), baseParams(), WithRetired(h))
			if err := restarted.RebuildFromTorrentFS(ctx); err != nil {
				t.Fatal(err)
			}
			if !fs.entries["hashA"].deleted || len(restarted.Pool()) != 0 {
				t.Fatal("retired task recovered as active or orphan retained")
			}
			h, err = OpenRetired(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(h.pending) != 0 || !h.hasID("hashA") {
				t.Fatal("cleanup not committed or permanent ban lost")
			}
		})
	}
}

func TestScan_RequiresFreeAndPrefersPreferredSection(t *testing.T) {
	paid := Candidate{TorrentID: "paid", Seeders: 20, Leechers: 100, Preferred: true}
	plain := Candidate{TorrentID: "plain-free", Seeders: 20, Leechers: 100, Free: true}
	preferred := Candidate{TorrentID: "preferred-free", Seeders: 20, Leechers: 100, Free: true, Preferred: true}
	// Site order deliberately puts the preferred free candidate last.
	site := &fakeSite{candidates: []Candidate{paid, plain, preferred}}
	fetch := &fakeFetcher{}
	fs := newFakeTorrentFS()
	for _, c := range site.candidates {
		setupLanding(site, fetch, fs, c)
	}
	p := baseParams()
	p.RequireFree = true
	p.PoolSize = 1
	s := New(site, fs, fetch, newFakeClock(time.Now()), p)
	if err := s.Scan(ctx); err != nil {
		t.Fatal(err)
	}
	pool := s.Pool()
	if len(pool) != 1 {
		t.Fatalf("pool = %d members, want 1", len(pool))
	}
	if pool[0].TorrentFSID != "hashpreferred-free" {
		t.Fatalf("pooled %s, want the preferred-section free candidate", pool[0].TorrentFSID)
	}
	if fs.addTorrentCalls != 1 {
		t.Fatalf("adds = %d, want 1 (paid candidates must never be downloaded)", fs.addTorrentCalls)
	}
}
