package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCache struct {
	states     map[string]CacheStatus
	inspectErr map[string]error
	refillErr  error
	calls      []string
	targets    []int64
}

func (f *fakeCache) InspectCache(_ context.Context, id string) (CacheStatus, error) {
	return f.states[id], f.inspectErr[id]
}
func (f *fakeCache) RefillCache(_ context.Context, id, category string, target int64) error {
	f.calls = append(f.calls, id)
	f.targets = append(f.targets, target)
	return f.refillErr
}
func cacheFixture(t *testing.T, n int) (*Scheduler, *fakeCache, *fakeClock) {
	t.Helper()
	fs := newFakeTorrentFS()
	c := &fakeCache{states: map[string]CacheStatus{}, inspectErr: map[string]error{}}
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		fs.entries[id] = &fakeTorrentFSEntry{infoHash: id, category: "boost-mteam"}
		c.states[id] = CacheStatus{TotalBytes: 1024 * 1024 * 1024, Category: "boost-mteam"}
	}
	s := New(&fakeSite{}, fs, &fakeFetcher{}, clock, baseParams(), WithCacheMaintenance(c))
	if err := s.RebuildFromTorrentFS(ctx); err != nil {
		t.Fatal(err)
	}
	return s, c, clock
}
func TestCacheMaintenance_Watermarks(t *testing.T) {
	for _, tt := range []struct {
		name          string
		cached, total int64
		want          int
		target        int64
	}{
		{"below", CacheLowWaterBytes - 1, 1024 << 20, 1, CacheTargetBytes},
		{"equal", CacheLowWaterBytes, 1024 << 20, 0, 0},
		{"above", 90 << 20, 1024 << 20, 0, 0},
		{"small", 0, 32 << 20, 1, 32 << 20},
		{"smallComplete", 32 << 20, 32 << 20, 0, 0},
		{"notReady", 0, 0, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, c, _ := cacheFixture(t, 1)
			st := c.states["a"]
			st.CachedBytes = tt.cached
			st.TotalBytes = tt.total
			c.states["a"] = st
			if err := s.DetectColdTorrents(ctx); err != nil {
				t.Fatal(err)
			}
			if len(c.calls) != tt.want {
				t.Fatalf("calls=%v want %d", c.calls, tt.want)
			}
			if tt.want > 0 && c.targets[0] != tt.target {
				t.Fatalf("target=%d", c.targets[0])
			}
		})
	}
}
func TestCacheMaintenance_ActiveTasksBoundAndNotReset(t *testing.T) {
	s, c, _ := cacheFixture(t, 4)
	st := c.states["a"]
	st.Active = true
	c.states["a"] = st
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 || c.calls[0] != "b" {
		t.Fatalf("running task replaced or budget exceeded: %v", c.calls)
	}
	st = c.states["b"]
	st.Active = true
	c.states["b"] = st
	c.calls = nil
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 0 {
		t.Fatalf("overcommitted: %v", c.calls)
	}
}
func TestCacheMaintenance_RetiredAndForeignUntouched(t *testing.T) {
	s, c, _ := cacheFixture(t, 3)
	if err := s.retired.RememberPending("a", nil); err != nil {
		t.Fatal(err)
	}
	st := c.states["b"]
	st.Category = "Movie"
	c.states["b"] = st
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 || c.calls[0] != "c" {
		t.Fatalf("touched retired/foreign torrent: %v", c.calls)
	}
}
func TestCacheMaintenance_ErrorBackoffAndSamplingUnaffected(t *testing.T) {
	s, c, clock := cacheFixture(t, 1)
	c.refillErr = errors.New("network")
	if err := s.DetectColdTorrents(ctx); err == nil {
		t.Fatal("expected refill failure")
	}
	first := s.pool["a"].samples[0].at
	clock.Advance(time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 {
		t.Fatal("retry ignored backoff")
	}
	clock.Advance(time.Minute)
	if err := s.DetectColdTorrents(ctx); err == nil {
		t.Fatal("expected second failure")
	}
	if got := s.pool["a"].nextRefillAt.Sub(clock.Now()); got != 4*time.Minute {
		t.Fatalf("backoff=%v", got)
	}
	c.refillErr = nil
	clock.Advance(4 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if s.pool["a"].refillFailures != 2 || !s.pool["a"].samples[0].at.Equal(first) || len(s.pool["a"].samples) != 4 {
		t.Fatal("refill changed sampling history or failed to reset backoff")
	}
}
func TestCacheMaintenance_UnknownTaskStateFailsClosed(t *testing.T) {
	s, c, _ := cacheFixture(t, 2)
	c.inspectErr["b"] = errors.New("unknown task state")
	if err := s.DetectColdTorrents(ctx); err == nil {
		t.Fatal("expected inspection failure")
	}
	if len(c.calls) != 0 {
		t.Fatalf("unknown concurrency budget: %v", c.calls)
	}
	if len(s.pool["a"].samples) != 1 {
		t.Fatal("cache failure prevented sampling")
	}
}
func TestCacheMaintenance_FairnessAndSuccessDebounce(t *testing.T) {
	s, c, clock := cacheFixture(t, 4)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 2 {
		t.Fatal(c.calls)
	}
	clock.Advance(2 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 4 || c.calls[2] != "c" || c.calls[3] != "d" {
		t.Fatalf("starved later members: %v", c.calls)
	}
}
