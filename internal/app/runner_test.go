package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeScheduler is a narrow, in-memory stand-in for *scheduler.Scheduler
// satisfying exactly the Scanner/ColdDetector/Rebuilder shapes Runner
// depends on, so these tests exercise only the loop orchestration (ticking,
// independence of the two loops, per-site error isolation, shutdown) and
// never touch a real Scheduler or its HTTP-backed Site/TorrentFSClient.
type fakeScheduler struct {
	mu              sync.Mutex
	rebuildCalls    int
	scanCalls       int
	coldCalls       int
	scanErrOnFirstN int
}

func (f *fakeScheduler) RebuildFromTorrentFS(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuildCalls++
	return nil
}

func (f *fakeScheduler) Scan(ctx context.Context) error {
	f.mu.Lock()
	f.scanCalls++
	n, errOnFirstN := f.scanCalls, f.scanErrOnFirstN
	f.mu.Unlock()
	if n <= errOnFirstN {
		return fmt.Errorf("fake scan error #%d", n)
	}
	return nil
}

func (f *fakeScheduler) DetectColdTorrents(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.coldCalls++
	return nil
}

func (f *fakeScheduler) counts() (rebuild, scan, cold int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rebuildCalls, f.scanCalls, f.coldCalls
}

// silentLogger discards output so test runs stay quiet; the fake errors
// injected below are expected and would otherwise be noisy.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitUntil polls check every 2ms until it returns true or timeout elapses,
// failing the test on timeout. Loop-driven behaviour is inherently
// time-based; polling with a generous timeout keeps these tests fast in
// the common case without being flaky under load.
func waitUntil(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestRun_RebuildsOnceThenScansRepeatedly(t *testing.T) {
	fake := &fakeScheduler{}
	r := &Runner{
		Sites: []SiteRunner{{
			Name:              "site-a",
			Scheduler:         fake,
			ScanInterval:      5 * time.Millisecond,
			ColdCheckInterval: time.Hour, // irrelevant to this test; keep it quiet
		}},
		Logger: silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	waitUntil(t, time.Second, func() bool {
		_, scan, _ := fake.counts()
		return scan >= 3
	})

	rebuild, _, _ := fake.counts()
	if rebuild != 1 {
		t.Errorf("rebuildCalls = %d, want exactly 1", rebuild)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRun_ColdLoopTicksIndependentlyOfScanLoop(t *testing.T) {
	fake := &fakeScheduler{}
	r := &Runner{
		Sites: []SiteRunner{{
			Name:              "site-a",
			Scheduler:         fake,
			ScanInterval:      time.Hour, // essentially never fires during this test
			ColdCheckInterval: 5 * time.Millisecond,
		}},
		Logger: silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	waitUntil(t, time.Second, func() bool {
		_, _, cold := fake.counts()
		return cold >= 3
	})

	_, scan, _ := fake.counts()
	if scan != 0 {
		t.Errorf("scanCalls = %d, want 0 (scan loop must not be driven by the cold-check ticker)", scan)
	}
}

func TestRun_ScanErrorDoesNotStopTheLoop(t *testing.T) {
	fake := &fakeScheduler{scanErrOnFirstN: 2}
	r := &Runner{
		Sites: []SiteRunner{{
			Name:              "site-a",
			Scheduler:         fake,
			ScanInterval:      5 * time.Millisecond,
			ColdCheckInterval: time.Hour,
		}},
		Logger: silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// The first two Scan calls fail; the loop must keep going well past
	// that point instead of exiting or panicking.
	waitUntil(t, time.Second, func() bool {
		_, scan, _ := fake.counts()
		return scan >= 5
	})
}

func TestRun_MultipleSitesRunIndependently(t *testing.T) {
	fakeA := &fakeScheduler{}
	fakeB := &fakeScheduler{}
	r := &Runner{
		Sites: []SiteRunner{
			{Name: "site-a", Scheduler: fakeA, ScanInterval: 5 * time.Millisecond, ColdCheckInterval: time.Hour},
			{Name: "site-b", Scheduler: fakeB, ScanInterval: 50 * time.Millisecond, ColdCheckInterval: time.Hour},
		},
		Logger: silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	waitUntil(t, time.Second, func() bool {
		_, scanA, _ := fakeA.counts()
		_, scanB, _ := fakeB.counts()
		return scanA >= 3 && scanB >= 1
	})
}

func TestRun_ReturnsPromptlyAfterContextCancelled(t *testing.T) {
	fake := &fakeScheduler{}
	r := &Runner{
		Sites: []SiteRunner{{
			Name:              "site-a",
			Scheduler:         fake,
			ScanInterval:      5 * time.Millisecond,
			ColdCheckInterval: 5 * time.Millisecond,
		}},
		Logger: silentLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	waitUntil(t, time.Second, func() bool {
		_, scan, cold := fake.counts()
		return scan >= 1 && cold >= 1
	})

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return promptly after context cancellation")
	}
}
