// Package app assembles pt-boost's resident process: one independent pair
// of loops (scan, cold-torrent detection) per configured site, run until
// the caller cancels the context. It depends only on the scheduler
// package's own method shapes (narrow interfaces below), so the loop
// orchestration itself can be tested with simple fakes instead of a real
// Scheduler and its HTTP-backed dependencies.
package app

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Scanner is satisfied by *scheduler.Scheduler. Kept as a narrow interface
// here so Runner's tests do not need a real Scheduler (or its Site/
// TorrentFSClient dependencies) to verify the loop behaviour itself.
type Scanner interface {
	Scan(ctx context.Context) error
}

// ColdDetector is satisfied by *scheduler.Scheduler.
type ColdDetector interface {
	DetectColdTorrents(ctx context.Context) error
}

// Rebuilder is satisfied by *scheduler.Scheduler.
type Rebuilder interface {
	RebuildFromTorrentFS(ctx context.Context) error
}

// SiteRunner is everything Runner needs to drive one site's pool
// maintenance: the scheduler-shaped dependency and the two independent
// periods design-overview.md calls for (scan_interval and a separate,
// shorter cold-detection period covering cold_window).
type SiteRunner struct {
	Name      string
	Scheduler interface {
		Scanner
		ColdDetector
		Rebuilder
	}
	ScanInterval      time.Duration
	ColdCheckInterval time.Duration
}

// Runner runs every configured site's scan loop and cold-detection loop
// concurrently and independently (per #6's acceptance criteria: the two
// loops never share a ticker, and one site's errors never stop another
// site or the process). Run blocks until ctx is done and every loop has
// exited.
type Runner struct {
	Sites  []SiteRunner
	Logger *slog.Logger
}

// Run starts all loops and blocks until ctx is cancelled and every
// goroutine has returned.
func (r *Runner) Run(ctx context.Context) {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var wg sync.WaitGroup
	for _, site := range r.Sites {
		wg.Add(2)
		go func(s SiteRunner) {
			defer wg.Done()
			runScanLoop(ctx, logger, s)
		}(site)
		go func(s SiteRunner) {
			defer wg.Done()
			runColdLoop(ctx, logger, s)
		}(site)
	}
	wg.Wait()
}

// runScanLoop rebuilds the in-memory pool from torrentfs-go once at
// startup (see GLOSSARY.md "本地分类标签": category tags are the sole
// restart-recovery mechanism, no local database), then scans on
// ScanInterval until ctx is done. A failed rebuild or scan is logged and
// the loop continues — this site's errors must never take down the
// process or another site's loop.
func runScanLoop(ctx context.Context, logger *slog.Logger, s SiteRunner) {
	if err := s.Scheduler.RebuildFromTorrentFS(ctx); err != nil {
		logger.Error("rebuild pool from torrentfs-go failed", "site", s.Name, "error", err)
	}

	ticker := time.NewTicker(s.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Scheduler.Scan(ctx); err != nil {
				logger.Error("scan failed", "site", s.Name, "error", err)
			}
		}
	}
}

// runColdLoop detects cold torrents on ColdCheckInterval until ctx is
// done, independently of runScanLoop's ticker.
func runColdLoop(ctx context.Context, logger *slog.Logger, s SiteRunner) {
	ticker := time.NewTicker(s.ColdCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Scheduler.DetectColdTorrents(ctx); err != nil {
				logger.Error("cold-torrent detection failed", "site", s.Name, "error", err)
			}
		}
	}
}
