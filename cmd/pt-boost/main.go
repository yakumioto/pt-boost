// Command pt-boost runs the resident hot-torrent-pool service described in
// docs/design-overview.md: one Scheduler per configured PT site, driving
// torrentfs-go over its REST API to keep each site's category-tagged pool
// filled with piece-cached torrents and to evict cold ones.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yakumioto/pt-boost/internal/app"
	"github.com/yakumioto/pt-boost/internal/config"
	"github.com/yakumioto/pt-boost/internal/scheduler"
	"github.com/yakumioto/pt-boost/internal/site/mteam"
	"github.com/yakumioto/pt-boost/internal/statusapi"
	"github.com/yakumioto/pt-boost/internal/torrentfs"
)

// coldCheckInterval is the cold-torrent detection period design-
// overview.md's workflow step ② suggests: short enough to resolve well
// inside any configured cold_window, and deliberately a separate ticker
// from each site's (much longer) scan_interval so the two loops never
// block each other.
const coldCheckInterval = 2 * time.Minute

func main() {
	configPath := flag.String("config", "config.yaml", "path to the pt-boost YAML config file")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if err := run(*configPath, logger); err != nil {
		logger.Error("pt-boost exited with error", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	torrentFS := torrentfs.New(cfg.TorrentFS.BaseURL, cfg.TorrentFS.Username, cfg.TorrentFS.Password)

	sites := make([]app.SiteRunner, 0, len(cfg.Sites))
	statusSites := make([]statusapi.Site, 0, len(cfg.Sites))
	for _, site := range cfg.Sites {
		// Every log line this site's Scheduler emits is tagged with its
		// site name up front, so operators reading combined stderr
		// output (or /status) can attribute an event without the
		// scheduler package itself knowing about multi-site wiring.
		siteLogger := logger.With("site", site.Name)

		// v1 wires every configured site to the M-Team adapter (see
		// GLOSSARY.md "站点适配器 (Site Adapter)": the Site interface is
		// generic, but only M-Team has a real implementation today).
		// Adding a second site type means branching on something like a
		// future site.Type field here, once a second adapter package
		// exists; nothing in internal/scheduler changes.
		mteamClient := mteam.New(mteam.Config{
			BaseURL:     site.BaseURL,
			APIKey:      site.APIKey,
			SeedersMin:  site.SeedersMin,
			LeechersMin: site.LeechersMin,
		}, mteam.WithSearchInterval(site.RequestInterval.Duration))

		sched := scheduler.New(mteamClient, torrentFS, mteamClient, scheduler.SystemClock{}, scheduler.Params{
			Category:          site.Category,
			PoolSize:          site.PoolSize,
			SeedersMin:        site.SeedersMin,
			LeechersMin:       site.LeechersMin,
			MaxScanPages:      site.MaxScanPages,
			PageSize:          site.PageSize,
			TargetShareBytes:  site.TargetShareBytes.Bytes,
			ColdWindow:        site.ColdWindow.Duration,
			ColdRateThreshold: site.ColdRateThreshold.BytesPerSecond,
		}, scheduler.WithLogger(siteLogger))

		sites = append(sites, app.SiteRunner{
			Name:              site.Name,
			Scheduler:         sched,
			ScanInterval:      site.ScanInterval.Duration,
			ColdCheckInterval: coldCheckInterval,
		})
		statusSites = append(statusSites, statusapi.Site{Name: site.Name, Reporter: sched})
	}

	// Cancel on SIGINT/SIGTERM so every site's ticker loops, the /status
	// server, and any in-flight HTTP call that respects ctx unwind
	// instead of leaving the process to hang or be killed mid-request.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	statusServer := &http.Server{
		Addr:    cfg.Status.ListenAddr,
		Handler: statusapi.NewHandler(statusSites),
	}
	statusServerDone := make(chan struct{})
	go func() {
		defer close(statusServerDone)
		logger.Info("status endpoint listening", "addr", cfg.Status.ListenAddr)
		if err := statusServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("status endpoint stopped unexpectedly", "error", err)
		}
	}()

	logger.Info("pt-boost starting", "sites", len(sites))
	runner := &app.Runner{Sites: sites, Logger: logger}
	runner.Run(ctx)

	// runner.Run only returns once ctx is cancelled (SIGINT/SIGTERM), so
	// shut the status server down on the same signal rather than leaving
	// it to be killed: give in-flight GET /status requests a short grace
	// period to finish instead of dropping them mid-response.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := statusServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("status endpoint did not shut down cleanly", "error", err)
	}
	<-statusServerDone

	logger.Info("pt-boost stopped")
	return nil
}
