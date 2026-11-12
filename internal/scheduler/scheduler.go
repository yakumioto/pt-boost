package scheduler

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Params holds the per-site tuning Scheduler needs. It mirrors the relevant
// fields of config.SiteConfig but is declared locally so this package does
// not depend on internal/config.
type Params struct {
	Category          string
	PoolSize          int
	SeedersMin        int
	LeechersMin       int
	MaxScanPages      int
	PageSize          int
	TargetShareBytes  int64
	ColdWindow        time.Duration
	ColdRateThreshold int64 // bytes per second
}

// sample is one {time, cumulative uploaded bytes} observation used to
// compute a pool member's rolling upload rate.
type sample struct {
	at            time.Time
	uploadedBytes int64
}

// member is one hot-torrent pool entry, tracked purely in memory (see
// GLOSSARY.md "热种池"; no local persistence, per design-overview.md §5).
type member struct {
	infoHash      string
	torrentFSID   string
	joinedAt      time.Time
	samples       []sample
	cold          bool
	lastRateBytes int64
}

// PoolMember is the read-only snapshot of one pool entry exposed to callers
// outside this package (e.g. a future /status endpoint).
type PoolMember struct {
	InfoHash    string
	TorrentFSID string
	JoinedAt    time.Time
	Cold        bool
}

// Scheduler owns one PT site's hot-torrent pool: scanning for candidates,
// filling/replacing the pool, detecting cold torrents, and landing new
// torrents through torrentfs-go. It depends only on the Site,
// TorrentFSClient, TorrentFileFetcher and Clock interfaces, so tests can
// exercise it end-to-end against in-memory fakes.
type Scheduler struct {
	site      Site
	torrentFS TorrentFSClient
	fetcher   TorrentFileFetcher
	clock     Clock
	params    Params

	pool map[string]*member // keyed by InfoHash
}

// New constructs a Scheduler for one site.
func New(site Site, torrentFS TorrentFSClient, fetcher TorrentFileFetcher, clock Clock, params Params) *Scheduler {
	return &Scheduler{
		site:      site,
		torrentFS: torrentFS,
		fetcher:   fetcher,
		clock:     clock,
		params:    params,
		pool:      make(map[string]*member),
	}
}

// Pool returns a snapshot of the current pool members, sorted by InfoHash
// for deterministic output.
func (s *Scheduler) Pool() []PoolMember {
	out := make([]PoolMember, 0, len(s.pool))
	for _, m := range s.pool {
		out = append(out, PoolMember{
			InfoHash:    m.infoHash,
			TorrentFSID: m.torrentFSID,
			JoinedAt:    m.joinedAt,
			Cold:        m.cold,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InfoHash < out[j].InfoHash })
	return out
}

// RebuildFromTorrentFS reconstructs the pool from torrentfs-go's own state
// by filtering ListTorrents on this site's category tag (see
// design-overview.md §5: no local persistence). Rebuilt members start with
// an empty sample window, so cold detection naturally needs up to
// ColdWindow before it can judge them again.
func (s *Scheduler) RebuildFromTorrentFS(ctx context.Context) error {
	views, err := s.torrentFS.ListTorrents(ctx)
	if err != nil {
		return fmt.Errorf("rebuilding pool: listing torrents: %w", err)
	}

	now := s.clock.Now()
	s.pool = make(map[string]*member)
	for _, v := range views {
		if v.Category != s.params.Category {
			continue
		}
		s.pool[v.InfoHash] = &member{
			infoHash:    v.InfoHash,
			torrentFSID: v.ID,
			joinedAt:    now,
		}
	}
	return nil
}

// Scan runs one scan-loop tick: search for candidates, filter out anything
// below threshold or already pooled, then either fill the pool (if it has
// room) or replace the worst currently-cold member with the best new
// candidate (if the pool is full and a cold member exists).
func (s *Scheduler) Scan(ctx context.Context) error {
	candidates, err := s.site.SearchHotTorrents(ctx, SearchParams{
		MaxPages: s.params.MaxScanPages,
		PageSize: s.params.PageSize,
	})
	if err != nil {
		return fmt.Errorf("scanning for hot torrents: %w", err)
	}

	for _, c := range candidates {
		if c.Seeders <= s.params.SeedersMin || c.Leechers <= s.params.LeechersMin {
			continue
		}
		if _, alreadyPooled := s.pool[c.InfoHash]; alreadyPooled {
			continue
		}

		if len(s.pool) < s.params.PoolSize {
			if err := s.land(ctx, c); err != nil {
				return fmt.Errorf("landing candidate %s: %w", c.TorrentID, err)
			}
			continue
		}

		worst := s.worstColdMember()
		if worst == nil {
			// Pool full, no cold member to replace: this round does
			// nothing further (design-overview.md §1.5).
			continue
		}
		if err := s.evict(ctx, worst); err != nil {
			return fmt.Errorf("evicting cold member %s: %w", worst.infoHash, err)
		}
		if err := s.land(ctx, c); err != nil {
			return fmt.Errorf("landing candidate %s: %w", c.TorrentID, err)
		}
	}

	return nil
}

// worstColdMember returns the cold member with the lowest last-measured
// rate, or nil if no member is currently cold.
func (s *Scheduler) worstColdMember() *member {
	var worst *member
	for _, m := range s.pool {
		if !m.cold {
			continue
		}
		if worst == nil || m.lastRateBytes < worst.lastRateBytes {
			worst = m
		}
	}
	return worst
}

// land runs the fixed four-step landing sequence (design-overview.md §3):
// download token -> fetch .torrent bytes -> add to torrentfs-go -> set
// category -> trigger piece download covering TargetShareBytes from byte
// 0. Any failure mid-sequence aborts this candidate for the round; it is
// not retried or rolled back here (see ticket Out of Scope: no saga).
func (s *Scheduler) land(ctx context.Context, c Candidate) error {
	token, err := s.site.GetDownloadToken(ctx, c.TorrentID)
	if err != nil {
		return fmt.Errorf("getting download token: %w", err)
	}

	torrentBytes, err := s.fetcher.Fetch(ctx, token)
	if err != nil {
		return fmt.Errorf("fetching .torrent bytes: %w", err)
	}

	id, err := s.torrentFS.AddTorrent(ctx, torrentBytes)
	if err != nil {
		return fmt.Errorf("adding torrent to torrentfs-go: %w", err)
	}

	if err := s.torrentFS.SetCategory(ctx, id, s.params.Category); err != nil {
		return fmt.Errorf("setting category: %w", err)
	}

	length := s.params.TargetShareBytes
	if err := s.torrentFS.TriggerPieceDownload(ctx, id, ByteRange{Offset: 0, Length: length}); err != nil {
		return fmt.Errorf("triggering piece download: %w", err)
	}

	s.pool[c.InfoHash] = &member{
		infoHash:    c.InfoHash,
		torrentFSID: id,
		joinedAt:    s.clock.Now(),
	}
	return nil
}

// evict removes a member from torrentfs-go and drops it (and its sample
// history) from the pool.
func (s *Scheduler) evict(ctx context.Context, m *member) error {
	if _, err := s.torrentFS.DeleteTorrent(ctx, m.torrentFSID); err != nil {
		return fmt.Errorf("deleting torrent: %w", err)
	}
	delete(s.pool, m.infoHash)
	return nil
}

// DetectColdTorrents runs one cold-detection tick (design-overview.md §2):
// sample every pool member's current UploadedBytes, prune samples older
// than ColdWindow, and mark a member cold only once its sample window
// spans the full ColdWindow and the resulting rate is below
// ColdRateThreshold. Members younger than ColdWindow are left un-judged
// (never marked cold) rather than guessed at from a partial window.
func (s *Scheduler) DetectColdTorrents(ctx context.Context) error {
	views, err := s.torrentFS.ListTorrents(ctx)
	if err != nil {
		return fmt.Errorf("detecting cold torrents: listing torrents: %w", err)
	}

	uploaded := make(map[string]int64, len(views))
	for _, v := range views {
		uploaded[v.InfoHash] = v.UploadedBytes
	}

	now := s.clock.Now()
	for infoHash, m := range s.pool {
		bytes, ok := uploaded[infoHash]
		if !ok {
			// torrentfs-go no longer reports this torrent; leave its last
			// known cold/rate state untouched rather than guessing.
			continue
		}

		m.samples = append(m.samples, sample{at: now, uploadedBytes: bytes})
		cutoff := now.Add(-s.params.ColdWindow)
		m.samples = pruneBefore(m.samples, cutoff)

		if len(m.samples) < 2 {
			continue
		}
		oldest := m.samples[0]
		newest := m.samples[len(m.samples)-1]
		span := newest.at.Sub(oldest.at)
		if span < s.params.ColdWindow {
			// Window doesn't yet cover the full ColdWindow: not enough
			// history to judge (covers both brand-new members and ones
			// rebuilt after a restart).
			continue
		}

		rate := int64(0)
		if span > 0 {
			rate = int64(float64(newest.uploadedBytes-oldest.uploadedBytes) / span.Seconds())
		}
		m.lastRateBytes = rate
		m.cold = rate < s.params.ColdRateThreshold
	}

	return nil
}

func pruneBefore(samples []sample, cutoff time.Time) []sample {
	i := 0
	for i < len(samples) && samples[i].at.Before(cutoff) {
		i++
	}
	// Keep one sample before the cutoff (if any) so the window has an
	// "oldest" point at or before cutoff to measure the full span from.
	if i > 1 {
		i--
	}
	return samples[i:]
}
