package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Params holds the per-site tuning Scheduler needs. It mirrors the relevant
// fields of config.SiteConfig but is declared locally so this package does
// not depend on internal/config.
type Params struct {
	Category          string
	PoolSize          int
	SeedersMin        int
	SeedersMax        int
	RequireFree       bool // skip candidates that would cost download quota // exclusive; 0 means no upper bound for legacy callers
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
	infoHash       string
	torrentFSID    string
	joinedAt       time.Time
	samples        []sample
	cold           bool
	lastRateBytes  int64
	lastRefillAt   time.Time
	nextRefillAt   time.Time
	refillFailures int
	awaitingRefill bool
}

// PoolMember is the read-only snapshot of one pool entry exposed to callers
// outside this package, e.g. the /status endpoint (internal/statusapi).
type PoolMember struct {
	InfoHash    string
	TorrentFSID string
	JoinedAt    time.Time
	Cold        bool
	// LastRateBytesPerSecond is the most recently computed upload rate
	// behind the Cold verdict. It is 0 until the member's sample window
	// has spanned a full ColdWindow at least once (see
	// DetectColdTorrents); a 0 here does not by itself mean the member is
	// cold, only that it hasn't been judged yet.
	LastRateBytesPerSecond int64
	// SampleCount is how many {time, uploaded_bytes} observations are
	// currently retained for this member (see DetectColdTorrents /
	// pruneBefore). 0 or 1 means the cold verdict has never been computed
	// yet — the member is too new, or was just rebuilt from torrentfs-go
	// after a restart (GLOSSARY.md "本地分类标签": restart recovery starts
	// every member's sample window empty again).
	SampleCount int
	// OldestSampleAt and NewestSampleAt bound the current sample window
	// (zero Time if SampleCount is 0). WindowSpan is their difference;
	// the member is only judged cold/active once WindowSpan reaches the
	// configured ColdWindow.
	OldestSampleAt time.Time
	NewestSampleAt time.Time
	WindowSpan     time.Duration
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
	logger    *slog.Logger
	retired   *Retired
	cache     CacheMaintainer

	// mu guards every field below. Runner (internal/app) drives Scan and
	// DetectColdTorrents for the same Scheduler from two independent,
	// concurrently-running goroutines (see #6: the scan loop and the
	// cold-detection loop never share a ticker), and Pool/LastScanAt are
	// additionally read from a third goroutine by the /status HTTP
	// handler (#7), without it the pool map would see concurrent
	// reads/writes from different goroutines. A single coarse mutex is
	// the simplest correct fix: each exported method here is called
	// relatively infrequently (on the order of once per scan_interval /
	// cold-check period / HTTP request), so simplicity wins over
	// minimizing how long the lock is held. DetectColdTorrents and
	// RebuildFromTorrentFS fetch from torrentfs-go before taking the
	// lock, since that network call never touches s.pool; Scan takes the
	// lock before its own network call (SearchHotTorrents) and holds it
	// through every land()/evict() network call in the loop, since this
	// is the simplest way to guarantee the whole decide-then-mutate
	// sequence per candidate is internally consistent. The tradeoff: a
	// slow Scan temporarily delays that site's /status reads and cold
	// check. Acceptable for v1; revisit if it becomes a real problem.
	mu sync.Mutex

	pool map[string]*member // keyed by torrentfs-go id
	// knownIDs caches site torrent id -> torrentfs-go id for every
	// candidate this process has added, so a steady-state scan does not
	// re-download the .torrent of members it already pooled. In-memory
	// only: after a restart it starts empty and each still-hot member
	// costs one extra download + idempotent add (ADR 0003).
	knownIDs   map[string]string
	lastScanAt time.Time
}

// Option configures optional Scheduler behaviour.
type Option func(*Scheduler)

// WithLogger sets the structured logger Scheduler uses for its key
// decision points (candidate landed, cold member replaced/evicted, cold
// verdict changes, pool rebuilt). Callers typically pass a logger already
// tagged with the site name (e.g. logger.With("site", name)) so every line
// this Scheduler emits is attributable without repeating that at each call
// site. Defaults to slog.Default() if not set.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Scheduler) { s.logger = logger }
}

// New constructs a Scheduler for one site.
func New(site Site, torrentFS TorrentFSClient, fetcher TorrentFileFetcher, clock Clock, params Params, opts ...Option) *Scheduler {
	s := &Scheduler{
		site:      site,
		torrentFS: torrentFS,
		fetcher:   fetcher,
		clock:     clock,
		params:    params,
		logger:    slog.Default(),
		pool:      make(map[string]*member),
		knownIDs:  make(map[string]string),
	}
	s.retired, _ = OpenRetired("") // application injects persistent history
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// LastScanAt reports when Scan was last invoked (regardless of whether it
// returned an error), or the zero time.Time if Scan has never run — e.g.
// right after process startup, before the first scan tick. Callers (the
// /status endpoint) should treat the zero value as "not scanned yet", not
// an error.
func (s *Scheduler) LastScanAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastScanAt
}

// Pool returns a snapshot of the current pool members, sorted by InfoHash
// for deterministic output.
func (s *Scheduler) Pool() []PoolMember {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]PoolMember, 0, len(s.pool))
	for _, m := range s.pool {
		pm := PoolMember{
			InfoHash:               m.infoHash,
			TorrentFSID:            m.torrentFSID,
			JoinedAt:               m.joinedAt,
			Cold:                   m.cold,
			LastRateBytesPerSecond: m.lastRateBytes,
			SampleCount:            len(m.samples),
		}
		if len(m.samples) > 0 {
			pm.OldestSampleAt = m.samples[0].at
			pm.NewestSampleAt = m.samples[len(m.samples)-1].at
			pm.WindowSpan = pm.NewestSampleAt.Sub(pm.OldestSampleAt)
		}
		out = append(out, pm)
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

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	s.pool = make(map[string]*member)
	for _, v := range views {
		if v.Category != s.params.Category {
			continue
		}
		if s.retired.hasID(v.ID) {
			if err := s.retired.RememberPending(v.ID, nil); err != nil {
				return err
			}
			continue
		}
		s.pool[v.ID] = &member{
			infoHash:    v.InfoHash,
			torrentFSID: v.ID,
			joinedAt:    now,
		}
	}
	if err := s.cleanupRetired(ctx); err != nil {
		return fmt.Errorf("recovering pending retirement: %w", err)
	}
	s.logger.Info("pool rebuilt from torrentfs-go",
		"category", s.params.Category,
		"members", len(s.pool),
		"torrents_seen", len(views),
	)
	return nil
}

// Scan runs one scan-loop tick: search for candidates, filter out anything
// below threshold or already known to be pooled, then either fill the pool
// (if it has room) or replace the worst currently-cold member with the
// best new candidate (if the pool is full and a cold member exists).
//
// A candidate carries no infohash, so "is it already pooled?" is answered
// in two layers (ADR 0003): the in-process knownIDs cache (site torrent id
// -> torrentfs-go id) short-circuits candidates this process has already
// added, and anything else is added to torrentfs-go first — an idempotent
// call — so the returned id can be checked against the pool. That is why
// add happens before evict: a duplicate candidate must never cost a cold
// member its slot.
func (s *Scheduler) Scan(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastScanAt = s.clock.Now()
	if err := s.cleanupRetired(ctx); err != nil {
		return err
	}

	candidates, err := s.site.SearchHotTorrents(ctx, SearchParams{
		MaxPages: s.params.MaxScanPages,
		PageSize: s.params.PageSize,
	})
	if err != nil {
		return fmt.Errorf("scanning for hot torrents: %w", err)
	}

	s.logger.Debug("scan finished searching",
		"candidates", len(candidates),
		"pool_size", len(s.pool),
		"pool_capacity", s.params.PoolSize,
	)

	// preexisting is torrentfs-go's id -> category snapshot, fetched
	// lazily right before this round's first add. It tells a torrent
	// this round's AddTorrent created apart from one that was already
	// there (and possibly owned by someone else).
	var preexisting map[string]string

	SortCandidates(candidates)

	for _, c := range candidates {
		if s.params.RequireFree && !c.Free {
			s.logger.Debug("candidate is not free, skipped", "torrent_id", c.TorrentID)
			continue
		}
		if c.Seeders <= s.params.SeedersMin || (s.params.SeedersMax > 0 && c.Seeders >= s.params.SeedersMax) || c.Leechers <= s.params.LeechersMin {
			s.logger.Debug("candidate below thresholds, skipped",
				"torrent_id", c.TorrentID,
				"seeders", c.Seeders,
				"leechers", c.Leechers,
				"seeders_min", s.params.SeedersMin,
				"leechers_min", s.params.LeechersMin,
			)
			continue
		}
		if s.retired.hasSiteID(c.TorrentID) {
			s.logger.Debug("permanently retired candidate skipped", "torrent_id", c.TorrentID)
			continue
		}
		if id, known := s.knownIDs[c.TorrentID]; known {
			if s.retired.hasID(id) {
				continue
			}

			if _, pooled := s.pool[id]; pooled {
				s.logger.Debug("candidate already in pool, skipped",
					"torrent_id", c.TorrentID, "torrentfs_id", id)
				continue
			}
		}

		var worst *member
		if len(s.pool) >= s.params.PoolSize {
			worst = s.worstColdMember()
			if worst == nil {
				// Pool full, no cold member to replace: this round
				// does nothing further (design-overview.md §1.5), so
				// no .torrent is downloaded for this candidate.
				s.logger.Debug("pool full and no cold member, candidate not landed",
					"torrent_id", c.TorrentID)
				continue
			}
		}

		if preexisting == nil {
			views, err := s.torrentFS.ListTorrents(ctx)
			if err != nil {
				return fmt.Errorf("listing torrents before landing: %w", err)
			}
			preexisting = make(map[string]string, len(views))
			for _, v := range views {
				preexisting[v.ID] = v.Category
			}
		}

		id, err := s.add(ctx, c)
		if err != nil {
			return fmt.Errorf("landing candidate %s: %w", c.TorrentID, err)
		}
		s.knownIDs[c.TorrentID] = id
		if s.retired.hasID(id) {
			if _, existed := preexisting[id]; existed {
				// Existing tasks outside this pool are never deleted or claimed.
				if err := s.retired.Remember(id, []string{c.TorrentID}); err != nil {
					return err
				}
			} else {
				if err := s.retired.RememberPending(id, []string{c.TorrentID}); err != nil {
					return err
				}
				if err := s.evict(ctx, &member{torrentFSID: id}); err != nil {
					return err
				}
			}
			continue
		}
		if _, pooled := s.pool[id]; pooled {
			// Idempotent re-add of a member this process didn't know
			// the site id for (typically: first scan after a restart).
			s.logger.Debug("candidate resolved to an existing pool member, skipped",
				"torrent_id", c.TorrentID, "torrentfs_id", id)
			continue
		}
		if category, existed := preexisting[id]; existed {
			// torrentfs-go already managed this torrent before this
			// round and it is not a pool member, so it is someone
			// else's: the user's own library (typically with no
			// category at all) or another site's pool. Never re-tag,
			// evict for, or delete it — pooling it would eventually
			// get it deleted as a cold member.
			s.logger.Info("candidate already managed outside this pool, skipped",
				"torrent_id", c.TorrentID, "torrentfs_id", id, "category", category)
			continue
		}

		if worst != nil {
			s.logger.Info("replacing cold pool member",
				"cold_torrentfs_id", worst.torrentFSID,
				"cold_last_rate_bytes_per_second", worst.lastRateBytes,
				"incoming_torrent_id", c.TorrentID,
				"incoming_torrentfs_id", id,
			)
			if err := s.evict(ctx, worst); err != nil {
				// Safe to delete: id was not in preexisting, so this
				// round's add created it.
				s.rollbackAdd(ctx, c, id)
				return fmt.Errorf("evicting cold member %s: %w", worst.torrentFSID, err)
			}
			delete(preexisting, worst.torrentFSID) // no stale ownership after confirmed deletion
		}
		if err := s.activate(ctx, c, id); err != nil {
			return fmt.Errorf("landing candidate %s: %w", c.TorrentID, err)
		}
		preexisting[id] = s.params.Category
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

// add runs the first half of the landing sequence (design-overview.md §3):
// download token -> fetch .torrent bytes -> add to torrentfs-go. It
// returns torrentfs-go's id for the torrent, which is the only identity
// Scheduler ever has for it. The torrent is not a pool member yet: it has
// no category until activate runs.
func (s *Scheduler) add(ctx context.Context, c Candidate) (string, error) {
	s.logger.Info("landing candidate",
		"torrent_id", c.TorrentID,
		"seeders", c.Seeders,
		"leechers", c.Leechers,
		"category", s.params.Category,
	)

	token, err := s.site.GetDownloadToken(ctx, c.TorrentID)
	if err != nil {
		return "", fmt.Errorf("getting download token: %w", err)
	}
	s.logger.Debug("landing step: got download token", "torrent_id", c.TorrentID)

	torrentBytes, err := s.fetcher.Fetch(ctx, token)
	if err != nil {
		return "", fmt.Errorf("fetching .torrent bytes: %w", err)
	}
	s.logger.Debug("landing step: fetched .torrent bytes",
		"torrent_id", c.TorrentID, "bytes", len(torrentBytes))

	id, err := s.torrentFS.AddTorrent(ctx, torrentBytes)
	if err != nil {
		return "", fmt.Errorf("adding torrent to torrentfs-go: %w", err)
	}
	s.logger.Debug("landing step: added torrent to torrentfs-go",
		"torrent_id", c.TorrentID, "torrentfs_id", id)
	return id, nil
}

// activate runs the second half of the landing sequence: set category ->
// trigger piece download covering TargetShareBytes from byte 0 -> record
// the pool member. Any failure mid-sequence aborts this candidate for the
// round; it is not rolled back here (see ticket Out of Scope: no saga).
func (s *Scheduler) activate(ctx context.Context, c Candidate, id string) error {
	if err := s.torrentFS.SetCategory(ctx, id, s.params.Category); err != nil {
		return fmt.Errorf("setting category: %w", err)
	}
	s.logger.Debug("landing step: set category",
		"torrent_id", c.TorrentID, "torrentfs_id", id, "category", s.params.Category)

	length := s.params.TargetShareBytes
	if err := s.torrentFS.TriggerPieceDownload(ctx, id, ByteRange{Offset: 0, Length: length}); err != nil {
		return fmt.Errorf("triggering piece download: %w", err)
	}
	s.logger.Debug("landing step: triggered piece download",
		"torrent_id", c.TorrentID, "torrentfs_id", id, "offset", 0, "length", length)

	s.pool[id] = &member{
		// torrentfs-go's id is the infohash (ADR 0003).
		infoHash:    id,
		torrentFSID: id,
		joinedAt:    s.clock.Now(),
	}
	s.logger.Info("candidate landed into pool",
		"torrent_id", c.TorrentID,
		"torrentfs_id", id,
		"pool_size", len(s.pool),
		"pool_capacity", s.params.PoolSize,
	)
	return nil
}

// rollbackAdd removes a torrent this round's add created but could not
// make room for, so a failed eviction does not leave an uncategorized
// orphan behind in torrentfs-go. Best effort: a failure here is only
// logged, since the caller is already returning the eviction error.
func (s *Scheduler) rollbackAdd(ctx context.Context, c Candidate, id string) {
	delete(s.knownIDs, c.TorrentID)
	op, err := s.torrentFS.DeleteTorrent(ctx, id)
	if err == nil {
		err = s.waitDeleted(ctx, op)
	}
	if err != nil {
		s.logger.Warn("rolling back added torrent failed; uncategorized torrent left in torrentfs-go",
			"torrent_id", c.TorrentID, "torrentfs_id", id, "error", err)
		return
	}
	s.logger.Info("rolled back added torrent after failed eviction",
		"torrent_id", c.TorrentID, "torrentfs_id", id)
}

// evict removes a member from torrentfs-go and drops it (and its sample
// history) from the pool.
func (s *Scheduler) evict(ctx context.Context, m *member) error {
	var aliases []string
	for siteID, id := range s.knownIDs {
		if id == m.torrentFSID {
			aliases = append(aliases, siteID)
		}
	}
	if err := s.retired.RememberPending(m.torrentFSID, aliases); err != nil {
		return fmt.Errorf("persisting permanent retirement before deletion: %w", err)
	}

	// A previous attempt may have completed remotely after a polling error.
	// Reconcile before retrying so a 404 does not strand the member forever.
	views, err := s.torrentFS.ListTorrents(ctx)
	if err != nil {
		return fmt.Errorf("checking retired torrent: %w", err)
	}
	present := false
	for _, v := range views {
		if v.ID == m.torrentFSID {
			if v.Category != "" && v.Category != s.params.Category {
				return fmt.Errorf("refusing to delete retired torrent %s now owned by category %q", v.ID, v.Category)
			}
			present = true
			break
		}
	}
	operationID := ""
	if present {
		operationID, err = s.torrentFS.DeleteTorrent(ctx, m.torrentFSID)
		if err != nil {
			return fmt.Errorf("deleting torrent: %w", err)
		}
		if err := s.waitDeleted(ctx, operationID); err != nil {
			return err
		}
	}
	if err := s.retired.complete(m.torrentFSID); err != nil {
		return err
	}
	delete(s.pool, m.torrentFSID)
	s.logger.Info("cold member evicted from pool",
		"torrentfs_id", m.torrentFSID,
		"last_rate_bytes_per_second", m.lastRateBytes,
		"operation_id", operationID,
		"pool_size", len(s.pool),
	)
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

	s.mu.Lock()
	defer s.mu.Unlock()

	uploaded := make(map[string]int64, len(views))
	for _, v := range views {
		uploaded[v.ID] = v.UploadedBytes
	}

	now := s.clock.Now()
	for id, m := range s.pool {
		bytes, ok := uploaded[id]
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
		wasCold := m.cold
		m.cold = rate < s.params.ColdRateThreshold
		if wasCold != m.cold {
			s.logger.Info("pool member cold verdict changed",
				"torrentfs_id", m.torrentFSID,
				"cold", m.cold,
				"rate_bytes_per_second", rate,
				"cold_rate_threshold", s.params.ColdRateThreshold,
				"window_seconds", span.Seconds(),
				"window_samples", len(m.samples),
			)
		}
	}

	return s.refillCaches(ctx)
}

func pruneBefore(samples []sample, cutoff time.Time) []sample {
	i := 0
	for i < len(samples) && samples[i].at.Before(cutoff) {
		i++
	}
	// Keep one sample before the cutoff (if any) so the window has an
	// "oldest" point at or before cutoff to measure the full span from.
	// Without it, a steadily ticking detector drops each sample the moment
	// it ages past the cutoff and the span never reaches ColdWindow.
	if i > 0 {
		i--
	}
	return samples[i:]
}

// waitDeleted distinguishes acceptance (202) from successful asynchronous
// completion. On failure/timeout the member remains tracked for retry.
func (s *Scheduler) waitDeleted(ctx context.Context, operationID string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		status, err := s.torrentFS.GetOperation(ctx, operationID)
		if err != nil {
			return fmt.Errorf("confirming deletion %s: %w", operationID, err)
		}
		if status.Error != "" {
			return fmt.Errorf("deletion %s failed: %s", operationID, status.Error)
		}
		if status.Done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for deletion %s: %w", operationID, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// cleanupRetired runs independently of search results and pool capacity.
// Failed/unfinished deletes survive process restarts, including retired aliases
// which never acquired a pool category.
func (s *Scheduler) cleanupRetired(ctx context.Context) error {
	for id := range s.retired.pending {
		if err := s.evict(ctx, &member{torrentFSID: id}); err != nil {
			return err
		}
	}
	return nil
}
