package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	CacheLowWaterBytes int64 = 64 * 1024 * 1024
	CacheTargetBytes   int64 = 128 * 1024 * 1024
	maxCacheDownloads        = 2
)

// CacheStatus describes resident (not cumulative downloaded) data and the
// torrentfs-go task currently responsible for fetching missing pieces.
type CacheStatus struct {
	CachedBytes int64
	TotalBytes  int64
	Active      bool
	Failed      bool // latest task failed or expired
	Category    string
}

// CacheMaintainer hides piece layout and task protocol details from policy.
// RefillCache must recheck the active task before submitting missing pieces.
type CacheMaintainer interface {
	InspectCache(ctx context.Context, id string) (CacheStatus, error)
	RefillCache(ctx context.Context, id, category string, targetBytes int64) error
}

// WithCacheMaintenance enables low/high water maintenance on each sampling tick.
func WithCacheMaintenance(client CacheMaintainer) Option {
	return func(s *Scheduler) { s.cache = client }
}

// refillCaches runs with s.mu held, so no Scan can retire a member between the
// ownership/retirement checks and task submission. Failures do not undo samples.
func (s *Scheduler) refillCaches(ctx context.Context) error {
	if s.cache == nil {
		return nil
	}
	now := s.clock.Now()
	members := make([]*member, 0, len(s.pool))
	statuses := make(map[string]CacheStatus, len(s.pool))
	active := 0
	var errs []error
	for id, m := range s.pool {
		state, err := s.cache.InspectCache(ctx, id)
		if err != nil {
			errs = append(errs, fmt.Errorf("inspecting cache %s: %w", id, err))
			continue
		}
		if state.Active {
			active++
		}
		if m.awaitingRefill && !state.Active {
			m.awaitingRefill = false
			if state.Failed {
				s.refillFailed(m, now, fmt.Errorf("asynchronous piece download failed or expired"))
			} else {
				m.refillFailures = 0
			}
		}
		statuses[id] = state
		members = append(members, m)
	}
	// An unknown task state could already consume a slot. Do not overcommit.
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].lastRefillAt.Equal(members[j].lastRefillAt) {
			return members[i].torrentFSID < members[j].torrentFSID
		}
		return members[i].lastRefillAt.Before(members[j].lastRefillAt)
	})
	started := 0
	for _, m := range members {
		id := m.torrentFSID
		state := statuses[id]
		if active >= maxCacheDownloads {
			break
		}
		if s.retired.hasID(id) || state.Category != s.params.Category || state.Active || state.TotalBytes <= 0 || state.CachedBytes < 0 {
			continue
		}
		target := min(CacheTargetBytes, state.TotalBytes)
		if state.CachedBytes >= CacheLowWaterBytes || state.CachedBytes >= target || now.Before(m.nextRefillAt) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		m.lastRefillAt = now
		if err := s.cache.RefillCache(ctx, id, s.params.Category, target); err != nil {
			s.refillFailed(m, now, err)
			errs = append(errs, fmt.Errorf("refilling cache %s: %w", id, err))
			// A timed-out PUT may have reached the server. Reserve a slot anyway.
			active++
			continue
		}
		m.awaitingRefill = true
		m.nextRefillAt = now.Add(2 * time.Minute)
		active++
		started++
		s.logger.Info("cache refill requested", "torrentfs_id", id, "cached_bytes", state.CachedBytes, "target_bytes", target)
	}
	s.logger.Info("cache maintenance completed", "members", len(members), "active_or_reserved", active, "started", started)
	return errors.Join(errs...)
}

func (s *Scheduler) refillFailed(m *member, now time.Time, err error) {
	m.refillFailures++
	delay := 2 * time.Minute
	for i := 1; i < m.refillFailures && delay < 30*time.Minute; i++ {
		delay *= 2
	}
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	m.nextRefillAt = now.Add(delay)
	s.logger.Warn("cache refill failed", "torrentfs_id", m.torrentFSID, "error", err, "retry_after", delay)
}
