package torrentfs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// Cache snapshots and task states follow torrentfs-go main @59364390.
// cached_bytes is resident in-memory cache, NOT cumulative network download.
// The server takes one cache snapshot for aggregate and per-piece counts.
type cacheSnapshot struct {
	Torrent struct {
		CachedBytes int64  `json:"cached_bytes"`
		TotalBytes  int64  `json:"total_bytes"`
		Category    string `json:"category"`
	} `json:"torrent"`
	MetainfoReady bool         `json:"metainfo_ready"`
	PieceLength   int64        `json:"piece_length"`
	Pieces        []cachePiece `json:"pieces"`
}

type cachePiece struct {
	Index       int64 `json:"index"`
	Cached      bool  `json:"cached"`
	CachedBytes int64 `json:"cached_bytes"`
}

type pieceTask struct {
	State string `json:"state"`
}

var _ scheduler.CacheMaintainer = (*Client)(nil)

func (c *Client) cacheSnapshot(ctx context.Context, id string) (cacheSnapshot, error) {
	var snapshot cacheSnapshot
	resp, err := c.authedRequest(ctx, http.MethodGet, "/api/v1/torrents/"+id+"/status", nil, "")
	if err != nil {
		return snapshot, fmt.Errorf("inspecting cache: %w", err)
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, "inspecting cache"); err != nil {
		return snapshot, err
	}
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return snapshot, fmt.Errorf("decoding cache status: %w", err)
	}
	return snapshot, nil
}

func (c *Client) pieceTaskState(ctx context.Context, id string) (string, error) {
	resp, err := c.authedRequest(ctx, http.MethodGet, "/api/v1/torrents/"+id+"/piece-download", nil, "")
	if err != nil {
		return "", fmt.Errorf("inspecting piece download: %w", err)
	}
	defer resp.Body.Close()
	// Only this documented 404 means no task. An unknown torrent or route is
	// not a task-free torrent and must not silently permit a replacing PUT.
	if resp.StatusCode == http.StatusNotFound {
		var body struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return "", fmt.Errorf("decoding missing piece download: %w", err)
		}
		if body.Error == "no piece download for torrent" {
			return "", nil
		}
		return "", fmt.Errorf("inspecting piece download failed with status 404: %s", body.Error)
	}
	if err := checkStatus(resp, "inspecting piece download"); err != nil {
		return "", err
	}
	var task pieceTask
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return "", fmt.Errorf("decoding piece download: %w", err)
	}
	switch task.State {
	case "running", "completed", "cancelled", "expired", "failed":
		return task.State, nil
	default:
		return "", fmt.Errorf("unknown piece download state %q", task.State)
	}
}

// InspectCache returns current resident bytes and whether the torrent's one
// piece-download task is running. Completed tasks do not prove cache residency:
// cache entries may have been evicted since completion. No writes are issued.
func (c *Client) InspectCache(ctx context.Context, id string) (scheduler.CacheStatus, error) {
	snapshot, err := c.cacheSnapshot(ctx, id)
	if err != nil {
		return scheduler.CacheStatus{}, err
	}
	state, err := c.pieceTaskState(ctx, id)
	if err != nil {
		return scheduler.CacheStatus{}, err
	}
	return scheduler.CacheStatus{CachedBytes: snapshot.Torrent.CachedBytes, TotalBytes: snapshot.Torrent.TotalBytes, Category: snapshot.Torrent.Category, Active: state == "running", Failed: state == "failed" || state == "expired"}, nil
}

// RefillCache starts one task containing only missing pieces, sufficient to
// reach targetBytes (clamped to torrent size, rounded up by at most one piece).
// It rechecks residency and task state rather than trusting an earlier Inspect.
// PUT replaces the previous task, so running tasks MUST be left untouched.
// Scheduler, not this HTTP adapter, owns the global concurrent-task budget.
func (c *Client) RefillCache(ctx context.Context, id, category string, targetBytes int64) error {
	if targetBytes <= 0 {
		return fmt.Errorf("invalid cache target %d", targetBytes)
	}
	c.refillMu.Lock()
	defer c.refillMu.Unlock()
	snapshot, err := c.cacheSnapshot(ctx, id)
	if err != nil {
		return err
	}
	if category == "" || snapshot.Torrent.Category != category {
		return fmt.Errorf("cache refill category changed: expected %q, got %q", category, snapshot.Torrent.Category)
	}
	state, err := c.pieceTaskState(ctx, id)
	if err != nil {
		return err
	}
	if state == "running" {
		return nil
	}
	ranges, err := missingPieceRanges(snapshot, targetBytes)
	if err != nil {
		return err
	}
	if len(ranges) == 0 {
		return nil
	}
	body, err := json.Marshal(pieceDownloadRequest{Ranges: ranges})
	if err != nil {
		return fmt.Errorf("encoding cache refill: %w", err)
	}
	resp, err := c.authedRequest(ctx, http.MethodPut, "/api/v1/torrents/"+id+"/piece-download", body, "application/json")
	if err != nil {
		return fmt.Errorf("refilling cache: %w", err)
	}
	defer resp.Body.Close()
	return checkStatus(resp, "refilling cache")
}

func missingPieceRanges(snapshot cacheSnapshot, targetBytes int64) ([]pieceRange, error) {
	total, step := snapshot.Torrent.TotalBytes, snapshot.PieceLength
	if !snapshot.MetainfoReady || total <= 0 || step <= 0 {
		return nil, fmt.Errorf("torrent metadata not ready for cache refill")
	}
	if snapshot.Torrent.CachedBytes < 0 || snapshot.Torrent.CachedBytes > total {
		return nil, fmt.Errorf("invalid resident cache bytes %d", snapshot.Torrent.CachedBytes)
	}
	if targetBytes > total {
		targetBytes = total
	}
	needed := targetBytes - snapshot.Torrent.CachedBytes
	if needed <= 0 {
		return nil, nil
	}
	count := (total-1)/step + 1
	if int64(len(snapshot.Pieces)) != count {
		return nil, fmt.Errorf("incomplete piece cache bitmap: got %d, want %d", len(snapshot.Pieces), count)
	}
	// Validate the complete bitmap before submitting any work. Indexes are
	// ordered and absolute in the upstream snapshot; never guess absent entries.
	var resident int64
	for i, p := range snapshot.Pieces {
		size := step
		if int64(i) == count-1 {
			size = total - int64(i)*step
		}
		if p.Index != int64(i) || p.CachedBytes < 0 || p.CachedBytes > size || (!p.Cached && p.CachedBytes != 0) {
			return nil, fmt.Errorf("invalid cache state for piece %d", i)
		}
		resident += p.CachedBytes
	}
	if resident != snapshot.Torrent.CachedBytes {
		return nil, fmt.Errorf("inconsistent resident cache bytes: pieces=%d aggregate=%d", resident, snapshot.Torrent.CachedBytes)
	}
	var ranges []pieceRange
	for _, p := range snapshot.Pieces {
		if p.Cached {
			continue
		}
		size := step
		if p.Index == count-1 {
			size = total - p.Index*step
		}
		if len(ranges) > 0 && ranges[len(ranges)-1].End == p.Index {
			ranges[len(ranges)-1].End++
		} else {
			ranges = append(ranges, pieceRange{Start: p.Index, End: p.Index + 1})
		}
		needed -= size
		if needed <= 0 {
			return ranges, nil
		}
	}
	return nil, fmt.Errorf("cache bitmap cannot satisfy target: %d bytes missing", needed)
}
