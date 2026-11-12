// Package scheduler implements pt-boost's core decision logic: per-site hot
// torrent pool maintenance (scan/filter/fill/replace), cold-torrent
// detection, and the fixed torrent-landing sequence described in
// docs/design-overview.md. The package depends only on the small
// interfaces declared in this file; concrete HTTP implementations live in
// internal/site/* and internal/torrentfs.
package scheduler

import (
	"context"
	"time"
)

// Candidate is one hot-torrent search result from a PT site, already known
// to carry enough identity to dedupe against the current pool (InfoHash)
// and to land later (TorrentID, the site's own identifier used to fetch a
// download token).
type Candidate struct {
	TorrentID string
	InfoHash  string
	Seeders   int
	Leechers  int
}

// Detail is the richer per-torrent information a Site can report. In v1 the
// scheduler only ever needs InfoHash from it (as a fallback identity source
// for adapters whose search results don't carry one directly), but the
// interface leaves room for more without changing its shape.
type Detail struct {
	InfoHash string
}

// SearchParams bounds one PT-site search round. A Site implementation owns
// its own internal pagination and inter-page rate limiting (page size,
// how many pages, delay between them); Scheduler only needs the combined,
// LEECHERS-descending result.
type SearchParams struct {
	MaxPages int
	PageSize int
}

// Site is the generic PT-site adapter interface (see GLOSSARY.md "站点适配器
// (Site Adapter)"). Each PT site provides one implementation; Scheduler's
// logic never changes when a new site is added.
type Site interface {
	// SearchHotTorrents returns candidates sorted by leechers descending,
	// across up to params.MaxPages pages of params.PageSize each.
	SearchHotTorrents(ctx context.Context, params SearchParams) ([]Candidate, error)
	// GetDownloadToken exchanges a site-native torrent id for a download
	// token (for M-Team, a signed URL) that TorrentFileFetcher can turn
	// into the raw .torrent bytes.
	GetDownloadToken(ctx context.Context, torrentID string) (string, error)
	// GetTorrentDetail fetches richer per-torrent information.
	GetTorrentDetail(ctx context.Context, torrentID string) (Detail, error)
}

// TorrentFileFetcher turns a download token (as returned by
// Site.GetDownloadToken) into the raw .torrent file bytes. It is a thin,
// site-agnostic HTTP dependency, kept separate from Site so Scheduler tests
// can fake "the network" without needing a fake HTTP server.
//
// Top-level spec (issue #1, User Story 29) literally asks for Scheduler to
// depend on "两个可替换的外部接口" (two swappable external interfaces):
// a site adapter and a torrentfs-go client. Declaring TorrentFileFetcher
// as its own Go interface, distinct from Site, means Scheduler actually
// depends on three Go interfaces (Site, TorrentFSClient,
// TorrentFileFetcher), not two. This is a deliberate, already-evaluated
// deviation from that literal wording, not an oversight: in production
// there are still only two physical objects (mteam.Client implements both
// Site and TorrentFileFetcher; torrentfs.Client implements
// TorrentFSClient alone), so User Story 29's real goal — Scheduler's
// decision logic being testable end-to-end against pure in-memory fakes,
// with no real network or HTTP server involved — is fully met either way.
// Splitting out TorrentFileFetcher additionally lets "download the raw
// .torrent bytes for a given token" be faked on its own, without having to
// spin up a fake HTTP server just to exercise that one step.
type TorrentFileFetcher interface {
	Fetch(ctx context.Context, downloadToken string) ([]byte, error)
}

// ByteRange is an inclusive-start, exclusive-end byte range within a
// torrent's data, as consumed by TorrentFSClient.TriggerPieceDownload.
type ByteRange struct {
	Offset int64
	Length int64
}

// TorrentView is torrentfs-go's view of one torrent it is managing, as
// reported by ListTorrents. InfoHash is the cross-system identity shared
// with Candidate; Category is the local category tag (see GLOSSARY.md "本地
// 分类标签") pt-boost uses to recognize its own pool members.
type TorrentView struct {
	ID            string
	InfoHash      string
	Category      string
	UploadedBytes int64
}

// OperationStatus reports progress of an asynchronous torrentfs-go
// operation (currently only used for DeleteTorrent's 202 response).
type OperationStatus struct {
	Done  bool
	Error string
}

// TorrentFSClient is pt-boost's only access point to a torrentfs-go
// instance (see ADR 0001: REST-only integration). It wraps the handful of
// /api/v1 endpoints the scheduler actually drives; login/token refresh is
// an implementation detail hidden behind this interface.
type TorrentFSClient interface {
	// AddTorrent uploads raw .torrent bytes and returns torrentfs-go's
	// internal id for it.
	AddTorrent(ctx context.Context, torrentBytes []byte) (id string, err error)
	// SetCategory tags a torrent with pt-boost's local category (see
	// GLOSSARY.md "本地分类标签"): the sole membership marker used to
	// rebuild the pool after a restart.
	SetCategory(ctx context.Context, id, category string) error
	// TriggerPieceDownload issues one PUT .../piece-download call for the
	// given byte range, bypassing any FUSE read (see GLOSSARY.md "主动取
	// 片").
	TriggerPieceDownload(ctx context.Context, id string, byteRange ByteRange) error
	// ListTorrents returns every torrent torrentfs-go currently manages.
	// Scheduler filters the result by category itself.
	ListTorrents(ctx context.Context) ([]TorrentView, error)
	// DeleteTorrent asynchronously removes a torrent and returns the
	// operation id to track completion.
	DeleteTorrent(ctx context.Context, id string) (operationID string, err error)
	// GetOperation reports the status of a previously started async
	// operation (e.g. from DeleteTorrent).
	GetOperation(ctx context.Context, operationID string) (OperationStatus, error)
}

// Clock is the time source Scheduler uses for sampling and window
// calculations, so tests can advance time deterministically instead of
// sleeping. See clock.go for the real (wall-clock) and fake implementations.
type Clock interface {
	Now() time.Time
}
