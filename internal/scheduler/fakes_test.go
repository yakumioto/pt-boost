package scheduler

import (
	"context"
	"fmt"
	"time"
)

// fakeSite is an in-memory Site double. Candidates is consumed verbatim by
// SearchHotTorrents (tests set it up per scenario); downloadTokens maps
// TorrentID -> token returned by GetDownloadToken.
type fakeSite struct {
	candidates     []Candidate
	downloadTokens map[string]string
	details        map[string]Detail
	calls          *[]string // optional shared call-order log, see newCallLog

	searchCalls int
	searchParam SearchParams
}

func (f *fakeSite) log(event string) {
	if f.calls != nil {
		*f.calls = append(*f.calls, event)
	}
}

func (f *fakeSite) SearchHotTorrents(ctx context.Context, params SearchParams) ([]Candidate, error) {
	f.log("Site.SearchHotTorrents")
	f.searchCalls++
	f.searchParam = params
	return f.candidates, nil
}

func (f *fakeSite) GetDownloadToken(ctx context.Context, torrentID string) (string, error) {
	f.log("Site.GetDownloadToken")
	token, ok := f.downloadTokens[torrentID]
	if !ok {
		return "", fmt.Errorf("fakeSite: no download token stubbed for torrent %q", torrentID)
	}
	return token, nil
}

func (f *fakeSite) GetTorrentDetail(ctx context.Context, torrentID string) (Detail, error) {
	d, ok := f.details[torrentID]
	if !ok {
		return Detail{}, fmt.Errorf("fakeSite: no detail stubbed for torrent %q", torrentID)
	}
	return d, nil
}

// fakeFetcher maps a download token to canned .torrent bytes.
type fakeFetcher struct {
	files map[string][]byte
	calls *[]string
}

func (f *fakeFetcher) Fetch(ctx context.Context, downloadToken string) ([]byte, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, "Fetcher.Fetch")
	}
	bytes, ok := f.files[downloadToken]
	if !ok {
		return nil, fmt.Errorf("fakeFetcher: no file stubbed for token %q", downloadToken)
	}
	return bytes, nil
}

// fakeTorrentFSEntry is one torrent as tracked by fakeTorrentFS, keyed by
// the id AddTorrent assigns it.
type fakeTorrentFSEntry struct {
	infoHash      string
	category      string
	uploadedBytes int64
	pieceRanges   []ByteRange
	deleted       bool
}

// fakeTorrentFS is an in-memory TorrentFSClient double. infoHashByBytes
// lets tests control which InfoHash a given set of .torrent bytes maps to,
// so AddTorrent can report a deterministic identity without a real
// bencode/SHA1 implementation.
type fakeTorrentFS struct {
	entries         map[string]*fakeTorrentFSEntry // by torrentfs id
	infoHashByBytes map[string]string              // string(bytes) -> infohash
	nextID          int
	calls           *[]string

	addTorrentCalls         int
	setCategoryCalls        []setCategoryCall
	triggerPieceDownloadLog []triggerPieceDownloadCall
	deleteTorrentCalls      []string
}

type setCategoryCall struct {
	id       string
	category string
}

type triggerPieceDownloadCall struct {
	id    string
	Range ByteRange
}

func newFakeTorrentFS() *fakeTorrentFS {
	return &fakeTorrentFS{
		entries:         make(map[string]*fakeTorrentFSEntry),
		infoHashByBytes: make(map[string]string),
	}
}

func (f *fakeTorrentFS) AddTorrent(ctx context.Context, torrentBytes []byte) (string, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, "TorrentFS.AddTorrent")
	}
	f.addTorrentCalls++
	infoHash, ok := f.infoHashByBytes[string(torrentBytes)]
	if !ok {
		return "", fmt.Errorf("fakeTorrentFS: no infohash stubbed for these bytes")
	}
	f.nextID++
	id := fmt.Sprintf("tfs-%d", f.nextID)
	f.entries[id] = &fakeTorrentFSEntry{infoHash: infoHash}
	return id, nil
}

func (f *fakeTorrentFS) SetCategory(ctx context.Context, id, category string) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, "TorrentFS.SetCategory")
	}
	f.setCategoryCalls = append(f.setCategoryCalls, setCategoryCall{id: id, category: category})
	e, ok := f.entries[id]
	if !ok {
		return fmt.Errorf("fakeTorrentFS: unknown id %q", id)
	}
	e.category = category
	return nil
}

func (f *fakeTorrentFS) TriggerPieceDownload(ctx context.Context, id string, byteRange ByteRange) error {
	if f.calls != nil {
		*f.calls = append(*f.calls, "TorrentFS.TriggerPieceDownload")
	}
	f.triggerPieceDownloadLog = append(f.triggerPieceDownloadLog, triggerPieceDownloadCall{id: id, Range: byteRange})
	e, ok := f.entries[id]
	if !ok {
		return fmt.Errorf("fakeTorrentFS: unknown id %q", id)
	}
	e.pieceRanges = append(e.pieceRanges, byteRange)
	return nil
}

func (f *fakeTorrentFS) ListTorrents(ctx context.Context) ([]TorrentView, error) {
	var out []TorrentView
	for id, e := range f.entries {
		if e.deleted {
			continue
		}
		out = append(out, TorrentView{
			ID:            id,
			InfoHash:      e.infoHash,
			Category:      e.category,
			UploadedBytes: e.uploadedBytes,
		})
	}
	return out, nil
}

func (f *fakeTorrentFS) DeleteTorrent(ctx context.Context, id string) (string, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, "TorrentFS.DeleteTorrent")
	}
	f.deleteTorrentCalls = append(f.deleteTorrentCalls, id)
	e, ok := f.entries[id]
	if !ok {
		return "", fmt.Errorf("fakeTorrentFS: unknown id %q", id)
	}
	e.deleted = true
	return "op-" + id, nil
}

func (f *fakeTorrentFS) GetOperation(ctx context.Context, operationID string) (OperationStatus, error) {
	return OperationStatus{Done: true}, nil
}

// setUploaded lets a test directly set a (possibly not-yet-added) torrent's
// cumulative uploaded bytes, simulating real BT upload progress between
// DetectColdTorrents ticks.
func (f *fakeTorrentFS) setUploaded(id string, bytes int64) {
	if e, ok := f.entries[id]; ok {
		e.uploadedBytes = bytes
	}
}

// fakeClock is a manually-advanced Clock for deterministic tests.
type fakeClock struct {
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
