package mteam

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

var (
	_ scheduler.Site               = (*Client)(nil)
	_ scheduler.TorrentFileFetcher = (*Client)(nil)
)

// searchRequest mirrors the TorrentSearch request schema documented in the
// M-Team API guide. Mode is set to "all" (one of the guide's documented
// enum values) so pt-boost's "scan every mode" default (see GLOSSARY.md /
// design-overview.md Q5) does not rely on an undocumented server default.
type searchRequest struct {
	PageNumber    int    `json:"pageNumber"`
	PageSize      int    `json:"pageSize"`
	Mode          string `json:"mode"`
	SortField     string `json:"sortField"`
	SortDirection string `json:"sortDirection"`
}

// searchResponseData is this adapter's best-effort understanding of
// /api/torrent/search's "data" payload: a page of results plus paging
// metadata. Unverified against an official schema — see package doc.
type searchResponseData struct {
	Data []searchResultItem `json:"data"`
}

type searchResultItem struct {
	ID     flexString    `json:"id"`
	Status torrentStatus `json:"status"`
}

type torrentStatus struct {
	Seeders  flexInt `json:"seeders"`
	Leechers flexInt `json:"leechers"`
}

// SearchHotTorrents implements scheduler.Site. It pages through
// /api/torrent/search sorted by LEECHERS descending (respecting the
// guide's ≥2.5s inter-search pacing) and returns one candidate per result,
// identified by M-Team's own torrent id only. It does not resolve an
// infohash: M-Team's API does not expose one (/api/torrent/detail returns
// infoHash: null in production), and Scheduler does not need it — see
// ADR 0003. Per issue #5, this method returns the raw candidate list
// unfiltered: it does not apply any seeders/leechers threshold —
// scheduler.Scheduler.Scan is the sole authority on pool-membership
// decisions.
func (c *Client) SearchHotTorrents(ctx context.Context, params scheduler.SearchParams) ([]scheduler.Candidate, error) {
	var candidates []scheduler.Candidate

	for page := 1; page <= params.MaxPages; page++ {
		c.paceSearch()

		req := searchRequest{
			PageNumber:    page,
			PageSize:      params.PageSize,
			Mode:          "all",
			SortField:     "LEECHERS",
			SortDirection: "DESC",
		}
		env, err := c.doJSON(ctx, "/api/torrent/search", req)
		if err != nil {
			return nil, fmt.Errorf("searching page %d: %w", page, err)
		}

		var data searchResponseData
		if err := json.Unmarshal(env.Data, &data); err != nil {
			return nil, fmt.Errorf("decoding search page %d: %w", page, err)
		}

		for _, item := range data.Data {
			candidates = append(candidates, scheduler.Candidate{
				TorrentID: string(item.ID),
				Seeders:   int(item.Status.Seeders),
				Leechers:  int(item.Status.Leechers),
			})
		}
		if len(data.Data) < params.PageSize {
			break // short page: no more results to page through
		}
	}

	return candidates, nil
}

// GetDownloadToken implements scheduler.Site. genDlToken must be sent as
// application/x-www-form-urlencoded — the guide documents JSON as a known
// server-side bug for this endpoint specifically.
func (c *Client) GetDownloadToken(ctx context.Context, torrentID string) (string, error) {
	c.paceOther()

	form := url.Values{"id": {torrentID}}
	env, err := c.doForm(ctx, "/api/torrent/genDlToken", form)
	if err != nil {
		return "", fmt.Errorf("getting download token for torrent %s: %w", torrentID, err)
	}

	var token string
	if err := json.Unmarshal(env.Data, &token); err != nil {
		return "", fmt.Errorf("decoding download token for torrent %s: %w", torrentID, err)
	}
	return token, nil
}

// Fetch implements scheduler.TorrentFileFetcher. M-Team's genDlToken is
// documented (and corroborated by public client implementations) to
// return a ready-to-use (pre-signed) absolute download URL, so fetching
// the raw .torrent bytes is normally a plain unauthenticated GET — no
// x-api-key needed. Given the same API's well-documented inconsistencies
// elsewhere (code/id/seeders/leechers field typing), this also tolerates a
// server that returns a bare path instead, resolving it against baseURL.
func (c *Client) Fetch(ctx context.Context, downloadToken string) ([]byte, error) {
	target := downloadToken
	if parsed, err := url.Parse(downloadToken); err == nil && parsed.Scheme == "" {
		target = c.baseURL + downloadToken
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("building download request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("downloading torrent: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloading torrent: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading torrent body: %w", err)
	}
	return body, nil
}
