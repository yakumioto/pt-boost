// Package mteam implements scheduler.Site and scheduler.TorrentFileFetcher
// against M-Team's (mTorrent) HTTP API, per ADR 0002: direct HTTP only, no
// headless-browser relay, even though M-Team's own production domains are
// known to intermittently block some IP ranges (see docs/adr/0002 for the
// rationale pt-boost accepts that tradeoff).
//
// M-Team has no published OpenAPI response schema for third-party
// consumption (the upstream guide this adapter was built from documents
// *request* parameters in detail but not the exact *response* field names);
// this package's envelope/searchResultItem types are
// this adapter's best-effort understanding, cross-checked against public
// M-Team API client implementations. If a real M-Team instance's response
// shape differs, that is an integration risk to re-verify against a live
// account — the same posture already taken for internal/torrentfs.
package mteam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSearchInterval = 2500 * time.Millisecond // ≥2.5s between search pages (guide: frontend uses util.sleep(2500))
	defaultDetailInterval = 1 * time.Second         // ≥1s between other calls
	maxRetries            = 3
)

// Config holds the fixed per-site settings for an M-Team Client. Pool-
// membership thresholds (seeders/leechers minimums) are deliberately not
// part of this Config: per issue #5, this adapter returns raw candidates
// only, and scheduler.Scheduler.Scan is the sole authority on threshold
// filtering.
type Config struct {
	BaseURL string
	APIKey  string
}

// Client implements scheduler.Site and scheduler.TorrentFileFetcher.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
	sleep      func(time.Duration)

	searchInterval time.Duration
	detailInterval time.Duration

	mu           sync.Mutex
	lastSearchAt time.Time
	lastDetailAt time.Time
	haveSearched bool
	haveDetailed bool
}

// Option configures optional Client behaviour.
type Option func(*Client)

func WithHTTPClient(c *http.Client) Option {
	return func(client *Client) { client.httpClient = c }
}

func WithSearchInterval(d time.Duration) Option {
	return func(client *Client) { client.searchInterval = d }
}

func WithDetailInterval(d time.Duration) Option {
	return func(client *Client) { client.detailInterval = d }
}

// withSleep overrides the pacing/backoff sleep function; used by tests to
// avoid real wall-clock delays while still exercising the pacing logic.
func withSleep(fn func(time.Duration)) Option {
	return func(client *Client) { client.sleep = fn }
}

func New(cfg Config, opts ...Option) *Client {
	c := &Client{
		baseURL:        strings.TrimRight(cfg.BaseURL, "/"),
		apiKey:         cfg.APIKey,
		httpClient:     http.DefaultClient,
		sleep:          time.Sleep,
		searchInterval: defaultSearchInterval,
		detailInterval: defaultDetailInterval,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// paceSearch blocks (via c.sleep) just long enough that consecutive search
// calls are at least searchInterval apart. The very first call never waits.
func (c *Client) paceSearch() {
	c.pace(&c.lastSearchAt, &c.haveSearched, c.searchInterval)
}

// paceOther is the same pacing for every non-search endpoint (genDlToken,
// detail): the guide requires ≥1s between "other" calls.
func (c *Client) paceOther() {
	c.pace(&c.lastDetailAt, &c.haveDetailed, c.detailInterval)
}

func (c *Client) pace(last *time.Time, have *bool, interval time.Duration) {
	c.mu.Lock()
	var wait time.Duration
	if *have {
		elapsed := time.Since(*last)
		if elapsed < interval {
			wait = interval - elapsed
		}
	}
	*have = true
	*last = time.Now().Add(wait)
	c.mu.Unlock()

	if wait > 0 {
		c.sleep(wait)
	}
}

// envelope is M-Team's documented uniform response wrapper: {code, message,
// data}. code is known to vary between a bare number, a numeric string, and
// the literal string "SUCCESS" (the guide explicitly warns about this), so
// it is decoded through flexCode rather than a fixed type.
type envelope struct {
	Code    flexCode        `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// unmarshalFlexible is the shared decoding strategy behind flexCode,
// flexInt, and flexString: M-Team encodes several fields inconsistently as
// either a JSON string or a JSON number, so each of these types tries one
// JSON kind first (via primary) and falls back to the other (via
// secondary) before giving up. The two callbacks encode which kind each
// type prefers and in which order, so this refactor changes none of their
// individual fallback directions.
func unmarshalFlexible[T any](data []byte, primary, secondary func([]byte) (T, bool), typeName string) (T, error) {
	if v, ok := primary(data); ok {
		return v, nil
	}
	if v, ok := secondary(data); ok {
		return v, nil
	}
	var zero T
	return zero, fmt.Errorf("%s: cannot decode %s", typeName, data)
}

func tryJSONString(data []byte) (string, bool) {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return "", false
	}
	return s, true
}

func tryJSONNumberAsString(data []byte) (string, bool) {
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return "", false
	}
	return n.String(), true
}

func tryJSONNumberAsInt64(data []byte) (int64, bool) {
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return 0, false
	}
	i, err := n.Int64()
	if err != nil {
		return 0, false
	}
	return i, true
}

func tryJSONStringAsInt64(data []byte) (int64, bool) {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, false
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return i, true
}

// flexCode decodes M-Team's inconsistently-typed "code" field (bare number,
// numeric string, or "SUCCESS") into a comparable string form. It tries a
// JSON string first, falling back to a JSON number converted to string.
type flexCode string

func (f *flexCode) UnmarshalJSON(data []byte) error {
	s, err := unmarshalFlexible(data, tryJSONString, tryJSONNumberAsString, "flexCode")
	if err != nil {
		return err
	}
	*f = flexCode(s)
	return nil
}

func (f flexCode) isSuccess() bool {
	return f == "0" || strings.EqualFold(string(f), "SUCCESS")
}

// flexInt decodes a field M-Team may encode as either a JSON number or a
// numeric string (e.g. seeders/leechers), matching the FlexInt-style
// workaround other public M-Team client implementations also needed. It
// tries a JSON number first, falling back to a numeric JSON string.
type flexInt int64

func (f *flexInt) UnmarshalJSON(data []byte) error {
	i, err := unmarshalFlexible(data, tryJSONNumberAsInt64, tryJSONStringAsInt64, "flexInt")
	if err != nil {
		return err
	}
	*f = flexInt(i)
	return nil
}

// flexString is the same string/number flexibility for id-like fields. It
// tries a JSON string first, falling back to a JSON number converted to
// string (the same order as flexCode).
type flexString string

func (f *flexString) UnmarshalJSON(data []byte) error {
	s, err := unmarshalFlexible(data, tryJSONString, tryJSONNumberAsString, "flexString")
	if err != nil {
		return err
	}
	*f = flexString(s)
	return nil
}

// doJSON POSTs a JSON body to path and returns the decoded envelope,
// retrying up to maxRetries times with 1s/2s/4s backoff on HTTP 429 (see
// the guide's rate-limit best practices). Pacing (paceSearch/paceOther)
// must be done by the caller before calling this.
func (c *Client) doJSON(ctx context.Context, path string, body any) (envelope, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return envelope{}, fmt.Errorf("encoding request: %w", err)
	}
	return c.doWithRetry(ctx, path, "application/json", encoded)
}

// doForm POSTs an application/x-www-form-urlencoded body — required by
// /api/torrent/genDlToken (JSON is a documented bug there).
func (c *Client) doForm(ctx context.Context, path string, form url.Values) (envelope, error) {
	return c.doWithRetry(ctx, path, "application/x-www-form-urlencoded", []byte(form.Encode()))
}

func (c *Client) doWithRetry(ctx context.Context, path, contentType string, body []byte) (envelope, error) {
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		env, status, err := c.doOnce(ctx, path, contentType, body)
		if err != nil {
			return envelope{}, err
		}
		if status == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("rate limited (429) by %s", path)
			if attempt < len(backoffs) {
				c.sleep(backoffs[attempt])
				continue
			}
			return envelope{}, fmt.Errorf("%w: exhausted %d retries", lastErr, maxRetries)
		}
		if status != http.StatusOK {
			return envelope{}, fmt.Errorf("%s returned HTTP %d", path, status)
		}
		if !env.Code.isSuccess() {
			return envelope{}, fmt.Errorf("%s API error: code=%s message=%s", path, env.Code, env.Message)
		}
		return env, nil
	}
	return envelope{}, lastErr
}

func (c *Client) doOnce(ctx context.Context, path, contentType string, body []byte) (envelope, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, strings.NewReader(string(body)))
	if err != nil {
		return envelope{}, 0, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return envelope{}, 0, fmt.Errorf("calling %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return envelope{}, resp.StatusCode, nil
	}
	if resp.StatusCode != http.StatusOK {
		return envelope{}, resp.StatusCode, nil
	}

	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return envelope{}, resp.StatusCode, fmt.Errorf("decoding response from %s: %w", path, err)
	}
	return env, resp.StatusCode, nil
}
