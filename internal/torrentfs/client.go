// Package torrentfs implements scheduler.TorrentFSClient against a real
// torrentfs-go instance's HTTP RESTful API (see ADR 0001: pt-boost
// integrates with torrentfs-go only over HTTP, never as an embedded Go
// library, since torrentfs-go has no exported package API).
//
// pt-boost treats torrentfs-go as an opaque, already-configured external
// service (see docs/design-overview.md "范围边界"): this client only calls
// the handful of /api/v1 endpoints the scheduler actually needs and never
// reads or depends on torrentfs-go's own deployment configuration.
package torrentfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// ErrInvalidTorrent is returned by AddTorrent when torrentfs-go rejects the
// supplied bytes as not a valid .torrent file (HTTP 400). Callers can
// distinguish this ("the candidate itself is bad, give up on it") from a
// transient/network error ("retry might help") with errors.Is.
var ErrInvalidTorrent = errors.New("torrentfs: torrent rejected as invalid")

// defaultTokenTTL is how long the client reuses a login token before
// proactively logging in again, ahead of torrentfs-go's own token_ttl
// (documented default 30m) expiring it server-side.
const defaultTokenTTL = 25 * time.Minute

// Client implements scheduler.TorrentFSClient over torrentfs-go's HTTP API.
type Client struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
	tokenTTL   time.Duration
	now        func() time.Time

	mu              sync.Mutex
	token           string
	tokenObtainedAt time.Time
}

// Option configures optional Client behaviour.
type Option func(*Client)

// WithHTTPClient overrides the default http.Client (e.g. for custom
// timeouts or transports).
func WithHTTPClient(c *http.Client) Option {
	return func(client *Client) { client.httpClient = c }
}

// WithTokenTTL overrides how long a token is reused before the client
// proactively re-authenticates.
func WithTokenTTL(ttl time.Duration) Option {
	return func(client *Client) { client.tokenTTL = ttl }
}

// withNow overrides the time source; used by tests to control token
// refresh deterministically without sleeping.
func withNow(now func() time.Time) Option {
	return func(client *Client) { client.now = now }
}

// New constructs a Client for the torrentfs-go instance at baseURL.
func New(baseURL, username, password string, opts ...Option) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		username:   username,
		password:   password,
		httpClient: http.DefaultClient,
		tokenTTL:   defaultTokenTTL,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

var _ scheduler.TorrentFSClient = (*Client)(nil)

// ensureToken logs in if there is no token yet or the cached one is older
// than tokenTTL. It is safe for concurrent use.
func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != "" && c.now().Sub(c.tokenObtainedAt) < c.tokenTTL {
		return c.token, nil
	}

	token, err := c.login(ctx)
	if err != nil {
		return "", fmt.Errorf("torrentfs: login: %w", err)
	}
	c.token = token
	c.tokenObtainedAt = c.now()
	return c.token, nil
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

func (c *Client) login(ctx context.Context) (string, error) {
	body, err := json.Marshal(loginRequest{Username: c.username, Password: c.password})
	if err != nil {
		return "", fmt.Errorf("encoding login request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/auth/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sending login request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, respBody)
	}

	var parsed loginResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("decoding login response: %w", err)
	}
	if parsed.Token == "" {
		return "", fmt.Errorf("login response did not contain a token")
	}
	return parsed.Token, nil
}

// authedRequest builds an HTTP request against baseURL+path with a fresh
// Authorization header, retrying the token lookup once if the first
// request comes back 401 (the server may have invalidated a token this
// client still thinks is fresh, e.g. after a restart).
func (c *Client) authedRequest(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	doOnce := func() (*http.Response, error) {
		token, err := c.ensureToken(ctx)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return c.httpClient.Do(req)
	}

	resp, err := doOnce()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized && body == nil {
		// Only safe to retry when the body isn't a one-shot reader that's
		// already been consumed.
		resp.Body.Close()
		c.mu.Lock()
		c.token = ""
		c.mu.Unlock()
		return doOnce()
	}
	return resp, nil
}

type addTorrentResponse struct {
	ID string `json:"id"`
}

// AddTorrent uploads raw .torrent bytes via multipart/form-data, field
// "file", and returns torrentfs-go's internal id for it.
func (c *Client) AddTorrent(ctx context.Context, torrentBytes []byte) (string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", "upload.torrent")
	if err != nil {
		return "", fmt.Errorf("building multipart body: %w", err)
	}
	if _, err := part.Write(torrentBytes); err != nil {
		return "", fmt.Errorf("writing torrent bytes: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("closing multipart body: %w", err)
	}

	token, err := c.ensureToken(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/torrents", &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("adding torrent: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusBadRequest {
		return "", fmt.Errorf("%w: %s", ErrInvalidTorrent, respBody)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("adding torrent failed with status %d: %s", resp.StatusCode, respBody)
	}

	var parsed addTorrentResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("decoding add-torrent response: %w", err)
	}
	if parsed.ID == "" {
		return "", fmt.Errorf("add-torrent response did not contain an id")
	}
	return parsed.ID, nil
}

type setCategoryRequest struct {
	Category string `json:"category"`
}

// SetCategory tags a torrent with pt-boost's local category (see
// GLOSSARY.md "本地分类标签").
func (c *Client) SetCategory(ctx context.Context, id, category string) error {
	body, err := json.Marshal(setCategoryRequest{Category: category})
	if err != nil {
		return fmt.Errorf("encoding set-category request: %w", err)
	}
	resp, err := c.authedRequest(ctx, http.MethodPut, "/api/v1/torrents/"+id+"/category", bytes.NewReader(body), "application/json")
	if err != nil {
		return fmt.Errorf("setting category: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("setting category failed with status %d: %s", resp.StatusCode, respBody)
	}
	return nil
}

type pieceDownloadRequest struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

// TriggerPieceDownload issues PUT .../piece-download for the given byte
// range (see GLOSSARY.md "主动取片").
func (c *Client) TriggerPieceDownload(ctx context.Context, id string, byteRange scheduler.ByteRange) error {
	body, err := json.Marshal(pieceDownloadRequest{Offset: byteRange.Offset, Length: byteRange.Length})
	if err != nil {
		return fmt.Errorf("encoding piece-download request: %w", err)
	}
	resp, err := c.authedRequest(ctx, http.MethodPut, "/api/v1/torrents/"+id+"/piece-download", bytes.NewReader(body), "application/json")
	if err != nil {
		return fmt.Errorf("triggering piece download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("triggering piece download failed with status %d: %s", resp.StatusCode, respBody)
	}
	return nil
}

type torrentListEntry struct {
	ID            string `json:"id"`
	InfoHash      string `json:"info_hash"`
	Category      string `json:"category"`
	UploadedBytes int64  `json:"uploaded_bytes"`
}

// ListTorrents returns every torrent torrentfs-go currently manages.
func (c *Client) ListTorrents(ctx context.Context) ([]scheduler.TorrentView, error) {
	resp, err := c.authedRequest(ctx, http.MethodGet, "/api/v1/torrents", nil, "")
	if err != nil {
		return nil, fmt.Errorf("listing torrents: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing torrents failed with status %d: %s", resp.StatusCode, respBody)
	}

	var entries []torrentListEntry
	if err := json.Unmarshal(respBody, &entries); err != nil {
		return nil, fmt.Errorf("decoding torrent list: %w", err)
	}

	out := make([]scheduler.TorrentView, len(entries))
	for i, e := range entries {
		out[i] = scheduler.TorrentView{
			ID:            e.ID,
			InfoHash:      e.InfoHash,
			Category:      e.Category,
			UploadedBytes: e.UploadedBytes,
		}
	}
	return out, nil
}

type deleteTorrentResponse struct {
	OperationID string `json:"operation_id"`
}

// DeleteTorrent asynchronously removes a torrent and returns the operation
// id to track completion.
func (c *Client) DeleteTorrent(ctx context.Context, id string) (string, error) {
	resp, err := c.authedRequest(ctx, http.MethodDelete, "/api/v1/torrents/"+id, nil, "")
	if err != nil {
		return "", fmt.Errorf("deleting torrent: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("deleting torrent failed with status %d: %s", resp.StatusCode, respBody)
	}

	var parsed deleteTorrentResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("decoding delete-torrent response: %w", err)
	}
	return parsed.OperationID, nil
}

type operationResponse struct {
	Done  bool   `json:"done"`
	Error string `json:"error"`
}

// GetOperation reports the status of a previously started async operation.
func (c *Client) GetOperation(ctx context.Context, operationID string) (scheduler.OperationStatus, error) {
	resp, err := c.authedRequest(ctx, http.MethodGet, "/api/v1/operations/"+operationID, nil, "")
	if err != nil {
		return scheduler.OperationStatus{}, fmt.Errorf("getting operation status: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return scheduler.OperationStatus{}, fmt.Errorf("getting operation status failed with status %d: %s", resp.StatusCode, respBody)
	}

	var parsed operationResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return scheduler.OperationStatus{}, fmt.Errorf("decoding operation status: %w", err)
	}
	return scheduler.OperationStatus{Done: parsed.Done, Error: parsed.Error}, nil
}
