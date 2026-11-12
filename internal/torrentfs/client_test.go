package torrentfs

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yakumioto/pt-boost/internal/scheduler"
)

// fakeServer stands in for a real torrentfs-go instance: it implements just
// enough of the /api/v1 HTTP surface (as pt-boost's client understands it)
// to drive end-to-end style tests without any network dependency or a real
// torrentfs-go binary. torrentfs-go has no published OpenAPI spec and no
// stable release yet (see docs/adr/0001's "事后验证"); this fixture encodes
// this client's best-effort understanding of the contract, which is the
// thing an actual wire-level integration test against a running
// torrentfs-go would need to re-validate.
type fakeServer struct {
	mu sync.Mutex

	validUsername, validPassword string
	loginCalls                   int
	failLoginWith                int // 0 = succeed

	nextID               int
	entries              map[string]*fakeEntry // id -> entry
	rejectAddTorrent     bool                  // simulate HTTP 400 "invalid torrent"
	authFailuresToInject int                   // first N authed requests get a 401

	setCategoryCalls   []categoryCall
	pieceDownloadCalls []pieceDownloadCall
	deleteCalls        []string
	operations         map[string]operationResponse
}

type fakeEntry struct {
	infoHash      string
	category      string
	uploadedBytes int64
	deleted       bool
}

type categoryCall struct {
	id       string
	category string
}

type pieceDownloadCall struct {
	id     string
	offset int64
	length int64
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		validUsername: "boost",
		validPassword: "secret",
		entries:       map[string]*fakeEntry{},
		operations:    map[string]operationResponse{},
	}
}

func (s *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/auth/login":
		s.handleLogin(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/torrents":
		s.requireAuth(w, r, s.handleAddTorrent)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/torrents":
		s.requireAuth(w, r, s.handleListTorrents)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/category"):
		s.requireAuth(w, r, s.handleSetCategory)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/piece-download"):
		s.requireAuth(w, r, s.handlePieceDownload)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/torrents/"):
		s.requireAuth(w, r, s.handleDeleteTorrent)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/operations/"):
		s.requireAuth(w, r, s.handleGetOperation)
	default:
		http.NotFound(w, r)
	}
}

func (s *fakeServer) requireAuth(w http.ResponseWriter, r *http.Request, next func(w http.ResponseWriter, r *http.Request)) {
	if s.authFailuresToInject > 0 {
		s.authFailuresToInject--
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	next(w, r)
}

func (s *fakeServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	s.loginCalls++
	if s.failLoginWith != 0 {
		w.WriteHeader(s.failLoginWith)
		return
	}
	var req loginRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Username != s.validUsername || req.Password != s.validPassword {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	writeJSON(w, http.StatusOK, loginResponse{Token: fmt.Sprintf("token-%d", s.loginCalls)})
}

func (s *fakeServer) handleAddTorrent(w http.ResponseWriter, r *http.Request) {
	if s.rejectAddTorrent {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("not a valid bencoded torrent"))
		return
	}

	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	part, err := mr.NextPart()
	if err != nil || part.FormName() != "file" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	content, err := io.ReadAll(part)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	s.nextID++
	id := fmt.Sprintf("t%d", s.nextID)
	hash := sha1.Sum(content)
	s.entries[id] = &fakeEntry{infoHash: hex.EncodeToString(hash[:])}
	writeJSON(w, http.StatusOK, addTorrentResponse{ID: id})
}

func (s *fakeServer) handleListTorrents(w http.ResponseWriter, r *http.Request) {
	var out []torrentListEntry
	for id, e := range s.entries {
		if e.deleted {
			continue
		}
		out = append(out, torrentListEntry{ID: id, InfoHash: e.infoHash, Category: e.category, UploadedBytes: e.uploadedBytes})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *fakeServer) handleSetCategory(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/category")
	e, ok := s.entries[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var req setCategoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	e.category = req.Category
	s.setCategoryCalls = append(s.setCategoryCalls, categoryCall{id: id, category: req.Category})
	w.WriteHeader(http.StatusOK)
}

func (s *fakeServer) handlePieceDownload(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/piece-download")
	if _, ok := s.entries[id]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var req pieceDownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.pieceDownloadCalls = append(s.pieceDownloadCalls, pieceDownloadCall{id: id, offset: req.Offset, length: req.Length})
	w.WriteHeader(http.StatusOK)
}

func (s *fakeServer) handleDeleteTorrent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/torrents/")
	e, ok := s.entries[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	e.deleted = true
	s.deleteCalls = append(s.deleteCalls, id)
	opID := "op-" + id
	s.operations[opID] = operationResponse{Done: true}
	writeJSON(w, http.StatusAccepted, deleteTorrentResponse{OperationID: opID})
}

func (s *fakeServer) handleGetOperation(w http.ResponseWriter, r *http.Request) {
	opID := strings.TrimPrefix(r.URL.Path, "/api/v1/operations/")
	status, ok := s.operations[opID]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *fakeServer) setUploaded(id string, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[id]; ok {
		e.uploadedBytes = bytes
	}
}

func extractID(path, suffix string) string {
	trimmed := strings.TrimPrefix(path, "/api/v1/torrents/")
	trimmed = strings.TrimSuffix(trimmed, suffix)
	return trimmed
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func newTestClient(t *testing.T, server *fakeServer, opts ...Option) (*Client, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(server)
	t.Cleanup(ts.Close)
	c := New(ts.URL, server.validUsername, server.validPassword, opts...)
	return c, ts
}

func TestLogin_CachesTokenAcrossRequests(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	if _, err := c.ListTorrents(ctx); err != nil {
		t.Fatalf("first ListTorrents returned error: %v", err)
	}
	if _, err := c.ListTorrents(ctx); err != nil {
		t.Fatalf("second ListTorrents returned error: %v", err)
	}

	if server.loginCalls != 1 {
		t.Errorf("login called %d times, want 1 (token should be cached)", server.loginCalls)
	}
}

func TestLogin_RefreshesTokenAfterTTLElapsed(t *testing.T) {
	server := newFakeServer()
	now := time.Now()
	c, _ := newTestClient(t, server, WithTokenTTL(10*time.Minute), withNow(func() time.Time { return now }))
	ctx := context.Background()

	if _, err := c.ListTorrents(ctx); err != nil {
		t.Fatalf("first ListTorrents returned error: %v", err)
	}
	now = now.Add(11 * time.Minute) // past the configured TTL
	if _, err := c.ListTorrents(ctx); err != nil {
		t.Fatalf("second ListTorrents returned error: %v", err)
	}

	if server.loginCalls != 2 {
		t.Errorf("login called %d times, want 2 (token should have been refreshed after TTL)", server.loginCalls)
	}
}

func TestLogin_FailureReturnsClearError(t *testing.T) {
	server := newFakeServer()
	server.failLoginWith = http.StatusUnauthorized
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	_, err := c.ListTorrents(ctx)
	if err == nil {
		t.Fatal("expected an error when login fails, got nil")
	}
	if !strings.Contains(err.Error(), "login") {
		t.Errorf("error = %q, want it to mention login failure", err.Error())
	}
}

func TestAddTorrent_Success_ReturnsID(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)

	id, err := c.AddTorrent(context.Background(), []byte("d8:announce...torrent-bytes"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
	if id == "" {
		t.Error("AddTorrent returned empty id")
	}
}

func TestAddTorrent_InvalidTorrent_ReturnsErrInvalidTorrent(t *testing.T) {
	server := newFakeServer()
	server.rejectAddTorrent = true
	c, _ := newTestClient(t, server)

	_, err := c.AddTorrent(context.Background(), []byte("not-a-torrent"))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, ErrInvalidTorrent) {
		t.Errorf("error = %v, want errors.Is(err, ErrInvalidTorrent) to be true", err)
	}
}

func TestSetCategory_Success(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, err := c.AddTorrent(ctx, []byte("content-a"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}

	if err := c.SetCategory(ctx, id, "boost-mteam"); err != nil {
		t.Fatalf("SetCategory returned error: %v", err)
	}

	if len(server.setCategoryCalls) != 1 || server.setCategoryCalls[0].id != id || server.setCategoryCalls[0].category != "boost-mteam" {
		t.Errorf("setCategoryCalls = %+v, want exactly one call for id=%s category=boost-mteam", server.setCategoryCalls, id)
	}
}

func TestTriggerPieceDownload_SendsConfiguredOffsetAndLength(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, err := c.AddTorrent(ctx, []byte("content-b"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}

	byteRange := scheduler.ByteRange{Offset: 0, Length: 256 * 1024 * 1024}
	if err := c.TriggerPieceDownload(ctx, id, byteRange); err != nil {
		t.Fatalf("TriggerPieceDownload returned error: %v", err)
	}

	if len(server.pieceDownloadCalls) != 1 {
		t.Fatalf("pieceDownloadCalls = %+v, want exactly one call", server.pieceDownloadCalls)
	}
	got := server.pieceDownloadCalls[0]
	if got.id != id || got.offset != byteRange.Offset || got.length != byteRange.Length {
		t.Errorf("piece download call = %+v, want id=%s offset=%d length=%d", got, id, byteRange.Offset, byteRange.Length)
	}
}

func TestListTorrents_ParsesEntries(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	idA, _ := c.AddTorrent(ctx, []byte("content-a"))
	idB, _ := c.AddTorrent(ctx, []byte("content-b"))
	_ = c.SetCategory(ctx, idA, "boost-mteam")
	_ = c.SetCategory(ctx, idB, "boost-mteam")
	server.setUploaded(idA, 12345)

	views, err := c.ListTorrents(ctx)
	if err != nil {
		t.Fatalf("ListTorrents returned error: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("ListTorrents returned %d entries, want 2", len(views))
	}
	byID := map[string]scheduler.TorrentView{}
	for _, v := range views {
		byID[v.ID] = v
	}
	if byID[idA].UploadedBytes != 12345 {
		t.Errorf("entry %s UploadedBytes = %d, want 12345", idA, byID[idA].UploadedBytes)
	}
	if byID[idA].Category != "boost-mteam" || byID[idB].Category != "boost-mteam" {
		t.Errorf("entries did not retain category: %+v", byID)
	}
	if byID[idA].InfoHash == "" || byID[idB].InfoHash == "" {
		t.Errorf("entries missing InfoHash: %+v", byID)
	}
}

func TestDeleteTorrent_ReturnsOperationID(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, err := c.AddTorrent(ctx, []byte("content-a"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}

	opID, err := c.DeleteTorrent(ctx, id)
	if err != nil {
		t.Fatalf("DeleteTorrent returned error: %v", err)
	}
	if opID == "" {
		t.Error("DeleteTorrent returned empty operation id")
	}

	views, err := c.ListTorrents(ctx)
	if err != nil {
		t.Fatalf("ListTorrents returned error: %v", err)
	}
	for _, v := range views {
		if v.ID == id {
			t.Errorf("deleted torrent %s still appears in ListTorrents", id)
		}
	}
}

func TestGetOperation_ReturnsDoneStatus(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, _ := c.AddTorrent(ctx, []byte("content-a"))
	opID, err := c.DeleteTorrent(ctx, id)
	if err != nil {
		t.Fatalf("DeleteTorrent returned error: %v", err)
	}

	status, err := c.GetOperation(ctx, opID)
	if err != nil {
		t.Fatalf("GetOperation returned error: %v", err)
	}
	if !status.Done {
		t.Errorf("operation status Done = false, want true")
	}
	if status.Error != "" {
		t.Errorf("operation status Error = %q, want empty", status.Error)
	}
}

func TestAuthedRequest_TransparentlyRetriesOnStale401(t *testing.T) {
	server := newFakeServer()
	server.authFailuresToInject = 1 // first authed call anywhere gets a 401
	c, _ := newTestClient(t, server)

	// The client must notice the 401, re-login, and retry — the caller
	// should not have to know anything went wrong.
	views, err := c.ListTorrents(context.Background())
	if err != nil {
		t.Fatalf("ListTorrents returned error despite stale-token retry support: %v", err)
	}
	if views != nil && len(views) != 0 {
		t.Errorf("expected an empty torrent list, got %v", views)
	}
	if server.loginCalls != 2 {
		t.Errorf("login called %d times, want 2 (initial + retry after 401)", server.loginCalls)
	}
}

func TestEndToEnd_AddCategorizePieceDownloadListDelete(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, err := c.AddTorrent(ctx, []byte("d8:announce...full-lifecycle-torrent"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}

	if err := c.SetCategory(ctx, id, "boost-mteam"); err != nil {
		t.Fatalf("SetCategory returned error: %v", err)
	}

	byteRange := scheduler.ByteRange{Offset: 0, Length: 256 * 1024 * 1024}
	if err := c.TriggerPieceDownload(ctx, id, byteRange); err != nil {
		t.Fatalf("TriggerPieceDownload returned error: %v", err)
	}

	views, err := c.ListTorrents(ctx)
	if err != nil {
		t.Fatalf("ListTorrents returned error: %v", err)
	}
	found := false
	for _, v := range views {
		if v.ID == id && v.Category == "boost-mteam" {
			found = true
		}
	}
	if !found {
		t.Fatalf("landed torrent not found with expected category in list: %+v", views)
	}

	opID, err := c.DeleteTorrent(ctx, id)
	if err != nil {
		t.Fatalf("DeleteTorrent returned error: %v", err)
	}
	status, err := c.GetOperation(ctx, opID)
	if err != nil {
		t.Fatalf("GetOperation returned error: %v", err)
	}
	if !status.Done {
		t.Fatalf("expected delete operation to be done, got %+v", status)
	}

	viewsAfterDelete, err := c.ListTorrents(ctx)
	if err != nil {
		t.Fatalf("ListTorrents after delete returned error: %v", err)
	}
	for _, v := range viewsAfterDelete {
		if v.ID == id {
			t.Fatalf("deleted torrent %s still present after delete", id)
		}
	}
}
