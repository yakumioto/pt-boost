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

// fakeServer stands in for a real torrentfs-go instance. It encodes the
// /api/v1 contract as verified against torrentfs-go main @ 5936439 and a
// running instance (2026-11-12), including the parts an earlier version of
// this fixture got wrong together with the client:
//   - login takes {"password"} only and rejects unknown fields with 400;
//   - add returns 201, is idempotent, and the id is the infohash;
//   - a category must be created (POST /api/v1/categories) before a
//     torrent can be tagged with it (otherwise 404 "unknown category");
//   - piece-download takes half-open piece-index ranges and rejects a
//     range past the last piece with 400;
//   - an operation reports {"state": "deleting"|"deleted"|...}.
type fakeServer struct {
	mu sync.Mutex

	validPassword string
	loginCalls    int
	failLoginWith int // 0 = succeed

	pieceLength, totalBytes int64 // reported by /status for every torrent
	categories              map[string]bool
	createCategoryCalls     int
	operationState          string // state reported for delete operations

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
	ranges []pieceRange
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		validPassword:  "secret",
		pieceLength:    4 << 20,
		totalBytes:     1000 << 20, // 250 pieces
		categories:     map[string]bool{},
		operationState: "deleted",
		entries:        map[string]*fakeEntry{},
		operations:     map[string]operationResponse{},
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
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/categories":
		s.requireAuth(w, r, s.handleCreateCategory)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
		s.requireAuth(w, r, s.handleStatus)
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
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // as torrentfs-go does: a "username" field is a 400
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Password != s.validPassword {
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

	hash := sha1.Sum(content)
	id := hex.EncodeToString(hash[:])
	if e, ok := s.entries[id]; !ok || e.deleted {
		s.entries[id] = &fakeEntry{infoHash: id}
	}
	writeJSON(w, http.StatusCreated, addTorrentResponse{ID: id})
}

func (s *fakeServer) handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	s.createCategoryCalls++
	var req createCategoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if s.categories[req.Name] {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "category already exists"})
		return
	}
	s.categories[req.Name] = true
	writeJSON(w, http.StatusCreated, map[string]string{"name": req.Name})
}

func (s *fakeServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := extractID(r.URL.Path, "/status")
	if e, ok := s.entries[id]; !ok || e.deleted {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"torrent":      map[string]any{"id": id, "total_bytes": s.totalBytes},
		"piece_length": s.pieceLength,
	})
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
	if !s.categories[req.Category] {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown category"})
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
	if len(req.Ranges) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ranges is required and must not be empty"})
		return
	}
	pieceCount := (s.totalBytes + s.pieceLength - 1) / s.pieceLength
	for _, rg := range req.Ranges {
		if rg.Start < 0 || rg.End <= rg.Start || rg.End > pieceCount {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "range is out of bounds"})
			return
		}
	}
	s.pieceDownloadCalls = append(s.pieceDownloadCalls, pieceDownloadCall{id: id, ranges: req.Ranges})
	writeJSON(w, http.StatusAccepted, map[string]any{"state": "running"})
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
	s.operations[opID] = operationResponse{State: s.operationState}
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
	c := New(ts.URL, "ignored-username", server.validPassword, opts...)
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

	// ADR 0003 depends on this: re-adding is a success with the same id.
	again, err := c.AddTorrent(context.Background(), []byte("d8:announce...torrent-bytes"))
	if err != nil {
		t.Fatalf("re-adding the same torrent returned error: %v", err)
	}
	if again != id {
		t.Errorf("re-add returned id %q, want the same id %q", again, id)
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

// TestSetCategory_CreatesCategoryOnce: torrentfs-go refuses to tag a
// torrent with a category that does not exist yet, so the client creates
// it first — once per process, and tolerating "already exists" (e.g. after
// a restart).
func TestSetCategory_CreatesCategoryOnce(t *testing.T) {
	server := newFakeServer()
	server.categories["boost-mteam"] = true // survives from a previous process
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	idA, err := c.AddTorrent(ctx, []byte("content-a"))
	if err != nil {
		t.Fatalf("AddTorrent returned error: %v", err)
	}
	idB, _ := c.AddTorrent(ctx, []byte("content-b"))

	for _, id := range []string{idA, idB} {
		if err := c.SetCategory(ctx, id, "boost-mteam"); err != nil {
			t.Fatalf("SetCategory(%s) returned error: %v", id, err)
		}
	}

	if len(server.setCategoryCalls) != 2 || server.setCategoryCalls[0] != (categoryCall{id: idA, category: "boost-mteam"}) {
		t.Errorf("setCategoryCalls = %+v, want two calls starting with id=%s category=boost-mteam", server.setCategoryCalls, idA)
	}
	if server.createCategoryCalls != 1 {
		t.Errorf("create-category called %d times, want 1", server.createCategoryCalls)
	}
}

func TestSetCategory_CreatesMissingCategory(t *testing.T) {
	server := newFakeServer()
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, _ := c.AddTorrent(ctx, []byte("content-a"))
	if err := c.SetCategory(ctx, id, "boost-mteam"); err != nil {
		t.Fatalf("SetCategory returned error: %v", err)
	}
	if !server.categories["boost-mteam"] || server.entries[id].category != "boost-mteam" {
		t.Errorf("category not created/applied: categories=%v entry=%+v", server.categories, server.entries[id])
	}
}

// TestTriggerPieceDownload_ConvertsBytesToPieceRange: the scheduler speaks
// bytes, torrentfs-go speaks half-open piece-index ranges.
func TestTriggerPieceDownload_ConvertsBytesToPieceRange(t *testing.T) {
	cases := []struct {
		name       string
		totalBytes int64
		byteRange  scheduler.ByteRange
		want       pieceRange
	}{
		{"whole pieces", 1000 << 20, scheduler.ByteRange{Offset: 0, Length: 256 << 20}, pieceRange{Start: 0, End: 64}},
		{"partial trailing piece is included", 1000 << 20, scheduler.ByteRange{Offset: 0, Length: 5 << 20}, pieceRange{Start: 0, End: 2}},
		{"clamped to a torrent smaller than the range", 10<<20 + 1, scheduler.ByteRange{Offset: 0, Length: 256 << 20}, pieceRange{Start: 0, End: 3}},
		{"non-zero offset", 1000 << 20, scheduler.ByteRange{Offset: 6 << 20, Length: 4 << 20}, pieceRange{Start: 1, End: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeServer()
			server.totalBytes = tc.totalBytes
			c, _ := newTestClient(t, server)
			ctx := context.Background()

			id, err := c.AddTorrent(ctx, []byte("content-b"))
			if err != nil {
				t.Fatalf("AddTorrent returned error: %v", err)
			}
			if err := c.TriggerPieceDownload(ctx, id, tc.byteRange); err != nil {
				t.Fatalf("TriggerPieceDownload returned error: %v", err)
			}

			if len(server.pieceDownloadCalls) != 1 {
				t.Fatalf("pieceDownloadCalls = %+v, want exactly one call", server.pieceDownloadCalls)
			}
			got := server.pieceDownloadCalls[0]
			if got.id != id || len(got.ranges) != 1 || got.ranges[0] != tc.want {
				t.Errorf("piece download call = %+v, want id=%s ranges=[%+v]", got, id, tc.want)
			}
		})
	}
}

func TestTriggerPieceDownload_MetadataNotReadyReturnsError(t *testing.T) {
	server := newFakeServer()
	server.pieceLength = 0
	c, _ := newTestClient(t, server)
	ctx := context.Background()

	id, _ := c.AddTorrent(ctx, []byte("content-b"))
	if err := c.TriggerPieceDownload(ctx, id, scheduler.ByteRange{Offset: 0, Length: 1 << 20}); err == nil {
		t.Fatal("want error when piece_length is unknown, got nil")
	}
	if len(server.pieceDownloadCalls) != 0 {
		t.Errorf("piece-download must not be sent without a piece length, got %+v", server.pieceDownloadCalls)
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

func TestGetOperation_DeletingIsNotDone(t *testing.T) {
	server := newFakeServer()
	server.operationState = "deleting"
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
	if status.Done {
		t.Errorf("operation status Done = true while state is deleting")
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

func TestAddTorrent_TransparentlyRetriesOnStale401(t *testing.T) {
	server := newFakeServer()
	server.authFailuresToInject = 1 // first authed call anywhere gets a 401
	c, _ := newTestClient(t, server)

	// AddTorrent builds its own multipart body ahead of calling
	// authedRequest; it must still survive a stale-401 retry (that body
	// is a []byte under the hood, safe to replay) rather than failing
	// outright like a true one-shot io.Reader would.
	id, err := c.AddTorrent(context.Background(), []byte("d8:announce...retry-torrent-bytes"))
	if err != nil {
		t.Fatalf("AddTorrent returned error despite stale-token retry support: %v", err)
	}
	if id == "" {
		t.Error("AddTorrent returned empty id")
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
