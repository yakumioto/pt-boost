package torrentfs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func cacheFixture(step, total int64, cached ...int) cacheSnapshot {
	var s cacheSnapshot
	s.MetainfoReady = true
	s.PieceLength = step
	s.Torrent.TotalBytes = total
	s.Torrent.Category = "boost-mteam"
	n := (total-1)/step + 1
	s.Pieces = make([]cachePiece, n)
	for i := range s.Pieces {
		s.Pieces[i].Index = int64(i)
	}
	for _, i := range cached {
		size := step
		if int64(i) == n-1 {
			size = total - int64(i)*step
		}
		s.Pieces[i].Cached = true
		s.Pieces[i].CachedBytes = size
		s.Torrent.CachedBytes += size
	}
	return s
}

// This HTTP fixture deliberately uses the real wire contract, including the
// distinct no-task 404 and replacement semantics of PUT. No torrentfs library
// is linked or imported by the client or these tests.
type cacheAPI struct {
	mu         sync.Mutex
	snapshot   cacheSnapshot
	task       string
	statusCode int
	taskCode   int
	taskBody   string
	putCode    int
	puts       []pieceDownloadRequest
}

func (a *cacheAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/api/v1/auth/login" {
		fmt.Fprint(w, `{"token":"test-token"}`)
		return
	}
	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "unauthorized", 401)
		return
	}
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/status"):
		if a.statusCode != 0 {
			w.WriteHeader(a.statusCode)
			fmt.Fprint(w, `{"error":"status unavailable"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(a.snapshot)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/piece-download"):
		if a.taskCode != 0 {
			w.WriteHeader(a.taskCode)
			fmt.Fprint(w, a.taskBody)
			return
		}
		if a.task == "" {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"error":"no piece download for torrent"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(pieceTask{State: a.task})
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/piece-download"):
		var req pieceDownloadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		a.puts = append(a.puts, req)
		if a.putCode != 0 {
			w.WriteHeader(a.putCode)
			fmt.Fprint(w, `{"error":"write failed"}`)
			return
		}
		a.task = "running"
		w.WriteHeader(202)
		fmt.Fprint(w, `{"state":"running","requested_pieces":1,"done_pieces":0}`)
	default:
		http.NotFound(w, r)
	}
}

func TestInspectCache(t *testing.T) {
	for _, state := range []string{"", "running", "completed", "cancelled", "expired", "failed"} {
		t.Run("task="+state, func(t *testing.T) {
			api := &cacheAPI{snapshot: cacheFixture(4, 15, 1, 3), task: state}
			srv := httptest.NewServer(api)
			defer srv.Close()
			c := New(srv.URL, "", "secret")
			s, err := c.InspectCache(context.Background(), "seed")
			if err != nil {
				t.Fatal(err)
			}
			if s.CachedBytes != 7 || s.TotalBytes != 15 || s.Category != "boost-mteam" || s.Active != (state == "running") || s.Failed != (state == "failed" || state == "expired") {
				t.Fatalf("unexpected cache status: %+v", s)
			}
			if len(api.puts) != 0 {
				t.Fatal("inspection wrote task")
			}
		})
	}
}

func TestRefillCache(t *testing.T) {
	const mib = int64(1 << 20)
	tests := []struct {
		name     string
		snapshot cacheSnapshot
		state    string
		target   int64
		want     []pieceRange
		wantErr  string
	}{
		{name: "scattered cached pieces are excluded", snapshot: cacheFixture(4*mib, 24*mib, 0, 2), target: 20 * mib, want: []pieceRange{{1, 2}, {3, 5}}},
		{name: "128MiB actual deficit", snapshot: cacheFixture(8*mib, 256*mib, 0, 2, 4, 6), target: 128 * mib, want: []pieceRange{{1, 2}, {3, 4}, {5, 6}, {7, 16}}},
		{name: "short last piece", snapshot: cacheFixture(4, 11, 0, 1), target: 100, want: []pieceRange{{2, 3}}},
		{name: "whole piece rounding", snapshot: cacheFixture(8, 32, 1), target: 9, want: []pieceRange{{0, 1}}},
		{name: "already sufficient", snapshot: cacheFixture(4, 12, 2), target: 4},
		{name: "running task never replaced", snapshot: cacheFixture(4, 12), state: "running", target: 12},
		{name: "expired task may retry", snapshot: cacheFixture(4, 12, 0), state: "expired", target: 12, want: []pieceRange{{1, 3}}},
		{name: "completed does not imply resident", snapshot: cacheFixture(4, 12), state: "completed", target: 8, want: []pieceRange{{0, 2}}},
		{name: "no metadata", snapshot: cacheSnapshot{}, target: 12, wantErr: "metadata not ready"},
		{name: "invalid target", snapshot: cacheFixture(4, 12), target: 0, wantErr: "invalid cache target"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &cacheAPI{snapshot: tt.snapshot, task: tt.state}
			api.snapshot.Torrent.Category = "boost-mteam"
			srv := httptest.NewServer(api)
			defer srv.Close()
			c := New(srv.URL, "", "secret")
			err := c.RefillCache(context.Background(), "seed", "boost-mteam", tt.target)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error=%v want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(tt.want) == 0 {
				if len(api.puts) != 0 {
					t.Fatalf("unexpected PUT: %+v", api.puts)
				}
				return
			}
			if len(api.puts) != 1 || !reflect.DeepEqual(api.puts[0].Ranges, tt.want) {
				t.Fatalf("PUTs=%+v want=%+v", api.puts, tt.want)
			}
		})
	}
}

func TestCacheAPIErrorsNeverSubmit(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*cacheAPI)
		want      string
	}{
		{"status failure", func(a *cacheAPI) { a.statusCode = 503 }, "status 503"},
		{"category ownership changed", func(a *cacheAPI) { a.snapshot.Torrent.Category = "manual" }, "category changed"},
		{"task failure", func(a *cacheAPI) { a.taskCode = 503; a.taskBody = `{}` }, "status 503"},
		{"unknown torrent not no task", func(a *cacheAPI) { a.taskCode = 404; a.taskBody = `{"error":"unknown torrent"}` }, "unknown torrent"},
		{"unknown task state", func(a *cacheAPI) { a.task = "queued" }, "unknown piece download state"},
		{"malformed task", func(a *cacheAPI) { a.taskCode = 200; a.taskBody = `not JSON` }, "decoding piece download"},
		{"missing bitmap entry", func(a *cacheAPI) { a.snapshot.Pieces = a.snapshot.Pieces[:2] }, "incomplete piece cache bitmap"},
		{"duplicate piece", func(a *cacheAPI) { a.snapshot.Pieces[1].Index = 0 }, "invalid cache state"},
		{"inconsistent aggregate", func(a *cacheAPI) { a.snapshot.Torrent.CachedBytes = 1 }, "inconsistent resident cache bytes"},
		{"false cached flag with bytes", func(a *cacheAPI) { a.snapshot.Pieces[1].CachedBytes = 2 }, "invalid cache state"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &cacheAPI{snapshot: cacheFixture(4, 12)}
			tt.configure(api)
			srv := httptest.NewServer(api)
			defer srv.Close()
			c := New(srv.URL, "", "secret")
			err := c.RefillCache(context.Background(), "seed", "boost-mteam", 12)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v want %q", err, tt.want)
			}
			if len(api.puts) != 0 {
				t.Fatal("submitted on inspection failure")
			}
		})
	}
}

func TestRefillCachePUTFailure(t *testing.T) {
	api := &cacheAPI{snapshot: cacheFixture(4, 12), putCode: 409}
	srv := httptest.NewServer(api)
	defer srv.Close()
	err := New(srv.URL, "", "secret").RefillCache(context.Background(), "seed", "boost-mteam", 12)
	if err == nil || !strings.Contains(err.Error(), "status 409") {
		t.Fatalf("error=%v", err)
	}
}

func TestConcurrentRefillDoesNotReplaceRunningTask(t *testing.T) {
	api := &cacheAPI{snapshot: cacheFixture(4, 12)}
	srv := httptest.NewServer(api)
	defer srv.Close()
	c := New(srv.URL, "", "secret")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.RefillCache(context.Background(), "seed", "boost-mteam", 12); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(api.puts) != 1 {
		t.Fatalf("got %d replacing PUTs, want 1", len(api.puts))
	}
}

func TestRefillCacheReinspectsAfterEarlierSnapshot(t *testing.T) {
	api := &cacheAPI{snapshot: cacheFixture(4, 12)}
	srv := httptest.NewServer(api)
	defer srv.Close()
	c := New(srv.URL, "", "secret")
	if _, err := c.InspectCache(context.Background(), "seed"); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	api.snapshot = cacheFixture(4, 12, 0, 1, 2)
	api.mu.Unlock()
	if err := c.RefillCache(context.Background(), "seed", "boost-mteam", 12); err != nil {
		t.Fatal(err)
	}
	if len(api.puts) != 0 {
		t.Fatal("refill used stale residency")
	}
}
