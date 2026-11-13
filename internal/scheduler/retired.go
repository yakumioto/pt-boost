package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Retired is the permanent, per-site eviction denylist plus a durable cleanup
// queue. Scheduler serializes access. Empty path is memory-only for tests.
type Retired struct {
	path    string
	Records map[string][]string // torrentfs id -> known site ids
	pending map[string]bool     // deletion intent, committed before issuing DELETE
}

type retiredFile struct {
	Version int                 `json:"version"`
	Records map[string][]string `json:"records"`
	Pending map[string]bool     `json:"pending,omitempty"`
}

// OpenRetired fails closed on invalid/unreadable state. Missing state is a
// fresh deployment; operators must preserve the configured state directory.
func OpenRetired(path string) (*Retired, error) {
	r := &Retired{path: path, Records: map[string][]string{}, pending: map[string]bool{}}
	if path == "" {
		return r, nil
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	var f retiredFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("reading permanent eviction history: %w", err)
	}
	if f.Version != 1 || f.Records == nil {
		return nil, fmt.Errorf("invalid permanent eviction history version or records")
	}
	for id := range f.Pending {
		if _, ok := f.Records[id]; !ok {
			return nil, fmt.Errorf("pending deletion without retirement record: %s", id)
		}
	}
	r.Records = f.Records
	if f.Pending != nil {
		r.pending = f.Pending
	}
	return r, nil
}

func (r *Retired) hasID(id string) bool { _, ok := r.Records[id]; return ok }
func (r *Retired) hasSiteID(id string) bool {
	for _, ids := range r.Records {
		for _, siteID := range ids {
			if siteID == id {
				return true
			}
		}
	}
	return false
}

func (r *Retired) Remember(id string, siteIDs []string) error { return r.remember(id, siteIDs, false) }

// RememberPending atomically commits permanent exclusion AND cleanup intent.
// Even a crash between this write and DELETE will be retried on startup.
func (r *Retired) RememberPending(id string, siteIDs []string) error {
	return r.remember(id, siteIDs, true)
}
func (r *Retired) remember(id string, siteIDs []string, pending bool) error {
	records := make(map[string][]string, len(r.Records)+1)
	for k, v := range r.Records {
		records[k] = append([]string(nil), v...)
	}
	ids := records[id]
	for _, siteID := range siteIDs {
		found := false
		for _, old := range ids {
			if old == siteID {
				found = true
				break
			}
		}
		if !found {
			ids = append(ids, siteID)
		}
	}
	records[id] = ids
	next := r.copyPending()
	if pending {
		next[id] = true
	}
	return r.commit(records, next)
}
func (r *Retired) copyPending() map[string]bool {
	next := make(map[string]bool, len(r.pending))
	for k, v := range r.pending {
		next[k] = v
	}
	return next
}
func (r *Retired) complete(id string) error {
	next := r.copyPending()
	delete(next, id)
	return r.commit(r.Records, next)
}
func (r *Retired) commit(records map[string][]string, pending map[string]bool) error {
	if r.path != "" {
		if err := os.MkdirAll(filepath.Dir(r.path), 0700); err != nil {
			return err
		}
		b, err := json.Marshal(retiredFile{Version: 1, Records: records, Pending: pending})
		if err != nil {
			return err
		}
		f, err := os.CreateTemp(filepath.Dir(r.path), ".retired-*")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		if _, err = f.Write(b); err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if err = os.Rename(f.Name(), r.path); err != nil {
			return err
		}
		d, err := os.Open(filepath.Dir(r.path))
		if err != nil {
			return err
		}
		err = d.Sync()
		closeErr = d.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	r.Records = records
	r.pending = pending
	return nil
}

func WithRetired(r *Retired) Option { return func(s *Scheduler) { s.retired = r } }
