package scheduler

import (
	"errors"
	"testing"
	"time"
)

func TestCacheMaintenance_DoesNotResetColdVerdictOrSamples(t *testing.T) {
	s, c, clock := cacheFixture(t, 1)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	initial := s.pool["a"].samples[0].at
	clock.Advance(20 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	m := s.pool["a"]
	if !m.cold || len(m.samples) != 2 || !m.samples[0].at.Equal(initial) {
		t.Fatal("refill reset cold verdict/history")
	}
	calls := len(c.calls)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != calls {
		t.Fatal("successful request retriggered without debounce")
	}
}

func TestCacheMaintenance_RetiredRunningStillConsumesBudget(t *testing.T) {
	s, c, _ := cacheFixture(t, 3)
	if err := s.retired.RememberPending("a", nil); err != nil {
		t.Fatal(err)
	}
	st := c.states["a"]
	st.Active = true
	c.states["a"] = st
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 || c.calls[0] != "b" {
		t.Fatalf("pending task omitted from budget: %v", c.calls)
	}
}

func TestCacheMaintenance_AsyncFailureBackoff(t *testing.T) {
	s, c, clock := cacheFixture(t, 1)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	st := c.states["a"]
	st.Failed = true
	c.states["a"] = st
	clock.Advance(2 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 {
		t.Fatal("async failure immediately retried")
	}
	clock.Advance(2 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 2 {
		t.Fatal("async retry did not occur")
	}
	clock.Advance(2 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if s.pool["a"].refillFailures != 2 || s.pool["a"].nextRefillAt.Sub(clock.Now()) != 4*time.Minute {
		t.Fatal("async failure did not increase backoff")
	}
	clock.Advance(2 * time.Minute)
	if err := s.DetectColdTorrents(ctx); err != nil {
		t.Fatal(err)
	}
	if s.pool["a"].refillFailures != 2 || len(c.calls) != 2 {
		t.Fatal("same failed task counted twice")
	}
}

func TestCacheMaintenance_BackoffCapsAtThirtyMinutes(t *testing.T) {
	s, c, clock := cacheFixture(t, 1)
	// Exercise repeated failures without waiting in real time.
	c.refillErr = errors.New("network")
	for i := 0; i < 8; i++ {
		if err := s.DetectColdTorrents(ctx); err == nil {
			t.Fatal("expected failure")
		}
		delay := s.pool["a"].nextRefillAt.Sub(clock.Now())
		if delay > 30*time.Minute {
			t.Fatalf("unbounded delay: %v", delay)
		}
		if i >= 4 && delay != 30*time.Minute {
			t.Fatalf("delay=%v want capped 30m", delay)
		}
		clock.Advance(delay)
	}
}
