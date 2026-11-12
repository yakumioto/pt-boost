package scheduler

import "time"

// SystemClock is the real, wall-clock Clock implementation used in
// production.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }
