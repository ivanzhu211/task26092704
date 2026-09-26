// Package rollstat implements rolling-window availability statistics
// for an intranet monitoring platform, backed by SQLite.
package rollstat

import (
	"errors"
	"fmt"
	"time"
)

// Sample is a single probe result produced by the probe engine.
type Sample struct {
	MonitorID string
	At        int64 // Unix milliseconds
	Ok        bool
	LatencyMs int64 // 0..60000
}

// Window is a rolling window length. Exactly three values are supported.
type Window time.Duration

const (
	Window24h = Window(24 * time.Hour)
	Window7d  = Window(7 * 24 * time.Hour)
	Window30d = Window(30 * 24 * time.Hour)
)

// ErrInvalidWindow is returned by Stats for unsupported window lengths.
var ErrInvalidWindow = errors.New("rollstat: window must be 24h, 7d or 30d")

// Metrics holds the three rolling-window indicators. A nil pointer means
// "no data" for that indicator; it is never reported as zero.
type Metrics struct {
	// Availability is the time-weighted up ratio over the window:
	// time in up state / time with any state. Nil if the monitor had
	// no state at any point inside the window.
	Availability *float64
	// AvgLatencyMs is the exact arithmetic mean of LatencyMs over
	// successful samples whose At falls inside the window.
	AvgLatencyMs *float64
	// P95LatencyMs approximates the nearest-rank p95 of the same sample
	// set with |v-q| <= 1%*q guaranteed for any distribution.
	P95LatencyMs *float64
	// RawRowsRead reports how many raw sample rows this Stats call read.
	// It is always <= MaxRawRowsPerStats.
	RawRowsRead int
}

// bucketMs is the aggregate bucket size. Raw sample rows are only ever
// read for the (at most) two buckets straddling the window boundaries.
const bucketMs int64 = 60 * 60 * 1000 // 1 hour

// maxSamplesPerBucket assumes the documented probe cadence: at most one
// sample per monitor per 30s (backoff only makes sampling sparser).
const maxSamplesPerBucket = 3600_000 / 30_000 // 120

// MaxRawRowsPerStats is the declared upper bound on the number of raw
// sample rows a single Stats call reads: two boundary buckets plus a
// constant number of point lookups (predecessor/successor/first/last).
const MaxRawRowsPerStats = 2*maxSamplesPerBucket + 8 // 248

func (w Window) duration() (time.Duration, error) {
	switch w {
	case Window24h, Window7d, Window30d:
		return time.Duration(w), nil
	default:
		return 0, fmt.Errorf("%w: got %v", ErrInvalidWindow, time.Duration(w))
	}
}
