// Package avail implements a rolling-window availability statistics store
// for an intranet monitoring platform, backed by SQLite.
//
// Samples arrive out of order and may be duplicated; duplicates of the same
// (MonitorID, At) pair are recorded only once. Statistics are computed over
// exact windows [now-W, now) without rounding to any bucket granularity.
package avail

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// bucketSizeMs is the aggregation bucket granularity. Buckets are an
// internal pre-aggregation device only: query windows are never rounded to
// bucket boundaries; partial buckets at the window edges are resolved by
// reading the (few) raw samples inside them.
const bucketSizeMs int64 = 60 * 60 * 1000 // 1 hour

// MaxRawRowsPerStats is the declared upper bound on the number of raw
// sample rows a single Stats call reads from the samples table.
//
// A Stats call reads raw samples only from (a) one carry-in lookup of the
// last sample before the window and (b) the at-most-two partial buckets at
// the window edges. With the guaranteed minimum probe interval of 30s, an
// hourly bucket holds at most 120 samples, hence 2*120+1 = 241.
const MaxRawRowsPerStats = 241

// Window is a rolling window length. Exactly three values are supported.
type Window time.Duration

const (
	Window24h = Window(24 * time.Hour)
	Window7d  = Window(7 * 24 * time.Hour)
	Window30d = Window(30 * 24 * time.Hour)
)

// Sample is a single probe outcome.
type Sample struct {
	MonitorID string
	At        int64 // Unix milliseconds
	OK        bool
	LatencyMs int64 // 0..60000
}

// Stats is the result of a Stats query. A nil pointer means "no data".
type Stats struct {
	// Availability is the time-weighted fraction of up-time over the
	// stateful part of the window. Nil when the monitor has no state
	// anywhere in the window.
	Availability *float64
	// AvgLatencyMs is the exact arithmetic mean of latencies of ok
	// samples inside the window. Nil when there are none.
	AvgLatencyMs *float64
	// P95LatencyMs approximates the nearest-rank p95 q of ok-sample
	// latencies in the window, with |v-q| <= 1%*q guaranteed.
	// Nil when there are no ok samples in the window.
	P95LatencyMs *float64
	// RawRowsRead reports how many raw sample rows this query read.
	// It never exceeds MaxRawRowsPerStats.
	RawRowsRead int64
}

// Store is a SQLite-backed statistics store.
type Store struct {
	db *sql.DB
}

// Open opens (creating if necessary) the store at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the store.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init() error {
	stmts := []string{
		`PRAGMA journal_mode = WAL`,
		`CREATE TABLE IF NOT EXISTS samples (
			monitor_id TEXT NOT NULL,
			at INTEGER NOT NULL,
			ok INTEGER NOT NULL,
			latency_ms INTEGER NOT NULL,
			PRIMARY KEY (monitor_id, at)
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS buckets (
			monitor_id TEXT NOT NULL,
			bucket INTEGER NOT NULL,
			n INTEGER NOT NULL,
			first_at INTEGER NOT NULL,
			last_at INTEGER NOT NULL,
			last_ok INTEGER NOT NULL,
			ok_tail INTEGER NOT NULL,
			ok_count INTEGER NOT NULL,
			ok_latency_sum INTEGER NOT NULL,
			hist BLOB NOT NULL,
			PRIMARY KEY (monitor_id, bucket)
		) WITHOUT ROWID`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("init: %q: %w", q, err)
		}
	}
	return nil
}

var errInvalidWindow = errors.New("avail: window must be 24h, 7d or 30d")
