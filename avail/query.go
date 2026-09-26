package avail

import (
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// windowMillis validates w and returns its length in milliseconds.
func windowMillis(w Window) (int64, error) {
	switch w {
	case Window24h, Window7d, Window30d:
		return int64(time.Duration(w) / time.Millisecond), nil
	}
	return 0, errInvalidWindow
}

// accumulator threads the carry-in state through chunks of raw samples and
// pre-aggregated interior buckets while walking the window left to right.
type accumulator struct {
	haveCarry bool
	carryOK   bool

	statefulMs int64
	okMs       int64

	okCount int64
	okSum   int64
	hist    histogram
}

// addRawChunk folds a run of raw samples covering [a, b) into the
// accumulator. samples must be sorted by At and lie inside [a, b).
func (ac *accumulator) addRawChunk(a, b int64, samples []Sample) {
	if len(samples) == 0 {
		if ac.haveCarry {
			ac.statefulMs += b - a
			if ac.carryOK {
				ac.okMs += b - a
			}
		}
		return
	}
	if ac.haveCarry {
		gap := samples[0].At - a
		ac.statefulMs += gap
		if ac.carryOK {
			ac.okMs += gap
		}
	}
	for i := 0; i+1 < len(samples); i++ {
		dur := samples[i+1].At - samples[i].At
		ac.statefulMs += dur
		if samples[i].OK {
			ac.okMs += dur
		}
	}
	last := samples[len(samples)-1]
	ac.statefulMs += b - last.At
	if last.OK {
		ac.okMs += b - last.At
	}
	for _, sm := range samples {
		if sm.OK {
			ac.okCount++
			ac.okSum += sm.LatencyMs
			ac.hist[binOf(sm.LatencyMs)]++
		}
	}
	ac.haveCarry = true
	ac.carryOK = last.OK
}

// addBucket folds one pre-aggregated interior bucket [start, start+B)
// into the accumulator.
func (ac *accumulator) addBucket(start, firstAt int64, lastOK, okTail, okCount, okSum int64, hist histogram) {
	if ac.haveCarry {
		head := firstAt - start
		ac.statefulMs += head
		if ac.carryOK {
			ac.okMs += head
		}
	}
	ac.statefulMs += start + bucketSizeMs - firstAt
	ac.okMs += okTail
	ac.okCount += okCount
	ac.okSum += okSum
	for b, c := range hist {
		ac.hist[b] += c
	}
	ac.haveCarry = true
	ac.carryOK = lastOK != 0
}

// addEmptyBucket folds one interior bucket that contains no samples: its
// whole duration carries the incoming state, if any.
func (ac *accumulator) addEmptyBucket() {
	if ac.haveCarry {
		ac.statefulMs += bucketSizeMs
		if ac.carryOK {
			ac.okMs += bucketSizeMs
		}
	}
}

// Stats computes the rolling-window statistics for monitorID over the
// exact interval [now-window, now).
//
// Raw samples are read only for the carry-in lookup and the at-most-two
// partial buckets at the window edges; interior buckets are served from
// pre-aggregated rows, so RawRowsRead never exceeds MaxRawRowsPerStats.
func (s *Store) Stats(monitorID string, window Window, now int64) (Stats, error) {
	w, err := windowMillis(window)
	if err != nil {
		return Stats{}, err
	}
	L, R := now-w, now
	var st Stats
	ac := &accumulator{hist: make(histogram)}

	// Carry-in: the last sample strictly before the window determines the
	// state at L (a sample exactly at L is picked up by the first chunk).
	var carryAt, carryOK int64
	err = s.db.QueryRow(
		`SELECT at, ok FROM samples WHERE monitor_id = ? AND at < ?
		 ORDER BY at DESC LIMIT 1`, monitorID, L).Scan(&carryAt, &carryOK)
	switch {
	case err == sql.ErrNoRows:
	case err != nil:
		return Stats{}, fmt.Errorf("avail: carry lookup: %w", err)
	default:
		st.RawRowsRead++
		ac.haveCarry = true
		ac.carryOK = carryOK != 0
	}

	firstB := bucketStart(L)
	lastB := bucketStart(R - 1)

	readChunk := func(a, b int64) error {
		rows, err := s.db.Query(
			`SELECT at, ok, latency_ms FROM samples
			 WHERE monitor_id = ? AND at >= ? AND at < ? ORDER BY at ASC`,
			monitorID, a, b)
		if err != nil {
			return fmt.Errorf("avail: read chunk: %w", err)
		}
		defer rows.Close()
		var samples []Sample
		for rows.Next() {
			var sm Sample
			var ok int64
			if err := rows.Scan(&sm.At, &ok, &sm.LatencyMs); err != nil {
				return err
			}
			sm.OK = ok != 0
			samples = append(samples, sm)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		st.RawRowsRead += int64(len(samples))
		ac.addRawChunk(a, b, samples)
		return nil
	}

	if firstB == lastB {
		if err := readChunk(L, R); err != nil {
			return Stats{}, err
		}
	} else {
		// Partial head bucket.
		if err := readChunk(L, firstB+bucketSizeMs); err != nil {
			return Stats{}, err
		}
		// Interior buckets from pre-aggregated rows.
		rows, err := s.db.Query(
			`SELECT bucket, first_at, last_ok, ok_tail, ok_count, ok_latency_sum, hist
			 FROM buckets WHERE monitor_id = ? AND bucket > ? AND bucket < ?
			 ORDER BY bucket ASC`, monitorID, firstB, lastB)
		if err != nil {
			return Stats{}, fmt.Errorf("avail: read buckets: %w", err)
		}
		type bucketRow struct {
			firstAt, lastOK, okTail, okCount, okSum int64
			hist                                    histogram
		}
		byStart := make(map[int64]bucketRow)
		for rows.Next() {
			var start, firstAt, lastOK, okTail, okCount, okSum int64
			var histBlob []byte
			if err := rows.Scan(&start, &firstAt, &lastOK, &okTail, &okCount, &okSum, &histBlob); err != nil {
				rows.Close()
				return Stats{}, err
			}
			h, err := decodeHistogram(histBlob)
			if err != nil {
				rows.Close()
				return Stats{}, err
			}
			byStart[start] = bucketRow{firstAt, lastOK, okTail, okCount, okSum, h}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return Stats{}, err
		}
		for b := firstB + bucketSizeMs; b < lastB; b += bucketSizeMs {
			if br, ok := byStart[b]; ok {
				ac.addBucket(b, br.firstAt, br.lastOK, br.okTail, br.okCount, br.okSum, br.hist)
			} else {
				ac.addEmptyBucket()
			}
		}
		// Partial tail bucket.
		if err := readChunk(lastB, R); err != nil {
			return Stats{}, err
		}
	}

	if ac.statefulMs > 0 {
		v := float64(ac.okMs) / float64(ac.statefulMs)
		st.Availability = &v
	}
	if ac.okCount > 0 {
		v := float64(ac.okSum) / float64(ac.okCount)
		st.AvgLatencyMs = &v
		st.P95LatencyMs = nearestRankP95(ac.hist, ac.okCount)
	}
	return st, nil
}

// nearestRankP95 returns the lower bound of the histogram bin containing
// the nearest-rank p95 of n values, which approximates the true p95 q with
// relative error at most 1%.
func nearestRankP95(h histogram, n int64) *float64 {
	rank := (95*n + 99) / 100 // ceil(0.95 * n)
	bins := make([]uint32, 0, len(h))
	for b := range h {
		bins = append(bins, b)
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i] < bins[j] })
	var cum int64
	for _, b := range bins {
		cum += int64(h[b])
		if cum >= rank {
			v := binLowerBound(b)
			return &v
		}
	}
	return nil // unreachable when n > 0
}
