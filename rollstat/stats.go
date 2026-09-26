package rollstat

import (
	"context"
	"database/sql"
	"sort"
)

// rawSample is one row read from the samples table.
type rawSample struct {
	at  int64
	ok  bool
	lat int64
}

// Stats computes the rolling-window metrics for monitorID over the exact
// half-open interval [now-window, now). now is an arbitrary Unix-ms value
// and is never rounded to any granularity.
//
// Raw sample rows are read only for the (at most two) buckets straddling
// the window boundaries plus a constant number of point lookups, so
// RawRowsRead is bounded by MaxRawRowsPerStats regardless of window
// length or sample count. Everything else is served from the per-bucket
// aggregate tables maintained by Record.
func (s *Store) Stats(monitorID string, window Window, now int64) (*Metrics, error) {
	d, err := window.duration()
	if err != nil {
		return nil, err
	}
	a := now - d.Milliseconds()
	b := now
	ctx := context.Background()
	m := &Metrics{}
	raw := 0

	// Point lookups bounding the raw-row count by a constant.
	var prevAt, lastAt int64
	var prevOk, lastOk bool
	hasPrev, err := s.sampleBefore(ctx, monitorID, a, &prevAt, &prevOk, &raw)
	if err != nil {
		return nil, err
	}
	hasLast, err := s.sampleBefore(ctx, monitorID, b, &lastAt, &lastOk, &raw)
	if err != nil {
		return nil, err
	}
	if !hasLast {
		// No sample at or before b: no state, no latency data in window.
		m.RawRowsRead = raw
		return m, nil
	}

	// Time with any state: from max(a, firstSampleAt) to b.
	stateTime := b - a
	if !hasPrev {
		firstAt, err := s.firstAtAfter(ctx, monitorID, a, &raw)
		if err != nil {
			return nil, err
		}
		stateTime = b - firstAt
	}

	bucketA := floorDiv(a, bucketMs)
	bucketB := floorDiv(b-1, bucketMs)

	upTime := int64(0)
	latCnt, latSum := int64(0), int64(0)
	histAcc := map[int]int64{}

	// consumeBucket folds the raw rows of one boundary bucket, restricted
	// to the sub-range [x, y), into the accumulators.
	consumeBucket := func(k, x, y int64) error {
		rows, err := s.bucketSamples(ctx, monitorID, k, &raw)
		if err != nil {
			return err
		}
		predOk, hasPred, err := s.predOkBefore(ctx, monitorID, k*bucketMs, &raw)
		if err != nil {
			return err
		}
		hasState, stateOk := hasPred, predOk
		t := k * bucketMs
		for _, r := range rows {
			if hasState && stateOk {
				if lo, hi := max(t, x), min(r.at, y); hi > lo {
					upTime += hi - lo
				}
			}
			hasState, stateOk, t = true, r.ok, r.at
			if r.ok && r.at >= x && r.at < y {
				latCnt++
				latSum += r.lat
				histAcc[latBucket(r.lat)]++
			}
		}
		// Trailing interval extends to the next sample or to +inf; either
		// way it covers the range up to y.
		if hasState && stateOk {
			if lo := max(t, x); y > lo {
				upTime += y - lo
			}
		}
		return nil
	}

	kLo, kHi := bucketA, bucketB+1 // interior bucket range [kLo, kHi)
	if bucketA == bucketB {
		if err := consumeBucket(bucketA, a, b); err != nil {
			return nil, err
		}
		kLo, kHi = 1, 0 // no interior
	} else {
		if a > bucketA*bucketMs { // a not bucket-aligned: partial head bucket
			if err := consumeBucket(bucketA, a, (bucketA+1)*bucketMs); err != nil {
				return nil, err
			}
			kLo = bucketA + 1
		}
		if b < (bucketB+1)*bucketMs { // b not bucket-aligned: partial tail bucket
			if err := consumeBucket(bucketB, bucketB*bucketMs, b); err != nil {
				return nil, err
			}
			kHi = bucketB
		}
	}

	if kLo < kHi {
		up, err := s.sumAvail(ctx, monitorID, kLo, kHi)
		if err != nil {
			return nil, err
		}
		upTime += up
		cnt, sum, err := s.sumLat(ctx, monitorID, kLo, kHi)
		if err != nil {
			return nil, err
		}
		latCnt += cnt
		latSum += sum
		if err := s.mergeHist(ctx, monitorID, kLo, kHi, histAcc); err != nil {
			return nil, err
		}
		// The newest sample's open interval [lastAt, +inf) is not stored in
		// any bucket; add its intersection with the interior range.
		gAt, gOk, hasG, err := s.globalLast(ctx, monitorID, &raw)
		if err != nil {
			return nil, err
		}
		if hasG && gOk && gAt < kHi*bucketMs {
			upTime += kHi*bucketMs - max(kLo*bucketMs, gAt)
		}
	}

	if stateTime > 0 {
		av := float64(upTime) / float64(stateTime)
		m.Availability = &av
	}
	if latCnt > 0 {
		avg := float64(latSum) / float64(latCnt)
		m.AvgLatencyMs = &avg
		rank := (95*latCnt + 99) / 100 // ceil(0.95 * n)
		keys := make([]int, 0, len(histAcc))
		for k := range histAcc {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		cum := int64(0)
		for _, k := range keys {
			cum += histAcc[k]
			if cum >= rank {
				v := bucketLow(k)
				m.P95LatencyMs = &v
				break
			}
		}
	}
	m.RawRowsRead = raw
	return m, nil
}

// sampleBefore finds the last sample with at < t. Counts one raw row.
func (s *Store) sampleBefore(ctx context.Context, monitorID string, t int64, at *int64, ok *bool, raw *int) (bool, error) {
	err := s.db.QueryRowContext(ctx,
		`SELECT at, ok FROM samples WHERE monitor_id = ? AND at < ?
		 ORDER BY at DESC LIMIT 1`, monitorID, t).Scan(at, ok)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	*raw++
	return true, nil
}

// firstAtAfter finds the first sample with at >= t. Counts one raw row.
func (s *Store) firstAtAfter(ctx context.Context, monitorID string, t int64, raw *int) (int64, error) {
	var at int64
	err := s.db.QueryRowContext(ctx,
		`SELECT at FROM samples WHERE monitor_id = ? AND at >= ?
		 ORDER BY at ASC LIMIT 1`, monitorID, t).Scan(&at)
	if err != nil {
		return 0, err
	}
	*raw++
	return at, nil
}

// bucketSamples reads all raw samples in bucket k. Counts len(rows) rows.
func (s *Store) bucketSamples(ctx context.Context, monitorID string, k int64, raw *int) ([]rawSample, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT at, ok, latency_ms FROM samples
		 WHERE monitor_id = ? AND at >= ? AND at < ? ORDER BY at`,
		monitorID, k*bucketMs, (k+1)*bucketMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []rawSample
	for rows.Next() {
		var r rawSample
		if err := rows.Scan(&r.at, &r.ok, &r.lat); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	*raw += len(out)
	return out, rows.Err()
}

// predOkBefore returns the ok flag of the last sample before t. Counts one row.
func (s *Store) predOkBefore(ctx context.Context, monitorID string, t int64, raw *int) (bool, bool, error) {
	var ok bool
	err := s.db.QueryRowContext(ctx,
		`SELECT ok FROM samples WHERE monitor_id = ? AND at < ?
		 ORDER BY at DESC LIMIT 1`, monitorID, t).Scan(&ok)
	if err == sql.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	*raw++
	return ok, true, nil
}

// globalLast returns the newest sample of the monitor. Counts one raw row.
func (s *Store) globalLast(ctx context.Context, monitorID string, raw *int) (int64, bool, bool, error) {
	var at int64
	var ok bool
	err := s.db.QueryRowContext(ctx,
		`SELECT at, ok FROM samples WHERE monitor_id = ?
		 ORDER BY at DESC LIMIT 1`, monitorID).Scan(&at, &ok)
	if err == sql.ErrNoRows {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, err
	}
	*raw++
	return at, ok, true, nil
}

func (s *Store) sumAvail(ctx context.Context, monitorID string, kLo, kHi int64) (int64, error) {
	var v int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(up_ms), 0) FROM avail
		 WHERE monitor_id = ? AND bucket >= ? AND bucket < ?`,
		monitorID, kLo, kHi).Scan(&v)
	return v, err
}

func (s *Store) sumLat(ctx context.Context, monitorID string, kLo, kHi int64) (int64, int64, error) {
	var cnt, sum int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(cnt), 0), COALESCE(SUM(sum), 0) FROM lat
		 WHERE monitor_id = ? AND bucket >= ? AND bucket < ?`,
		monitorID, kLo, kHi).Scan(&cnt, &sum)
	return cnt, sum, err
}

func (s *Store) mergeHist(ctx context.Context, monitorID string, kLo, kHi int64, acc map[int]int64) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT lat_bucket, SUM(cnt) FROM hist
		 WHERE monitor_id = ? AND bucket >= ? AND bucket < ?
		 GROUP BY lat_bucket`, monitorID, kLo, kHi)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var lb int
		var c int64
		if err := rows.Scan(&lb, &c); err != nil {
			return err
		}
		acc[lb] += c
	}
	return rows.Err()
}

func floorDiv(x, m int64) int64 {
	q := x / m
	if x%m != 0 && x < 0 {
		q--
	}
	return q
}
