package avail

import (
	"database/sql"
	"fmt"
)

// bucketStart returns the start of the bucket containing at (floor
// division, safe for negative timestamps).
func bucketStart(at int64) int64 {
	r := at % bucketSizeMs
	if r < 0 {
		r += bucketSizeMs
	}
	return at - r
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// Record writes one sample. Duplicate (MonitorID, At) deliveries are
// ignored. The bucket aggregates of the affected bucket are updated
// incrementally using only the new sample and its immediate neighbours
// inside the bucket, so the cost of a Record does not grow with the
// monitor's total history.
func (s *Store) Record(sm Sample) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.Exec(
		`INSERT OR IGNORE INTO samples (monitor_id, at, ok, latency_ms) VALUES (?, ?, ?, ?)`,
		sm.MonitorID, sm.At, boolToInt(sm.OK), sm.LatencyMs)
	if err != nil {
		return fmt.Errorf("avail: insert sample: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return tx.Commit() // duplicate delivery, already accounted for
	}

	b := bucketStart(sm.At)
	end := b + bucketSizeMs

	var (
		nSamples, firstAt, lastAt, lastOk, okTail, okCount, okSum int64
		histBlob                                                  []byte
	)
	row := tx.QueryRow(
		`SELECT n, first_at, last_at, last_ok, ok_tail, ok_count, ok_latency_sum, hist
		 FROM buckets WHERE monitor_id = ? AND bucket = ?`, sm.MonitorID, b)
	err = row.Scan(&nSamples, &firstAt, &lastAt, &lastOk, &okTail, &okCount, &okSum, &histBlob)
	switch {
	case err == sql.ErrNoRows:
		// First sample in this bucket.
		nSamples, firstAt, lastAt = 0, sm.At, sm.At
		lastOk = boolToInt(sm.OK)
		okTail, okCount, okSum = lastOk*(end-sm.At), 0, 0
		histBlob = nil
	case err != nil:
		return fmt.Errorf("avail: load bucket: %w", err)
	default:
		ok := boolToInt(sm.OK)
		switch {
		case sm.At < firstAt:
			// New head segment [sm.At, firstAt) carries the new state.
			okTail += ok * (firstAt - sm.At)
			firstAt = sm.At
		case sm.At > lastAt:
			// Segment [lastAt, end) previously carried lastOk; it is now
			// split at sm.At into [lastAt, sm.At) with lastOk (unchanged
			// total contribution) and [sm.At, end) with the new state.
			okTail += (end - sm.At) * (ok - lastOk)
			lastAt = sm.At
			lastOk = ok
		default:
			// Late sample strictly inside the bucket: it splits the
			// segment of its predecessor. Find predecessor and successor
			// within this bucket.
			var predAt, predOk, succAt int64
			err := tx.QueryRow(
				`SELECT at, ok FROM samples
				 WHERE monitor_id = ? AND at < ? AND at >= ?
				 ORDER BY at DESC LIMIT 1`, sm.MonitorID, sm.At, b).Scan(&predAt, &predOk)
			if err != nil {
				return fmt.Errorf("avail: find predecessor: %w", err)
			}
			err = tx.QueryRow(
				`SELECT at FROM samples
				 WHERE monitor_id = ? AND at > ? AND at < ?
				 ORDER BY at ASC LIMIT 1`, sm.MonitorID, sm.At, end).Scan(&succAt)
			if err != nil {
				return fmt.Errorf("avail: find successor: %w", err)
			}
			// Segment [predAt, succAt) carried predOk; now [predAt, sm.At)
			// keeps predOk and [sm.At, succAt) carries the new state.
			okTail += (succAt - sm.At) * (ok - predOk)
		}
	}

	nSamples++
	if sm.OK {
		okCount++
		okSum += sm.LatencyMs
	}
	hist, err := decodeHistogram(histBlob)
	if err != nil {
		return err
	}
	if sm.OK {
		hist[binOf(sm.LatencyMs)]++
	}

	_, err = tx.Exec(
		`INSERT INTO buckets
		 (monitor_id, bucket, n, first_at, last_at, last_ok, ok_tail, ok_count, ok_latency_sum, hist)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (monitor_id, bucket) DO UPDATE SET
		 n = excluded.n, first_at = excluded.first_at, last_at = excluded.last_at,
		 last_ok = excluded.last_ok, ok_tail = excluded.ok_tail,
		 ok_count = excluded.ok_count, ok_latency_sum = excluded.ok_latency_sum,
		 hist = excluded.hist`,
		sm.MonitorID, b, nSamples, firstAt, lastAt, lastOk, okTail, okCount, okSum,
		encodeHistogram(hist))
	if err != nil {
		return fmt.Errorf("avail: upsert bucket: %w", err)
	}
	return tx.Commit()
}
