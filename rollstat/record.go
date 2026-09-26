package rollstat

import (
	"context"
	"database/sql"
)

// Record writes one sample. Duplicate (MonitorID, At) deliveries are
// ignored (content is guaranteed identical). Late samples take effect on
// the very next Stats call: Record incrementally maintains per-bucket
// aggregates, touching only the buckets overlapped by the single state
// interval the new sample splits, so its cost is bounded by the sampling
// gap around the sample, never by the monitor's total history.
func (s *Store) Record(sm Sample) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists int
	err = tx.QueryRowContext(ctx,
		`SELECT 1 FROM samples WHERE monitor_id = ? AND at = ?`,
		sm.MonitorID, sm.At).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil {
		return tx.Commit() // duplicate delivery: already recorded
	}

	// Predecessor and successor define the state interval being split.
	var prevAt, nextAt int64
	var prevOk bool
	hasPrev, hasNext := false, false
	err = tx.QueryRowContext(ctx,
		`SELECT at, ok FROM samples WHERE monitor_id = ? AND at < ?
		 ORDER BY at DESC LIMIT 1`, sm.MonitorID, sm.At).Scan(&prevAt, &prevOk)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	hasPrev = err == nil
	err = tx.QueryRowContext(ctx,
		`SELECT at FROM samples WHERE monitor_id = ? AND at > ?
		 ORDER BY at ASC LIMIT 1`, sm.MonitorID, sm.At).Scan(&nextAt)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	hasNext = err == nil

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO samples (monitor_id, at, ok, latency_ms) VALUES (?, ?, ?, ?)`,
		sm.MonitorID, sm.At, sm.Ok, sm.LatencyMs); err != nil {
		return err
	}

	// Latency aggregates: only successful samples participate.
	if sm.Ok {
		b := sm.At / bucketMs
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO lat (monitor_id, bucket, cnt, sum) VALUES (?, ?, 1, ?)
			 ON CONFLICT (monitor_id, bucket)
			 DO UPDATE SET cnt = cnt + 1, sum = sum + excluded.sum`,
			sm.MonitorID, b, sm.LatencyMs); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO hist (monitor_id, bucket, lat_bucket, cnt) VALUES (?, ?, ?, 1)
			 ON CONFLICT (monitor_id, bucket, lat_bucket)
			 DO UPDATE SET cnt = cnt + 1`,
			sm.MonitorID, b, latBucket(sm.LatencyMs)); err != nil {
			return err
		}
	}

	// Availability aggregates. Buckets store up-milliseconds contributed by
	// closed state intervals [t_i, t_{i+1}) only; the trailing open interval
	// of the newest sample is resolved at query time.
	switch {
	case hasPrev && hasNext:
		if prevOk != sm.Ok {
			// [sm.At, nextAt) flips from prevOk to sm.Ok.
			sign := int64(1)
			if !sm.Ok {
				sign = -1
			}
			if err := addAvailRange(ctx, tx, sm.MonitorID, sm.At, nextAt, sign); err != nil {
				return err
			}
		}
	case !hasPrev && hasNext:
		// New first sample: [sm.At, nextAt) gains state sm.Ok.
		if sm.Ok {
			if err := addAvailRange(ctx, tx, sm.MonitorID, sm.At, nextAt, 1); err != nil {
				return err
			}
		}
	case hasPrev && !hasNext:
		// Newest sample: prev's interval was open (unstored); it is now the
		// closed interval [prevAt, sm.At) and must be materialized.
		if prevOk {
			if err := addAvailRange(ctx, tx, sm.MonitorID, prevAt, sm.At, 1); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// addAvailRange adds sign*overlap milliseconds to every bucket overlapped
// by the half-open interval [lo, hi).
func addAvailRange(ctx context.Context, tx *sql.Tx, monitorID string, lo, hi int64, sign int64) error {
	if hi <= lo {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO avail (monitor_id, bucket, up_ms) VALUES (?, ?, ?)
		 ON CONFLICT (monitor_id, bucket)
		 DO UPDATE SET up_ms = up_ms + excluded.up_ms`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for b := lo / bucketMs; b*bucketMs < hi; b++ {
		overlap := min(hi, (b+1)*bucketMs) - max(lo, b*bucketMs)
		if overlap <= 0 {
			continue
		}
		if _, err := stmt.ExecContext(ctx, monitorID, b, sign*overlap); err != nil {
			return err
		}
	}
	return nil
}
