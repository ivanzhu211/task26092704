# rollstat

Rolling-window availability statistics for an intranet monitoring
platform, in Go on top of SQLite (pure-Go driver `modernc.org/sqlite`,
no cgo, no services).

## API

- `Open(path) / Close()` — all state lives in one SQLite file; reopening
  never changes query results.
- `Record(Sample{MonitorID, At, Ok, LatencyMs})` — ingests one sample.
  Duplicate `(MonitorID, At)` deliveries are ignored. Out-of-order and
  arbitrarily late samples take effect on the very next `Stats` call.
- `Stats(monitorID, window, now)` — `window` is `Window24h`, `Window7d`
  or `Window30d`; the interval is exactly `[now-window, now)` with no
  rounding to any granularity. Returns `Metrics`; a nil pointer means
  "no data" (never 0). `Metrics.RawRowsRead` reports how many raw sample
  rows the call read.

## Design

Per monitor, Record incrementally maintains hourly aggregate buckets
(`bucketMs = 1h`):

- `avail(bucket) -> up_ms`: milliseconds of up-state inside the bucket,
  summed over *closed* state intervals `[t_i, t_{i+1})`. Inserting a late
  sample only rewrites the buckets overlapped by the single interval it
  splits, so Record cost is bounded by the local sampling gap, never by
  total history. The newest sample's open (unterminated) interval is not
  stored; Stats resolves it with one point lookup.
- `lat(bucket) -> (cnt, sum)`: exact latency sum/count of ok samples.
- `hist(bucket, lat_bucket) -> cnt`: latency histogram of ok samples with
  geometric buckets of ratio 1.005, so the bucket lower bound is always
  within 0.5% of any value in it — the p95 estimate satisfies
  `|v - q| <= 1% * q` for *every* distribution, including bimodal ones
  where p95 sits exactly on a jump. It is computed from per-value counts,
  never by combining per-segment p95s.

Stats reads raw sample rows only for the (at most two) buckets straddling
the window boundaries, plus a constant number of point lookups
(predecessor / first / last). Everything between comes from the aggregate
tables. With the documented probe cadence (>= 30s between samples) a
bucket holds at most 120 samples, giving the declared bound
`MaxRawRowsPerStats = 248`, independent of window length and sample count.

- Availability is time-weighted: state at `t` is the last sample at
  `<= t`; time before the first sample has no state. Backoff gaps are
  weighted by duration, not by sample count.
- Average latency is the exact mean of ok samples in the window.
- p95 is nearest-rank over ok samples in the window, approximated via the
  merged exact histogram counts.

## Test

`go test ./...` runs a randomized differential test against a brute-force
reference that scans raw samples directly. Generated data includes
multi-hour backoff gaps, shuffled and duplicated deliveries, a sample
delivered late into a >1-day hole, a bimodal latency distribution with
p95 exactly on the jump, and unaligned `now` values; all three windows
are checked before and after close/reopen.
