package rollstat

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// ---------- brute-force reference ----------

type refStore struct {
	byMonitor map[string]map[int64]Sample
}

func newRefStore() *refStore {
	return &refStore{byMonitor: map[string]map[int64]Sample{}}
}

func (r *refStore) record(s Sample) {
	m := r.byMonitor[s.MonitorID]
	if m == nil {
		m = map[int64]Sample{}
		r.byMonitor[s.MonitorID] = m
	}
	m[s.At] = s // identical content on duplicates: overwrite is fine
}

type refMetrics struct {
	avail *float64
	avg   *float64
	p95   *float64
}

func (r *refStore) stats(monitorID string, window Window, now int64) refMetrics {
	d := time.Duration(window)
	a := now - d.Milliseconds()
	b := now
	set := r.byMonitor[monitorID]
	samples := make([]Sample, 0, len(set))
	for _, s := range set {
		samples = append(samples, s)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].At < samples[j].At })

	var out refMetrics

	// Time-weighted availability over [a, b).
	var up, state int64
	for i, s := range samples {
		if s.At >= b {
			break
		}
		end := b
		if i+1 < len(samples) && samples[i+1].At < b {
			end = samples[i+1].At
		}
		lo := s.At
		if lo < a {
			lo = a
		}
		if end > lo {
			state += end - lo
			if s.Ok {
				up += end - lo
			}
		}
	}
	if state > 0 {
		v := float64(up) / float64(state)
		out.avail = &v
	}

	// Latency metrics over ok samples with at in [a, b).
	var lats []int64
	var sum int64
	for _, s := range samples {
		if s.At >= a && s.At < b && s.Ok {
			lats = append(lats, s.LatencyMs)
			sum += s.LatencyMs
		}
	}
	if len(lats) > 0 {
		v := float64(sum) / float64(len(lats))
		out.avg = &v
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		rank := (95*len(lats) + 99) / 100 // ceil(0.95*n)
		q := float64(lats[rank-1])
		out.p95 = &q
	}
	return out
}

// ---------- test data generation ----------

const testBase = int64(1_700_000_000_000) + 12345 // unaligned ms offset

func clampLat(v float64) int64 {
	if v < 0 {
		return 0
	}
	if v > 60000 {
		return 60000
	}
	return int64(v)
}

// genMonitors builds the scenario sample sets. Every monitor id maps to a
// list of samples in chronological order (delivery order is decided later).
func genMonitors(rng *rand.Rand) map[string][]Sample {
	out := map[string][]Sample{}

	// M1: dense 30s probing for 3 days, with failure bursts that trigger
	// circuit-breaker backoff leaving multi-hour sampling gaps.
	{
		var samples []Sample
		t := testBase
		end := testBase + 3*24*3600*1000
		burst := 0 // remaining failures in the current burst
		consec := 0
		for t < end {
			ok := true
			if burst > 0 {
				ok = false
				burst--
			} else if rng.Float64() < 0.02 {
				ok = false
				burst = rng.Intn(10) + 5 // burst of 5-14 consecutive failures
			}
			lat := clampLat(50 + rng.ExpFloat64()*200)
			gap := int64(30_000)
			if !ok {
				lat = 60000
				consec++
				// circuit-breaker backoff: 30s, 1m, 2m, ... capped at 4h
				gap = min(int64(30_000)<<uint(min(consec, 9)), int64(4*3600*1000))
			} else {
				consec = 0
			}
			samples = append(samples, Sample{MonitorID: "m1", At: t, Ok: ok, LatencyMs: lat})
			t += gap
		}
		out["m1"] = samples
	}

	// M2: 31 days at 5-15min cadence, bimodal latency (94% low, 6% high)
	// so that nearest-rank p95 sits exactly on the jump between clusters.
	{
		var samples []Sample
		t := testBase
		end := testBase + 31*24*3600*1000
		for t < end {
			ok := rng.Float64() > 0.03
			var lat int64
			if rng.Float64() < 0.94 {
				lat = clampLat(100 + rng.NormFloat64()*20)
			} else {
				lat = clampLat(50000 + rng.NormFloat64()*3000)
			}
			if !ok {
				lat = 60000
			}
			samples = append(samples, Sample{MonitorID: "m2", At: t, Ok: ok, LatencyMs: lat})
			gap := int64(5+rng.Intn(10)) * 60_000
			if !ok && rng.Float64() < 0.5 {
				gap = int64(1+rng.Intn(7)) * 3600_000 // backoff gap, hours
			}
			t += gap
		}
		out["m2"] = samples
	}

	// M3: hourly probing for 10 days with a 2-day hole; the hole's middle
	// sample is delivered late (appended by the caller after shuffling).
	{
		var samples []Sample
		t := testBase
		end := testBase + 10*24*3600*1000
		for t < end {
			if t > testBase+3*24*3600*1000 && t < testBase+5*24*3600*1000 && t != testBase+4*24*3600*1000 {
				t += 3600_000
				continue
			}
			ok := t < testBase+4*24*3600*1000 // state flips inside the hole
			samples = append(samples, Sample{MonitorID: "m3", At: t, Ok: ok, LatencyMs: 100 + t%500})
			t += 3600_000
		}
		out["m3"] = samples
	}

	// M4: a single sample only.
	out["m4"] = []Sample{{MonitorID: "m4", At: testBase + 987_654, Ok: true, LatencyMs: 42}}

	// M5: exactly 95% low / 5% high within one hour: p95 rank is the last
	// element of the low cluster (jump point right after it).
	{
		var samples []Sample
		for i := 0; i < 100; i++ {
			lat := int64(200)
			if i >= 95 {
				lat = 40000
			}
			samples = append(samples, Sample{
				MonitorID: "m5",
				At:        testBase + int64(i)*30_000,
				Ok:        true,
				LatencyMs: lat,
			})
		}
		out["m5"] = samples
	}

	return out
}

// lateM3 returns the sample that lands between two existing m3 samples
// more than a day apart and must be delivered last.
func lateM3() Sample {
	return Sample{MonitorID: "m3", At: testBase + 4*24*3600*1000, Ok: false, LatencyMs: 30000}
}

// ---------- helpers ----------

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func checkMetrics(t *testing.T, label string, got *Metrics, ref refMetrics) {
	t.Helper()
	if got.RawRowsRead > MaxRawRowsPerStats {
		t.Errorf("%s: RawRowsRead=%d exceeds declared bound %d",
			label, got.RawRowsRead, MaxRawRowsPerStats)
	}
	if ref.avail == nil {
		if got.Availability != nil {
			t.Errorf("%s: availability: want nil, got %v", label, *got.Availability)
		}
	} else {
		if got.Availability == nil {
			t.Errorf("%s: availability: want %v, got nil", label, *ref.avail)
		} else if !almostEqual(*got.Availability, *ref.avail) {
			t.Errorf("%s: availability: want %v, got %v", label, *ref.avail, *got.Availability)
		}
	}
	if ref.avg == nil {
		if got.AvgLatencyMs != nil {
			t.Errorf("%s: avg: want nil, got %v", label, *got.AvgLatencyMs)
		}
	} else {
		if got.AvgLatencyMs == nil {
			t.Errorf("%s: avg: want %v, got nil", label, *ref.avg)
		} else if !almostEqual(*got.AvgLatencyMs, *ref.avg) {
			t.Errorf("%s: avg: want %v, got %v", label, *ref.avg, *got.AvgLatencyMs)
		}
	}
	if ref.p95 == nil {
		if got.P95LatencyMs != nil {
			t.Errorf("%s: p95: want nil, got %v", label, *got.P95LatencyMs)
		}
	} else {
		if got.P95LatencyMs == nil {
			t.Errorf("%s: p95: want %v, got nil", label, *ref.p95)
		} else if math.Abs(*got.P95LatencyMs-*ref.p95) > 0.01**ref.p95 {
			t.Errorf("%s: p95: q=%v, got %v (outside 1%% bound)", label, *ref.p95, *got.P95LatencyMs)
		}
	}
}

// ---------- tests ----------

func TestRandomizedAgainstBruteForce(t *testing.T) {
	for _, seed := range []int64{20260927, 1, 42, 777} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			testRandomized(t, seed)
		})
	}
}

func testRandomized(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	monitors := genMonitors(rng)

	// Build the delivery stream: all samples, ~10% duplicated, shuffled;
	// the late m3 sample goes last.
	var deliveries []Sample
	for _, samples := range monitors {
		for _, s := range samples {
			deliveries = append(deliveries, s)
			if rng.Float64() < 0.1 {
				deliveries = append(deliveries, s) // duplicate delivery
			}
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) {
		deliveries[i], deliveries[j] = deliveries[j], deliveries[i]
	})
	deliveries = append(deliveries, lateM3())

	// Guard against vacuous scenarios: the generated data must actually
	// contain the required ingredients.
	var dupes int
	seen := map[Sample]int{}
	for _, s := range deliveries {
		seen[s]++
	}
	for _, n := range seen {
		if n > 1 {
			dupes++
		}
	}
	if dupes == 0 {
		t.Fatal("scenario broken: no duplicate deliveries")
	}
	maxGap := func(id string) int64 {
		ss := monitors[id]
		var g int64
		for i := 1; i < len(ss); i++ {
			if d := ss[i].At - ss[i-1].At; d > g {
				g = d
			}
		}
		return g
	}
	if g := maxGap("m1"); g < 3*3600*1000 {
		t.Fatalf("scenario broken: m1 max backoff gap %v < 3h", g)
	}
	if g := maxGap("m2"); g < 3*3600*1000 {
		t.Fatalf("scenario broken: m2 max backoff gap %v < 3h", g)
	}
	{
		late := lateM3()
		ss := monitors["m3"]
		var prev, next *Sample
		for i := range ss {
			if ss[i].At < late.At {
				prev = &ss[i]
			}
			if ss[i].At > late.At && next == nil {
				next = &ss[i]
			}
		}
		if prev == nil || next == nil || next.At-prev.At <= 24*3600*1000 {
			t.Fatal("scenario broken: late sample not between samples >1 day apart")
		}
	}

	ref := newRefStore()
	path := filepath.Join(t.TempDir(), "stats.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range deliveries {
		if err := store.Record(s); err != nil {
			t.Fatalf("Record(%+v): %v", s, err)
		}
		ref.record(s)
	}

	windows := []Window{Window24h, Window7d, Window30d}
	monitorIDs := []string{"m1", "m2", "m3", "m4", "m5", "m-absent"}

	// Unaligned query times: random ms offsets across and beyond the span.
	var nows []int64
	for i := 0; i < 12; i++ {
		nows = append(nows, testBase-3600_000+rng.Int63n(33*24*3600*1000)+int64(rng.Intn(1000)))
	}
	nows = append(nows,
		testBase,                          // exactly the first sample
		testBase+4*24*3600*1000,           // exactly the late sample
		testBase+4*24*3600*1000+1,         // 1ms after it
		(testBase/bucketMs+10)*bucketMs,   // bucket-aligned now
		(testBase/bucketMs+10)*bucketMs+1, // 1ms past a boundary
		testBase+40*24*3600*1000,          // long after the last sample
	)

	runAll := func(store *Store, phase string) {
		t.Helper()
		for _, id := range monitorIDs {
			for _, w := range windows {
				for _, now := range nows {
					got, err := store.Stats(id, w, now)
					if err != nil {
						t.Fatalf("%s: Stats(%s, %v, %d): %v", phase, id, time.Duration(w), now, err)
					}
					want := ref.stats(id, w, now)
					checkMetrics(t, phase+" "+id+" "+time.Duration(w).String(), got, want)
				}
			}
		}
	}

	runAll(store, "fresh")

	// Persistence: results must be identical after close/reopen.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	runAll(store2, "reopened")
}

func TestLatBucketDomain(t *testing.T) {
	for v := int64(0); v <= 60000; v++ {
		k := latBucket(v)
		lo := bucketLow(k)
		hi := bucketLow(k + 1)
		if !(lo <= float64(v) && float64(v) < hi) {
			t.Fatalf("v=%d: bucket %d = [%v, %v) does not contain v", v, k, lo, hi)
		}
	}
}

func TestInvalidWindow(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "x.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Stats("m1", Window(48*time.Hour), testBase); err == nil {
		t.Fatal("expected error for unsupported window")
	}
}
