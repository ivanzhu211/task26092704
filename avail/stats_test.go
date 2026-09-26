package avail

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// bruteForce is the reference implementation: it keeps every unique raw
// sample and answers queries by scanning them directly.
type bruteForce struct {
	byMonitor map[string]map[int64]Sample
}

func newBruteForce() *bruteForce {
	return &bruteForce{byMonitor: make(map[string]map[int64]Sample)}
}

func (b *bruteForce) record(sm Sample) {
	m := b.byMonitor[sm.MonitorID]
	if m == nil {
		m = make(map[int64]Sample)
		b.byMonitor[sm.MonitorID] = m
	}
	m[sm.At] = sm // identical redeliveries collapse
}

func (b *bruteForce) sorted(monitorID string) []Sample {
	m := b.byMonitor[monitorID]
	out := make([]Sample, 0, len(m))
	for _, sm := range m {
		out = append(out, sm)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At < out[j].At })
	return out
}

func (b *bruteForce) stats(monitorID string, window Window, now int64) (avail, avg, p95 *float64) {
	w, _ := windowMillis(window)
	L, R := now-w, now
	samples := b.sorted(monitorID)

	// Availability: walk segments of constant state.
	var stateful, okDur int64
	haveState := false
	state := false
	prev := L
	i := sort.Search(len(samples), func(i int) bool { return samples[i].At >= L })
	if i > 0 {
		haveState = true
		state = samples[i-1].OK
	}
	flush := func(to int64) {
		if haveState && to > prev {
			stateful += to - prev
			if state {
				okDur += to - prev
			}
		}
		prev = to
	}
	for ; i < len(samples) && samples[i].At < R; i++ {
		flush(samples[i].At)
		state = samples[i].OK
		haveState = true
	}
	flush(R)
	if stateful > 0 {
		v := float64(okDur) / float64(stateful)
		avail = &v
	}

	// Latency metrics over ok samples inside [L, R).
	var lats []int64
	var sum int64
	for _, sm := range samples {
		if sm.At >= L && sm.At < R && sm.OK {
			lats = append(lats, sm.LatencyMs)
			sum += sm.LatencyMs
		}
	}
	if len(lats) > 0 {
		v := float64(sum) / float64(len(lats))
		avg = &v
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		rank := (95*len(lats) + 99) / 100
		q := float64(lats[rank-1])
		p95 = &q
	}
	return avail, avg, p95
}

const dayMs = 24 * 60 * 60 * 1000

// scenario builds the required sample mix and returns unique samples plus
// a designated late sample (to be recorded only after all others).
func scenario(rng *rand.Rand, base int64) (samples []Sample, late Sample) {
	add := func(id string, at int64, ok bool, lat int64) {
		samples = append(samples, Sample{MonitorID: id, At: at, OK: ok, LatencyMs: lat})
	}

	// "dense": 30s interval for 12 hours (worst case for the raw-row bound).
	t := base
	for i := 0; i < 1440; i++ {
		ok := rng.Float64() > 0.05
		add("dense", t, ok, int64(rng.Intn(300)))
		t += 30_000
	}

	// "normal": 40 days, interval 30s..60min, failure streaks trigger
	// backoff gaps of 1..8 hours with sparse sampling.
	t = base
	failStreak := 0
	for t < base+40*dayMs {
		ok := rng.Float64() > 0.2
		lat := int64(50 + rng.Intn(250))
		if ok {
			failStreak = 0
		} else {
			failStreak++
			lat = 0
		}
		add("normal", t, ok, lat)
		gap := int64(30_000 + rng.Intn(3_600_000-30_000))
		if failStreak > 0 {
			gap = int64(3_600_000 + rng.Int63n(7*3_600_000)) // 1..8h backoff gap
		}
		t += gap
	}

	// "bimodal-a": p95 sits exactly on the low cluster (95x100ms, 5x10000ms).
	spread := int64(20 * dayMs / 100)
	for i := 0; i < 100; i++ {
		lat := int64(100)
		if i >= 95 {
			lat = 10_000
		}
		add("bimodal-a", base+int64(i)*spread, true, lat)
	}
	// "bimodal-b": p95 jumps to the high cluster (94x100ms, 6x10000ms).
	for i := 0; i < 100; i++ {
		lat := int64(100)
		if i >= 94 {
			lat = 10_000
		}
		add("bimodal-b", base+int64(i)*spread, true, lat)
	}

	// "sparse": long multi-hour/day gaps, extreme latencies 0 and 60000.
	t = base
	for i := 0; i < 40; i++ {
		ok := i%3 != 0
		lat := int64(0)
		if i%2 == 0 {
			lat = 60_000
		}
		add("sparse", t, ok, lat)
		t += int64(3_600_000 + rng.Int63n(2*dayMs))
	}

	// "late": two samples more than a day apart, plus a sample that
	// arrives late and lands between them.
	add("late", base, true, 120)
	add("late", base+2*dayMs+5_000_000, false, 0)
	late = Sample{MonitorID: "late", At: base + dayMs + 2_500_000, OK: true, LatencyMs: 240}
	return samples, late
}

func runScenario(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	path := filepath.Join(t.TempDir(), "stats.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000) + rng.Int63n(10_000_000) // unaligned

	samples, late := scenario(rng, base)

	// Out-of-order delivery with duplicates.
	deliveries := append([]Sample(nil), samples...)
	for _, sm := range samples {
		if rng.Float64() < 0.3 {
			deliveries = append(deliveries, sm)
		}
	}
	rng.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })
	for _, sm := range deliveries {
		if err := store.Record(sm); err != nil {
			t.Fatal(err)
		}
	}
	// The late sample lands between two samples recorded long before.
	if err := store.Record(late); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(late); err != nil { // duplicate of the late sample
		t.Fatal(err)
	}

	brute := newBruteForce()
	for _, sm := range samples {
		brute.record(sm)
	}
	brute.record(late)

	monitors := []string{"dense", "normal", "bimodal-a", "bimodal-b", "sparse", "late", "unknown"}
	windows := []Window{Window24h, Window7d, Window30d}
	var nows []int64
	for i := 0; i < 8; i++ {
		nows = append(nows, base+rng.Int63n(45*dayMs)) // unaligned now
	}
	nows = append(nows, base+20*dayMs+777_777, base+60*dayMs+3_333)

	check := func(store *Store, tag string) {
		for _, id := range monitors {
			for _, w := range windows {
				for _, now := range nows {
					got, err := store.Stats(id, w, now)
					if err != nil {
						t.Fatalf("%s Stats(%s,%v,%d): %v", tag, id, time.Duration(w), now, err)
					}
					if got.RawRowsRead > MaxRawRowsPerStats {
						t.Errorf("%s %s/%v/%d: RawRowsRead=%d exceeds bound %d",
							tag, id, time.Duration(w), now, got.RawRowsRead, MaxRawRowsPerStats)
					}
					wantAvail, wantAvg, wantP95 := brute.stats(id, w, now)
					compareFloat(t, tag, id, w, now, "availability", got.Availability, wantAvail, 1e-12)
					compareFloat(t, tag, id, w, now, "avgLatency", got.AvgLatencyMs, wantAvg, 1e-9)
					if wantP95 == nil {
						if got.P95LatencyMs != nil {
							t.Errorf("%s %s/%v/%d p95: got %v, want nil", tag, id, time.Duration(w), now, *got.P95LatencyMs)
						}
					} else if got.P95LatencyMs == nil {
						t.Errorf("%s %s/%v/%d p95: got nil, want %v", tag, id, time.Duration(w), now, *wantP95)
					} else if q := *wantP95; math.Abs(*got.P95LatencyMs-q) > 0.01*q+1e-9 {
						t.Errorf("%s %s/%v/%d p95: got %v, want within 1%% of %v",
							tag, id, time.Duration(w), now, *got.P95LatencyMs, q)
					}
				}
			}
		}
	}

	check(store, "fresh")
	// Reopen: results must be identical after a restart.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	check(store2, "reopened")
}

func compareFloat(t *testing.T, tag, id string, w Window, now int64, name string, got, want *float64, tol float64) {
	t.Helper()
	switch {
	case want == nil && got == nil:
	case want == nil || got == nil:
		t.Errorf("%s %s/%v/%d %s: got %v, want %v", tag, id, time.Duration(w), now, name, ptrStr(got), ptrStr(want))
	case math.Abs(*got-*want) > tol:
		t.Errorf("%s %s/%v/%d %s: got %v, want %v", tag, id, time.Duration(w), now, name, *got, *want)
	}
}

func ptrStr(p *float64) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *p)
}

func TestRandomizedAgainstBruteForce(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runScenario(t, seed)
		})
	}
}

func TestHistogramBins(t *testing.T) {
	for _, x := range []int64{0, 1, 2, 99, 100, 101, 999, 1000, 30000, 59999, 60000} {
		b := binOf(x)
		if binLo[b] > x {
			t.Errorf("binOf(%d)=%d: lower bound %d exceeds value", x, b, binLo[b])
		}
		if int(b)+1 < len(binLo) && binLo[b+1] <= x {
			t.Errorf("binOf(%d)=%d: next lower bound %d <= value", x, b, binLo[b+1])
		}
		if x > 0 && float64(x-binLo[b]) > 0.01*float64(binLo[b])+1e-9 {
			t.Errorf("bin %d for %d wider than 1%%: [%d, ...)", b, x, binLo[b])
		}
	}
	// Round-trip encoding.
	h := histogram{0: 3, 5: 1, 700: 42}
	got, err := decodeHistogram(encodeHistogram(h))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(h) {
		t.Fatalf("round-trip: got %v, want %v", got, h)
	}
	for b, c := range h {
		if got[b] != c {
			t.Errorf("round-trip bin %d: got %d, want %d", b, got[b], c)
		}
	}
}
