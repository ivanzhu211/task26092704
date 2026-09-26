package rollstat

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// BenchmarkStats30d fills several monitors with 30 days of 30s-cadence
// samples (the per-monitor scale of the target deployment) and measures
// Stats over the full 30d window.
func BenchmarkStats30d(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	store, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	monitors := 3
	now := testBase + 31*24*3600*1000
	for m := 0; m < monitors; m++ {
		id := fmt.Sprintf("mon-%d", m)
		for t := testBase; t < now; t += 30_000 {
			ok := rng.Float64() > 0.01
			lat := clampLat(100 + rng.ExpFloat64()*500)
			if !ok {
				lat = 60000
			}
			if err := store.Record(Sample{MonitorID: id, At: t, Ok: ok, LatencyMs: lat}); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := store.Stats("mon-0", Window30d, now-12345)
		if err != nil {
			b.Fatal(err)
		}
		if m.Availability == nil || m.P95LatencyMs == nil {
			b.Fatal("expected data")
		}
		b.ReportMetric(float64(m.RawRowsRead), "rawrows")
	}
}
