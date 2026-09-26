package rollstat

import "math"

// latBucketRatio is the geometric width of latency histogram buckets.
// Any value q in bucket k satisfies low(k) <= q < low(k)*ratio, so
// returning low(k) as the p95 estimate keeps |v-q| <= 0.5%*q, comfortably
// inside the required 1% bound for every possible distribution.
const latBucketRatio = 1.005

var logLatBucketRatio = math.Log(latBucketRatio)

// latBucket maps a latency in ms to a histogram bucket index.
// Bucket 0 holds exactly {0}; bucket k>=1 covers
// [ratio^(k-1), ratio^k). Latencies are in [0, 60000], so at most ~2207
// distinct buckets exist and histograms stay small and sparse.
func latBucket(latencyMs int64) int {
	if latencyMs <= 0 {
		return 0
	}
	return 1 + int(math.Log(float64(latencyMs))/logLatBucketRatio)
}

// bucketLow returns the lower bound of a latency histogram bucket.
func bucketLow(bucket int) float64 {
	if bucket <= 0 {
		return 0
	}
	return math.Pow(latBucketRatio, float64(bucket-1))
}
