package avail

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// histogram is a sparse count map over latency bins. Bin k (k >= 1) covers
// [binLo[k], binLo[k+1]) and bin 0 covers exactly {0}. Bin boundaries are
// integers with binLo[k+1]-1 <= 1.01*binLo[k], so returning the lower bound
// of the bin containing the true nearest-rank quantile q yields a value v
// with |v-q| <= 1%*q for every possible distribution (including bimodal
// ones), because the relative width of every bin is at most 1%.
type histogram map[uint32]uint64

// binLo[i] is the inclusive lower bound of bin i. Computed with integer
// recurrence so bin widths never exceed 1% of their lower bound.
var binLo []int64

func init() {
	binLo = append(binLo, 0) // bin 0: latency exactly 0
	for lo := int64(1); lo <= 60000; {
		binLo = append(binLo, lo)
		next := int64(float64(lo)*1.01) + 1
		if next <= lo {
			next = lo + 1
		}
		lo = next
	}
}

// binOf maps a latency in [0, 60000] to its bin index.
func binOf(latencyMs int64) uint32 {
	// Largest i with binLo[i] <= latencyMs.
	lo, hi := 0, len(binLo)
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if binLo[mid] <= latencyMs {
			lo = mid
		} else {
			hi = mid
		}
	}
	return uint32(lo)
}

// binLowerBound returns the lower bound (inclusive) of a bin.
func binLowerBound(b uint32) float64 {
	return float64(binLo[b])
}

// encodeHistogram serializes h sorted by bin index as
// uvarint(bin-delta) uvarint(count) pairs.
func encodeHistogram(h histogram) []byte {
	bins := make([]uint32, 0, len(h))
	for b := range h {
		bins = append(bins, b)
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i] < bins[j] })
	out := make([]byte, 0, len(bins)*4)
	var buf [binary.MaxVarintLen64]byte
	prev := uint32(0)
	for i, b := range bins {
		delta := b
		if i > 0 {
			delta = b - prev
		}
		n := binary.PutUvarint(buf[:], uint64(delta))
		out = append(out, buf[:n]...)
		n = binary.PutUvarint(buf[:], h[b])
		out = append(out, buf[:n]...)
		prev = b
	}
	return out
}

// decodeHistogram parses a blob produced by encodeHistogram.
func decodeHistogram(blob []byte) (histogram, error) {
	h := make(histogram)
	prev := uint32(0)
	first := true
	for len(blob) > 0 {
		delta, n := binary.Uvarint(blob)
		if n <= 0 {
			return nil, fmt.Errorf("avail: corrupt histogram blob")
		}
		blob = blob[n:]
		count, n := binary.Uvarint(blob)
		if n <= 0 {
			return nil, fmt.Errorf("avail: corrupt histogram blob")
		}
		blob = blob[n:]
		b := uint32(delta)
		if !first {
			b = prev + uint32(delta)
		}
		first = false
		if int(b) >= len(binLo) {
			return nil, fmt.Errorf("avail: histogram bin %d out of range", b)
		}
		h[b] += count
		prev = b
	}
	return h, nil
}
