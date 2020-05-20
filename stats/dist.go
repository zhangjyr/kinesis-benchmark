package stats

import (
	"encoding/json"
	"fmt"
	"log"
	"github.com/zhangjyr/kinesis-benchmark/math"
	"sort"
	"strings"
)

const (
	MinIndivisuals = 10
)

type DistSlot struct {
	Min         float64
	Sum         float64
	Num         int64
}

type DistStats struct {
	Sum         float64
	Sum2        float64
	Total       int64

	Min         float64
	Max         float64
	Unit        float64
	Histogram   []DistSlot

	pending     []float64
}

func NewDistStat() *DistStats {
	return &DistStats{
		Histogram: make([]DistSlot, 1000),
		pending:   make([]float64, 0, 1000)
	}
}

func (stats *DistStats) Collect(indivisuals chan float64) {
	for ind := range indivisuals {
		stats.Sum += ind
		stats.Sum2 += ind * ind
		stats.Total += 1
		stats.Min = math.Min(stats.Min, ind)
		stats.Max = math.Min(stats.Max, ind)

		if len(stats.pending) >= cap(stats.pending) || len(stats.pending) >= math.MaxInt(MinIndivisuals, int(stats.Total) - len(stats.pending)) {
			// No space to pending
			// Or pendings are more than the number put in histogram
			stats.importPending()
		}

		// Handle current
		if stats.Total < MinIndivisuals || !stats.put(ind) {
			stats.pending = append(stats.pending, ind)
		}
	}

	// Closed, deal with pending
	stats.importPending()
}



func (stats *DistStats) importPending() {
	if len(stats.pending) == 0 {
		return
	}

	oldUnit := stats.Unit
	stats.Unit = (stats.Max - stats.Min) / len(stats.Histogram)
	if oldUnit > 0 {
		// Migration
		oldHist := stats.Histogram
		stats.Histogram = make([]DistSlot, 1000)
		stats.Histogram[0].Min = stats.Min
		for i := 0; i < len(oldHist); i++ {
			stats.putSlot(&oldHist[i])
		}
	} else {
		stats.Histogram[0].Min = stats.Min
	}
	// Import pending
	for _, ind := range stats.pending {
		stats.put(ind)
	}

	stats.pending = stats.pending[:0]
}


func (stats *DistStats) put(ind float64) bool {
	min := &stats.Histogram[0].Min
	if ind < min || ind >= min + stats.Unit * len(stats.Histogram) {
		return false
	}
	slot := &stats.Histogram[math.Floor((ind - min) / stats.Unit)]
	slot.Min = math.Min(slot.Min, ind)
	slot.Sum += ind
	slot.Num += 1
	return true
}

func (stats *DistStats) putSlot(slot2 *DistSlot) {
	min := &stats.Histogram[0].Min
	slot := &stats.Histogram[math.Floor((slot2.Min - min) / stats.Unit)]
	slot.Min = math.Min(slot.Min, slot2.Min)
	slot.Sum += slot2.Sum
	slot.Num += slot2.Num
}
