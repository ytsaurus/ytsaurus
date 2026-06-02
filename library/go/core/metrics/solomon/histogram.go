package solomon

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"go.uber.org/atomic"

	"go.ytsaurus.tech/library/go/core/metrics"
	"go.ytsaurus.tech/library/go/core/xerrors"
)

var (
	_ metrics.Histogram = (*Histogram)(nil)
	_ metrics.Timer     = (*Histogram)(nil)
	_ Metric            = (*Histogram)(nil)
)

type Histogram struct {
	baseMetric

	bucketBounds []float64
	bucketValues []int64
	infValue     atomic.Int64
	mutex        sync.Mutex
}

func NewHistogram(name string, bucketBounds []float64, bucketValues []int64, infValue int64, opts ...MetricOpt) Histogram {
	mOpts := MetricsOpts{}
	for _, op := range opts {
		op(&mOpts)
	}
	mType := typeHistogram
	if mOpts.rated {
		mType = typeRatedHistogram
	}
	return Histogram{
		baseMetric:   newBaseMetric(name, mType, opts...),
		bucketBounds: bucketBounds,
		bucketValues: bucketValues,
		infValue:     *atomic.NewInt64(infValue),
	}
}

type histogram struct {
	Bounds  []float64 `json:"bounds"`
	Buckets []int64   `json:"buckets"`
	Inf     int64     `json:"inf,omitempty"`
}

func (h *histogram) writeHistogram(w io.Writer) error {
	// add 1 to buckets length for inf bucket
	err := writeULEB128(w, uint32(len(h.Buckets)+1))
	if err != nil {
		return xerrors.Errorf("writeULEB128 size histogram buckets failed: %w", err)
	}

	for _, upperBound := range h.Bounds {
		err = binary.Write(w, binary.LittleEndian, float64(upperBound))
		if err != nil {
			return xerrors.Errorf("binary.Write upper bound failed: %w", err)
		}
	}
	// write inf bound
	err = binary.Write(w, binary.LittleEndian, math.MaxFloat64)
	if err != nil {
		return xerrors.Errorf("binary.Write inf bound failed: %w", err)
	}

	for _, bucketValue := range h.Buckets {
		err = binary.Write(w, binary.LittleEndian, uint64(bucketValue))
		if err != nil {
			return xerrors.Errorf("binary.Write histogram buckets failed: %w", err)
		}
	}
	// write inf bucket value
	err = binary.Write(w, binary.LittleEndian, uint64(h.Inf))
	if err != nil {
		return xerrors.Errorf("binary.Write inf bucket failed: %w", err)
	}

	return nil
}

func (h *Histogram) RecordValue(value float64) {
	boundIndex := sort.SearchFloat64s(h.bucketBounds, value)

	if boundIndex < len(h.bucketValues) {
		h.mutex.Lock()
		h.bucketValues[boundIndex] += 1
		h.mutex.Unlock()
	} else {
		h.infValue.Inc()
	}
}

func (h *Histogram) RecordDuration(value time.Duration) {
	h.RecordValue(value.Seconds())
}

func (h *Histogram) Reset() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.bucketValues = make([]int64, len(h.bucketValues))
	h.infValue.Store(0)
}

func (h *Histogram) Value() any {
	return histogram{
		Bounds:  h.bucketBounds,
		Buckets: h.bucketValues,
		Inf:     h.infValue.Load(),
	}
}

// MarshalJSON implements json.Marshaler.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	valuesCopy := make([]int64, len(h.bucketValues))
	h.mutex.Lock()
	copy(valuesCopy, h.bucketValues)
	h.mutex.Unlock()
	return json.Marshal(struct {
		Type      string            `json:"type"`
		Labels    map[string]string `json:"labels"`
		Histogram histogram         `json:"hist"`
		Timestamp *int64            `json:"ts,omitempty"`
		MemOnly   bool              `json:"memOnly,omitempty"`
	}{
		Type: h.metricType.String(),
		Histogram: histogram{
			Bounds:  h.bucketBounds,
			Buckets: valuesCopy,
			Inf:     h.infValue.Load(),
		},
		Labels:    h.labelsWithName(),
		Timestamp: tsAsRef(h.timestamp),
		MemOnly:   h.memOnly,
	})
}

// Snapshot returns independent copy on metric.
func (h *Histogram) Snapshot() Metric {
	h.mutex.Lock()
	bucketValues := slices.Clone(h.bucketValues)
	h.mutex.Unlock()

	return &Histogram{
		baseMetric:   h.copy(),
		bucketBounds: slices.Clone(h.bucketBounds),
		bucketValues: bucketValues,
		infValue:     *atomic.NewInt64(h.infValue.Load()),
	}
}

// InitBucketValues cleans internal bucketValues and saves new values in order.
// Length of internal bucketValues stays unchanged.
// If length of slice in argument bucketValues more than length of internal one,
// the first extra element of bucketValues is stored in infValue.
func (h *Histogram) InitBucketValues(bucketValues []int64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.bucketValues = make([]int64, len(h.bucketValues))
	h.infValue.Store(0)
	copy(h.bucketValues, bucketValues)
	if len(bucketValues) > len(h.bucketValues) {
		h.infValue.Store(bucketValues[len(h.bucketValues)])
	}
}
