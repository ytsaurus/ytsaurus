package solomon

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

type spackVersion uint16

const (
	version11 spackVersion = 0x0101
	version12 spackVersion = 0x0102
)

type spackFlag byte

const (
	memOnlyFlag spackFlag = 0b0000_0001
)

type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) binaryWrite(data interface{}) {
	if ew.err != nil {
		return
	}
	switch t := data.(type) {
	case uint8:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint8))
	case uint16:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint16))
	case uint32:
		ew.err = binary.Write(ew.w, binary.LittleEndian, data.(uint32))
	default:
		ew.err = xerrors.Errorf("binaryWrite not supported type %v", t)
	}
}

func writeULEB128(w io.Writer, value uint32) error {
	remaining := value >> 7
	for remaining != 0 {
		err := binary.Write(w, binary.LittleEndian, uint8(value&0x7f|0x80))
		if err != nil {
			return xerrors.Errorf("binary.Write failed: %w", err)
		}
		value = remaining
		remaining >>= 7
	}
	err := binary.Write(w, binary.LittleEndian, uint8(value&0x7f))
	if err != nil {
		return xerrors.Errorf("binary.Write failed: %w", err)
	}
	return err
}

type spackMetric struct {
	flags uint8

	nameValueIndex uint32
	labelsCount    uint32
	labels         bytes.Buffer

	metric Metric
}

func (se *spackEncoder) addValue(value string) error {
	if _, ok := se.valuesIdx[value]; ok {
		return nil
	}

	se.valuesIdx[value] = se.valueCounter
	se.valueCounter++

	if _, err := se.labelValuePool.WriteString(value); err != nil {
		return err
	}
	if err := se.labelValuePool.WriteByte(0); err != nil {
		return err
	}

	return nil
}

func (se *spackEncoder) addName(name string) error {
	if _, ok := se.namesIdx[name]; ok {
		return nil
	}

	se.namesIdx[name] = se.nameCounter
	se.nameCounter++

	if _, err := se.labelNamePool.WriteString(name); err != nil {
		return err
	}
	if err := se.labelNamePool.WriteByte(0); err != nil {
		return err
	}

	return nil
}

// addToLabelPools adds a label name and value to the encoder's label pools if they don't already exist.
// This method is used to deduplicate label names and values across all metrics.
func (se *spackEncoder) addToLabelPools(name, value string) error {
	err := se.addName(name)
	if err != nil {
		return err
	}

	err = se.addValue(value)
	if err != nil {
		return err
	}

	return nil
}

func (s *spackMetric) writeLabel(se *spackEncoder, name string, value string) error {
	s.labelsCount++

	err := se.addToLabelPools(name, value)
	if err != nil {
		return err
	}

	err = writeULEB128(&s.labels, se.namesIdx[name])
	if err != nil {
		return err
	}
	err = writeULEB128(&s.labels, se.valuesIdx[value])
	if err != nil {
		return err
	}

	return nil
}

func (s *spackMetric) writeMetric(w io.Writer, version spackVersion) error {
	metricValueType := valueTypeOneWithoutTS
	if s.metric.getTimestamp() != nil {
		metricValueType = valueTypeOneWithTS
	}
	// library/cpp/monlib/encode/spack/spack_v1_encoder.cpp?rev=r9098142#L190
	types := uint8(s.metric.getType()<<2) | uint8(metricValueType)
	err := binary.Write(w, binary.LittleEndian, types)
	if err != nil {
		return xerrors.Errorf("binary.Write types failed: %w", err)
	}

	err = binary.Write(w, binary.LittleEndian, s.flags)
	if err != nil {
		return xerrors.Errorf("binary.Write flags failed: %w", err)
	}
	if version >= version12 {
		err = writeULEB128(w, s.nameValueIndex)
		if err != nil {
			return xerrors.Errorf("writeULEB128 name value index: %w", err)
		}
	}
	err = writeULEB128(w, s.labelsCount)
	if err != nil {
		return xerrors.Errorf("writeULEB128 labels count failed: %w", err)
	}

	_, err = w.Write(s.labels.Bytes()) // s.writeLabels(w)
	if err != nil {
		return xerrors.Errorf("write labels failed: %w", err)
	}
	if s.metric.getTimestamp() != nil {
		err = binary.Write(w, binary.LittleEndian, uint32(s.metric.getTimestamp().Unix()))
		if err != nil {
			return xerrors.Errorf("write timestamp failed: %w", err)
		}
	}

	switch s.metric.getType() {
	case typeGauge:
		err = binary.Write(w, binary.LittleEndian, s.metric.Value().(float64))
		if err != nil {
			return xerrors.Errorf("binary.Write gauge value failed: %w", err)
		}
	case typeIGauge:
		err = binary.Write(w, binary.LittleEndian, s.metric.Value().(int64))
		if err != nil {
			return xerrors.Errorf("binary.Write igauge value failed: %w", err)
		}
	case typeCounter, typeRated:
		err = binary.Write(w, binary.LittleEndian, uint64(s.metric.Value().(int64)))
		if err != nil {
			return xerrors.Errorf("binary.Write counter value failed: %w", err)
		}
	case typeHistogram, typeRatedHistogram:
		h := s.metric.Value().(histogram)
		err = h.writeHistogram(w)
		if err != nil {
			return xerrors.Errorf("writeHistogram failed: %w", err)
		}
	default:
		return xerrors.Errorf("unknown metric type: %v", s.metric.getType())
	}
	return nil
}

func (s *spackMetric) calculateMetricFlags() uint8 {
	var flags uint8
	if s.metric.isMemOnly() {
		flags |= uint8(memOnlyFlag)
	}
	return flags
}

func (s *spackMetric) getMetricNameValue(name string) string {
	value := s.metric.Name()

	if lvalue, ok := s.metric.Labels()[name]; ok {
		value = lvalue
	}

	return value
}

func (s *spackMetric) reset() {
	s.flags = 0
	s.nameValueIndex = 0
	s.labelsCount = 0
	s.labels.Reset()
	s.metric = nil
}

type SpackOpts func(*spackEncoder)

func WithVersion12() func(*spackEncoder) {
	return func(se *spackEncoder) {
		se.version = version12
	}
}

type spackEncoder struct {
	context     context.Context
	compression uint8
	version     spackVersion

	nameCounter  uint32
	valueCounter uint32

	labelNamePool  bytes.Buffer
	labelValuePool bytes.Buffer

	namesIdx  map[string]uint32
	valuesIdx map[string]uint32

	metrics     Metrics
	metricsPool *sync.Pool
}

func NewSpackEncoder(ctx context.Context, compression CompressionType, metrics *Metrics, opts ...SpackOpts) *spackEncoder {
	if metrics == nil {
		metrics = &Metrics{}
	}
	se := &spackEncoder{
		context:     ctx,
		compression: uint8(compression),
		version:     version11,
		metrics:     *metrics,
		namesIdx:    make(map[string]uint32),
		valuesIdx:   make(map[string]uint32),
		metricsPool: &sync.Pool{
			New: func() any {
				return &spackMetric{}
			},
		},
	}
	for _, op := range opts {
		op(se)
	}
	return se
}

func (se *spackEncoder) writeLabels() ([]*spackMetric, error) {
	if err := se.processCommonLabels(); err != nil {
		return nil, err
	}

	spackMetrics := make([]*spackMetric, len(se.metrics.metrics))
	for idx, metric := range se.metrics.metrics {
		sMetric, err := se.processMetric(metric)
		if err != nil {
			return nil, err
		}
		spackMetrics[idx] = sMetric
	}

	return spackMetrics, nil
}

func (se *spackEncoder) processCommonLabels() error {
	commonLabels := se.metrics.CommonLabels()
	for name, value := range commonLabels {
		if err := se.addToLabelPools(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (se *spackEncoder) processMetric(metric Metric) (*spackMetric, error) {
	m := se.metricsPool.Get().(*spackMetric)
	m.metric = metric
	m.flags = m.calculateMetricFlags()

	if err := se.processMetricNameTag(m); err != nil {
		return nil, err
	}

	for name, value := range metric.Labels() {
		if name == metric.getNameTag() {
			continue
		}
		if se.version == version12 && name == "name" {
			continue
		}
		if cValue, ok := se.metrics.CommonLabels()[name]; ok && cValue == value {
			continue
		}
		if err := m.writeLabel(se, name, value); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (se *spackEncoder) processMetricNameTag(m *spackMetric) error {
	switch se.version {
	case version11:
		return se.processNameTagV11(m)
	case version12:
		return se.processNameTagV12(m)
	default:
		return xerrors.Errorf("unsupported version: %v", se.version)
	}
}

func (se *spackEncoder) processNameTagV11(m *spackMetric) error {
	nameTag := m.metric.getNameTag()

	return m.writeLabel(se, nameTag, m.getMetricNameValue(nameTag))
}

func (se *spackEncoder) processNameTagV12(m *spackMetric) error {
	value := m.getMetricNameValue("name")

	err := se.addValue(value)
	if err != nil {
		return err
	}

	m.nameValueIndex = se.valuesIdx[value]
	return nil
}

func (se *spackEncoder) Encode(w io.Writer) (written int, err error) {
	spackMetrics, err := se.writeLabels()
	if err != nil {
		return written, xerrors.Errorf("writeLabels failed: %w", err)
	}
	defer func() {
		for _, m := range spackMetrics {
			m.reset()
			se.metricsPool.Put(m)
		}
	}()
	err = se.writeHeader(w)
	if err != nil {
		return written, xerrors.Errorf("writeHeader failed: %w", err)
	}
	written += HeaderSize
	compression := CompressionType(se.compression)

	cw := newCompressedWriter(w, compression)

	err = se.writeLabelNamesPool(cw)
	if err != nil {
		return written, xerrors.Errorf("writeLabelNamesPool failed: %w", err)
	}

	err = se.writeLabelValuesPool(cw)
	if err != nil {
		return written, xerrors.Errorf("writeLabelValuesPool failed: %w", err)
	}

	err = se.writeCommonTime(cw)
	if err != nil {
		return written, xerrors.Errorf("writeCommonTime failed: %w", err)
	}

	err = se.writeCommonLabels(cw)
	if err != nil {
		return written, xerrors.Errorf("writeCommonLabels failed: %w", err)
	}

	err = se.writeMetricsData(cw, spackMetrics)
	if err != nil {
		return written, xerrors.Errorf("writeMetricsData failed: %w", err)
	}

	err = cw.Close()
	if err != nil {
		return written, xerrors.Errorf("close failed: %w", err)
	}

	switch compression {
	case CompressionNone:
		written += cw.(*noCompressionWriteCloser).written
	case CompressionLz4:
		written += cw.(*lz4CompressionWriteCloser).written
	}

	return written, nil
}

func (se *spackEncoder) writeHeader(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}
	ew := &errWriter{w: w}
	ew.binaryWrite(uint16(0x5053))                  // Magic
	ew.binaryWrite(uint16(se.version))              // Version
	ew.binaryWrite(uint16(24))                      // HeaderSize
	ew.binaryWrite(uint8(0))                        // TimePrecision(SECONDS)
	ew.binaryWrite(uint8(se.compression))           // CompressionAlg
	ew.binaryWrite(uint32(se.labelNamePool.Len()))  // LabelNamesSize
	ew.binaryWrite(uint32(se.labelValuePool.Len())) // LabelValuesSize
	ew.binaryWrite(uint32(len(se.metrics.metrics))) // MetricsCount
	ew.binaryWrite(uint32(len(se.metrics.metrics))) // PointsCount
	if ew.err != nil {
		return xerrors.Errorf("binaryWrite failed: %w", ew.err)
	}
	return nil
}

func (se *spackEncoder) writeLabelNamesPool(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}
	_, err := w.Write(se.labelNamePool.Bytes())
	if err != nil {
		return xerrors.Errorf("write labelNamePool failed: %w", err)
	}
	return nil
}

func (se *spackEncoder) writeLabelValuesPool(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	_, err := w.Write(se.labelValuePool.Bytes())
	if err != nil {
		return xerrors.Errorf("write labelValuePool failed: %w", err)
	}
	return nil
}

func (se *spackEncoder) writeCommonTime(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	if se.metrics.timestamp == nil {
		return binary.Write(w, binary.LittleEndian, uint32(0))
	}
	return binary.Write(w, binary.LittleEndian, uint32(se.metrics.timestamp.Unix()))
}

func (se *spackEncoder) writeCommonLabels(w io.Writer) error {
	if se.context.Err() != nil {
		return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
	}

	commonLabels := se.metrics.CommonLabels()
	if len(commonLabels) == 0 {
		_, err := w.Write([]byte{0})
		if err != nil {
			return xerrors.Errorf("write commonLabels failed: %w", err)
		}
		return nil
	}

	err := writeULEB128(w, uint32(len(commonLabels)))
	if err != nil {
		return xerrors.Errorf("write commonLabels count failed: %w", err)
	}

	for name, value := range commonLabels {
		nameIdx, nameOk := se.namesIdx[name]
		if !nameOk {
			return xerrors.Errorf("commonLabel name not found in pool: name=%s", name)
		}

		err = writeULEB128(w, nameIdx)
		if err != nil {
			return xerrors.Errorf("write commonLabel name index failed: %w", err)
		}

		valueIdx, valueOk := se.valuesIdx[value]
		if !valueOk {
			return xerrors.Errorf("commonLabel value not found in pool: name=%s value=%s", name, value)
		}

		err = writeULEB128(w, valueIdx)
		if err != nil {
			return xerrors.Errorf("write commonLabel value index failed: %w", err)
		}
	}
	return nil
}

func (se *spackEncoder) writeMetricsData(w io.Writer, metrics []*spackMetric) error {
	for _, s := range metrics {
		if se.context.Err() != nil {
			return xerrors.Errorf("streamSpack context error: %w", se.context.Err())
		}

		err := s.writeMetric(w, se.version)
		if err != nil {
			return xerrors.Errorf("write metric failed: %w", err)
		}
	}
	return nil
}
