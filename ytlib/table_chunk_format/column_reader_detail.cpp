#include "column_reader_detail.h"

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedSegmentReaderBase::TUnversionedSegmentReaderBase(
    TRef data,
    const TSegmentMeta& meta,
    int columnIndex,
    int columnId)
    : Data_(data)
    , Meta_(meta)
    , ColumnIndex_(columnIndex)
    , ColumnId_(columnId)
    , SegmentStartRowIndex_(meta.chunk_row_count() - meta.row_count())
{ }

bool TUnversionedSegmentReaderBase::EndOfSegment() const
{
    return SegmentRowIndex_ == Meta_.row_count();
}

i64 TUnversionedSegmentReaderBase::GetSegmentRowIndex(i64 rowIndex) const
{
    return rowIndex - SegmentStartRowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedValueExtractorBase::TVersionedValueExtractorBase(bool aggregate)
    : Aggregate_(aggregate)
{ }

ui32 TVersionedValueExtractorBase::GetTimestampIndex(i64 valueIndex) const
{
    return TimestampIndexReader_[valueIndex];
}

bool TVersionedValueExtractorBase::GetAggregate(i64 valueIndex) const
{
    return Aggregate_ ? AggregateBitmap_[valueIndex] : false;
}

size_t TVersionedValueExtractorBase::InitTimestampIndexReader(const char* ptr)
{
    const char* begin = ptr;

    TimestampIndexReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
    ptr += TimestampIndexReader_.GetByteSize();

    if (Aggregate_) {
        AggregateBitmap_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            TimestampIndexReader_.GetSize());
        ptr += AggregateBitmap_.GetByteSize();
    }

    return ptr - begin;
}

////////////////////////////////////////////////////////////////////////////////

TDenseVersionedValueExtractorBase::TDenseVersionedValueExtractorBase(const TSegmentMeta& meta, bool aggregate)
    : TVersionedValueExtractorBase(aggregate)
    , DenseVersionedMeta_(meta.GetExtension(TDenseVersionedSegmentMeta::dense_versioned_segment_meta))
{ }

std::pair<ui32, ui32> TDenseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex, ui32 lowerTimestampIndex)
{
    i64 upperValueIndex = GetLowerValueIndex(segmentRowIndex + 1);
    i64 valueIndex = LowerBound(
        GetLowerValueIndex(segmentRowIndex),
        upperValueIndex,
        [&] (i64 currentValueIndex) {
            return GetTimestampIndex(currentValueIndex) < lowerTimestampIndex;
        });

    return std::make_pair(valueIndex, upperValueIndex);
}

std::pair<ui32, ui32> TDenseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex)
{
    return std::make_pair(
        GetLowerValueIndex(segmentRowIndex), 
        GetLowerValueIndex(segmentRowIndex + 1));
}

i64 TDenseVersionedValueExtractorBase::GetLowerValueIndex(i64 segmentRowIndex) const
{
    if (segmentRowIndex == 0) {
        return 0;
    } else {
        return DenseVersionedMeta_.expected_values_per_row() * segmentRowIndex +
           ZigZagDecode32(ValuesPerRowDiffReader_[segmentRowIndex - 1]);
    }
}

ui32 TDenseVersionedValueExtractorBase::GetValueCount(i64 segmentRowIndex) const
{
    return GetLowerValueIndex(segmentRowIndex + 1) - GetLowerValueIndex(segmentRowIndex);
}

size_t TDenseVersionedValueExtractorBase::InitDenseReader(const char* ptr)
{
    const char* begin = ptr;
    ValuesPerRowDiffReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
    ptr += ValuesPerRowDiffReader_.GetByteSize();

    ptr += InitTimestampIndexReader(ptr);

    return ptr - begin;
}

////////////////////////////////////////////////////////////////////////////////

TSparseVersionedValueExtractorBase::TSparseVersionedValueExtractorBase(const TSegmentMeta& meta, bool aggregate)
    : TVersionedValueExtractorBase(aggregate)
{ }

i64 TSparseVersionedValueExtractorBase::GetLowerValueIndex(i64 segmentRowIndex, int valueIndex) const
{
    return LowerBound(
        valueIndex,
        RowIndexReader_.GetSize(),
        [&] (i64 currentValueIndex) {
            return RowIndexReader_[currentValueIndex] < segmentRowIndex;
        });
}

i64 TSparseVersionedValueExtractorBase::GetRowIndex(i64 valueIndex) const
{
    return RowIndexReader_[valueIndex];
}

i64 TSparseVersionedValueExtractorBase::GetValueCount() const
{
    return RowIndexReader_.GetSize();
}

std::pair<ui32, ui32> TSparseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex, i64 valueIndex, ui32 lowerTimestampIndex)
{
    i64 upperValueIndex = GetLowerValueIndex(segmentRowIndex + 1, valueIndex);
    i64 currentValueIndex = LowerBound(
        valueIndex,
        upperValueIndex,
        [&] (i64 currentValueIndex) {
            return GetTimestampIndex(currentValueIndex) < lowerTimestampIndex;
        });

    return std::make_pair(currentValueIndex, upperValueIndex);
}

std::pair<ui32, ui32> TSparseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex, i64 valueIndex)
{
    i64 upperValueIndex = GetLowerValueIndex(segmentRowIndex + 1, valueIndex);
    return std::make_pair(valueIndex, upperValueIndex);
}

size_t TSparseVersionedValueExtractorBase::InitSparseReader(const char* ptr)
{
    const char* begin = ptr;
    RowIndexReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
    ptr += RowIndexReader_.GetByteSize();

    ptr += InitTimestampIndexReader(ptr);

    return ptr - begin;
}

////////////////////////////////////////////////////////////////////////////////

TVersionedColumnReaderBase::TVersionedColumnReaderBase(const TColumnMeta& columnMeta, int columnId, bool aggregate)
    : TColumnReaderBase(columnMeta)
    , ColumnId_(columnId)
    , Aggregate_(aggregate)
{ }

void TVersionedColumnReaderBase::GetValueCounts(TMutableRange<ui32> valueCounts)
{
    EnsureSegmentReader();
    SegmentReader_->GetValueCounts(valueCounts);
}

void TVersionedColumnReaderBase::ReadValues(
    TMutableRange<TMutableVersionedRow> rows,
    TRange<std::pair<ui32, ui32>> timestampIndexRanges,
    bool produceAllVersions)
{
    i64 readRowCount = 0;
    while (readRowCount < rows.Size()) {
        YCHECK(CurrentSegmentIndex_ <= LastBlockSegmentIndex_);
        EnsureSegmentReader();

        i64 count = SegmentReader_->ReadValues(
            rows.Slice(rows.Begin() + readRowCount, rows.End()),
            timestampIndexRanges.Slice(readRowCount, timestampIndexRanges.Size()),
            produceAllVersions);

        readRowCount += count;
        CurrentRowIndex_ += count;

        if (SegmentReader_->EndOfSegment()) {
            SegmentReader_.reset();
            ++CurrentSegmentIndex_;
        }
    }
}

void TVersionedColumnReaderBase::ReadAllValues(TMutableRange<TMutableVersionedRow> rows)
{
    i64 readRowCount = 0;
    while (readRowCount < rows.Size()) {
        YCHECK(CurrentSegmentIndex_ <= LastBlockSegmentIndex_);
        EnsureSegmentReader();

        i64 count = SegmentReader_->ReadAllValues(
            rows.Slice(rows.Begin() + readRowCount, rows.End()));

        readRowCount += count;
        CurrentRowIndex_ += count;

        if (SegmentReader_->EndOfSegment()) {
            SegmentReader_.reset();
            ++CurrentSegmentIndex_;
        }
    }
}

void TVersionedColumnReaderBase::ResetSegmentReader()
{
    SegmentReader_.reset();
}

void TVersionedColumnReaderBase::EnsureSegmentReader()
{
    if (!SegmentReader_) {
        SegmentReader_ = CreateSegmentReader(CurrentSegmentIndex_);
    }
    SegmentReader_->SkipToRowIndex(CurrentRowIndex_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
