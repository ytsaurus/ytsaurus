#include "column_reader_detail.h"

#include <yt/yt/client/table_client/columnar.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedSegmentReaderBase::TUnversionedSegmentReaderBase(
    TRef data,
    const TSegmentMeta& meta,
    int columnIndex,
    int columnId,
    EValueType valueType,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
    : Data_(data)
    , Meta_(meta)
    , ColumnIndex_(columnIndex)
    , ColumnId_(columnId)
    , ValueType_(valueType)
    , SortOrder_(sortOrder)
    , SegmentStartRowIndex_(meta.chunk_row_count() - meta.row_count())
    , HunkColumnFlag_(static_cast<bool>(columnSchema.MaxInlineHunkSize()))
{ }

i64 TUnversionedSegmentReaderBase::EstimateDataWeight(i64 lowerRowIndex, i64 upperRowIndex)
{
    return (upperRowIndex - lowerRowIndex) * GetDataWeight(ValueType_);
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

const char* TVersionedValueExtractorBase::InitTimestampIndexReader(const char* ptr)
{
    TimestampIndexReader_ = TBitPackedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
    ptr += TimestampIndexReader_.GetByteSize();

    if (Aggregate_) {
        AggregateBitmap_ = TReadOnlyBitmap(ptr, TimestampIndexReader_.GetSize());
        ptr += AlignUp(AggregateBitmap_.GetByteSize(), SerializationAlignment);
    }

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

TDenseVersionedValueExtractorBase::TDenseVersionedValueExtractorBase(const TSegmentMeta& meta, bool aggregate)
    : TVersionedValueExtractorBase(aggregate)
    , DenseVersionedMeta_(meta.GetExtension(TDenseVersionedSegmentMeta::dense_versioned_segment_meta))
{ }

std::pair<ui32, ui32> TDenseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex, ui32 lowerTimestampIndex)
{
    i64 upperValueIndex = GetLowerValueIndex(segmentRowIndex + 1);
    i64 valueIndex = BinarySearch(
        GetLowerValueIndex(segmentRowIndex),
        upperValueIndex,
        [&] (i64 currentValueIndex) {
            return GetTimestampIndex(currentValueIndex) < lowerTimestampIndex;
        });

    return std::pair(valueIndex, upperValueIndex);
}

std::pair<ui32, ui32> TDenseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex)
{
    return std::pair(
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

const char* TDenseVersionedValueExtractorBase::InitDenseReader(const char* ptr)
{
    ValuesPerRowDiffReader_ = TBitPackedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
    ptr += ValuesPerRowDiffReader_.GetByteSize();

    ptr = InitTimestampIndexReader(ptr);

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

TSparseVersionedValueExtractorBase::TSparseVersionedValueExtractorBase(bool aggregate)
    : TVersionedValueExtractorBase(aggregate)
{ }

i64 TSparseVersionedValueExtractorBase::GetLowerValueIndex(i64 segmentRowIndex, int valueIndex) const
{
    return BinarySearch(
        valueIndex,
        RowIndexReader_.GetSize(),
        [&] (i64 currentValueIndex) {
            return static_cast<i64>(RowIndexReader_[currentValueIndex]) < segmentRowIndex;
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
    i64 currentValueIndex = BinarySearch(
        valueIndex,
        upperValueIndex,
        [&] (i64 currentValueIndex) {
            return GetTimestampIndex(currentValueIndex) < lowerTimestampIndex;
        });

    return std::pair(currentValueIndex, upperValueIndex);
}

std::pair<ui32, ui32> TSparseVersionedValueExtractorBase::GetValueIndexRange(i64 segmentRowIndex, i64 valueIndex)
{
    i64 upperValueIndex = GetLowerValueIndex(segmentRowIndex + 1, valueIndex);
    return std::pair(valueIndex, upperValueIndex);
}

const char* TSparseVersionedValueExtractorBase::InitSparseReader(const char* ptr)
{
    RowIndexReader_ = TBitPackedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
    ptr += RowIndexReader_.GetByteSize();

    ptr = InitTimestampIndexReader(ptr);

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

TColumnReaderBase::TColumnReaderBase(const NProto::TColumnMeta& columnMeta)
    : ColumnMeta_(columnMeta)
{ }

void TColumnReaderBase::Rearm()
{
    RearmSegmentReader();
}

void TColumnReaderBase::SetCurrentBlock(TSharedRef block, int blockIndex)
{
    ResetCurrentSegmentReader();
    Block_ = std::move(block);
    CurrentBlockIndex_ = blockIndex;

    if (CurrentSegmentMeta().block_index() != CurrentBlockIndex_) {
        CurrentSegmentIndex_ = FindFirstBlockSegment();
    }

    LastBlockSegmentIndex_ = FindLastBlockSegment();
    YT_VERIFY(LastBlockSegmentIndex_ >= CurrentSegmentIndex_);
}

void TColumnReaderBase::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(rowIndex >= CurrentRowIndex_);

    CurrentRowIndex_ = rowIndex;
    int segmentIndex = FindSegmentByRowIndex(rowIndex);

    YT_VERIFY(segmentIndex >= CurrentSegmentIndex_);
    if (segmentIndex != CurrentSegmentIndex_) {
        CurrentSegmentIndex_ = segmentIndex;
        ResetCurrentSegmentReader();
    }
}

i64 TColumnReaderBase::GetCurrentRowIndex() const
{
    return CurrentRowIndex_;
}

i64 TColumnReaderBase::GetBlockUpperRowIndex() const
{
    if (LastBlockSegmentIndex_ < 0) {
        return 0;
    } else {
        return ColumnMeta_.segments(LastBlockSegmentIndex_).chunk_row_count();
    }
}

i64 TColumnReaderBase::GetReadyUpperRowIndex() const
{
    if (CurrentSegmentIndex_ == ColumnMeta_.segments_size()) {
        return CurrentRowIndex_;
    } else {
        return CurrentSegmentMeta().chunk_row_count();
    }
}

int TColumnReaderBase::GetCurrentBlockIndex() const
{
    return CurrentBlockIndex_;
}

int TColumnReaderBase::GetCurrentSegmentIndex() const
{
    return CurrentSegmentIndex_;
}

std::optional<int> TColumnReaderBase::GetNextBlockIndex() const
{
    return (LastBlockSegmentIndex_ + 1) == ColumnMeta_.segments_size()
        ? std::nullopt
        : std::make_optional(static_cast<int>(ColumnMeta_.segments(LastBlockSegmentIndex_ + 1).block_index()));
}

const NProto::TSegmentMeta& TColumnReaderBase::CurrentSegmentMeta() const
{
    return ColumnMeta_.segments(CurrentSegmentIndex_);
}

int TColumnReaderBase::FindSegmentByRowIndex(i64 rowIndex) const
{
    auto it = std::upper_bound(
        ColumnMeta_.segments().begin() + CurrentSegmentIndex_,
        ColumnMeta_.segments().end(),
        rowIndex,
        [] (i64 index, const NProto::TSegmentMeta& segmentMeta) {
            return index < segmentMeta.chunk_row_count();
        }
    );

    return std::distance(ColumnMeta_.segments().begin(), it);
}

i64 TColumnReaderBase::GetSegmentStartRowIndex(int segmentIndex) const
{
    auto meta = ColumnMeta_.segments(segmentIndex);
    return meta.chunk_row_count() - meta.row_count();
}

int TColumnReaderBase::FindFirstBlockSegment() const
{
    auto it = std::lower_bound(
        ColumnMeta_.segments().begin() + CurrentSegmentIndex_,
        ColumnMeta_.segments().end(),
        CurrentBlockIndex_,
        [] (const NProto::TSegmentMeta& segmentMeta, int blockIndex) {
            return segmentMeta.block_index() < blockIndex;
        }
    );
    YT_VERIFY(it != ColumnMeta_.segments().end());
    return std::distance(ColumnMeta_.segments().begin(), it);
}

int TColumnReaderBase::FindLastBlockSegment() const
{
    auto it = std::upper_bound(
        ColumnMeta_.segments().begin() + CurrentSegmentIndex_,
        ColumnMeta_.segments().end(),
        CurrentBlockIndex_,
        [] (int blockIndex, const NProto::TSegmentMeta& segmentMeta) {
            return blockIndex < segmentMeta.block_index();
        }
    );

    YT_VERIFY(it != ColumnMeta_.segments().begin());
    return std::distance(ColumnMeta_.segments().begin(), it - 1);
}

void TColumnReaderBase::ResetCurrentSegmentReaderOnEos()
{
    if (CurrentRowIndex_ == CurrentSegmentMeta().chunk_row_count()) {
        ResetCurrentSegmentReader();
        ++CurrentSegmentIndex_;
    }
}

void TColumnReaderBase::EnsureCurrentSegmentReader()
{
    if (!GetCurrentSegmentReader()) {
        YT_VERIFY(CurrentSegmentIndex_ <= LastBlockSegmentIndex_);
        CreateCurrentSegmentReader();
    }
}

void TColumnReaderBase::RearmSegmentReader()
{
    ResetCurrentSegmentReaderOnEos();
    EnsureCurrentSegmentReader();
    GetCurrentSegmentReader()->SkipToRowIndex(CurrentRowIndex_);
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedColumnReaderBase::TUnversionedColumnReaderBase(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
    : TColumnReaderBase(columnMeta)
    , ColumnIndex_(columnIndex)
    , ColumnId_(columnId)
    , SortOrder_(sortOrder)
    , ColumnSchema_(columnSchema)
{ }

void TUnversionedColumnReaderBase::ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows)
{
    DoReadValues(rows);
}

void TUnversionedColumnReaderBase::ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows)
{
    DoReadValues(rows);
}

int TUnversionedColumnReaderBase::GetBatchColumnCount()
{
    EnsureCurrentSegmentReader();
    return SegmentReader_->GetBatchColumnCount();
}

void TUnversionedColumnReaderBase::ReadColumnarBatch(
    TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns,
    i64 rowCount)
{
    EnsureCurrentSegmentReader();
    SegmentReader_->ReadColumnarBatch(columns, rowCount);
    CurrentRowIndex_ += rowCount;
}

i64 TUnversionedColumnReaderBase::EstimateDataWeight(i64 lowerRowIndex, i64 upperRowIndex)
{
    EnsureCurrentSegmentReader();
    return SegmentReader_->EstimateDataWeight(lowerRowIndex, upperRowIndex);
}

ISegmentReaderBase* TUnversionedColumnReaderBase::GetCurrentSegmentReader() const
{
    return SegmentReader_.get();
}

void TUnversionedColumnReaderBase::ResetCurrentSegmentReader()
{
    SegmentReader_.reset();
}

void TUnversionedColumnReaderBase::CreateCurrentSegmentReader()
{
    SegmentReader_ = CreateSegmentReader(CurrentSegmentIndex_);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedColumnReaderBase::TVersionedColumnReaderBase(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
    : TColumnReaderBase(columnMeta)
    , ColumnId_(columnId)
    , ColumnSchema_(columnSchema)
{ }

void TVersionedColumnReaderBase::ReadValueCounts(TMutableRange<ui32> valueCounts)
{
    RearmSegmentReader();
    SegmentReader_->ReadValueCounts(valueCounts);
}

void TVersionedColumnReaderBase::ReadValues(
    TMutableRange<TMutableVersionedRow> rows,
    TRange<std::pair<ui32, ui32>> timestampIndexRanges,
    bool produceAllVersions)
{
    i64 readRowCount = 0;
    while (readRowCount < std::ssize(rows)) {
        RearmSegmentReader();
        i64 count = SegmentReader_->ReadValues(
            rows.Slice(rows.Begin() + readRowCount, rows.End()),
            timestampIndexRanges.Slice(readRowCount, timestampIndexRanges.Size()),
            produceAllVersions);

        readRowCount += count;
        CurrentRowIndex_ += count;
    }
}

void TVersionedColumnReaderBase::ReadAllValues(TMutableRange<TMutableVersionedRow> rows)
{
    i64 readRowCount = 0;
    while (readRowCount < std::ssize(rows)) {
        RearmSegmentReader();
        i64 count = SegmentReader_->ReadAllValues(
            rows.Slice(rows.Begin() + readRowCount, rows.End()));

        readRowCount += count;
        CurrentRowIndex_ += count;
    }
}

ISegmentReaderBase* TVersionedColumnReaderBase::GetCurrentSegmentReader() const
{
    return SegmentReader_.get();
}

void TVersionedColumnReaderBase::ResetCurrentSegmentReader()
{
    SegmentReader_.reset();
}

void TVersionedColumnReaderBase::CreateCurrentSegmentReader()
{
    SegmentReader_ = CreateSegmentReader(CurrentSegmentIndex_);
}

////////////////////////////////////////////////////////////////////////////////

void ReadColumnarNullBitmap(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& nullBitmap = column->NullBitmap.emplace();
    nullBitmap.Data = bitmap;
}

void ReadColumnarIntegerValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    NTableClient::EValueType valueType,
    ui64 baseValue,
    TRange<ui64> data)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BaseValue = baseValue;
    values.BitWidth = 64;
    values.ZigZagEncoded = (valueType == EValueType::Int64);
    values.Data = TRef(data.Begin(), data.End());
}

void ReadColumnarBooleanValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BitWidth = 1;
    values.Data = bitmap;
}

template <typename T>
void ReadColumnarFloatingPointValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRange<T> data)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BitWidth = sizeof(T) * 8;
    values.Data = TRef(data.Begin(), data.End());
}

template
void ReadColumnarFloatingPointValues<float>(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRange<float> data);

template
void ReadColumnarFloatingPointValues<double>(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRange<double> data);

void ReadColumnarStringValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    ui32 avgLength,
    TRange<ui32> offsets,
    TRef stringData)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BitWidth = 32;
    values.ZigZagEncoded = true;
    values.Data = TRef(offsets.Begin(), offsets.End());

    auto& strings = column->Strings.emplace();
    strings.AvgLength = avgLength;
    strings.Data = stringData;
}

void ReadColumnarDictionary(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* primaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TColumn* dictionaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TDictionaryId dictionaryId,
    NTableClient::TLogicalTypePtr type,
    i64 startIndex,
    i64 valueCount,
    TRange<ui32> ids)
{
    primaryColumn->StartIndex = startIndex;
    primaryColumn->ValueCount = valueCount;

    dictionaryColumn->Type = type && type->GetMetatype() == ELogicalMetatype::Optional
        ? type->AsOptionalTypeRef().GetElement()
        : type;

    auto& primaryValues = primaryColumn->Values.emplace();
    primaryValues.BitWidth = 32;
    primaryValues.Data = TRef(ids.Begin(), ids.End());

    auto& dictionary = primaryColumn->Dictionary.emplace();
    dictionary.DictionaryId = dictionaryId;
    dictionary.ZeroMeansNull = true;
    dictionary.ValueColumn = dictionaryColumn;
}

void ReadColumnarRle(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* primaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TColumn* rleColumn,
    NTableClient::TLogicalTypePtr type,
    i64 startIndex,
    i64 valueCount,
    TRange<ui64> indexes)
{
    primaryColumn->StartIndex = startIndex;
    primaryColumn->ValueCount = valueCount;

    rleColumn->Type = std::move(type);

    auto& primaryValues = primaryColumn->Values.emplace();
    primaryValues.BitWidth = 64;
    primaryValues.Data = TRef(indexes.Begin(), indexes.End());

    auto& rle = primaryColumn->Rle.emplace();
    rle.ValueColumn = rleColumn;

    rleColumn->StartIndex = TranslateRleStartIndex(indexes, startIndex);
    rleColumn->ValueCount = TranslateRleEndIndex(indexes, startIndex + valueCount) - rleColumn->StartIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
