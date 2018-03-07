#pragma once

#include "public.h"

#include "column_reader.h"
#include "compressed_integer_vector.h"
#include "helpers.h"

#include <yt/ytlib/table_chunk_format/column_meta.pb.h>

#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/zigzag.h>
#include <yt/core/misc/algorithm_helpers.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct ISegmentReaderBase
    : public TNonCopyable
{
    virtual ~ISegmentReaderBase() = default;

    virtual void SkipToRowIndex(i64 rowIndex) = 0;

    virtual bool EndOfSegment() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IUnversionedSegmentReader
    : public ISegmentReaderBase
{
    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) = 0;

    //! Last value of the segment.
    virtual NTableClient::TUnversionedValue GetLastValue() const = 0;

    virtual i64 GetLowerRowIndex(
        const NTableClient::TUnversionedValue& value, 
        i64 rowIndexLimit) const = 0;

    virtual i64 GetUpperRowIndex(
        const NTableClient::TUnversionedValue& value, 
        i64 rowIndexLimit) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVersionedSegmentReader
    : public ISegmentReaderBase
{
    //! Transactional read.
    virtual i64 ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows,
        TRange<std::pair<ui32, ui32>> timestampIndexRanges) = 0;

    //! Compaction read.
    virtual i64 ReadAllValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) = 0;

    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedSegmentReaderBase
    : public IUnversionedSegmentReader
{
public:
    TUnversionedSegmentReaderBase(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnIndex,
        int columnId);

    virtual bool EndOfSegment() const;

protected:
    const TRef Data_;
    const NProto::TSegmentMeta& Meta_;
    const int ColumnIndex_;
    const int ColumnId_;

    const i64 SegmentStartRowIndex_;

    i64 SegmentRowIndex_ = 0;

    i64 GetSegmentRowIndex(i64 rowIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

template <NTableClient::EValueType ValueType, class TValueExtractor>
class TDenseUnversionedSegmentReader
    : public TUnversionedSegmentReaderBase
{
public:
    TDenseUnversionedSegmentReader(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnIndex,
        int columnId)
        : TUnversionedSegmentReaderBase(data, meta, columnIndex, columnId)
        , ValueExtractor_(data, meta)
    { }

    virtual i64 GetLowerRowIndex(const NTableClient::TUnversionedValue& value, i64 upperRowIndex) const override
    {
        i64 index = LowerBound(
            SegmentRowIndex_,
            std::min(GetSegmentRowIndex(upperRowIndex), Meta_.row_count()),
            [&] (i64 segmentRowIndex) {
                NTableClient::TUnversionedValue currentValue;
                SetValue(&currentValue, segmentRowIndex);
                return CompareValues<ValueType>(currentValue, value) < 0;
            });
        return SegmentStartRowIndex_ + index;
    }

    virtual i64 GetUpperRowIndex(const NTableClient::TUnversionedValue& value, i64 upperRowIndex) const override
    {
        i64 index = LowerBound(
            SegmentRowIndex_,
            std::min(GetSegmentRowIndex(upperRowIndex), Meta_.row_count()),
            [&] (i64 segmentRowIndex) {
                NTableClient::TUnversionedValue currentValue;
                SetValue(&currentValue, segmentRowIndex);
                return CompareValues<ValueType>(currentValue, value) <= 0;
            });
        return SegmentStartRowIndex_ + index;
    }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        YCHECK(GetSegmentRowIndex(rowIndex) >= SegmentRowIndex_);
        SegmentRowIndex_ = GetSegmentRowIndex(rowIndex);
    }

    virtual NTableClient::TUnversionedValue GetLastValue() const override
    {
        NTableClient::TUnversionedValue value;
        SetValue(&value, Meta_.row_count() - 1);
        return value;
    }

    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        return DoReadValues(rows);
    }

    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) override
    {
        return DoReadValues(rows);
    }

private:
    TValueExtractor ValueExtractor_;

    void SetValue(NTableClient::TUnversionedValue* value, i64 rowIndex) const
    {
        ValueExtractor_.ExtractValue(value, rowIndex, ColumnId_, false);
    }

    template<class TRow>
    i64 DoReadValues(TMutableRange<TRow> rows)
    {
        i64 rangeRowIndex = 0;
        i64 segmentRowIndex = SegmentRowIndex_;

        while (rangeRowIndex < rows.Size() && segmentRowIndex < Meta_.row_count()) {
            auto row = rows[rangeRowIndex];
            if (row) {
                YCHECK(GetUnversionedValueCount(row) > ColumnIndex_);
                SetValue(&GetUnversionedValue(row, ColumnIndex_), segmentRowIndex);
            }

            ++segmentRowIndex;
            ++rangeRowIndex;
        }

        SegmentRowIndex_ = segmentRowIndex;
        return rangeRowIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool Scan>
class TRleValueExtractorBase
{
public:
    i64 GetValueCount() const
    {
        return RowIndexReader_.GetSize();
    }

    i64 GetRowIndex(i64 valueIndex) const
    {
        return RowIndexReader_[valueIndex];
    }

protected:
    using TRowIndexReader = TCompressedUnsignedVectorReader<ui64, Scan>;
    TRowIndexReader RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <NTableClient::EValueType ValueType, class TRleValueExtractor>
class TRleUnversionedSegmentReader
    : public TUnversionedSegmentReaderBase
{
public:
    TRleUnversionedSegmentReader(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnIndex,
        int columnId)
        : TUnversionedSegmentReaderBase(data, meta, columnIndex, columnId)
        , ValueExtractor_(data, meta)
    { }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        auto segmentRowIndex = GetSegmentRowIndex(rowIndex);
        YCHECK(segmentRowIndex >= SegmentRowIndex_);
        if (segmentRowIndex > SegmentRowIndex_) {
            SegmentRowIndex_ = segmentRowIndex;

            ValueIndex_ = LowerBound(
                ValueIndex_,
                ValueExtractor_.GetValueCount(),
                [&] (i64 valueIndex) {
                    return ValueExtractor_.GetRowIndex(valueIndex) < SegmentRowIndex_;
                }) - 1;
        }
    }

    virtual NTableClient::TUnversionedValue GetLastValue() const override
    {
        NTableClient::TUnversionedValue value;
        SetValue(&value, ValueExtractor_.GetValueCount() - 1);
        return value;
    }

    virtual i64 GetLowerRowIndex(const NTableClient::TUnversionedValue& value, i64 rowIndexLimit) const override
    {
        i64 upperValueIndex = GetUpperValueIndex(rowIndexLimit);
        i64 valueIndex = LowerBound(
            ValueIndex_,
            upperValueIndex,
            [&] (i64 valueIndex) {
                NTableClient::TUnversionedValue currentValue;
                SetValue(&currentValue, valueIndex);
                return CompareValues<ValueType>(currentValue, value) < 0;
            });

        return std::min(GetValueLowerRowIndex(valueIndex), rowIndexLimit);
    }

    virtual i64 GetUpperRowIndex(const NTableClient::TUnversionedValue& value, i64 rowIndexLimit) const override
    {
        i64 upperValueIndex = GetUpperValueIndex(rowIndexLimit);
        i64 valueIndex = LowerBound(
            ValueIndex_,
            upperValueIndex,
            [&] (i64 valueIndex) {
                NTableClient::TUnversionedValue currentValue;
                SetValue(&currentValue, valueIndex);
                return CompareValues<ValueType>(currentValue, value) <= 0;
            });

        return std::min(GetValueLowerRowIndex(valueIndex), rowIndexLimit);
    }

    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        return DoReadValues(rows);
    }

    virtual i64 ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) override
    {
        return DoReadValues(rows);
    }

private:
    TRleValueExtractor ValueExtractor_;
    i64 ValueIndex_ = 0;

    i64 GetUpperValueIndex(i64 rowIndex) const
    {
        i64 upperValueIndex;
        if (GetSegmentRowIndex(rowIndex) >= Meta_.row_count()) {
            upperValueIndex = ValueExtractor_.GetValueCount();
        } else {
            upperValueIndex = LowerBound(
                ValueIndex_,
                ValueExtractor_.GetValueCount(),
                [&] (i64 valueIndex) {
                    return ValueExtractor_.GetRowIndex(valueIndex) < GetSegmentRowIndex(rowIndex);
                });
        }
        return upperValueIndex;
    }

    i64 GetValueLowerRowIndex(i64 valueIndex) const
    {
        return SegmentStartRowIndex_ + std::max(
            SegmentRowIndex_,
            valueIndex < ValueExtractor_.GetValueCount()
                ? ValueExtractor_.GetRowIndex(valueIndex)
                : Meta_.row_count());
    }

    void SetValue(NTableClient::TUnversionedValue* value) const
    {
        SetValue(value, ValueIndex_);
    }

    void SetValue(NTableClient::TUnversionedValue* value, i64 valueIndex) const
    {
        ValueExtractor_.ExtractValue(value, valueIndex, ColumnId_, false);
    }

    template <class TRow>
    i64 DoReadValues(TMutableRange<TRow> rows)
    {
        i64 rangeRowIndex = 0;
        while (rangeRowIndex < rows.Size() && SegmentRowIndex_ < Meta_.row_count()) {
            i64 valueRowCount = ValueIndex_ + 1 == ValueExtractor_.GetValueCount()
                ? Meta_.row_count()
                : ValueExtractor_.GetRowIndex(ValueIndex_ + 1);
            i64 segmentRowIndex = SegmentRowIndex_;

            NTableClient::TUnversionedValue value;
            SetValue(&value);

            while (segmentRowIndex < valueRowCount && rangeRowIndex < rows.Size()) {
                auto row = rows[rangeRowIndex];
                if (row) {
                    GetUnversionedValue(row, ColumnIndex_) = value;
                }
                ++rangeRowIndex;
                ++segmentRowIndex;
            }

            SegmentRowIndex_ = segmentRowIndex;
            if (SegmentRowIndex_ == valueRowCount) {
                ++ValueIndex_;
            }
        }
        return rangeRowIndex;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColumnReaderBase
    : public virtual IColumnReaderBase
{
public:
    explicit TColumnReaderBase(const NProto::TColumnMeta& columnMeta)
        : ColumnMeta_(columnMeta)
    { }

    virtual void ResetBlock(TSharedRef block, int blockIndex) override
    {
        ResetSegmentReader();
        Block_ = std::move(block);
        CurrentBlockIndex_ = blockIndex;

        if (CurrentSegmentMeta().block_index() != CurrentBlockIndex_) {
            CurrentSegmentIndex_ = FindFirstBlockSegment();
        }

        LastBlockSegmentIndex_ = FindLastBlockSegment();
        YCHECK(LastBlockSegmentIndex_ >= CurrentSegmentIndex_);
    }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        YCHECK(rowIndex >= CurrentRowIndex_);

        CurrentRowIndex_ = rowIndex;
        int segmentIndex = FindSegmentByRow(rowIndex);

        YCHECK(segmentIndex >= CurrentSegmentIndex_);
        if (segmentIndex != CurrentSegmentIndex_) {
            CurrentSegmentIndex_ = segmentIndex;
            ResetSegmentReader();
        }
    }

    virtual i64 GetCurrentRowIndex() const override
    {
        return CurrentRowIndex_;
    }

    virtual i64 GetBlockUpperRowIndex() const override
    {
        if (LastBlockSegmentIndex_ < 0) {
            return 0;
        } else {
            return ColumnMeta_.segments(LastBlockSegmentIndex_).chunk_row_count();
        }
    }

    virtual i64 GetReadyUpperRowIndex() const override
    {
        if (CurrentSegmentIndex_ == ColumnMeta_.segments_size()) {
            return CurrentRowIndex_;
        } else {
            return CurrentSegmentMeta().chunk_row_count();
        }
    }

    virtual int GetCurrentBlockIndex() const override
    {
        return CurrentBlockIndex_;
    }

    virtual TNullable<int> GetNextBlockIndex() const override
    {
        return (LastBlockSegmentIndex_ + 1) == ColumnMeta_.segments_size()
           ? Null
           : MakeNullable(static_cast<int>(ColumnMeta_.segments(LastBlockSegmentIndex_ + 1).block_index()));
    }

protected:
    TSharedRef Block_;

    const NProto::TColumnMeta& ColumnMeta_;

    int CurrentBlockIndex_ = -1;
    int CurrentSegmentIndex_ = 0;
    i64 CurrentRowIndex_ = 0;

    //! Index of the last segment in current block.
    int LastBlockSegmentIndex_ = -1;


    virtual void ResetSegmentReader() = 0;

    const NProto::TSegmentMeta& CurrentSegmentMeta() const
    {
        return ColumnMeta_.segments(CurrentSegmentIndex_);
    }

    i64 GetSegmentStartRowIndex(int segmentIndex) const
    {
        auto meta = ColumnMeta_.segments(segmentIndex);
        return meta.chunk_row_count() - meta.row_count();
    }

    int FindFirstBlockSegment() const
    {
        auto it = std::lower_bound(
            ColumnMeta_.segments().begin() + CurrentSegmentIndex_,
            ColumnMeta_.segments().end(),
            CurrentBlockIndex_,
            [] (const NProto::TSegmentMeta& segmentMeta, int blockIndex) {
                return segmentMeta.block_index() < blockIndex;
            }
        );
        YCHECK(it != ColumnMeta_.segments().end());
        return std::distance(ColumnMeta_.segments().begin(), it);
    }

    int FindLastBlockSegment() const
    {
        auto it = std::upper_bound(
            ColumnMeta_.segments().begin() + CurrentSegmentIndex_,
            ColumnMeta_.segments().end(),
            CurrentBlockIndex_,
            [] (int blockIndex, const NProto::TSegmentMeta& segmentMeta) {
                return blockIndex < segmentMeta.block_index();
            }
        );

        YCHECK(it != ColumnMeta_.segments().begin());
        return std::distance(ColumnMeta_.segments().begin(), it - 1);
    }

    int FindSegmentByRow(i64 rowIndex) const
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
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedColumnReaderBase
    : public TColumnReaderBase
    , public IUnversionedColumnReader
{
public:
    TUnversionedColumnReaderBase(const NProto::TColumnMeta& columnMeta, int columnIndex, int columnId)
        : TColumnReaderBase(columnMeta)
        , ColumnIndex_(columnIndex)
        , ColumnId_(columnId)
    { }

    virtual void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        DoReadValues(rows);
    }

    virtual void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) override
    {
        DoReadValues(rows);
    }

protected:
    const int ColumnIndex_;
    const int ColumnId_;

    std::unique_ptr<IUnversionedSegmentReader> SegmentReader_;

    virtual std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool scan = true) = 0;

    template <class TSegmentReader>
    std::unique_ptr<IUnversionedSegmentReader> DoCreateSegmentReader(const NProto::TSegmentMeta& meta)
    {
        IUnversionedSegmentReader* reader = new TSegmentReader(
            TRef(Block_.Begin() + meta.offset(), meta.size()),
            meta,
            ColumnIndex_,
            ColumnId_);

        return std::unique_ptr<IUnversionedSegmentReader>(reader);
    }

    virtual void ResetSegmentReader() override
    {
        SegmentReader_.reset();
    }

    template <class TRow>
    void DoReadValues(TMutableRange<TRow> rows)
    {
        i64 readRowCount = 0;
        while (readRowCount < rows.Size()) {
            YCHECK(CurrentSegmentIndex_ <= LastBlockSegmentIndex_);
            if (!SegmentReader_) {
                SegmentReader_ = CreateSegmentReader(CurrentSegmentIndex_);
            }

            SegmentReader_->SkipToRowIndex(CurrentRowIndex_);

            i64 count = SegmentReader_->ReadValues(rows.Slice(rows.Begin() + readRowCount, rows.End()));

            readRowCount += count;
            CurrentRowIndex_ += count;

            if (SegmentReader_->EndOfSegment()) {
                SegmentReader_.reset();
                ++CurrentSegmentIndex_;
            }
        }
    }

    template <NTableClient::EValueType ValueType>
    std::pair<i64, i64> DoGetEqualRange(
        const NTableClient::TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex)
    {
        // Use lookup segment readers while GetEqualRange.
        YCHECK(lowerRowIndex <= upperRowIndex);

        if (lowerRowIndex == upperRowIndex) {
            return std::make_pair(lowerRowIndex, upperRowIndex);
        }

        int segmentLimit = FindSegmentByRow(upperRowIndex - 1);
        segmentLimit = std::min(segmentLimit, LastBlockSegmentIndex_);

        // Get lower limit for range.
        int lowerSegmentIndex = FindSegmentByRow(lowerRowIndex);
        auto lowerSegmentReader = CreateSegmentReader(lowerSegmentIndex, false);

        while (lowerSegmentIndex < segmentLimit &&
            CompareValues<ValueType>(lowerSegmentReader->GetLastValue(), value) < 0)
        {
            lowerSegmentReader = CreateSegmentReader(++lowerSegmentIndex, false);
        }

        if (lowerRowIndex > GetSegmentStartRowIndex(lowerSegmentIndex)) {
            lowerSegmentReader->SkipToRowIndex(lowerRowIndex);
        }

        lowerRowIndex = lowerSegmentReader->GetLowerRowIndex(value, upperRowIndex);

        // Get upper limit for range.
        int upperSegmentIndex = lowerSegmentIndex;
        auto upperSegmentReader = CreateSegmentReader(upperSegmentIndex, false);

        while (upperSegmentIndex < segmentLimit &&
            CompareValues<ValueType>(upperSegmentReader->GetLastValue(), value) <= 0)
        {
            upperSegmentReader = CreateSegmentReader(++upperSegmentIndex, false);
        }

        if (lowerRowIndex > GetSegmentStartRowIndex(upperSegmentIndex)) {
            upperSegmentReader->SkipToRowIndex(lowerRowIndex);
        }

        upperRowIndex = upperSegmentReader->GetUpperRowIndex(value, upperRowIndex);

        return std::make_pair(lowerRowIndex, upperRowIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedValueExtractorBase
{
public:
    TVersionedValueExtractorBase(bool aggregate);

    ui32 GetTimestampIndex(i64 valueIndex) const;

    bool GetAggregate(i64 valueIndex) const;

protected:
    const bool Aggregate_;

    TCompressedUnsignedVectorReader<ui32> TimestampIndexReader_;
    TReadOnlyBitmap<ui64> AggregateBitmap_;

    size_t InitTimestampIndexReader(const char* ptr);
};

////////////////////////////////////////////////////////////////////////////////

class TDenseVersionedValueExtractorBase
    : public TVersionedValueExtractorBase
{
public:
    TDenseVersionedValueExtractorBase(const NProto::TSegmentMeta& meta, bool aggregate);
        
    std::pair<ui32, ui32> GetValueIndexRange(i64 segmentRowIndex, ui32 lowerTimestampIndex);

    // For compaction read.
    std::pair<ui32, ui32> GetValueIndexRange(i64 segmentRowIndex);


    i64 GetLowerValueIndex(i64 segmentRowIndex) const;

    ui32 GetValueCount(i64 segmentRowIndex) const;

    size_t InitDenseReader(const char* ptr);
private:
    const NProto::TDenseVersionedSegmentMeta& DenseVersionedMeta_;
    TCompressedUnsignedVectorReader<ui32> ValuesPerRowDiffReader_;
};

////////////////////////////////////////////////////////////////////////////////

class TSparseVersionedValueExtractorBase
    : public TVersionedValueExtractorBase
{
public:
    TSparseVersionedValueExtractorBase(const NProto::TSegmentMeta& meta, bool aggregate);

    i64 GetLowerValueIndex(i64 segmentRowIndex, int valueIndex) const;

    i64 GetRowIndex(i64 valueIndex) const;
    
    i64 GetValueCount() const;

    std::pair<ui32, ui32> GetValueIndexRange(i64 segmentRowIndex, i64 valueIndex, ui32 lowerTimestampIndex);
    std::pair<ui32, ui32> GetValueIndexRange(i64 segmentRowIndex, i64 valueIndex);

protected:
    size_t InitSparseReader(const char* ptr);

private:
    TCompressedUnsignedVectorReader<ui64> RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValueExtractor>
class TVersionedSegmentReaderBase
    : public IVersionedSegmentReader
{
public:
    TVersionedSegmentReaderBase(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnId,
        bool aggregate)
        : Data_(data)
        , Meta_(meta)
        , Aggregate_(aggregate)
        , ColumnId_(columnId)
        , SegmentStartRowIndex_(meta.chunk_row_count() - meta.row_count())
        , ValueExtractor_(data, meta, aggregate)
    { }

    virtual bool EndOfSegment() const
    {
        return SegmentRowIndex_ == Meta_.row_count();
    }

protected:
    const TRef Data_;
    const NProto::TSegmentMeta& Meta_;
    const bool Aggregate_;
    const int ColumnId_;

    const i64 SegmentStartRowIndex_;

    i64 SegmentRowIndex_ = 0;

    TValueExtractor ValueExtractor_;


    i64 GetSegmentRowIndex(i64 rowIndex) const
    {
        return rowIndex - SegmentStartRowIndex_;
    }

    void DoSetValues(
        NTableClient::TMutableVersionedRow row,
        const std::pair<ui32, ui32>& timestampIndexRange,
        const std::pair<ui32, ui32>& valueIndexRange)
    {
        ui32 valueIndex = valueIndexRange.first;
        ui32 upperValueIndex = valueIndexRange.second;
        for (; valueIndex < upperValueIndex; ++valueIndex) {
            ui32 timestampIndex = ValueExtractor_.GetTimestampIndex(valueIndex);
            if (timestampIndex >= timestampIndexRange.second) {
                // Value in given timestamp range doesn't exist.
                return;
            }

            auto* value = row.BeginValues() + row.GetValueCount();
            row.SetValueCount(row.GetValueCount() + 1);
            value->Timestamp = timestampIndex;

            bool aggregate = ValueExtractor_.GetAggregate(valueIndex);
            ValueExtractor_.ExtractValue(value, valueIndex, ColumnId_, aggregate);

            if (!Aggregate_) {
                // If column is not aggregate we emit only first value.
                return;
            }
        }
    }

    void DoSetAllValues(
        NTableClient::TMutableVersionedRow row,
        const std::pair<ui32, ui32>& valueIndexRange)
    {
        ui32 valueIndex = valueIndexRange.first;
        ui32 upperValueIndex = valueIndexRange.second;
        for (; valueIndex < upperValueIndex; ++valueIndex) {
            auto* value = row.BeginValues() + row.GetValueCount();
            row.SetValueCount(row.GetValueCount() + 1);
            value->Timestamp = ValueExtractor_.GetTimestampIndex(valueIndex);
            bool aggregate = ValueExtractor_.GetAggregate(valueIndex);
            ValueExtractor_.ExtractValue(value, valueIndex, ColumnId_, aggregate);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValueExtractor>
class TDenseVersionedSegmentReader
    : public TVersionedSegmentReaderBase<TValueExtractor>
{
public:
    TDenseVersionedSegmentReader(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnId,
        bool aggregate)
        : TVersionedSegmentReaderBase<TValueExtractor>(data, meta, columnId, aggregate)
    { }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        YCHECK(GetSegmentRowIndex(rowIndex) >= SegmentRowIndex_);
        SegmentRowIndex_ = GetSegmentRowIndex(rowIndex);
    }

    virtual i64 ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows,
        TRange<std::pair<ui32, ui32>> timestampIndexRanges) override
    {
        YCHECK(rows.Size() == timestampIndexRanges.Size());

        i64 rangeRowIndex = 0;
        while (rangeRowIndex < rows.Size() && SegmentRowIndex_ < Meta_.row_count()) {
            auto row = rows[rangeRowIndex];
            if (row) {
                SetValues(row, timestampIndexRanges[rangeRowIndex]);
            }

            ++SegmentRowIndex_;
            ++rangeRowIndex;
        }
        return rangeRowIndex;
    }

    virtual i64 ReadAllValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        i64 rangeRowIndex = 0;
        while (rangeRowIndex < rows.Size() && SegmentRowIndex_ < Meta_.row_count()) {
            auto row = rows[rangeRowIndex];
            YCHECK(row);
            SetAllValues(row);

            ++SegmentRowIndex_;
            ++rangeRowIndex;
        }
        return rangeRowIndex;
    }

    void GetValueCounts(TMutableRange<ui32> valueCounts) const
    {
        YCHECK(SegmentRowIndex_ + valueCounts.Size() <= Meta_.row_count());

        for (i64 rangeRowIndex = 0; rangeRowIndex < valueCounts.Size(); ++rangeRowIndex) {
            valueCounts[rangeRowIndex] = ValueExtractor_.GetValueCount(SegmentRowIndex_ + rangeRowIndex);
        }
    }

private:
    using TVersionedSegmentReaderBase<TValueExtractor>::GetSegmentRowIndex;
    using TVersionedSegmentReaderBase<TValueExtractor>::ValueExtractor_;
    using TVersionedSegmentReaderBase<TValueExtractor>::SegmentRowIndex_;
    using TVersionedSegmentReaderBase<TValueExtractor>::Meta_;
    using TVersionedSegmentReaderBase<TValueExtractor>::DoSetValues;
    using TVersionedSegmentReaderBase<TValueExtractor>::DoSetAllValues;

    void SetValues(NTableClient::TMutableVersionedRow row, const std::pair<ui32, ui32>& timestampIndexRange)
    {
        auto valueIndexRange = ValueExtractor_.GetValueIndexRange(
            SegmentRowIndex_,
            timestampIndexRange.first);

        DoSetValues(row, timestampIndexRange, valueIndexRange);
    }

    void SetAllValues(NTableClient::TMutableVersionedRow row)
    {
        auto valueIndexRange = ValueExtractor_.GetValueIndexRange(SegmentRowIndex_);
        DoSetAllValues(row, valueIndexRange);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValueExtractor>
class TSparseVersionedSegmentReader
    : public TVersionedSegmentReaderBase<TValueExtractor>
{
public:
    TSparseVersionedSegmentReader(
        TRef data,
        const NProto::TSegmentMeta& meta,
        int columnId,
        bool aggregate)
        : TVersionedSegmentReaderBase<TValueExtractor>(data, meta, columnId, aggregate)
    { }

    virtual void SkipToRowIndex(i64 rowIndex) override
    {
        YCHECK(GetSegmentRowIndex(rowIndex) >= SegmentRowIndex_);

        if (GetSegmentRowIndex(rowIndex) > SegmentRowIndex_) {
            SegmentRowIndex_ = GetSegmentRowIndex(rowIndex);

            ValueIndex_ = ValueExtractor_.GetLowerValueIndex(SegmentRowIndex_, ValueIndex_);
        }
    }

    virtual i64 ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows,
        TRange<std::pair<ui32, ui32>> timestampIndexRanges) override
    {
        YCHECK(rows.Size() == timestampIndexRanges.Size());

        i64 rangeRowIndex = 0;
        while (rangeRowIndex < rows.Size() && SegmentRowIndex_ < Meta_.row_count()) {
            if (ValueIndex_ == ValueExtractor_.GetValueCount()) {
                // We reached the last value in the segment, left rows are empty.
                i64 rowsToSkip = std::min(
                    static_cast<i64>(rows.Size() - rangeRowIndex),
                    Meta_.row_count() - SegmentRowIndex_);

                SegmentRowIndex_ += rowsToSkip;
                rangeRowIndex += rowsToSkip;
                break;
            }

            if (SegmentRowIndex_ < ValueExtractor_.GetRowIndex(ValueIndex_)) {
                // Skip rows up to index of current value.
                i64 rowsToSkip = std::min(
                    static_cast<i64>(rows.Size() - rangeRowIndex),
                    ValueExtractor_.GetRowIndex(ValueIndex_) - SegmentRowIndex_);

                SegmentRowIndex_ += rowsToSkip;
                rangeRowIndex += rowsToSkip;
                continue;
            }

            YCHECK(SegmentRowIndex_ == ValueExtractor_.GetRowIndex(ValueIndex_));

            auto row = rows[rangeRowIndex];
            if (row){
                SetValues(row, timestampIndexRanges[rangeRowIndex]);
            }

            ++SegmentRowIndex_;
            ++rangeRowIndex;

            ValueIndex_ = ValueExtractor_.GetLowerValueIndex(SegmentRowIndex_, ValueIndex_);
        }
        return rangeRowIndex;
    }

    virtual i64 ReadAllValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        i64 rangeRowIndex = 0;
        while (rangeRowIndex < rows.Size() && SegmentRowIndex_ < Meta_.row_count()) {
            if (ValueIndex_ == ValueExtractor_.GetValueCount()) {
                // We reached the last value in the segment, left rows are empty.
                i64 rowsToSkip = std::min(
                    static_cast<i64>(rows.Size() - rangeRowIndex),
                    Meta_.row_count() - SegmentRowIndex_);

                SegmentRowIndex_ += rowsToSkip;
                rangeRowIndex += rowsToSkip;
                break;
            }

            if (SegmentRowIndex_ < ValueExtractor_.GetRowIndex(ValueIndex_)) {
                // Skip rows up to index of current value.
                i64 rowsToSkip = std::min(
                    static_cast<i64>(rows.Size() - rangeRowIndex),
                    ValueExtractor_.GetRowIndex(ValueIndex_) - SegmentRowIndex_);

                SegmentRowIndex_ += rowsToSkip;
                rangeRowIndex += rowsToSkip;
                continue;
            }

            YCHECK(SegmentRowIndex_ == ValueExtractor_.GetRowIndex(ValueIndex_));

            auto row = rows[rangeRowIndex];
            SetAllValues(row);

            ++SegmentRowIndex_;
            ++rangeRowIndex;

            ValueIndex_ = ValueExtractor_.GetLowerValueIndex(SegmentRowIndex_, ValueIndex_);
        }
        return rangeRowIndex;
    }

    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) const override
    {
        YCHECK(SegmentRowIndex_ + valueCounts.Size() <= Meta_.row_count());

        i64 rangeRowIndex = 0;
        i64 currentValueIndex = ValueIndex_;
        i64 currentRowIndex = SegmentRowIndex_;
        while (rangeRowIndex < valueCounts.Size()) {
            if (currentValueIndex == ValueExtractor_.GetValueCount()) {
                // We reached the last value in the segment, left rows are empty.
                for (; rangeRowIndex < valueCounts.Size(); ++rangeRowIndex) {
                    valueCounts[rangeRowIndex] = 0;
                }
                break;
            }

            if (currentRowIndex < ValueExtractor_.GetRowIndex(currentValueIndex)) {
                // Skip rows up to index of current value.
                for (;
                    currentRowIndex < ValueExtractor_.GetRowIndex(currentValueIndex) &&
                    rangeRowIndex < valueCounts.Size();
                    ++rangeRowIndex, ++currentRowIndex)
                {
                    valueCounts[rangeRowIndex] = 0;
                }
                continue;
            }

            YCHECK(currentRowIndex == ValueExtractor_.GetRowIndex(currentValueIndex));
            ui32 count = 0;
            while (currentValueIndex < ValueExtractor_.GetValueCount() && 
                currentRowIndex == ValueExtractor_.GetRowIndex(currentValueIndex)) 
            {
                ++count;
                ++currentValueIndex;
            }

            valueCounts[rangeRowIndex] = count;
            ++rangeRowIndex;
            ++currentRowIndex;
        }
    }

private:
    i64 ValueIndex_ = 0;

    using TVersionedSegmentReaderBase<TValueExtractor>::GetSegmentRowIndex;
    using TVersionedSegmentReaderBase<TValueExtractor>::ValueExtractor_;
    using TVersionedSegmentReaderBase<TValueExtractor>::SegmentRowIndex_;
    using TVersionedSegmentReaderBase<TValueExtractor>::Meta_;
    using TVersionedSegmentReaderBase<TValueExtractor>::DoSetValues;
    using TVersionedSegmentReaderBase<TValueExtractor>::DoSetAllValues;

    //! Returns number of values set (might be more than 1 if aggregate).
    void SetValues(NTableClient::TMutableVersionedRow row, const std::pair<ui32, ui32>& timestampIndexRange)
    {
        auto valueIndexRange = ValueExtractor_.GetValueIndexRange(
            SegmentRowIndex_,
            ValueIndex_,
            timestampIndexRange.first);

        DoSetValues(row, timestampIndexRange, valueIndexRange);
    }

    void SetAllValues(NTableClient::TMutableVersionedRow row)
    {
        auto valueIndexRange = ValueExtractor_.GetValueIndexRange(
            SegmentRowIndex_,
            ValueIndex_);

        DoSetAllValues(row, valueIndexRange);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedColumnReaderBase
    : public IVersionedColumnReader
    , public TColumnReaderBase
{
public:
    TVersionedColumnReaderBase(const NProto::TColumnMeta& columnMeta, int columnId, bool aggregate);

    virtual void GetValueCounts(TMutableRange<ui32> valueCounts) override;

    virtual void ReadValues(
        TMutableRange<NTableClient::TMutableVersionedRow> rows,
        TRange<std::pair<ui32, ui32>> timestampIndexRanges) override;

    virtual void ReadAllValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override;

protected:
    const int ColumnId_;
    const bool Aggregate_;

    std::unique_ptr<IVersionedSegmentReader> SegmentReader_;


    virtual std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) = 0;

    virtual void ResetSegmentReader() override;

    void EnsureSegmentReader();

    template <class TSegmentReader>
    std::unique_ptr<IVersionedSegmentReader> DoCreateSegmentReader(const NProto::TSegmentMeta& meta)
    {
        const char* segmentBegin = Block_.Begin() + meta.offset();
        return std::make_unique<TSegmentReader>(
            TRef(segmentBegin, segmentBegin + meta.size()),
            meta,
            ColumnId_,
            Aggregate_);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
