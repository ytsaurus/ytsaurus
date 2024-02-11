#include "null_column_reader.h"

#include "column_reader_detail.h"
#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TNullValueExtractor
{
public:
    TNullValueExtractor(
        TRef /*data*/,
        const TSegmentMeta& /*meta*/)
    { }

    void ExtractValue(TUnversionedValue* value, i64 /*valueIndex*/, int id, EValueFlags flags) const
    {
        YT_ASSERT(None(flags));
        *value = MakeUnversionedNullValue(id, flags);
    }

    int GetBatchColumnCount()
    {
        return 1;
    }

    void ReadColumnarBatch(
        i64 /*startRowIndex*/,
        i64 rowCount,
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 1);
        columns[0].ValueCount = rowCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedNullColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    using TUnversionedColumnReaderBase::TUnversionedColumnReaderBase;

    std::pair<i64, i64> GetEqualRange(
        const TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        return DoGetEqualRange<EValueType::Null>(
            value,
            lowerRowIndex,
            upperRowIndex);
    }

    i64 EstimateDataWeight(i64 /*lowerRowIndex*/, i64 /*upperRowIndex*/) override
    {
        return 0;
    }

private:
    std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool /*scan*/) override
    {
        using TSegmentReader = TDenseUnversionedSegmentReader<
            EValueType::Null,
            TNullValueExtractor>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        return DoCreateSegmentReader<TSegmentReader>(meta);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlocklessUnversionedNullColumnReader
    : public IUnversionedColumnReader
{
public:
    TBlocklessUnversionedNullColumnReader(int columnIndex, int id, std::optional<ESortOrder> sortOrder)
        : ColumnIndex_(columnIndex)
        , ColumnId_(id)
        , SortOrder_(sortOrder)
    { }

    void SetCurrentBlock(TSharedRef /*block*/, int /*blockIndex*/) override
    {
        YT_ABORT();
    }

    void Rearm() override
    { }

    void SkipToRowIndex(i64 rowIndex) override
    {
        RowIndex_ = rowIndex;
    }

    i64 GetCurrentRowIndex() const override
    {
        return RowIndex_;
    }

    i64 GetBlockUpperRowIndex() const override
    {
        return std::numeric_limits<i64>::max();
    }

    i64 GetReadyUpperRowIndex() const override
    {
        return GetBlockUpperRowIndex();
    }

    int GetCurrentBlockIndex() const override
    {
        YT_ABORT();
    }

    int GetCurrentSegmentIndex() const override
    {
        YT_ABORT();
    }

    std::optional<int> GetNextBlockIndex() const override
    {
        return std::nullopt;
    }

    std::pair<i64, i64> GetEqualRange(const TUnversionedValue& value, i64 lowerRowIndex, i64 upperRowIndex) override
    {
        YT_VERIFY(SortOrder_);

        bool less = value.Type < EValueType::Null;
        if (SortOrder_ == ESortOrder::Descending) {
            less ^= true;
        }

        if (less) {
            return std::pair(lowerRowIndex, lowerRowIndex);
        } else if (value.Type == EValueType::Null) {
            return std::pair(lowerRowIndex, upperRowIndex);
        } else {
            return std::pair(upperRowIndex, upperRowIndex);
        }
    }

    void ReadValues(TMutableRange<NTableClient::TMutableVersionedRow> rows) override
    {
        DoReadValues(rows);
    }

    void ReadValues(TMutableRange<NTableClient::TMutableUnversionedRow> rows) override
    {
        DoReadValues(rows);
    }

    int GetBatchColumnCount() override
    {
        return 1;
    }

    void ReadColumnarBatch(
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns,
        i64 rowCount) override
    {
        YT_VERIFY(columns.size() == 1);
        auto& column = columns[0];
        column.Type = SimpleLogicalType(ESimpleLogicalValueType::Null);
        column.ValueCount = rowCount;
    }

    i64 EstimateDataWeight(i64 /*lowerRowIndex*/, i64 /*upperRowIndex*/) override
    {
        return 0;
    }

private:
    const int ColumnIndex_;
    const int ColumnId_;
    const std::optional<ESortOrder> SortOrder_;

    i64 RowIndex_ = 0;

    template <class TRow>
    void DoReadValues(TMutableRange<TRow> rows)
    {
        for (auto row : rows) {
            if (row) {
                GetUnversionedValue(row, ColumnIndex_) = MakeUnversionedSentinelValue(EValueType::Null, ColumnId_);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedNullColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedNullColumnReader>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateBlocklessUnversionedNullColumnReader(
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder)
{
    return std::make_unique<TBlocklessUnversionedNullColumnReader>(
        columnIndex,
        columnId,
        sortOrder);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
