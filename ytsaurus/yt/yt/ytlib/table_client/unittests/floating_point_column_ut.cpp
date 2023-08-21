#include <yt/yt/core/test_framework/framework.h>

#include "column_format_ut.h"

#include <yt/yt/ytlib/table_chunk_format/floating_point_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/floating_point_column_reader.h>

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

class TVersionedFloatingPointColumnTest
    : public TVersionedColumnTestBase
    , public ::testing::WithParamInterface<ESimpleLogicalValueType>
{
protected:
    static constexpr TTimestamp TimestampBase = 1000000;

    struct TValue
    {
        std::optional<double> Data;
        TTimestamp Timestamp;
    };

    TVersionedFloatingPointColumnTest()
        : TVersionedColumnTestBase(NoAggregateSchema)
    { }

    TVersionedValue MakeValue(const TValue& value) const
    {
        if (value.Data) {
            return MakeVersionedDoubleValue(
                *value.Data,
                value.Timestamp,
                ColumnId);
        } else {
            return MakeVersionedSentinelValue(
                EValueType::Null,
                value.Timestamp,
                ColumnId);
        }
    }

    TVersionedRow CreateRow(const std::vector<TValue>& values) const
    {
        std::vector<TVersionedValue> versionedValues;
        for (const auto& value : values) {
            versionedValues.push_back(MakeValue(value));
        }
        return CreateRowWithValues(versionedValues);
    }

    void AppendExtremeRow(std::vector<TVersionedRow>* rows)
    {
        rows->push_back(CreateRow({
            {std::numeric_limits<double>::max(), TimestampBase},
            {std::numeric_limits<double>::min(), TimestampBase + 1},
            {std::nullopt, TimestampBase + 2}}));
    }

    std::vector<TVersionedRow> CreateDirectDense()
    {
        std::vector<TVersionedRow> rows;

        for (int i = 0; i < 100 * 100; ++i) {
            rows.push_back(CreateRow({
                {i * 3.25 , TimestampBase + i * 10},
                {i * 10 * 3.25, TimestampBase + (i + 2) * 10}}));
        }
        rows.push_back(CreateRowWithValues({}));
        AppendExtremeRow(&rows);
        return rows;
    }

    std::vector<TVersionedRow> CreateDirectSparse()
    {
        std::vector<TVersionedRow> rows;
        for (int i = 0; i < 1000; ++i) {
            rows.push_back(CreateRow({
                {i * 3.25, TimestampBase + i * 10},
                {i * 10 * 3.25, TimestampBase + (i + 2) * 10}}));

            for (int j = 0; j < 10; ++j) {
                rows.push_back(CreateRowWithValues({}));
            }
        }
        AppendExtremeRow(&rows);
        return rows;
    }

    std::vector<TVersionedRow> GetOriginalRows()
    {
        std::vector<TVersionedRow> expected;
        AppendVector(&expected, CreateDirectDense());
        AppendVector(&expected, CreateDirectSparse());
        return expected;
    }

    void Write(IValueColumnWriter* columnWriter) override
    {
        WriteSegment(columnWriter, CreateDirectDense());
        WriteSegment(columnWriter, CreateDirectSparse());
    }

    void DoReadValues(TTimestamp timestamp, int padding = 500)
    {
        auto originalRows = GetOriginalRows();
        Validate(originalRows, padding, originalRows.size() - padding, timestamp);
    }

    std::unique_ptr<IVersionedColumnReader> DoCreateColumnReader() override
    {
        switch (GetParam()) {
            case ESimpleLogicalValueType::Float:
                return CreateVersionedFloatingPointColumnReader<float>(ColumnMeta_, ColumnId, ColumnSchema_);
            case ESimpleLogicalValueType::Double:
                return CreateVersionedFloatingPointColumnReader<double>(ColumnMeta_, ColumnId, ColumnSchema_);
            default:
                YT_ABORT();
        }
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        switch (GetParam()) {
            case ESimpleLogicalValueType::Float:
                return CreateVersionedFloatingPointColumnWriter<float>(ColumnId, ColumnSchema_, blockWriter);
            case ESimpleLogicalValueType::Double:
                return CreateVersionedFloatingPointColumnWriter<double>(ColumnId, ColumnSchema_, blockWriter);
            default:
                YT_ABORT();
        }
    }
};

TEST_P(TVersionedFloatingPointColumnTest, ReadValues)
{
    DoReadValues(TimestampBase + 80000);
}

TEST_P(TVersionedFloatingPointColumnTest, ReadValuesMinTimestamp)
{
    DoReadValues(MinTimestamp);
}

TEST_P(TVersionedFloatingPointColumnTest, ReadValuesMaxTimestamp)
{
    DoReadValues(MaxTimestamp);
}

INSTANTIATE_TEST_SUITE_P(
    TVersionedFloatingPointColumnTest,
    TVersionedFloatingPointColumnTest,
    ::testing::Values(
        ESimpleLogicalValueType::Float,
        ESimpleLogicalValueType::Double));

////////////////////////////////////////////////////////////////////////////////

class TUnversionedFloatingPointSegmentTest
    : public TUnversionedColumnTestBase<double>
    , public ::testing::WithParamInterface<ESimpleLogicalValueType>
{
protected:
    std::optional<double> DecodeValueFromColumn(
        const IUnversionedColumnarRowBatch::TColumn* column,
        i64 index) override
    {
        index -= column->StartIndex;

        ResolveRleEncoding(column, index);

        if (column->NullBitmap && GetBit(*column->NullBitmap, index)) {
            return std::nullopt;
        }

        switch (GetParam()) {
            case ESimpleLogicalValueType::Float:
                return static_cast<double>(DecodeFloatingPointFromColumn<float>(*column, index));
            case ESimpleLogicalValueType::Double:
                return DecodeFloatingPointFromColumn<double>(*column, index);
            default:
                YT_ABORT();
        }
    }

    void Write(IValueColumnWriter* columnWriter) override
    {
        // Segment 1 - 5 values.
        WriteSegment(columnWriter, {std::nullopt, 1.0, 2.0, 3.0, 4.0});
        // Segment 2 - 1 value.
        WriteSegment(columnWriter, {5.0});
        // Segment 3 - 4 values.
        WriteSegment(columnWriter, {6.0, 7.0, 8.0, 9.0});
    }

    std::unique_ptr<IUnversionedColumnReader> DoCreateColumnReader() override
    {
        switch (GetParam()) {
            case ESimpleLogicalValueType::Float:
                return CreateUnversionedFloatingPointColumnReader<float>(ColumnMeta_, ColumnIndex, ColumnId, ESortOrder::Ascending, TColumnSchema());
            case ESimpleLogicalValueType::Double:
                return CreateUnversionedFloatingPointColumnReader<double>(ColumnMeta_, ColumnIndex, ColumnId, ESortOrder::Ascending, TColumnSchema());
            default:
                YT_ABORT();
        }
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        switch (GetParam()) {
            case ESimpleLogicalValueType::Float:
                return CreateUnversionedFloatingPointColumnWriter<float>(ColumnIndex, blockWriter);
            case ESimpleLogicalValueType::Double:
                return CreateUnversionedFloatingPointColumnWriter<double>(ColumnIndex, blockWriter);
            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TUnversionedFloatingPointSegmentTest, GetEqualRange)
{
    auto reader = CreateColumnReader();
    EXPECT_EQ(std::make_pair(8L, 8L), reader->GetEqualRange(MakeValue(7.5), 7, 8));
    EXPECT_EQ(std::make_pair(0L, 0L), reader->GetEqualRange(MakeValue(std::nullopt), 0, 0));
    EXPECT_EQ(std::make_pair(8L, 8L), reader->GetEqualRange(MakeValue(7.5), 2, 9));
}

TEST_P(TUnversionedFloatingPointSegmentTest, ReadValues)
{
    auto actual = AllocateRows(3);

    // Test null rows.
    actual.push_back(TMutableVersionedRow());

    auto reader = CreateColumnReader();
    reader->SkipToRowIndex(3);
    reader->ReadValues(TMutableRange<TMutableVersionedRow>(actual.data(), actual.size()));

    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(0, CompareRowValues(MakeValue(3.0 + i), actual[i].Keys()[0]));
    }
}

TEST_P(TUnversionedFloatingPointSegmentTest, ReadNull)
{
    auto reader = CreateColumnReader();
    auto rows = AllocateRows(3);
    reader->ReadValues(TMutableRange<TMutableVersionedRow>(rows.data(), rows.size()));
    EXPECT_EQ(MakeValue(std::nullopt), rows.front().Keys()[0]);
}

INSTANTIATE_TEST_SUITE_P(
    TUnversionedFloatingPointSegmentTest,
    TUnversionedFloatingPointSegmentTest,
    ::testing::Values(
        ESimpleLogicalValueType::Float,
        ESimpleLogicalValueType::Double));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
