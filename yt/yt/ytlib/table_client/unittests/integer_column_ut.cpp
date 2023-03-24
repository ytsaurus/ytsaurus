#include "column_format_ut.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/integer_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/integer_column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/helpers.h>
#include <yt/yt/ytlib/table_chunk_format/private.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

namespace NYT::NTableClient {
namespace {

using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

template <class TStored>
class TVersionedIntegerColumnTest
    : public TVersionedColumnTestBase
{
protected:
    const TStored Base;
    const TTimestamp TimestampBase = 1000000;

    struct TValue
    {
        std::optional<TStored> Data;
        TTimestamp Timestamp;
        bool Aggregate;
    };

    TVersionedIntegerColumnTest(TStored base, TColumnSchema columnSchema)
        : TVersionedColumnTestBase(std::move(columnSchema))
        , Base(base)
    { }

    TVersionedValue MakeValue(const TValue& value) const
    {
        return ToVersionedValue(
            value.Data,
            nullptr,
            value.Timestamp,
            ColumnId,
            ColumnSchema_.Aggregate() && value.Aggregate ? EValueFlags::Aggregate : EValueFlags::None);
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
            {std::numeric_limits<TStored>::max(), TimestampBase, false},
            {std::numeric_limits<TStored>::min(), TimestampBase + 1, true},
            {std::nullopt, TimestampBase + 2, false}}));
    }

    std::vector<TVersionedRow> CreateDirectDense()
    {
        std::vector<TVersionedRow> rows;

        for (int i = 0; i < 100 * 100; ++i) {
            rows.push_back(CreateRow({
                {Base + i , TimestampBase + i * 10, false},
                {Base + i * 10, TimestampBase + (i + 2) * 10, true}}));
        }
        rows.push_back(CreateRowWithValues({}));
        AppendExtremeRow(&rows);
        return rows;
    }

    std::vector<TVersionedRow> CreateDictionaryDense()
    {
        std::vector<TVersionedRow> rows;
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                rows.push_back(CreateRow({
                    {Base + j, TimestampBase + i * 10, true},
                    {std::nullopt, TimestampBase + (i + 1) * 10, false},
                    {Base + j * 10, TimestampBase + (i + 2) * 10, true}}));
            }
            rows.push_back(CreateRowWithValues({}));
        }
        AppendExtremeRow(&rows);
        return rows;
    }

    std::vector<TVersionedRow> CreateDirectSparse()
    {
        std::vector<TVersionedRow> rows;
        AppendExtremeRow(&rows);
        for (int i = 0; i < 1000; ++i) {
            rows.push_back(CreateRow({
                {Base + i, TimestampBase + i * 10, true},
                {Base + i * 10, TimestampBase + (i + 2) * 10, true}}));
            for (int j = 0; j < 10; ++j) {
                rows.push_back(CreateRowWithValues({}));
            }
        }
        // Finish segment with empty rows.
        return rows;
    }

    std::vector<TVersionedRow> CreateEmpty()
    {
        std::vector<TVersionedRow> rows;
        for (int i = 0; i < 100 * 100; ++i) {
            rows.push_back(CreateRowWithValues({}));
        }
        return rows;
    }

    std::vector<TVersionedRow> CreateDictionarySparse()
    {
        std::vector<TVersionedRow> rows;
        for (int i = 0; i < 1000; ++i) {
            rows.push_back(CreateRow({
                {Base + i, TimestampBase + i * 10, false},
                {std::nullopt, TimestampBase + (i + 2) * 10, false}}));
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
        AppendVector(&expected, CreateDictionaryDense());
        AppendVector(&expected, CreateDirectSparse());
        AppendVector(&expected, CreateDictionarySparse());
        AppendVector(&expected, CreateEmpty());
        return expected;
    }

    void Write(IValueColumnWriter* columnWriter) override
    {
        WriteSegment(columnWriter, CreateDirectDense());
        WriteSegment(columnWriter, CreateDictionaryDense());
        WriteSegment(columnWriter, CreateDirectSparse());
        WriteSegment(columnWriter, CreateDictionarySparse());
        WriteSegment(columnWriter, CreateEmpty());
    }

    void DoCheckSegmentType()
    {
        EXPECT_EQ(5, ColumnMeta_.segments_size());

        auto checkSegment = [&] (EVersionedIntegerSegmentType segmentType, int segmentIndex) {
            EXPECT_EQ(segmentType, EVersionedIntegerSegmentType(ColumnMeta_.segments(segmentIndex).type())) << Format("%lv", segmentType);
        };

        checkSegment(EVersionedIntegerSegmentType::DirectDense, 0);
        checkSegment(EVersionedIntegerSegmentType::DictionaryDense, 1);
        checkSegment(EVersionedIntegerSegmentType::DirectSparse, 2);
        checkSegment(EVersionedIntegerSegmentType::DictionarySparse, 3);
        checkSegment(EVersionedIntegerSegmentType::DirectDense, 4);
    }

    void DoReadValues(TTimestamp timestamp, int padding = 500)
    {
        auto originalRows = GetOriginalRows();
        Validate(originalRows, padding, originalRows.size() - padding, timestamp);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_VERSIONED_TESTS(TTest)          \
    TEST_F(TTest, CheckSegmentType)            \
    {                                          \
        DoCheckSegmentType();                  \
    }                                          \
    TEST_F(TTest, ReadValues)                  \
    {                                          \
        DoReadValues(TimestampBase + 80000);   \
    }                                          \
    TEST_F(TTest, ReadValuesMinTimestamp)      \
    {                                          \
        DoReadValues(MinTimestamp);            \
    }                                          \
    TEST_F(TTest, ReadValuesMaxTimestamp)      \
    {                                          \
        DoReadValues(MaxTimestamp);            \
    }

////////////////////////////////////////////////////////////////////////////////

class TAggregateVersionedInt64ColumnTest
    : public TVersionedIntegerColumnTest<i64>
{
public:
    TAggregateVersionedInt64ColumnTest()
        : TVersionedIntegerColumnTest<i64>(-1234, AggregateSchema)
    { }

    std::unique_ptr<IVersionedColumnReader> DoCreateColumnReader() override
    {
        return CreateVersionedInt64ColumnReader(ColumnMeta_, ColumnId, ColumnSchema_);
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedInt64ColumnWriter(ColumnId, ColumnSchema_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TAggregateVersionedInt64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedInt64ColumnTest
    : public TVersionedIntegerColumnTest<i64>
{
public:
    TSimpleVersionedInt64ColumnTest()
        : TVersionedIntegerColumnTest<i64>(-1234, NoAggregateSchema)
    { }

    std::unique_ptr<IVersionedColumnReader> DoCreateColumnReader() override
    {
        return CreateVersionedInt64ColumnReader(ColumnMeta_, ColumnId, ColumnSchema_);
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedInt64ColumnWriter(ColumnId, ColumnSchema_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TSimpleVersionedInt64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TAggregateVersionedUint64ColumnTest
    : public TVersionedIntegerColumnTest<ui64>
{
public:
    TAggregateVersionedUint64ColumnTest()
        : TVersionedIntegerColumnTest<ui64>(1234, AggregateSchema)
    { }

    std::unique_ptr<IVersionedColumnReader> DoCreateColumnReader() override
    {
        return CreateVersionedUint64ColumnReader(ColumnMeta_, ColumnId, ColumnSchema_);
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedUint64ColumnWriter(ColumnId, ColumnSchema_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TAggregateVersionedUint64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedUint64ColumnTest
    : public TVersionedIntegerColumnTest<ui64>
{
public:
    TSimpleVersionedUint64ColumnTest()
        : TVersionedIntegerColumnTest<ui64>(1234, NoAggregateSchema)
    { }

    std::unique_ptr<IVersionedColumnReader> DoCreateColumnReader() override
    {
        return CreateVersionedUint64ColumnReader(ColumnMeta_, ColumnId, ColumnSchema_);
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedUint64ColumnWriter(ColumnId, ColumnSchema_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TSimpleVersionedUint64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TUnversionedIntegerColumnTestBase
    : public TUnversionedColumnTestBase<TValue>
{
protected:
    const TValue Base_;

    explicit TUnversionedIntegerColumnTestBase(TValue base)
        : Base_(base)
    { }

    using TUnversionedColumnTestBase<TValue>::ColumnMeta_;
    using TUnversionedColumnTestBase<TValue>::CreateRows;
    using TUnversionedColumnTestBase<TValue>::WriteSegment;
    using TUnversionedColumnTestBase<TValue>::ValidateRows;
    using TUnversionedColumnTestBase<TValue>::ValidateColumn;
    using TUnversionedColumnTestBase<TValue>::CreateColumnReader;

    std::vector<std::optional<TValue>> CreateDirectDense()
    {
        std::vector<std::optional<TValue>> data;
        for (int i = 0; i < 100 * 100; ++i) {
            data.push_back(Base_ + i);
        }
        AppendExtremeValues(&data);
        return data;
    }

    std::vector<std::optional<TValue>> CreateDictionaryDense()
    {
        std::vector<std::optional<TValue>> data;
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                data.push_back(Base_ + j * 1024);
            }
        }
        AppendExtremeValues(&data);
        return data;
    }

    std::vector<std::optional<TValue>> CreateDictionaryRle()
    {
        std::vector<std::optional<TValue>> data;
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                data.push_back(Base_ + (j / 25) * 1024);
            }
            for (int j = 0; j < 2; ++j) {
                data.push_back(std::nullopt);
            }
        }
        AppendExtremeValues(&data);
        return data;
    }

    std::vector<std::optional<TValue>> CreateDirectRle()
    {
        std::vector<std::optional<TValue>> data;
        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 100; ++j) {
                data.push_back(Base_ + i);
            }
            data.push_back(std::nullopt);
        }
        return data;
    }

    std::optional<TValue> DecodeValueFromColumn(
        const IUnversionedColumnarRowBatch::TColumn* column,
        i64 index) override
    {
        YT_VERIFY(column->StartIndex >= 0);
        index += column->StartIndex;

        ResolveRleEncoding(column, index);

        if (!ResolveDictionaryEncoding(column, index)) {
            return std::nullopt;
        }

        if (IsColumnValueNull(column, index)) {
            return std::nullopt;
        }

        return DecodeIntegerFromColumn<TValue>(*column, index);
    }

    void Write(IValueColumnWriter* columnWriter) override
    {
        WriteSegment(columnWriter, CreateDirectDense());
        WriteSegment(columnWriter, CreateDirectRle());
        WriteSegment(columnWriter, CreateDictionaryDense());
        WriteSegment(columnWriter, CreateDictionaryRle());
    }

    void DoCheckSegmentTypes()
    {
        EXPECT_EQ(4, ColumnMeta_.segments_size());
        auto checkSegment = [&] (EUnversionedIntegerSegmentType segmentType, int segmentIndex) {
            EXPECT_EQ(segmentType, EUnversionedIntegerSegmentType(ColumnMeta_.segments(segmentIndex).type()));
        };

        checkSegment(EUnversionedIntegerSegmentType::DirectDense, 0);
        checkSegment(EUnversionedIntegerSegmentType::DirectRle, 1);
        checkSegment(EUnversionedIntegerSegmentType::DictionaryDense, 2);
        checkSegment(EUnversionedIntegerSegmentType::DictionaryRle, 3);
    }

    void DoReadValues(int startRowIndex, int rowCount, int step)
    {
        std::vector<std::optional<TValue>> expected;
        AppendVector(&expected, CreateDirectDense());
        AppendVector(&expected, CreateDirectRle());
        AppendVector(&expected, CreateDictionaryDense());
        AppendVector(&expected, CreateDictionaryRle());

        auto rows = CreateRows(expected);

        for (int rowIndex = startRowIndex;
            rowIndex < std::ssize(expected) && rowIndex + rowCount < std::ssize(expected);
            rowIndex += step)
        {
            ValidateRows(rows, rowIndex, rowCount);
            ValidateColumn(expected, rowIndex, rowCount);
        }
    }

    void AppendExtremeValues(std::vector<std::optional<TValue>>* values)
    {
        values->push_back(std::numeric_limits<TValue>::max());
        values->push_back(std::numeric_limits<TValue>::min());
        values->push_back(std::nullopt);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedInt64ColumnTest
    : public TUnversionedIntegerColumnTestBase<i64>
{
protected:
    TUnversionedInt64ColumnTest()
        : TUnversionedIntegerColumnTestBase<i64>(-12340000)
    { }

    std::unique_ptr<IUnversionedColumnReader> DoCreateColumnReader() override
    {
        return CreateUnversionedInt64ColumnReader(ColumnMeta_, ColumnIndex, ColumnId, std::nullopt, TColumnSchema());
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateUnversionedInt64ColumnWriter(
            ColumnIndex,
            blockWriter);
    }
};

TEST_F(TUnversionedInt64ColumnTest, CheckSegmentTypes)
{
    DoCheckSegmentTypes();
}

TEST_F(TUnversionedInt64ColumnTest, ReadValues)
{
    DoReadValues(0, 35000, 5000);
    DoReadValues(0, 5, 1);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedUint64ColumnTest
    : public TUnversionedIntegerColumnTestBase<ui64>
{
protected:
    TUnversionedUint64ColumnTest()
        : TUnversionedIntegerColumnTestBase<ui64>(1234)
    { }

    std::unique_ptr<IUnversionedColumnReader> DoCreateColumnReader() override
    {
        return CreateUnversionedUint64ColumnReader(ColumnMeta_, ColumnIndex, ColumnId, std::nullopt, TColumnSchema());
    }

    std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateUnversionedUint64ColumnWriter(
            ColumnIndex,
            blockWriter);
    }
};

TEST_F(TUnversionedUint64ColumnTest, CheckSegmentTypes)
{
    DoCheckSegmentTypes();
}

TEST_F(TUnversionedUint64ColumnTest, ReadValues)
{
    DoReadValues(0, 35000, 5000);
    DoReadValues(0, 5, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
