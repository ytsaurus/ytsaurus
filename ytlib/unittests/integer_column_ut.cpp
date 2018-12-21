#include <yt/core/test_framework/framework.h>

#include "column_format_ut.h"

#include <yt/ytlib/table_chunk_format/integer_column_writer.h>
#include <yt/ytlib/table_chunk_format/integer_column_reader.h>
#include <yt/ytlib/table_chunk_format/private.h>
#include <yt/ytlib/table_chunk_format/public.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

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

    TVersionedIntegerColumnTest(TStored base, bool aggregate)
        : TVersionedColumnTestBase(aggregate)
        , Base(base)
    { }

    TVersionedValue MakeValue(const TValue& value) const
    {
        if (value.Data) {
            return DoMakeVersionedValue(
                *value.Data,
                value.Timestamp,
                ColumnId,
                Aggregate_ ? value.Aggregate : false);
        } else {
            return MakeVersionedSentinelValue(
                EValueType::Null,
                value.Timestamp,
                ColumnId,
                false);
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

    virtual void Write(IValueColumnWriter* columnWriter) override
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
        : TVersionedIntegerColumnTest<i64>(-1234, true)
    { }

    virtual std::unique_ptr<IVersionedColumnReader> CreateColumnReader() override
    {
        return CreateVersionedInt64ColumnReader(ColumnMeta_, ColumnId, Aggregate_);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedInt64ColumnWriter(ColumnId, Aggregate_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TAggregateVersionedInt64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedInt64ColumnTest
    : public TVersionedIntegerColumnTest<i64>
{
public:
    TSimpleVersionedInt64ColumnTest()
        : TVersionedIntegerColumnTest<i64>(-1234, false)
    { }

    virtual std::unique_ptr<IVersionedColumnReader> CreateColumnReader() override
    {
        return CreateVersionedInt64ColumnReader(ColumnMeta_, ColumnId, Aggregate_);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedInt64ColumnWriter(ColumnId, Aggregate_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TSimpleVersionedInt64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TAggregateVersionedUint64ColumnTest
    : public TVersionedIntegerColumnTest<ui64>
{
public:
    TAggregateVersionedUint64ColumnTest()
        : TVersionedIntegerColumnTest<ui64>(1234, true)
    { }

    virtual std::unique_ptr<IVersionedColumnReader> CreateColumnReader() override
    {
        return CreateVersionedUint64ColumnReader(ColumnMeta_, ColumnId, Aggregate_);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedUint64ColumnWriter(ColumnId, Aggregate_, blockWriter);
    }
};

DEFINE_VERSIONED_TESTS(TAggregateVersionedUint64ColumnTest)

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedUint64ColumnTest
    : public TVersionedIntegerColumnTest<ui64>
{
public:
    TSimpleVersionedUint64ColumnTest()
        : TVersionedIntegerColumnTest<ui64>(1234, false)
    { }

    virtual std::unique_ptr<IVersionedColumnReader> CreateColumnReader() override
    {
        return CreateVersionedUint64ColumnReader(ColumnMeta_, ColumnId, Aggregate_);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
    {
        return CreateVersionedUint64ColumnWriter(ColumnId, Aggregate_, blockWriter);
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

    TUnversionedIntegerColumnTestBase(TValue base)
        : Base_(base)
    { }

    using TUnversionedColumnTestBase<TValue>::ColumnMeta_;
    using TUnversionedColumnTestBase<TValue>::CreateRows;
    using TUnversionedColumnTestBase<TValue>::WriteSegment;
    using TUnversionedColumnTestBase<TValue>::Validate;

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

    void Write(IValueColumnWriter* columnWriter)
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

    void DoReadValues(int startRowIndex, int rowCount)
    {
        std::vector<std::optional<TValue>> expected;
        AppendVector(&expected, CreateDirectDense());
        AppendVector(&expected, CreateDirectRle());
        AppendVector(&expected, CreateDictionaryDense());
        AppendVector(&expected, CreateDictionaryRle());

        Validate(CreateRows(expected), startRowIndex, rowCount);
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

    virtual std::unique_ptr<IUnversionedColumnReader> CreateColumnReader() override
    {
        return CreateUnversionedInt64ColumnReader(ColumnMeta_, ColumnIndex, ColumnId);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
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
    DoReadValues(5000, 35000);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedUint64ColumnTest
    : public TUnversionedIntegerColumnTestBase<ui64>
{
protected:
    TUnversionedUint64ColumnTest()
        : TUnversionedIntegerColumnTestBase<ui64>(1234)
    { }

    virtual std::unique_ptr<IUnversionedColumnReader> CreateColumnReader() override
    {
        return CreateUnversionedUint64ColumnReader(ColumnMeta_, ColumnIndex, ColumnId);
    }

    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) override
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
    DoReadValues(5000, 35000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
