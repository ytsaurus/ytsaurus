#pragma once

#include "framework.h"

#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/compression/codec.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue DoMakeUnversionedValue(ui64 value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(i64 value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(double value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(Stroka value, int columnnId);
NTableClient::TUnversionedValue DoMakeUnversionedValue(bool value, int columnId);

NTableClient::TVersionedValue DoMakeVersionedValue(
    ui64 value,
    NTableClient::TTimestamp timestamp,
    int columnnId,
    bool aggregate);

NTableClient::TVersionedValue DoMakeVersionedValue(
    i64 value,
    NTableClient::TTimestamp timestamp,
    int columnnId,
    bool aggregate);

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp timestamp);

template<class T>
void AppendVector(std::vector<T>* data, const std::vector<T> toAppend)
{
    data->insert(data->end(), toAppend.begin(), toAppend.end());
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedColumnTestBase
    : public ::testing::Test
{
protected:
    NTableClient::TRowBufferPtr RowBuffer_ = New<NTableClient::TRowBuffer>();
    TSharedRef Data_;
    NProto::TColumnMeta ColumnMeta_;

    std::unique_ptr<IVersionedColumnReader> Reader_;

    TChunkedMemoryPool Pool_;
    bool Aggregate_;

    const int ColumnId = 0;
    const int MaxValueCount = 10;

    TVersionedColumnTestBase(bool aggregate)
        : Aggregate_(aggregate)
    {}

    virtual void SetUp() override
    {
        TDataBlockWriter blockWriter;
        auto columnWriter = CreateColumnWriter(&blockWriter);

        Write(columnWriter.get());

        auto block = blockWriter.DumpBlock(0, 8);
        auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
        Data_ = codec->Compress(block.Data);

        ColumnMeta_ = columnWriter->ColumnMeta();

        Reader_ = CreateColumnReader();
        Reader_->ResetBlock(Data_, 0);
    }

    NTableClient::TVersionedRow CreateRowWithValues(const std::vector<NTableClient::TVersionedValue>& values) const
    {
        NTableClient::TVersionedRowBuilder builder(RowBuffer_);

        for (const auto& value : values) {
            builder.AddValue(value);
        }

        return builder.FinishRow();
    }

    void WriteSegment(IValueColumnWriter* columnWriter, const std::vector<NTableClient::TVersionedRow>& rows)
    {
        columnWriter->WriteValues(MakeRange(rows));
        columnWriter->FinishCurrentSegment();
    }

    void Validate(
        const std::vector<NTableClient::TVersionedRow>& original,
        int beginRowIndex,
        int endRowIndex,
        NTableClient::TTimestamp timestamp)
    {
        auto actual = AllocateRows(endRowIndex - beginRowIndex);

        auto originalRange = TRange<NTableClient::TVersionedRow>(
            original.data() + beginRowIndex,
            original.data() + endRowIndex);

        auto expected = GetExpectedRows(originalRange, timestamp);

        auto timestampIndexRanges = GetTimestampIndexRanges(originalRange, timestamp);

        Reader_->SkipToRowIndex(beginRowIndex);
        Reader_->ReadValues(
            TMutableRange<NTableClient::TMutableVersionedRow>(actual.data(), actual.size()),
            MakeRange(timestampIndexRanges));


        ASSERT_EQ(expected.size(), actual.size());
        for (int rowIndex = 0; rowIndex < expected.size(); ++rowIndex) {
            NTableClient::TVersionedRow expectedRow = expected[rowIndex];
            NTableClient::TVersionedRow actualRow = actual[rowIndex];

            ASSERT_EQ(expectedRow.GetValueCount(), actualRow.GetValueCount()) << Format("Row index - %v", rowIndex);
            for (int valueIndex = 0; valueIndex < expectedRow.GetValueCount(); ++valueIndex) {
                ValidateValues(
                    expectedRow.BeginValues()[valueIndex],
                    actualRow.BeginValues()[valueIndex],
                    rowIndex);
            }
        }
    }

    void ValidateValues(const NTableClient::TVersionedValue& expected, const NTableClient::TVersionedValue& actual, i64 rowIndex)
    {
        ASSERT_EQ(expected.Aggregate, actual.Aggregate) << Format("Row index - %v", rowIndex);
        ASSERT_EQ(expected.Timestamp, actual.Timestamp) << Format("Row index - %v", rowIndex);
        ASSERT_EQ(0, NTableClient::CompareRowValues(expected, actual)) << Format("Row index - %v", rowIndex);
    }

    std::vector<NTableClient::TMutableVersionedRow> AllocateRows(int count)
    {
        std::vector<NTableClient::TMutableVersionedRow> rows;
        while (rows.size() < count) {
            rows.push_back(NTableClient::TMutableVersionedRow::Allocate(&Pool_, 0, MaxValueCount, 0, 0));
            rows.back().SetValueCount(0);
        }
        return rows;
    }

    std::vector<NTableClient::TVersionedRow> GetExpectedRows(
        TRange<NTableClient::TVersionedRow> rows,
        NTableClient::TTimestamp timestamp) const
    {
        std::vector<NTableClient::TVersionedRow> expected;
        for (auto row : rows) {
            // Find delete timestamp.
            NTableClient::TTimestamp deleteTimestamp = NTableClient::NullTimestamp;
            for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                if (*deleteIt <= timestamp) {
                    deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
                }
            }

            // Find values.
            std::vector<NTableClient::TVersionedValue> values;
            for (auto valueIt = row.BeginValues(); valueIt != row.EndValues(); ++valueIt) {
                if (valueIt->Id == ColumnId &&
                    valueIt->Timestamp <= timestamp &&
                    valueIt->Timestamp > deleteTimestamp)
                {
                    values.push_back(*valueIt);
                }
            }

            // Build row.
            NTableClient::TVersionedRowBuilder builder(RowBuffer_);
            for (const auto& value : values) {
                builder.AddValue(value);
                if (!Aggregate_) {
                    break;
                }
            }
            auto expectedRow = builder.FinishRow();

            // Replace timestamps with indexes.
            for (const auto* valueIt = expectedRow.BeginValues(); valueIt != expectedRow.EndValues(); ++valueIt) {
                for (ui32 timestampIndex = 0; timestampIndex < row.GetWriteTimestampCount(); ++timestampIndex) {
                    if (valueIt->Timestamp == row.BeginWriteTimestamps()[timestampIndex]) {
                        const_cast<NTableClient::TVersionedValue*>(valueIt)->Timestamp = timestampIndex;
                    }
                }
            }

            expected.push_back(expectedRow);
        }
        return expected;
    }

    virtual void Write(IValueColumnWriter* columnWriter) = 0;
    virtual std::unique_ptr<IVersionedColumnReader> CreateColumnReader() = 0;
    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TUnversionedColumnTestBase
    : public ::testing::Test
{
protected:
    NTableClient::TRowBufferPtr RowBuffer_ = New<NTableClient::TRowBuffer>();
    TSharedRef Data_;
    NProto::TColumnMeta ColumnMeta_;

    std::unique_ptr<IUnversionedColumnReader> Reader_;

    TChunkedMemoryPool Pool_;

    const int ColumnId = 0;
    const int ColumnIndex = 0;

    virtual void SetUp() override
    {
        TDataBlockWriter blockWriter;
        auto columnWriter = CreateColumnWriter(&blockWriter);

        Write(columnWriter.get());

        auto block = blockWriter.DumpBlock(0, 8);
        auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
        Data_ = codec->Compress(block.Data);

        ColumnMeta_ = columnWriter->ColumnMeta();

        Reader_ = CreateColumnReader();
        Reader_->ResetBlock(Data_, 0);
    }

    NTableClient::TUnversionedValue MakeValue(const TNullable<TValue>& value)
    {
        if (value) {
            return DoMakeUnversionedValue(*value, ColumnId);
        } else {
            return MakeUnversionedSentinelValue(NTableClient::EValueType::Null, ColumnId);
        }
    }

    std::vector<NTableClient::TVersionedRow> CreateRows(std::vector<TNullable<TValue>> values)
    {
        std::vector<NTableClient::TVersionedRow> rows;
        for (const auto& value : values) {
            auto row = NTableClient::TMutableVersionedRow::Allocate(&Pool_, 1, 0, 0, 0);
            *row.BeginKeys() = MakeValue(value);
            rows.push_back(row);
        }
        return rows;
    }

    std::vector<NTableClient::TMutableVersionedRow> AllocateRows(int count)
    {
        std::vector<NTableClient::TMutableVersionedRow> rows;
        for (int i = 0; i < count; ++i) {
            auto row = NTableClient::TMutableVersionedRow::Allocate(&Pool_, 1, 0, 0, 0);
            rows.push_back(row);
        }
        return rows;
    }

    void WriteSegment(IValueColumnWriter* columnWriter, std::vector<TNullable<TValue>> values)
    {
        auto rows = CreateRows(values);
        columnWriter->WriteValues(MakeRange(rows));
        columnWriter->FinishCurrentSegment();
    }

    void ValidateEqual(
        TRange<NTableClient::TVersionedRow> expected,
        const std::vector<NTableClient::TMutableVersionedRow>& actual)
    {
        EXPECT_EQ(expected.Size(), actual.size());

        for (int i = 0; i < expected.Size(); ++i) {
            NTableClient::TVersionedRow row = actual[i];
            const auto& actualValue = *row.BeginKeys();
            const auto& expectedValue = *expected[i].BeginKeys();

            EXPECT_EQ(expectedValue, actualValue) << "Row index " << i;
        }
    }

    void Validate(
        const std::vector<NTableClient::TVersionedRow>& expected,
        int startRowIndex,
        int rowCount)
    {
        auto actual = AllocateRows(rowCount);

        Reader_->SkipToRowIndex(startRowIndex);
        Reader_->ReadValues(TMutableRange<NTableClient::TMutableVersionedRow>(actual.data(), actual.size()));

        const auto* expectedBegin = expected.data() + startRowIndex;
        ValidateEqual(MakeRange(expectedBegin, expectedBegin + rowCount), actual);
    }

    std::vector<TNullable<TValue>> MakeVector(int count, const TValue& value)
    {
        return std::vector<TNullable<TValue>>(count, value);
    }

    virtual void Write(IValueColumnWriter* columnWriter) = 0;
    virtual std::unique_ptr<IUnversionedColumnReader> CreateColumnReader() = 0;
    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT

