#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/unittests/column_format_helpers/column_format_helpers.h>

#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/compression/codec.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TSingleColumnWriter
{
public:
    using TWriterCreatorFunc = std::function<std::unique_ptr<IValueColumnWriter>(TDataBlockWriter*)>;
    explicit TSingleColumnWriter(TWriterCreatorFunc writerCreator);
    std::pair<TSharedRef, NProto::TColumnMeta> WriteSingleSegmentBlock(const std::vector<NTableClient::TUnversionedOwningRow>& rows);

private:
    TDataBlockWriter BlockWriter_;
    std::unique_ptr<IValueColumnWriter> ValueColumnWriter_;
    i64 RowCount_ = 0;
    i64 BlockIndex_ = 0;
};

class TSingleColumnReader
{
public:
    using TReaderCreatorFunc = std::function<
        std::unique_ptr<IUnversionedColumnReader>(const NProto::TColumnMeta&, int, int)
    >;
    explicit TSingleColumnReader(TReaderCreatorFunc readerCreator);

    std::vector<NTableClient::TUnversionedOwningRow> ReadBlock(const TSharedRef& data, const NProto::TColumnMeta& meta, ui16 columnId);

private:
    TReaderCreatorFunc ReaderCreatorFunc_;
};

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
    { }

    virtual void SetUp() override;

    NTableClient::TVersionedRow CreateRowWithValues(const std::vector<NTableClient::TVersionedValue>& values) const;

    void WriteSegment(IValueColumnWriter* columnWriter, const std::vector<NTableClient::TVersionedRow>& rows);

    void Validate(
        const std::vector<NTableClient::TVersionedRow>& original,
        int beginRowIndex,
        int endRowIndex,
        NTableClient::TTimestamp timestamp);

    void ValidateValues(const NTableClient::TVersionedValue& expected, const NTableClient::TVersionedValue& actual, i64 rowIndex);

    std::vector<NTableClient::TMutableVersionedRow> AllocateRows(int count);

    std::vector<NTableClient::TVersionedRow> GetExpectedRows(
        TRange<NTableClient::TVersionedRow> rows,
        NTableClient::TTimestamp timestamp) const;

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

    NTableClient::TUnversionedValue MakeValue(const std::optional<TValue>& value)
    {
        if (value) {
            return DoMakeUnversionedValue(*value, ColumnId);
        } else {
            return MakeUnversionedSentinelValue(NTableClient::EValueType::Null, ColumnId);
        }
    }

    std::vector<NTableClient::TVersionedRow> CreateRows(std::vector<std::optional<TValue>> values)
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

    void WriteSegment(IValueColumnWriter* columnWriter, std::vector<std::optional<TValue>> values)
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

    std::vector<std::optional<TValue>> MakeVector(int count, const TValue& value)
    {
        return std::vector<std::optional<TValue>>(count, value);
    }

    virtual void Write(IValueColumnWriter* columnWriter) = 0;
    virtual std::unique_ptr<IUnversionedColumnReader> CreateColumnReader() = 0;
    virtual std::unique_ptr<IValueColumnWriter> CreateColumnWriter(TDataBlockWriter* blockWriter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat

