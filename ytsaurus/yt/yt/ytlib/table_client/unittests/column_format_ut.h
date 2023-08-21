#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSingleColumnWriter
{
public:
    using TWriterCreatorFunc = std::function<std::unique_ptr<NTableChunkFormat::IValueColumnWriter>(
        NTableChunkFormat::TDataBlockWriter*)>;
    explicit TSingleColumnWriter(TWriterCreatorFunc writerCreator);
    std::pair<TSharedRef, NTableChunkFormat::NProto::TColumnMeta> WriteSingleSegmentBlock(
        const std::vector<TUnversionedOwningRow>& rows);

private:
    NTableChunkFormat::TDataBlockWriter BlockWriter_;
    std::unique_ptr<NTableChunkFormat::IValueColumnWriter> ValueColumnWriter_;
    i64 RowCount_ = 0;
    i64 BlockIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSingleColumnReader
{
public:
    using TReaderCreatorFunc = std::function<std::unique_ptr<NTableChunkFormat::IUnversionedColumnReader>(
        const NTableChunkFormat::NProto::TColumnMeta&,
        int,
        int,
        std::optional<ESortOrder>,
        const NTableClient::TColumnSchema& columnSchema)>;
    explicit TSingleColumnReader(TReaderCreatorFunc readerCreator);

    std::vector<TUnversionedOwningRow> ReadBlock(
        const TSharedRef& data,
        const NTableChunkFormat::NProto::TColumnMeta& meta,
        ui16 columnId);

private:
    TReaderCreatorFunc ReaderCreatorFunc_;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedColumnTestBase
    : public ::testing::Test
{
protected:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    TSharedRef Data_;
    NTableChunkFormat::NProto::TColumnMeta ColumnMeta_;

    TChunkedMemoryPool Pool_;

    TColumnSchema ColumnSchema_;

    static inline const auto NoAggregateSchema = TColumnSchema();
    static inline const auto AggregateSchema = TColumnSchema().SetAggregate("max");

    static constexpr int ColumnId = 0;
    static constexpr int MaxValueCount = 10;

    explicit TVersionedColumnTestBase(TColumnSchema columnSchema);

    void SetUp() override;

    std::unique_ptr<NTableChunkFormat::IVersionedColumnReader> CreateColumnReader();

    TVersionedRow CreateRowWithValues(const std::vector<TVersionedValue>& values) const;

    void WriteSegment(
        NTableChunkFormat::IValueColumnWriter* columnWriter,
        const std::vector<TVersionedRow>& rows);

    void Validate(
        const std::vector<TVersionedRow>& original,
        int beginRowIndex,
        int endRowIndex,
        TTimestamp timestamp);

    void ValidateValues(
        const TVersionedValue& expected,
        const TVersionedValue& actual,
        i64 rowIndex);

    std::vector<TMutableVersionedRow> AllocateRows(int count);

    std::vector<TVersionedRow> GetExpectedRows(
        TRange<TVersionedRow> rows,
        TTimestamp timestamp) const;

    virtual void Write(NTableChunkFormat::IValueColumnWriter* columnWriter) = 0;
    virtual std::unique_ptr<NTableChunkFormat::IVersionedColumnReader> DoCreateColumnReader() = 0;
    virtual std::unique_ptr<NTableChunkFormat::IValueColumnWriter> CreateColumnWriter(
        NTableChunkFormat::TDataBlockWriter* blockWriter) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TUnversionedColumnTestBase
    : public ::testing::Test
{
protected:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    TSharedRef Data_;
    NTableChunkFormat::NProto::TColumnMeta ColumnMeta_;

    TChunkedMemoryPool Pool_;

    static constexpr int ColumnId = 0;
    static constexpr int ColumnIndex = 0;

    void SetUp() override
    {
        NTableChunkFormat::TDataBlockWriter blockWriter;
        auto columnWriter = CreateColumnWriter(&blockWriter);

        Write(columnWriter.get());

        auto block = blockWriter.DumpBlock(0, 8);
        auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
        Data_ = codec->Compress(block.Data);

        ColumnMeta_ = columnWriter->ColumnMeta();
    }

    std::unique_ptr<NTableChunkFormat::IUnversionedColumnReader> CreateColumnReader()
    {
        auto reader = DoCreateColumnReader();
        reader->SetCurrentBlock(Data_, 0);
        return reader;
    }

    TUnversionedValue MakeValue(const std::optional<TValue>& value)
    {
        return NTableClient::ToUnversionedValue(value, RowBuffer_, ColumnId);
    }

    std::vector<TVersionedRow> CreateRows(const std::vector<std::optional<TValue>>& values)
    {
        std::vector<TVersionedRow> rows;
        for (const auto& value : values) {
            auto row = TMutableVersionedRow::Allocate(&Pool_, 1, 0, 0, 0);
            row.Keys()[0] = MakeValue(value);
            rows.push_back(row);
        }
        return rows;
    }

    std::vector<TMutableVersionedRow> AllocateRows(int count)
    {
        std::vector<TMutableVersionedRow> rows;
        for (int i = 0; i < count; ++i) {
            auto row = TMutableVersionedRow::Allocate(&Pool_, 1, 0, 0, 0);
            rows.push_back(row);
        }
        return rows;
    }

    void WriteSegment(NTableChunkFormat::IValueColumnWriter* columnWriter, std::vector<std::optional<TValue>> values)
    {
        auto rows = CreateRows(values);
        columnWriter->WriteVersionedValues(MakeRange(rows));
        columnWriter->FinishCurrentSegment();
    }

    void ValidateEqual(
        TRange<TVersionedRow> expected,
        const std::vector<TMutableVersionedRow>& actual)
    {
        EXPECT_EQ(expected.Size(), actual.size());

        for (int i = 0; i < std::ssize(expected); ++i) {
            TVersionedRow row = actual[i];
            const auto& actualValue = row.Keys()[0];
            const auto& expectedValue = expected[i].Keys()[0];

            EXPECT_EQ(expectedValue, actualValue) << "Row index " << i;
        }
    }

    void ValidateRows(
        const std::vector<TVersionedRow>& expected,
        int startRowIndex,
        int rowCount)
    {
        auto reader = CreateColumnReader();
        reader->SkipToRowIndex(startRowIndex);

        auto actual = AllocateRows(rowCount);
        reader->ReadValues(TMutableRange<TMutableVersionedRow>(actual.data(), actual.size()));

        const auto* expectedBegin = expected.data() + startRowIndex;
        ValidateEqual(MakeRange(expectedBegin, expectedBegin + rowCount), actual);
    }

    std::vector<std::optional<TValue>> MakeVector(int count, const TValue& value)
    {
        return std::vector<std::optional<TValue>>(count, value);
    }

    void ValidateSegmentPart(
        const std::vector<IUnversionedColumnarRowBatch::TColumn>& columns,
        const std::vector<std::optional<TValue>>& expected,
        int startIndex,
        int count)
    {
        const auto& primaryColumn = columns[0];
        for (int index = startIndex; index < startIndex + count; ++index) {
            EXPECT_EQ(expected[index], DecodeValueFromColumn(&primaryColumn, index - startIndex))
                << "Row index " << index;
        }
    }

    void ValidateColumn(
        const std::vector<std::optional<TValue>>& expected,
        int startRowIndex,
        int rowCount)
    {
        int currentRowIndex = startRowIndex;
        int endRowIndex = startRowIndex + rowCount;
        auto reader = CreateColumnReader();
        while (currentRowIndex < endRowIndex) {
            reader->SkipToRowIndex(currentRowIndex);
            reader->Rearm();
            i64 batchEndRowIndex = std::min(static_cast<int>(reader->GetReadyUpperRowIndex()), endRowIndex);
            std::vector<IUnversionedColumnarRowBatch::TColumn> columns;
            columns.resize(reader->GetBatchColumnCount());
            reader->ReadColumnarBatch(
                TMutableRange<IUnversionedColumnarRowBatch::TColumn>(columns.data(), columns.size()),
                batchEndRowIndex - currentRowIndex);
            ValidateSegmentPart(columns, expected, currentRowIndex, batchEndRowIndex - currentRowIndex);
            currentRowIndex = batchEndRowIndex;
        }
    }

    virtual void Write(NTableChunkFormat::IValueColumnWriter* columnWriter) = 0;
    virtual std::unique_ptr<NTableChunkFormat::IUnversionedColumnReader> DoCreateColumnReader() = 0;
    virtual std::unique_ptr<NTableChunkFormat::IValueColumnWriter> CreateColumnWriter(
        NTableChunkFormat::TDataBlockWriter* blockWriter) = 0;
    virtual std::optional<TValue> DecodeValueFromColumn(
        const IUnversionedColumnarRowBatch::TColumn* column,
        i64 index) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

