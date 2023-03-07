#include "column_format_ut.h"

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSingleColumnWriter::TSingleColumnWriter(TWriterCreatorFunc writerCreator)
    : ValueColumnWriter_(writerCreator(&BlockWriter_))
{ }

std::pair<TSharedRef, NProto::TColumnMeta> TSingleColumnWriter::WriteSingleSegmentBlock(
    const std::vector<TUnversionedOwningRow>& rows)
{
    std::vector<TUnversionedRow> nonOwningRows;
    nonOwningRows.reserve(rows.size());

    for (auto row : rows) {
        nonOwningRows.emplace_back(row);
    }

    ValueColumnWriter_->WriteUnversionedValues(nonOwningRows);
    ValueColumnWriter_->FinishCurrentSegment();
    RowCount_ += rows.size();

    auto block = BlockWriter_.DumpBlock(BlockIndex_, RowCount_);
    auto* codec = NCompression::GetCodec(NCompression::ECodec::None);
    const auto data = codec->Compress(block.Data);
    auto columnMeta = ValueColumnWriter_->ColumnMeta();

    return std::pair(data, columnMeta);
}

TSingleColumnReader::TSingleColumnReader(TReaderCreatorFunc readerCreator)
     : ReaderCreatorFunc_(readerCreator)
{ }

std::vector<TUnversionedOwningRow> TSingleColumnReader::ReadBlock(const TSharedRef& data, const NProto::TColumnMeta& meta, ui16 columnId)
{
    auto reader = ReaderCreatorFunc_(meta, 0, columnId);
    reader->ResetBlock(data, 0);
    i64 totalRowCount = 0;
    for (const auto& segment : meta.segments()) {
        totalRowCount += segment.row_count();
    }

    TChunkedMemoryPool pool;
    std::vector<TMutableUnversionedRow> mutableRows;
    mutableRows.reserve(totalRowCount);
    for (auto i = 0; i < totalRowCount; ++i) {
        mutableRows.push_back(TMutableUnversionedRow::Allocate(&pool, 1));
    }

    reader->ReadValues(TMutableRange(mutableRows));

    std::vector<TUnversionedOwningRow> rows;
    rows.reserve(mutableRows.size());
    for (const auto& r : mutableRows) {
        rows.emplace_back(r);
    }
    return rows;
}

////////////////////////////////////////////////////////////////////////////////

void TVersionedColumnTestBase::SetUp()
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

TVersionedRow TVersionedColumnTestBase::CreateRowWithValues(const std::vector<TVersionedValue>& values) const
{
    TVersionedRowBuilder builder(RowBuffer_);

    for (const auto& value : values) {
        builder.AddValue(value);
    }

    return builder.FinishRow();
}

void TVersionedColumnTestBase::WriteSegment(IValueColumnWriter* columnWriter, const std::vector<TVersionedRow>& rows)
{
    columnWriter->WriteValues(MakeRange(rows));
    columnWriter->FinishCurrentSegment();
}

void TVersionedColumnTestBase::Validate(
    const std::vector<TVersionedRow>& original,
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
        MakeRange(timestampIndexRanges),
        false);


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

void TVersionedColumnTestBase::ValidateValues(const TVersionedValue& expected, const TVersionedValue& actual, i64 rowIndex)
{
    ASSERT_EQ(expected.Aggregate, actual.Aggregate) << Format("Row index - %v", rowIndex);
    ASSERT_EQ(expected.Timestamp, actual.Timestamp) << Format("Row index - %v", rowIndex);
    ASSERT_EQ(0, CompareRowValues(expected, actual)) << Format("Row index - %v", rowIndex);
}

std::vector<TMutableVersionedRow> TVersionedColumnTestBase::AllocateRows(int count)
{
    std::vector<TMutableVersionedRow> rows;
    while (rows.size() < count) {
        rows.push_back(TMutableVersionedRow::Allocate(&Pool_, 0, MaxValueCount, 0, 0));
        rows.back().SetValueCount(0);
    }
    return rows;
}

std::vector<TVersionedRow> TVersionedColumnTestBase::GetExpectedRows(
    TRange<TVersionedRow> rows,
    TTimestamp timestamp) const
{
    std::vector<TVersionedRow> expected;
    for (auto row : rows) {
        // Find delete timestamp.
        TTimestamp deleteTimestamp = NullTimestamp;
        for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
            if (*deleteIt <= timestamp) {
                deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
            }
        }

        // Find values.
        std::vector<TVersionedValue> values;
        for (auto valueIt = row.BeginValues(); valueIt != row.EndValues(); ++valueIt) {
            if (valueIt->Id == ColumnId &&
                valueIt->Timestamp <= timestamp &&
                valueIt->Timestamp > deleteTimestamp)
            {
                values.push_back(*valueIt);
            }
        }

        // Build row.
        TVersionedRowBuilder builder(RowBuffer_);
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
                    const_cast<TVersionedValue*>(valueIt)->Timestamp = timestampIndex;
                }
            }
        }

        expected.push_back(expectedRow);
    }
    return expected;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
