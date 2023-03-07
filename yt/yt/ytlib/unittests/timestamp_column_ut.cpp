#include <yt/core/test_framework/framework.h>

#include "column_format_ut.h"

#include <yt/ytlib/table_chunk_format/timestamp_writer.h>
#include <yt/ytlib/table_chunk_format/timestamp_reader.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////
/*
 *  segment 1
 *  row 1 === 10 ==>
 *            del
 *
 *  row 2 == 10 == 20 ==>
 *           wr    del
 *
 *  segment 2
 *  row 3 == 10 == 20 == 30 == 40 == 50 == 60 ==>
 *           wr    del   wr    wr   del    wr
 */

class TTimestampColumnTest
    : public ::testing::Test
{
protected:
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    TSharedRef Data_;
    NProto::TColumnMeta ColumnMeta_;

    std::vector<TVersionedRow> CreateSegment1()
    {
        return std::vector<TVersionedRow> {
            CreateRowWithTimestamps({}, {10}),
            CreateRowWithTimestamps({10}, {20})
        };
    }

    std::vector<TVersionedRow> CreateSegment2()
    {
        return std::vector<TVersionedRow> {
            CreateRowWithTimestamps({10, 30, 40, 60}, {20, 50})
        };
    }

    std::vector<TVersionedRow> CreateOriginalRows()
    {
        std::vector<TVersionedRow> original;
        AppendVector(&original, CreateSegment1());
        AppendVector(&original, CreateSegment2());
        return original;
    }

    void SetUp()
    {
        TDataBlockWriter blockWriter;
        auto timestampWriter = CreateTimestampWriter(&blockWriter);

        // Write 3 rows with given timestamp records, split into 2 segments.
        timestampWriter->WriteTimestamps(MakeRange(CreateSegment1()));
        timestampWriter->FinishCurrentSegment();

        timestampWriter->WriteTimestamps(MakeRange(CreateSegment2()));
        auto block = blockWriter.DumpBlock(0 /* block index */, 3 /* row count */);

        auto* codec = GetCodec(ECodec::None);
        Data_ = codec->Compress(block.Data);
        ColumnMeta_ = timestampWriter->ColumnMeta();
    }

    TVersionedRow CreateRowWithTimestamps(
        std::vector<TTimestamp> writeTimestamps,
        std::vector<TTimestamp> deleteTimestamps)
    {
        TVersionedRowBuilder builder(RowBuffer_);

        for (auto deleteTimestamp : deleteTimestamps) {
            builder.AddDeleteTimestamp(deleteTimestamp);
        }

        for (auto writeTimestamp : writeTimestamps) {
            builder.AddValue(MakeVersionedSentinelValue(EValueType::Null, writeTimestamp));
        }

        return builder.FinishRow();
    }

    void TestScanReader(
        i64 beginRowIndex,
        i64 endRowIndex,
        TTimestamp timestamp)
    {
        TScanTransactionTimestampReader reader(ColumnMeta_, timestamp);
        reader.ResetBlock(Data_, 0);

        reader.SkipToRowIndex(beginRowIndex);

        i64 lowerRowIndex = beginRowIndex;
        i64 upperRowIndex = std::min(reader.GetReadyUpperRowIndex(), endRowIndex);
        while (lowerRowIndex < endRowIndex) {
            reader.PrepareRows(upperRowIndex - lowerRowIndex);

            // Validate ranges.
            auto actualRanges = reader.GetTimestampIndexRanges(upperRowIndex - lowerRowIndex);
            auto expectedRanges = GetExpectedIndexRanges(timestamp);
            ValidateEqual(
                MakeRange(expectedRanges.data() + lowerRowIndex, expectedRanges.data() + upperRowIndex),
                actualRanges);

            // Validate delete timestamps.
            auto expectedDeleteTimestamps = GetExpectedDeleteTimestamps(timestamp);
            for (int i = 0; i < actualRanges.Size(); ++i) {
                EXPECT_EQ(
                    reader.GetDeleteTimestamp(lowerRowIndex + i),
                    expectedDeleteTimestamps[lowerRowIndex + i]) << Format("Row index - %v", lowerRowIndex + i);
            }

            auto originalRows = CreateOriginalRows();
            // Validate write timestamps.
            for (int i = 0; i < actualRanges.Size(); ++i) {
                auto expectedRange = expectedRanges[lowerRowIndex + i];
                TTimestamp expectedWriteTimestamp = expectedRange.first == expectedRange.second
                    ? NullTimestamp
                    : originalRows[lowerRowIndex + i].BeginWriteTimestamps()[expectedRange.first];

                EXPECT_EQ(
                    expectedWriteTimestamp,
                    reader.GetWriteTimestamp(lowerRowIndex + i)) << Format("Row index - %v", lowerRowIndex + i);

                for (ui32 index = expectedRange.first; index < expectedRange.second; ++index) {
                    EXPECT_EQ(
                        originalRows[lowerRowIndex + i].BeginWriteTimestamps()[index],
                        reader.GetValueTimestamp(lowerRowIndex + i, index)) << Format("Row index - %v", lowerRowIndex + i);
                }
            }

            reader.SkipPreparedRows();
            lowerRowIndex = upperRowIndex;
            upperRowIndex = std::min(reader.GetReadyUpperRowIndex(), endRowIndex);
        }
    }

    template <class T>
    void ValidateEqual(TRange<T> expected, TRange<T> actual)
    {
        EXPECT_EQ(expected.Size(), actual.Size());

        for (int i = 0; i < expected.Size(); ++i) {
            EXPECT_EQ(expected[i], actual[i]);
        }
    }

    std::vector<std::pair<ui32, ui32>> GetExpectedIndexRanges(TTimestamp timestamp)
    {
        auto rows = CreateOriginalRows();

        std::vector<std::pair<ui32, ui32>> expected;
        auto deleteTimestamps = GetExpectedDeleteTimestamps(timestamp);
        for (int i = 0; i < rows.size(); ++i) {
            auto row = rows[i];
            auto deleteTimestamp = deleteTimestamps[i];

            ui32 lowerTimestampIndex = 0;
            while (lowerTimestampIndex < row.GetWriteTimestampCount() &&
                   row.BeginWriteTimestamps()[lowerTimestampIndex] > timestamp)
            {
                ++lowerTimestampIndex;
            }

            ui32 upperTimestampIndex = lowerTimestampIndex;
            while (upperTimestampIndex < row.GetWriteTimestampCount() &&
                   row.BeginWriteTimestamps()[upperTimestampIndex] > deleteTimestamp)
            {
                ++upperTimestampIndex;
            }

            expected.push_back(std::make_pair(lowerTimestampIndex, upperTimestampIndex));
        }

        return expected;
    }

    std::vector<TTimestamp> GetExpectedDeleteTimestamps(TTimestamp timestamp)
    {
        std::vector<TTimestamp> expected;
        for (auto row : CreateOriginalRows()) {
            // Find delete timestamp.
            TTimestamp deleteTimestamp = NullTimestamp;
            for (auto deleteIt = row.BeginDeleteTimestamps(); deleteIt != row.EndDeleteTimestamps(); ++deleteIt) {
                if (*deleteIt <= timestamp) {
                    deleteTimestamp = std::max(*deleteIt, deleteTimestamp);
                }
            }
            expected.push_back(deleteTimestamp);
        }
        return expected;
    }

/*
    void TestLookupReader(
        i64 rowIndex,
        TTimestamp timestamp,
        TTimestamp expectedDeleteTimestamp,
        TTimestamp expectedWriteTimestamp,
        std::pair<ui32, ui32> expectedTimestampIndexRange)
    {
        TLookupTransactionTimestampReader reader(&ColumnMeta_, timestamp);
        reader.ResetBlock(Data_, 0);
        EXPECT_TRUE(reader.SkipToRow(rowIndex));

        EXPECT_EQ(expectedDeleteTimestamp, reader.GetDeleteTimestamp());
        EXPECT_EQ(expectedWriteTimestamp, reader.GetWriteTimestamp());
        EXPECT_EQ(expectedTimestampIndexRange, reader.GetTimestampIndexRange());
    }
 */
};

TEST_F(TTimestampColumnTest, ScanAllRows)
{
    TestScanReader(0, 3, 5);
    TestScanReader(0, 3, 10);
    TestScanReader(0, 3, 15);
    TestScanReader(0, 3, 20);
    TestScanReader(0, 3, 45);
    TestScanReader(0, 3, MaxTimestamp);
}

// TODO(psushin): test lookup and compaction reader.

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
