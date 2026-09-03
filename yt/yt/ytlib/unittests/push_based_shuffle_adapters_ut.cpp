#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer_adapter.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader_adapter.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NPushBasedShuffleClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MakeSchema()
{
    // The columns are optional, as they are in an intermediate schema derived from a
    // user table.
    return New<TTableSchema>(std::vector{
        TColumnSchema(
            "key",
            MakeLogicalType(ESimpleLogicalValueType::String, /*required*/ false),
            ESortOrder::Ascending),
        TColumnSchema(
            "value",
            MakeLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false)),
    });
}

////////////////////////////////////////////////////////////////////////////////

//! Records the rows handed to it and lets the test resolve their writes.
class TMockShuffleWriter
    : public IPushBasedShuffleWriter
{
public:
    TFuture<void> Write(TRange<TUnversionedRow> rows) override
    {
        for (auto row : rows) {
            Rows_.push_back(RowBuffer_->CaptureRow(row));
        }

        WritePromise_ = NewPromise<void>();
        return WritePromise_.ToFuture();
    }

    TFuture<void> Close() override
    {
        Closed_ = true;
        return OKFuture;
    }

    const std::vector<TUnversionedRow>& GetRows() const
    {
        return Rows_;
    }

    bool IsClosed() const
    {
        return Closed_;
    }

    void SetWriteDone()
    {
        WritePromise_.Set();
    }

private:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    std::vector<TUnversionedRow> Rows_;
    TPromise<void> WritePromise_;
    bool Closed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, RowsAreWrittenInSchemaOrder)
{
    auto schema = MakeSchema();
    auto underlyingWriter = New<TMockShuffleWriter>();
    auto adapter = CreateShuffleWriterAdapter(underlyingWriter, schema);

    auto rowBuffer = New<TRowBuffer>();
    // The value precedes the key, and the second row omits the value entirely.
    auto firstRow = rowBuffer->AllocateUnversioned(2);
    firstRow[0] = MakeUnversionedInt64Value(42, /*id*/ 1);
    firstRow[1] = MakeUnversionedStringValue("a", /*id*/ 0);

    auto secondRow = rowBuffer->AllocateUnversioned(1);
    secondRow[0] = MakeUnversionedStringValue("b", /*id*/ 0);

    std::vector<TUnversionedRow> rows{firstRow, secondRow};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));

    ASSERT_EQ(std::ssize(underlyingWriter->GetRows()), 2);

    auto writtenFirstRow = underlyingWriter->GetRows()[0];
    ASSERT_EQ(writtenFirstRow.GetCount(), 2u);
    EXPECT_EQ(writtenFirstRow[0].Id, 0);
    EXPECT_EQ(writtenFirstRow[0].AsStringBuf(), TStringBuf("a"));
    EXPECT_EQ(writtenFirstRow[1].Id, 1);
    EXPECT_EQ(writtenFirstRow[1].Data.Int64, 42);

    auto writtenSecondRow = underlyingWriter->GetRows()[1];
    ASSERT_EQ(writtenSecondRow.GetCount(), 2u);
    EXPECT_EQ(writtenSecondRow[0].AsStringBuf(), TStringBuf("b"));
    EXPECT_EQ(writtenSecondRow[1].Type, EValueType::Null);

    auto dataStatistics = adapter->GetDataStatistics();
    EXPECT_EQ(dataStatistics.row_count(), 2);
    EXPECT_GT(dataStatistics.data_weight(), 0);

    // A second write, issued once the first one has resolved.
    underlyingWriter->SetWriteDone();

    auto thirdRow = rowBuffer->AllocateUnversioned(1);
    thirdRow[0] = MakeUnversionedStringValue("c", /*id*/ 0);

    std::vector<TUnversionedRow> moreRows{thirdRow};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(moreRows)));

    ASSERT_EQ(std::ssize(underlyingWriter->GetRows()), 3);
    EXPECT_EQ(underlyingWriter->GetRows()[0][0].AsStringBuf(), TStringBuf("a"));
    EXPECT_EQ(underlyingWriter->GetRows()[1][0].AsStringBuf(), TStringBuf("b"));
    EXPECT_EQ(underlyingWriter->GetRows()[2][0].AsStringBuf(), TStringBuf("c"));

    EXPECT_EQ(adapter->GetDataStatistics().row_count(), 3);
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, WriteIsReadyOnlyAfterUnderlyingWrite)
{
    auto schema = MakeSchema();
    auto underlyingWriter = New<TMockShuffleWriter>();
    auto adapter = CreateShuffleWriterAdapter(underlyingWriter, schema);

    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(1);
    row[0] = MakeUnversionedStringValue("a", /*id*/ 0);

    std::vector<TUnversionedRow> rows{row};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));
    EXPECT_FALSE(adapter->GetReadyEvent().IsSet());

    underlyingWriter->SetWriteDone();
    EXPECT_TRUE(adapter->GetReadyEvent().IsSet());

    Y_UNUSED(adapter->Close());
    EXPECT_TRUE(underlyingWriter->IsClosed());
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, ValueOutsideOfSchemaIsRejected)
{
    auto schema = MakeSchema();
    auto underlyingWriter = New<TMockShuffleWriter>();
    auto adapter = CreateShuffleWriterAdapter(underlyingWriter, schema);

    auto rowBuffer = New<TRowBuffer>();
    auto badRow = rowBuffer->AllocateUnversioned(1);
    badRow[0] = MakeUnversionedStringValue("a", /*id*/ 5);

    std::vector<TUnversionedRow> rows{badRow};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));

    // The batch is rejected through the ready event rather than by throwing, and the
    // failure sticks.
    ASSERT_TRUE(adapter->GetReadyEvent().IsSet());
    EXPECT_FALSE(adapter->GetReadyEvent().GetOrCrash().IsOK());
    EXPECT_TRUE(underlyingWriter->GetRows().empty());

    auto goodRow = rowBuffer->AllocateUnversioned(1);
    goodRow[0] = MakeUnversionedStringValue("a", /*id*/ 0);

    std::vector<TUnversionedRow> goodRows{goodRow};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(goodRows)));
    EXPECT_FALSE(adapter->GetReadyEvent().GetOrCrash().IsOK());
    EXPECT_TRUE(underlyingWriter->GetRows().empty());

    // A lost batch must not be committed as a clean shuffle.
    auto closeResult = adapter->Close();
    ASSERT_TRUE(closeResult.IsSet());
    EXPECT_FALSE(closeResult.GetOrCrash().IsOK());
    EXPECT_FALSE(underlyingWriter->IsClosed());
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, DuplicateValueIdIsRejected)
{
    auto schema = MakeSchema();
    auto adapter = CreateShuffleWriterAdapter(New<TMockShuffleWriter>(), schema);

    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(2);
    row[0] = MakeUnversionedStringValue("a", /*id*/ 0);
    row[1] = MakeUnversionedStringValue("b", /*id*/ 0);

    std::vector<TUnversionedRow> rows{row};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));
    EXPECT_FALSE(adapter->GetReadyEvent().GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, ValueOfWrongTypeIsRejected)
{
    auto schema = MakeSchema();
    auto underlyingWriter = New<TMockShuffleWriter>();
    auto adapter = CreateShuffleWriterAdapter(underlyingWriter, schema);

    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(1);
    // The key column is a string.
    row[0] = MakeUnversionedInt64Value(42, /*id*/ 0);

    std::vector<TUnversionedRow> rows{row};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));
    EXPECT_FALSE(adapter->GetReadyEvent().GetOrCrash().IsOK());
    EXPECT_TRUE(underlyingWriter->GetRows().empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(ShuffleWriterAdapter, NullRowIsRejected)
{
    auto schema = MakeSchema();
    auto underlyingWriter = New<TMockShuffleWriter>();
    auto adapter = CreateShuffleWriterAdapter(underlyingWriter, schema);

    std::vector<TUnversionedRow> rows{TUnversionedRow()};
    EXPECT_FALSE(adapter->Write(TRange<TUnversionedRow>(rows)));
    EXPECT_FALSE(adapter->GetReadyEvent().GetOrCrash().IsOK());
    EXPECT_TRUE(underlyingWriter->GetRows().empty());
}

////////////////////////////////////////////////////////////////////////////////

//! Emits the prepared batches and then an empty one. A batch may be left pending, so that
//! the test resolves it.
class TMockSortReader
    : public ISortReader
{
public:
    explicit TMockSortReader(std::vector<TSharedRange<TUnversionedRow>> batches, bool pending = false)
        : Batches_(std::move(batches))
        , Pending_(pending)
    { }

    TFuture<TSharedRange<TUnversionedRow>> Read() override
    {
        auto batch = BatchIndex_ == std::ssize(Batches_)
            ? TSharedRange<TUnversionedRow>()
            : Batches_[BatchIndex_++];

        if (!Pending_) {
            return MakeFuture(batch);
        }

        ReadPromise_ = NewPromise<TSharedRange<TUnversionedRow>>();
        PendingBatch_ = std::move(batch);
        return ReadPromise_.ToFuture();
    }

    void SetReadDone()
    {
        ReadPromise_.Set(std::move(PendingBatch_));
    }

    void SetReadFailed(TError error)
    {
        ReadPromise_.Set(std::move(error));
    }

    void AddChunk(
        NChunkClient::TChunkId /*chunkId*/,
        NChunkClient::TChunkReplicaWithMediumList /*replicas*/,
        i64 /*startRecordIndex*/,
        std::optional<i64> /*rangeEndRecordIndex*/) override
    { }

    void SetNoMoreChunks() override
    { }

    void FinishAtCurrentCommittedRecordCount() override
    { }

private:
    const std::vector<TSharedRange<TUnversionedRow>> Batches_;
    const bool Pending_;

    i64 BatchIndex_ = 0;
    TPromise<TSharedRange<TUnversionedRow>> ReadPromise_;
    TSharedRange<TUnversionedRow> PendingBatch_;
};

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TUnversionedRow> MakeBatch(const TRowBufferPtr& rowBuffer, std::vector<TStringBuf> keys)
{
    std::vector<TUnversionedRow> rows;
    for (auto key : keys) {
        auto row = rowBuffer->AllocateUnversioned(1);
        row[0] = MakeUnversionedStringValue(key, /*id*/ 0);
        rows.push_back(row);
    }

    return MakeSharedRange(std::move(rows), rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

TEST(SortReaderAdapter, ReadsBatchesUntilExhausted)
{
    auto rowBuffer = New<TRowBuffer>();
    auto makeBatch = [&] (std::vector<TStringBuf> keys) {
        return MakeBatch(rowBuffer, std::move(keys));
    };

    auto schema = MakeSchema();
    auto adapter = CreateSortReaderAdapter(
        New<TMockSortReader>(std::vector{makeBatch({"a", "b"}), makeBatch({"c"})}),
        TNameTable::FromSchema(*schema),
        /*totalRowCount*/ 3);

    TRowBatchReadOptions options;

    auto firstBatch = adapter->Read(options);
    ASSERT_TRUE(firstBatch);
    EXPECT_EQ(std::ssize(firstBatch->MaterializeRows()), 2);

    auto secondBatch = adapter->Read(options);
    ASSERT_TRUE(secondBatch);
    EXPECT_EQ(std::ssize(secondBatch->MaterializeRows()), 1);

    // An empty range from the sort reader ends the stream.
    EXPECT_FALSE(adapter->Read(options));
    EXPECT_TRUE(adapter->IsFetchingCompleted());

    auto dataStatistics = adapter->GetDataStatistics();
    EXPECT_EQ(dataStatistics.row_count(), 3);
    EXPECT_EQ(adapter->GetTotalRowCount(), 3);
}

////////////////////////////////////////////////////////////////////////////////

TEST(SortReaderAdapter, PendingReadYieldsEmptyBatch)
{
    auto rowBuffer = New<TRowBuffer>();
    auto schema = MakeSchema();
    auto underlyingReader = New<TMockSortReader>(
        std::vector{MakeBatch(rowBuffer, {"a"})},
        /*pending*/ true);
    auto adapter = CreateSortReaderAdapter(
        underlyingReader,
        TNameTable::FromSchema(*schema),
        /*totalRowCount*/ 1);

    TRowBatchReadOptions options;

    // While the sort reader is working the adapter has nothing to return, and the caller
    // is told to wait.
    auto pendingBatch = adapter->Read(options);
    ASSERT_TRUE(pendingBatch);
    EXPECT_TRUE(pendingBatch->IsEmpty());
    EXPECT_FALSE(adapter->GetReadyEvent().IsSet());

    underlyingReader->SetReadDone();
    EXPECT_TRUE(adapter->GetReadyEvent().IsSet());

    auto batch = adapter->Read(options);
    ASSERT_TRUE(batch);
    EXPECT_EQ(std::ssize(batch->MaterializeRows()), 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(SortReaderAdapter, FailedReadIsSticky)
{
    auto rowBuffer = New<TRowBuffer>();
    auto schema = MakeSchema();
    auto underlyingReader = New<TMockSortReader>(
        std::vector{MakeBatch(rowBuffer, {"a"})},
        /*pending*/ true);
    auto adapter = CreateSortReaderAdapter(
        underlyingReader,
        TNameTable::FromSchema(*schema),
        /*totalRowCount*/ 1);

    TRowBatchReadOptions options;

    auto pendingBatch = adapter->Read(options);
    ASSERT_TRUE(pendingBatch);
    EXPECT_TRUE(pendingBatch->IsEmpty());

    underlyingReader->SetReadFailed(TError("Sort reader failed"));

    // The error is reported through the ready event, and the reader stays on it.
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto batch = adapter->Read(options);
        ASSERT_TRUE(batch);
        EXPECT_TRUE(batch->IsEmpty());

        auto readyEvent = adapter->GetReadyEvent();
        ASSERT_TRUE(readyEvent.IsSet());
        EXPECT_FALSE(readyEvent.GetOrCrash().IsOK());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPushBasedShuffleClient
