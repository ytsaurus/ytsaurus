#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/partition_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/public.h>
#include <yt/yt/ytlib/push_based_shuffle_client/record_format.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/threading/event_count.h>

#include <algorithm>
#include <random>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

ISortReaderPtr CreateSortReaderForTesting(
    TSortReaderConfigPtr config,
    IPushBasedPartitionReaderPtr underlyingReader,
    TComparator comparator,
    TSortReaderMode mode,
    IInvokerPtr invoker,
    IInvokerPtr sortInvoker);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int KeyColumnId = 0;
constexpr int PayloadColumnId = 1;
constexpr int MapperIdColumnId = 10;
constexpr int RowIdColumnId = 11;

////////////////////////////////////////////////////////////////////////////////

class TMockPartitionReader
    : public IPushBasedPartitionReader
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, AddChunkCallCount);
    DEFINE_BYVAL_RO_PROPERTY(int, SetNoMoreChunksCallCount);
    DEFINE_BYVAL_RO_PROPERTY(bool, ReadCanceled);

    TFuture<TShuffleReadBatchPtr> Read() override
    {
        YT_VERIFY(!PendingPromise_);
        PendingPromise_ = NewPromise<TShuffleReadBatchPtr>();
        PendingPromise_.OnCanceled(BIND_NO_PROPAGATE(
            &TMockPartitionReader::OnReadCanceled,
            MakeWeak(this)));
        return PendingPromise_.ToFuture();
    }

    void AddChunk(
        TChunkId /*chunkId*/,
        TChunkReplicaWithMediumList /*replicas*/,
        i64 /*startRecordIndex*/,
        std::optional<i64> /*rangeEndRecordIndex*/) override
    {
        ++AddChunkCallCount_;
    }

    void SetNoMoreChunks() override
    {
        ++SetNoMoreChunksCallCount_;
    }

    bool HasPendingRead() const
    {
        return static_cast<bool>(PendingPromise_);
    }

    void FulfillRead(TShuffleReadBatchPtr batch)
    {
        YT_VERIFY(PendingPromise_);
        auto promise = std::move(PendingPromise_);
        promise.Set(std::move(batch));
    }

    void FailRead(const TError& error)
    {
        YT_VERIFY(PendingPromise_);
        auto promise = std::move(PendingPromise_);
        promise.Set(error);
    }

private:
    TPromise<TShuffleReadBatchPtr> PendingPromise_;

    void OnReadCanceled(const TError& error) noexcept
    {
        ReadCanceled_ = true;
        auto promise = std::move(PendingPromise_);
        promise.Set(TError(NYT::EErrorCode::Canceled, "Mock read canceled") << error);
    }
};

using TMockPartitionReaderPtr = TIntrusivePtr<TMockPartitionReader>;

////////////////////////////////////////////////////////////////////////////////

struct TSortReaderTestTag
{ };

class TSortReaderTest
    : public ::testing::Test
{
protected:
    TSortReaderTest()
        : ActionQueue_(New<TActionQueue>("SortReaderTest"))
        , RowBuffer_(New<TRowBuffer>(TSortReaderTestTag()))
        , MockReader_(New<TMockPartitionReader>())
    { }

    void TearDown() override
    {
        ActionQueue_->Shutdown(/*graceful*/ true);
    }

    IInvokerPtr GetInvoker() const
    {
        return ActionQueue_->GetInvoker();
    }

    TSortReaderConfigPtr MakeConfig(
        int bucketRowCount = 10'000,
        i64 maxRowsPerRead = 10'000,
        i64 maxDataWeightPerRead = 16_MB)
    {
        auto config = New<TSortReaderConfig>();
        config->BucketRowCount = bucketRowCount;
        config->MaxRowsPerRead = maxRowsPerRead;
        config->MaxDataWeightPerRead = maxDataWeightPerRead;
        return config;
    }

    TComparator MakeComparator(int length = 1, ESortOrder sortOrder = ESortOrder::Ascending)
    {
        return TComparator(std::vector<ESortOrder>(length, sortOrder));
    }

    ISortReaderPtr MakeIdentityFreeReader(
        TSortReaderConfigPtr config = {},
        TComparator comparator = {})
    {
        return CreateSortReaderForTesting(
            config ? std::move(config) : MakeConfig(),
            MockReader_,
            comparator.GetLength() > 0 ? std::move(comparator) : MakeComparator(),
            TValidMapperIds{},
            GetInvoker(),
            GetInvoker());
    }

    ISortReaderPtr MakeIdentityPreservingReader(
        TSortReaderConfigPtr config = {},
        TComparator comparator = {})
    {
        return CreateSortReaderForTesting(
            config ? std::move(config) : MakeConfig(),
            MockReader_,
            comparator.GetLength() > 0 ? std::move(comparator) : MakeComparator(),
            TIdentityColumnIds{
                .MapperId = MapperIdColumnId,
                .RowId = RowIdColumnId,
            },
            GetInvoker(),
            GetInvoker());
    }

    TUnversionedRow MakeRow(i64 key, i64 payload)
    {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(key, KeyColumnId));
        builder.AddValue(MakeUnversionedInt64Value(payload, PayloadColumnId));
        return RowBuffer_->CaptureRow(builder.GetRow());
    }

    TShuffleReadRecord MakeRecord(i32 mapperId, i64 startRow, std::vector<TUnversionedRow> rows)
    {
        return TShuffleReadRecord{
            .Header = TRecordHeader{
                .RowCount = static_cast<i32>(std::ssize(rows)),
                .MapperId = mapperId,
                .StartRow = startRow,
            },
            .Rows = MakeSharedRange(std::move(rows), RowBuffer_),
        };
    }

    TShuffleReadRecord MakeIdentityPreservingRecord(
        i32 mapperId,
        i64 startRow,
        std::vector<TUnversionedRow> rows,
        int mapperIdColumnId = MapperIdColumnId,
        int rowIdColumnId = RowIdColumnId)
    {
        for (int index = 0; index < std::ssize(rows); ++index) {
            auto source = rows[index];
            auto extended = RowBuffer_->AllocateUnversioned(source.GetCount() + 2);
            std::copy(source.Begin(), source.End(), extended.Begin());
            extended[source.GetCount()] = MakeUnversionedInt64Value(
                mapperId,
                mapperIdColumnId);
            extended[source.GetCount() + 1] = MakeUnversionedInt64Value(
                startRow + index,
                rowIdColumnId);
            rows[index] = extended;
        }
        return MakeRecord(mapperId, startRow, std::move(rows));
    }

    static TShuffleReadBatchPtr MakeBatch(std::vector<TShuffleReadRecord> records, bool finished)
    {
        auto batch = New<TShuffleReadBatch>();
        batch->Records = std::move(records);
        batch->Finished = finished;
        return batch;
    }

    void DrainInvoker()
    {
        auto barrier = NewPromise<void>();
        GetInvoker()->Invoke(BIND([barrier] () mutable { barrier.Set(); }));
        WaitFor(barrier.ToFuture())
            .ThrowOnError();
    }

    void FeedBatch(std::vector<TShuffleReadRecord> records, bool finished)
    {
        DrainInvoker();
        YT_VERIFY(MockReader_->HasPendingRead());
        MockReader_->FulfillRead(MakeBatch(std::move(records), finished));
        DrainInvoker();
    }

    std::vector<TSharedRange<TUnversionedRow>> ReadAll(const ISortReaderPtr& reader)
    {
        std::vector<TSharedRange<TUnversionedRow>> batches;
        while (true) {
            auto rows = WaitFor(reader->Read())
                .ValueOrThrow();
            if (rows.Empty()) {
                break;
            }
            batches.push_back(std::move(rows));
        }
        return batches;
    }

    static std::vector<std::pair<i64, i64>> ToKeyPayloadPairs(
        const std::vector<TSharedRange<TUnversionedRow>>& batches)
    {
        std::vector<std::pair<i64, i64>> result;
        for (const auto& batch : batches) {
            for (auto row : batch) {
                std::optional<i64> key;
                std::optional<i64> payload;
                for (const auto& value : row) {
                    if (value.Id == KeyColumnId && value.Type == EValueType::Int64) {
                        key = value.Data.Int64;
                    }
                    if (value.Id == PayloadColumnId && value.Type == EValueType::Int64) {
                        payload = value.Data.Int64;
                    }
                }
                result.emplace_back(key.value_or(-1), payload.value_or(-1));
            }
        }
        return result;
    }

    struct TExtendedRowView
    {
        i64 Key = -1;
        i64 Payload = -1;
        i64 MapperId = -1;
        i64 RowId = -1;
        int ValueCount = 0;
    };

    static std::vector<TExtendedRowView> ToExtendedViews(
        const std::vector<TSharedRange<TUnversionedRow>>& batches)
    {
        std::vector<TExtendedRowView> result;
        for (const auto& batch : batches) {
            for (auto row : batch) {
                TExtendedRowView view;
                view.ValueCount = static_cast<int>(row.GetCount());
                for (const auto& value : row) {
                    if (value.Type != EValueType::Int64) {
                        continue;
                    }
                    if (value.Id == KeyColumnId) {
                        view.Key = value.Data.Int64;
                    } else if (value.Id == PayloadColumnId) {
                        view.Payload = value.Data.Int64;
                    } else if (value.Id == MapperIdColumnId) {
                        view.MapperId = value.Data.Int64;
                    } else if (value.Id == RowIdColumnId) {
                        view.RowId = value.Data.Int64;
                    }
                }
                result.push_back(view);
            }
        }
        return result;
    }

    TActionQueuePtr ActionQueue_;
    TRowBufferPtr RowBuffer_;
    TMockPartitionReaderPtr MockReader_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortReaderTest, ForwardsPartitionControl)
{
    auto reader = MakeIdentityPreservingReader();

    reader->AddChunk(TChunkId(1, 2, 3, 4), {}, 5, 10);
    reader->SetNoMoreChunks();

    EXPECT_EQ(MockReader_->GetAddChunkCallCount(), 1);
    EXPECT_EQ(MockReader_->GetSetNoMoreChunksCallCount(), 1);
}

////////////////////////////////////////////////////////////////////////////////

class TSortReaderDeathTest
    : public TSortReaderTest
{
protected:
    void SetUp() override
    {
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

TEST_F(TSortReaderDeathTest, OverlappingReads)
{
    auto reader = MakeIdentityFreeReader();
    FeedBatch({}, /*finished*/ true);

    auto blockerStarted = NewPromise<void>();
    auto unblock = NewPromise<void>();
    GetInvoker()->Invoke(BIND([blockerStarted, unblock] {
        blockerStarted.Set();
        WaitFor(unblock.ToFuture())
            .ThrowOnError();
    }));
    WaitFor(blockerStarted.ToFuture())
        .ThrowOnError();

    EXPECT_DEATH({
        Y_UNUSED(reader->Read());
        Y_UNUSED(reader->Read());
    }, "!ReadInProgress_\\.exchange");

    unblock.Set();
    DrainInvoker();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortReaderTest, EmptyPartitionIdentityFree)
{
    auto reader = MakeIdentityFreeReader();
    FeedBatch({}, /*finished*/ true);

    auto batches = ReadAll(reader);
    EXPECT_TRUE(batches.empty());

    // End of stream remains empty.
    auto rows = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(rows.Empty());
}

TEST_F(TSortReaderTest, EmptyPartitionIdentityPreserving)
{
    auto reader = MakeIdentityPreservingReader();
    FeedBatch({}, /*finished*/ true);

    auto batches = ReadAll(reader);
    EXPECT_TRUE(batches.empty());
}

TEST_F(TSortReaderTest, SortsAcrossRecordsAndBatchesIdentityFree)
{
    auto reader = MakeIdentityFreeReader();

    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(5, 50), MakeRow(3, 30)})}, /*finished*/ false);
    FeedBatch({MakeRecord(/*mapperId*/ 1, /*startRow*/ 0, {MakeRow(4, 40), MakeRow(1, 10)})}, /*finished*/ true);

    auto batches = ReadAll(reader);
    auto pairs = ToKeyPayloadPairs(batches);
    EXPECT_EQ(pairs, (std::vector<std::pair<i64, i64>>{{1, 10}, {3, 30}, {4, 40}, {5, 50}}));
}

TEST_F(TSortReaderTest, IdentityFreeModeEmitsRowsZeroCopy)
{
    auto reader = MakeIdentityFreeReader();

    auto row1 = MakeRow(2, 20);
    auto row2 = MakeRow(1, 10);
    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {row1, row2})}, /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    ASSERT_EQ(std::ssize(batches[0]), 2);
    // Row structs are reused.
    EXPECT_EQ(batches[0][0].Begin(), row2.Begin());
    EXPECT_EQ(batches[0][1].Begin(), row1.Begin());
}

TEST_F(TSortReaderTest, SortsByKeyPrefix)
{
    auto reader = MakeIdentityFreeReader();

    auto makeRow = [&] (i64 key, i64 laterValue) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(key, /*id*/ 7));
        builder.AddValue(MakeUnversionedInt64Value(laterValue, KeyColumnId));
        return RowBuffer_->CaptureRow(builder.GetRow());
    };

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {makeRow(2, 0), makeRow(1, 100)})},
        /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    ASSERT_EQ(std::ssize(batches[0]), 2);
    EXPECT_EQ(batches[0][0][0].Data.Int64, 1);
    EXPECT_EQ(batches[0][1][0].Data.Int64, 2);
}

TEST_F(TSortReaderTest, NullKeySortsFirst)
{
    auto reader = MakeIdentityFreeReader();

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedNullValue(KeyColumnId));
    builder.AddValue(MakeUnversionedInt64Value(77, PayloadColumnId));
    auto nullKeyRow = RowBuffer_->CaptureRow(builder.GetRow());

    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10), nullKeyRow})}, /*finished*/ true);

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));
    EXPECT_EQ(pairs, (std::vector<std::pair<i64, i64>>{{-1, 77}, {1, 10}}));
}

TEST_F(TSortReaderTest, DescendingSortOrder)
{
    auto reader = MakeIdentityFreeReader(
        MakeConfig(),
        MakeComparator(/*length*/ 1, ESortOrder::Descending));

    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10), MakeRow(3, 30), MakeRow(2, 20)})}, /*finished*/ true);

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));
    EXPECT_EQ(pairs, (std::vector<std::pair<i64, i64>>{{3, 30}, {2, 20}, {1, 10}}));
}

TEST_F(TSortReaderTest, StringKeysSortLexicographically)
{
    auto reader = MakeIdentityFreeReader();

    auto makeStringRow = [&] (TStringBuf key, i64 payload) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(key, KeyColumnId));
        builder.AddValue(MakeUnversionedInt64Value(payload, PayloadColumnId));
        return RowBuffer_->CaptureRow(builder.GetRow());
    };

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {makeStringRow("b", 2), makeStringRow("a", 1), makeStringRow("c", 3)})},
        /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    std::vector<TStringBuf> keys;
    for (auto row : batches[0]) {
        for (const auto& value : row) {
            if (value.Id == KeyColumnId) {
                keys.push_back(value.AsStringBuf());
            }
        }
    }
    EXPECT_EQ(keys, (std::vector<TStringBuf>{"a", "b", "c"}));
}

TEST_F(TSortReaderTest, MergesAcrossBuckets)
{
    // Force three buckets.
    auto reader = MakeIdentityFreeReader(MakeConfig(/*bucketRowCount*/ 2));

    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(6, 60), MakeRow(1, 10), MakeRow(5, 50)})}, /*finished*/ false);
    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 3, {MakeRow(2, 20), MakeRow(4, 40), MakeRow(3, 30)})}, /*finished*/ true);

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));
    EXPECT_EQ(pairs, (std::vector<std::pair<i64, i64>>{
        {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}, {6, 60}}));
}

TEST_F(TSortReaderTest, ParallelSortInvoker)
{
    // Exercise cross-invoker paths with concurrent bucket sorts.
    auto sortPool = CreateThreadPool(/*threadCount*/ 2, "SortPool");
    auto reader = CreateSortReaderForTesting(
        MakeConfig(/*bucketRowCount*/ 2),
        MockReader_,
        MakeComparator(),
        TValidMapperIds{},
        GetInvoker(),
        sortPool->GetInvoker());

    std::vector<TUnversionedRow> rows;
    for (i64 index = 0; index < 100; ++index) {
        rows.push_back(MakeRow(/*key*/ 99 - index, /*payload*/ 99 - index));
    }
    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, std::move(rows))}, /*finished*/ true);

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));
    ASSERT_EQ(std::ssize(pairs), 100);
    for (i64 index = 0; index < 100; ++index) {
        EXPECT_EQ(pairs[index].first, index);
    }

    sortPool->Shutdown();
}

TEST_F(TSortReaderTest, QueuedSortDoesNotRetainReader)
{
    auto sortQueue = New<TActionQueue>("SortReaderBlockedSort");
    TEvent blockerStarted;
    TEvent unblock;
    sortQueue->GetInvoker()->Invoke(BIND([&] {
        blockerStarted.NotifyAll();
        unblock.Wait();
    }));
    blockerStarted.Wait();

    auto reader = CreateSortReaderForTesting(
        MakeConfig(/*bucketRowCount*/ 1),
        MockReader_,
        MakeComparator(),
        TValidMapperIds{},
        GetInvoker(),
        sortQueue->GetInvoker());
    auto weakReader = MakeWeak(reader);

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10)})},
        /*finished*/ true);
    reader.Reset();

    EXPECT_FALSE(weakReader.Lock());

    unblock.NotifyAll();
    sortQueue->Shutdown(/*graceful*/ true);
    DrainInvoker();
}

TEST_F(TSortReaderTest, ReadParkedUntilStreamFinishes)
{
    auto reader = MakeIdentityFreeReader();

    auto future = reader->Read();
    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10)})}, /*finished*/ false);
    EXPECT_FALSE(future.IsSet());

    FeedBatch({}, /*finished*/ true);
    auto rows = WaitFor(future)
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(rows), 1);
}

TEST_F(TSortReaderTest, MaxRowsPerReadSplitsOutput)
{
    auto reader = MakeIdentityFreeReader(
        MakeConfig(/*bucketRowCount*/ 10'000, /*maxRowsPerRead*/ 2));

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0,
            {MakeRow(1, 1), MakeRow(2, 2), MakeRow(3, 3), MakeRow(4, 4), MakeRow(5, 5)})},
        /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 3);
    EXPECT_EQ(std::ssize(batches[0]), 2);
    EXPECT_EQ(std::ssize(batches[1]), 2);
    EXPECT_EQ(std::ssize(batches[2]), 1);
}

TEST_F(TSortReaderTest, MaxDataWeightPerReadSplitsOutput)
{
    // The soft cap still emits one row.
    auto reader = MakeIdentityFreeReader(
        MakeConfig(/*bucketRowCount*/ 10'000, /*maxRowsPerRead*/ 10'000, /*maxDataWeightPerRead*/ 1));

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 1), MakeRow(2, 2), MakeRow(3, 3)})},
        /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 3);
    for (const auto& batch : batches) {
        EXPECT_EQ(std::ssize(batch), 1);
    }
}

TEST_F(TSortReaderTest, IdentityFreeModeKeepsIdenticalContentRows)
{
    auto reader = MakeIdentityFreeReader();

    // Identical rows with different row ids must both survive.
    FeedBatch({MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10), MakeRow(1, 10)})}, /*finished*/ true);

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));
    EXPECT_EQ(pairs, (std::vector<std::pair<i64, i64>>{{1, 10}, {1, 10}}));
}

TEST_F(TSortReaderTest, IdentityPreservingModeEmitsIdentityValues)
{
    auto reader = MakeIdentityPreservingReader();

    FeedBatch(
        {MakeIdentityPreservingRecord(
            /*mapperId*/ 7,
            /*startRow*/ 100,
            {MakeRow(2, 20), MakeRow(1, 10)})},
        /*finished*/ true);

    auto views = ToExtendedViews(ReadAll(reader));
    ASSERT_EQ(std::ssize(views), 2);

    // Row ids are StartRow plus record offsets.
    EXPECT_EQ(views[0].Key, 1);
    EXPECT_EQ(views[0].MapperId, 7);
    EXPECT_EQ(views[0].RowId, 101);
    EXPECT_EQ(views[0].ValueCount, 4);

    EXPECT_EQ(views[1].Key, 2);
    EXPECT_EQ(views[1].MapperId, 7);
    EXPECT_EQ(views[1].RowId, 100);
    EXPECT_EQ(views[1].ValueCount, 4);
}

TEST_F(TSortReaderTest, IdentityPreservingModeEmitsRowsZeroCopy)
{
    auto reader = MakeIdentityPreservingReader();
    auto record = MakeIdentityPreservingRecord(
        /*mapperId*/ 7,
        /*startRow*/ 100,
        {MakeRow(2, 20), MakeRow(1, 10)});
    auto row1 = record.Rows[0];
    auto row2 = record.Rows[1];

    FeedBatch({std::move(record)}, /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    ASSERT_EQ(std::ssize(batches[0]), 2);
    EXPECT_EQ(batches[0][0].Begin(), row2.Begin());
    EXPECT_EQ(batches[0][1].Begin(), row1.Begin());
}

TEST_F(TSortReaderTest, IdentityPreservingModeIdentityTiebreak)
{
    auto reader = MakeIdentityPreservingReader();

    // Equal keys are ordered by (mapper id, row id).
    FeedBatch(
        {
            MakeIdentityPreservingRecord(/*mapperId*/ 2, /*startRow*/ 5, {MakeRow(1, 25)}),
            MakeIdentityPreservingRecord(/*mapperId*/ 1, /*startRow*/ 9, {MakeRow(1, 19)}),
            MakeIdentityPreservingRecord(/*mapperId*/ 1, /*startRow*/ 3, {MakeRow(1, 13)}),
        },
        /*finished*/ true);

    auto views = ToExtendedViews(ReadAll(reader));
    ASSERT_EQ(std::ssize(views), 3);
    EXPECT_EQ(views[0].Payload, 13);
    EXPECT_EQ(views[1].Payload, 19);
    EXPECT_EQ(views[2].Payload, 25);
}

TEST_F(TSortReaderTest, EmptyComparatorSortsByIdentity)
{
    // Identity alone defines the order.
    auto reader = CreateSortReaderForTesting(
        MakeConfig(),
        MockReader_,
        TComparator(),
        TIdentityColumnIds{
            .MapperId = MapperIdColumnId,
            .RowId = RowIdColumnId,
        },
        GetInvoker(),
        GetInvoker());

    FeedBatch(
        {
            MakeIdentityPreservingRecord(/*mapperId*/ 2, /*startRow*/ 0, {MakeRow(1, 20)}),
            MakeIdentityPreservingRecord(/*mapperId*/ 1, /*startRow*/ 5, {MakeRow(2, 15)}),
            MakeIdentityPreservingRecord(/*mapperId*/ 1, /*startRow*/ 0, {MakeRow(3, 10)}),
        },
        /*finished*/ true);

    auto views = ToExtendedViews(ReadAll(reader));
    ASSERT_EQ(std::ssize(views), 3);
    EXPECT_EQ(views[0].Payload, 10);
    EXPECT_EQ(views[1].Payload, 15);
    EXPECT_EQ(views[2].Payload, 20);
}

TEST_F(TSortReaderTest, IdentityPreservingModeKeepsSupersededMappers)
{
    auto reader = MakeIdentityPreservingReader();

    FeedBatch(
        {
            MakeIdentityPreservingRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10)}),
            MakeIdentityPreservingRecord(/*mapperId*/ 1, /*startRow*/ 0, {MakeRow(2, 20)}),
        },
        /*finished*/ true);

    auto views = ToExtendedViews(ReadAll(reader));
    EXPECT_EQ(std::ssize(views), 2);
}

TEST_F(TSortReaderTest, IdentityPreservingModeStringValuesStayShallow)
{
    auto reader = MakeIdentityPreservingReader();

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, KeyColumnId));
    builder.AddValue(MakeUnversionedStringValue("payload-bytes", PayloadColumnId));
    auto row = RowBuffer_->CaptureRow(builder.GetRow());
    const char* originalData = row[1].Data.String;
    auto record = MakeIdentityPreservingRecord(/*mapperId*/ 0, /*startRow*/ 0, {row});
    auto input = record.Rows[0];

    FeedBatch({std::move(record)}, /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    ASSERT_EQ(std::ssize(batches[0]), 1);
    auto emitted = batches[0][0];
    ASSERT_EQ(static_cast<int>(emitted.GetCount()), 4);
    EXPECT_EQ(emitted.Begin(), input.Begin());
    EXPECT_EQ(emitted[1].Data.String, originalData);
}

TEST_F(TSortReaderTest, RetainedBatchesKeepStringPayloadsAlive)
{
    auto reader = MakeIdentityFreeReader();

    // After FeedBatch(), only reader retention keeps string storage alive.
    auto localBuffer = New<TRowBuffer>(TSortReaderTestTag());
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, KeyColumnId));
    builder.AddValue(MakeUnversionedStringValue("retained-payload", PayloadColumnId));
    auto row = localBuffer->CaptureRow(builder.GetRow());

    TShuffleReadRecord record{
        .Header = TRecordHeader{
            .RowCount = 1,
            .MapperId = 0,
            .StartRow = 0,
        },
        .Rows = MakeSharedRange(std::vector<TUnversionedRow>{row}, std::move(localBuffer)),
    };
    FeedBatch({std::move(record)}, /*finished*/ true);

    auto batches = ReadAll(reader);
    ASSERT_EQ(std::ssize(batches), 1);
    ASSERT_EQ(std::ssize(batches[0]), 1);
    auto emitted = batches[0][0];
    ASSERT_EQ(static_cast<int>(emitted.GetCount()), 2);
    EXPECT_EQ(emitted[1].AsStringBuf(), "retained-payload");
}

TEST_F(TSortReaderTest, OutputBatchRetainsInputWithoutRetainingReader)
{
    auto reader = MakeIdentityFreeReader();
    auto weakReader = MakeWeak(reader);

    auto localBuffer = New<TRowBuffer>(TSortReaderTestTag());
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, KeyColumnId));
    builder.AddValue(MakeUnversionedStringValue("retained-payload", PayloadColumnId));
    auto row = localBuffer->CaptureRow(builder.GetRow());

    TShuffleReadRecord record{
        .Header = TRecordHeader{
            .RowCount = 1,
            .MapperId = 0,
            .StartRow = 0,
        },
        .Rows = MakeSharedRange(std::vector<TUnversionedRow>{row}, std::move(localBuffer)),
    };
    FeedBatch({std::move(record)}, /*finished*/ true);

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    DrainInvoker();
    reader.Reset();

    EXPECT_FALSE(weakReader.Lock());
    ASSERT_EQ(std::ssize(batch), 1);
    EXPECT_EQ(batch[0][1].AsStringBuf(), "retained-payload");
}

TEST_F(TSortReaderTest, FinishedReaderDoesNotRetainInput)
{
    auto reader = MakeIdentityFreeReader();

    auto inputBuffer = New<TRowBuffer>(TSortReaderTestTag());
    auto weakInputBuffer = MakeWeak(inputBuffer);
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, KeyColumnId));
    builder.AddValue(MakeUnversionedStringValue("retained-payload", PayloadColumnId));
    auto row = inputBuffer->CaptureRow(builder.GetRow());

    TShuffleReadRecord record{
        .Header = TRecordHeader{
            .RowCount = 1,
            .MapperId = 0,
            .StartRow = 0,
        },
        .Rows = MakeSharedRange(
            std::vector<TUnversionedRow>{row},
            std::move(inputBuffer)),
    };
    FeedBatch({std::move(record)}, /*finished*/ true);

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(weakInputBuffer.Lock());
    EXPECT_TRUE(WaitFor(reader->Read()).ValueOrThrow().Empty());

    batch = {};
    EXPECT_FALSE(weakInputBuffer.Lock());
}

TEST_F(TSortReaderTest, IdentityPreservingModePayloadsSurviveOutputBatchDrops)
{
    // Drop each one-row batch before the next Read().
    auto reader = MakeIdentityPreservingReader(
        MakeConfig(/*bucketRowCount*/ 10'000, /*maxRowsPerRead*/ 1));

    auto localBuffer = New<TRowBuffer>(TSortReaderTestTag());
    std::vector<TUnversionedRow> rows;
    for (i64 index = 0; index < 3; ++index) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(index, KeyColumnId));
        builder.AddValue(MakeUnversionedStringValue("payload-string", PayloadColumnId));
        auto source = localBuffer->CaptureRow(builder.GetRow());
        auto extended = localBuffer->AllocateUnversioned(source.GetCount() + 2);
        std::copy(source.Begin(), source.End(), extended.Begin());
        extended[source.GetCount()] = MakeUnversionedInt64Value(0, MapperIdColumnId);
        extended[source.GetCount() + 1] = MakeUnversionedInt64Value(index, RowIdColumnId);
        rows.push_back(extended);
    }
    TShuffleReadRecord record{
        .Header = TRecordHeader{
            .RowCount = 3,
            .MapperId = 0,
            .StartRow = 0,
        },
        .Rows = MakeSharedRange(std::move(rows), std::move(localBuffer)),
    };
    FeedBatch({std::move(record)}, /*finished*/ true);

    for (int index = 0; index < 3; ++index) {
        auto batch = WaitFor(reader->Read())
            .ValueOrThrow();
        ASSERT_EQ(std::ssize(batch), 1);
        EXPECT_EQ(batch[0][1].AsStringBuf(), "payload-string");
    }
    auto tail = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(tail.Empty());
}

TEST_F(TSortReaderTest, UnderlyingReadErrorPropagates)
{
    auto reader = MakeIdentityFreeReader();

    auto future = reader->Read();
    DrainInvoker();
    MockReader_->FailRead(TError("L2 read failed"));
    DrainInvoker();

    // Subsequent reads fail too.
    EXPECT_FALSE(WaitFor(future).IsOK());
    EXPECT_FALSE(WaitFor(reader->Read()).IsOK());
}

TEST_F(TSortReaderTest, CancelingPendingReadCancelsReader)
{
    auto reader = MakeIdentityFreeReader();

    auto future = reader->Read();
    DrainInvoker();
    EXPECT_TRUE(future.Cancel(TError("test cancellation")));
    DrainInvoker();

    EXPECT_TRUE(MockReader_->GetReadCanceled());

    auto error = WaitFor(future);
    ASSERT_FALSE(error.IsOK());
    EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled).has_value());

    auto nextError = WaitFor(reader->Read());
    ASSERT_FALSE(nextError.IsOK());
    EXPECT_TRUE(nextError.FindMatching(NYT::EErrorCode::Canceled).has_value());
}

TEST_F(TSortReaderTest, CancelingReadWhileSortPendingCancelsReader)
{
    auto sortQueue = New<TActionQueue>("SortReaderBlockedSort");
    TEvent blockerStarted;
    TEvent unblock;
    sortQueue->GetInvoker()->Invoke(BIND([&] {
        blockerStarted.NotifyAll();
        unblock.Wait();
    }));
    blockerStarted.Wait();

    auto reader = CreateSortReaderForTesting(
        MakeConfig(/*bucketRowCount*/ 1),
        MockReader_,
        MakeComparator(),
        TValidMapperIds{},
        GetInvoker(),
        sortQueue->GetInvoker());
    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {MakeRow(1, 10)})},
        /*finished*/ true);

    auto future = reader->Read();
    DrainInvoker();
    EXPECT_TRUE(future.Cancel(TError("test cancellation")));
    DrainInvoker();

    auto error = WaitFor(future);
    ASSERT_FALSE(error.IsOK());
    EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled).has_value());

    auto nextError = WaitFor(reader->Read());
    ASSERT_FALSE(nextError.IsOK());
    EXPECT_TRUE(nextError.FindMatching(NYT::EErrorCode::Canceled).has_value());

    unblock.NotifyAll();
    sortQueue->Shutdown(/*graceful*/ true);
    DrainInvoker();
}

TEST_F(TSortReaderTest, CancelingReadReleasesInputAfterPendingSortFinishes)
{
    auto sortQueue = New<TActionQueue>("SortReaderBlockedSort");
    TEvent blockerStarted;
    TEvent unblock;
    sortQueue->GetInvoker()->Invoke(BIND([&] {
        blockerStarted.NotifyAll();
        unblock.Wait();
    }));
    blockerStarted.Wait();

    auto reader = CreateSortReaderForTesting(
        MakeConfig(/*bucketRowCount*/ 1),
        MockReader_,
        MakeComparator(),
        TValidMapperIds{},
        GetInvoker(),
        sortQueue->GetInvoker());

    auto inputBuffer = New<TRowBuffer>(TSortReaderTestTag());
    auto weakInputBuffer = MakeWeak(inputBuffer);
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(1, KeyColumnId));
    builder.AddValue(MakeUnversionedInt64Value(10, PayloadColumnId));
    auto row = inputBuffer->CaptureRow(builder.GetRow());
    TShuffleReadRecord record{
        .Header = TRecordHeader{
            .RowCount = 1,
            .MapperId = 0,
            .StartRow = 0,
        },
        .Rows = MakeSharedRange(
            std::vector<TUnversionedRow>{row},
            std::move(inputBuffer)),
    };
    FeedBatch({std::move(record)}, /*finished*/ true);

    auto future = reader->Read();
    DrainInvoker();
    EXPECT_TRUE(future.Cancel(TError("test cancellation")));
    DrainInvoker();
    EXPECT_TRUE(weakInputBuffer.Lock());

    unblock.NotifyAll();
    sortQueue->Shutdown(/*graceful*/ true);
    DrainInvoker();

    EXPECT_FALSE(weakInputBuffer.Lock());
}

TEST_F(TSortReaderTest, IncomparableKeysFailReader)
{
    auto reader = MakeIdentityFreeReader();

    // YSON maps are incomparable.
    auto makeAnyKeyRow = [&] (TStringBuf yson, i64 payload) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedAnyValue(yson, KeyColumnId));
        builder.AddValue(MakeUnversionedInt64Value(payload, PayloadColumnId));
        return RowBuffer_->CaptureRow(builder.GetRow());
    };

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {makeAnyKeyRow("{a=1}", 1), makeAnyKeyRow("{b=2}", 2)})},
        /*finished*/ true);

    auto result = WaitFor(reader->Read());
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TSortReaderTest, IncomparableKeysAtMergeFailReader)
{
    // One-row buckets defer comparison to merge-heap construction.
    auto reader = MakeIdentityFreeReader(MakeConfig(/*bucketRowCount*/ 1));

    auto makeAnyKeyRow = [&] (TStringBuf yson, i64 payload) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedAnyValue(yson, KeyColumnId));
        builder.AddValue(MakeUnversionedInt64Value(payload, PayloadColumnId));
        return RowBuffer_->CaptureRow(builder.GetRow());
    };

    FeedBatch(
        {MakeRecord(/*mapperId*/ 0, /*startRow*/ 0, {makeAnyKeyRow("{a=1}", 1), makeAnyKeyRow("{b=2}", 2)})},
        /*finished*/ true);

    auto result = WaitFor(reader->Read());
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TSortReaderTest, RandomizedAgainstReference)
{
    std::mt19937 rng(20260716);
    std::uniform_int_distribution<i64> keyDistribution(0, 49);

    struct TExpectedRow
    {
        i64 Key = 0;
        i64 Payload = 0;
        i32 MapperId = 0;
        i64 RowId = 0;
    };

    std::vector<TShuffleReadRecord> records;
    std::vector<TExpectedRow> expected;
    i64 payloadCounter = 0;
    for (i32 mapperId = 0; mapperId < 3; ++mapperId) {
        i64 nextRowId = 0;
        for (int recordIndex = 0; recordIndex < 20; ++recordIndex) {
            std::vector<TUnversionedRow> rows;
            std::vector<TExpectedRow> expectedRecord;
            i64 startRow = nextRowId;
            for (int i = 0; i < 5; ++i) {
                i64 key = keyDistribution(rng);
                i64 payload = payloadCounter++;
                rows.push_back(MakeRow(key, payload));
                expectedRecord.push_back(TExpectedRow{key, payload, mapperId, nextRowId++});
            }
            expected.insert(expected.end(), expectedRecord.begin(), expectedRecord.end());
            records.push_back(MakeIdentityPreservingRecord(mapperId, startRow, std::move(rows)));
        }
    }
    std::shuffle(records.begin(), records.end(), rng);

    auto reader = MakeIdentityPreservingReader(MakeConfig(/*bucketRowCount*/ 37));
    for (i64 index = 0; index < std::ssize(records); ++index) {
        FeedBatch({records[index]}, /*finished*/ index + 1 == std::ssize(records));
    }

    std::sort(
        expected.begin(),
        expected.end(),
        [] (const TExpectedRow& lhs, const TExpectedRow& rhs) {
            return std::tie(lhs.Key, lhs.MapperId, lhs.RowId) <
                std::tie(rhs.Key, rhs.MapperId, rhs.RowId);
        });

    auto views = ToExtendedViews(ReadAll(reader));
    ASSERT_EQ(std::ssize(views), std::ssize(expected));
    for (i64 index = 0; index < std::ssize(views); ++index) {
        EXPECT_EQ(views[index].Key, expected[index].Key) << "at index " << index;
        EXPECT_EQ(views[index].Payload, expected[index].Payload) << "at index " << index;
        EXPECT_EQ(views[index].MapperId, expected[index].MapperId) << "at index " << index;
        EXPECT_EQ(views[index].RowId, expected[index].RowId) << "at index " << index;
    }
}

TEST_F(TSortReaderTest, RandomizedAgainstReferenceIdentityFree)
{
    std::mt19937 rng(20260717);
    std::uniform_int_distribution<i64> keyDistribution(0, 49);

    std::vector<TShuffleReadRecord> records;
    std::vector<std::pair<i64, i64>> expected;
    i64 payloadCounter = 0;
    for (i32 mapperId = 0; mapperId < 3; ++mapperId) {
        i64 nextRowId = 0;
        for (int recordIndex = 0; recordIndex < 20; ++recordIndex) {
            std::vector<TUnversionedRow> rows;
            std::vector<std::pair<i64, i64>> expectedRecord;
            i64 startRow = nextRowId;
            for (int i = 0; i < 5; ++i) {
                i64 key = keyDistribution(rng);
                i64 payload = payloadCounter++;
                rows.push_back(MakeRow(key, payload));
                ++nextRowId;
                expectedRecord.emplace_back(key, payload);
            }
            expected.insert(expected.end(), expectedRecord.begin(), expectedRecord.end());
            records.push_back(MakeRecord(mapperId, startRow, std::move(rows)));
        }
    }
    std::shuffle(records.begin(), records.end(), rng);

    auto reader = MakeIdentityFreeReader(MakeConfig(/*bucketRowCount*/ 37));
    for (i64 index = 0; index < std::ssize(records); ++index) {
        FeedBatch({records[index]}, /*finished*/ index + 1 == std::ssize(records));
    }

    auto pairs = ToKeyPayloadPairs(ReadAll(reader));

    // Equal-key order is unspecified; compare multisets.
    for (i64 index = 1; index < std::ssize(pairs); ++index) {
        EXPECT_LE(pairs[index - 1].first, pairs[index].first) << "at index " << index;
    }
    std::sort(pairs.begin(), pairs.end());
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(pairs, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPushBasedShuffleClient
