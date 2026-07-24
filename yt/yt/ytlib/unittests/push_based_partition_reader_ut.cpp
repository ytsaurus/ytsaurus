#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_reader.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/partition_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/public.h>
#include <yt/yt/ytlib/push_based_shuffle_client/record_format.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/blob.h>
#include <library/cpp/yt/memory/weak_ptr.h>

#include <memory>
#include <string>

namespace NYT::NPushBasedShuffleClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NTableClient;

// Test seam: forward-declare the factory defined in partition_reader.cpp;
// keeps the shared signature local to tests without a separate header.
using TCreateChunkSessionReaderCallback = std::function<
    NDistributedChunkSessionClient::IDistributedChunkSessionReaderPtr(
        TChunkId chunkId,
        TChunkReplicaList replicas,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex)>;

IPushBasedPartitionReaderPtr CreatePushBasedPartitionReaderForTesting(
    TPartitionReaderConfigPtr config,
    TCreateChunkSessionReaderCallback createDistributedChunkSessionReader,
    IInvokerPtr invoker,
    TRecordHeaderFilter recordHeaderFilter = {},
    std::optional<TIdentityColumnIds> identityColumnIds = {});

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

class TMockChunkSessionReader
    : public IDistributedChunkSessionReader
{
public:
    TFuture<TChunkReadResult> Read() override
    {
        YT_VERIFY(!PendingPromise_);
        PendingPromise_ = NewPromise<TChunkReadResult>();
        PendingPromise_.OnCanceled(BIND_NO_PROPAGATE(
            &TMockChunkSessionReader::OnReadCanceled,
            MakeWeak(this)));
        return PendingPromise_.ToFuture();
    }

    void SetAllWritersFinished() override
    {
        SetAllWritersFinishedCalled_ = true;
    }

    void SetAllWritersFinished(i64 /*finalRecordCount*/, i64 /*finalCompressedDataSize*/) override
    {
        SetAllWritersFinishedCalled_ = true;
    }

    TDistributedChunkSessionReaderStatisticsConstPtr GetStatistics() const override
    {
        return New<TDistributedChunkSessionReaderStatistics>();
    }

    // Test helpers.
    void SetNextReadResult(TChunkReadResult result)
    {
        YT_VERIFY(PendingPromise_);
        auto promise = std::move(PendingPromise_);
        promise.Set(std::move(result));
    }

    void FailNextRead(const TError& error)
    {
        YT_VERIFY(PendingPromise_);
        auto promise = std::move(PendingPromise_);
        promise.Set(error);
    }

    bool HasPendingRead() const
    {
        return static_cast<bool>(PendingPromise_);
    }

    TPromise<TChunkReadResult> PendingReadPromise()
    {
        return PendingPromise_;
    }

    bool WasSetAllWritersFinishedCalled() const
    {
        return SetAllWritersFinishedCalled_;
    }

    bool WasReadCanceled() const
    {
        return ReadCanceled_;
    }

private:
    TPromise<TChunkReadResult> PendingPromise_;
    bool SetAllWritersFinishedCalled_ = false;
    bool ReadCanceled_ = false;

    void OnReadCanceled(const TError& error) noexcept
    {
        ReadCanceled_ = true;
        auto promise = std::move(PendingPromise_);
        promise.Set(TError(NYT::EErrorCode::Canceled, "Mock read canceled") << error);
    }
};

using TMockChunkSessionReaderPtr = TIntrusivePtr<TMockChunkSessionReader>;

////////////////////////////////////////////////////////////////////////////////

class TPartitionReaderTest
    : public ::testing::Test
{
protected:
    TPartitionReaderTest()
        : ActionQueue_(New<TActionQueue>("ShuffleReaderTest"))
    { }

    void TearDown() override
    {
        // Graceful=true drains any still-queued closures (their TBindStates
        // would otherwise leak under LSan). Tests typically DrainInvoker()
        // before tearing down, but Read()-on-no-data leaves a pending closure
        // on the queue when the test exits early.
        ActionQueue_->Shutdown(/*graceful*/ true);
    }

    IInvokerPtr Invoker() const
    {
        return ActionQueue_->GetInvoker();
    }

    TPartitionReaderConfigPtr MakeConfig(i64 maxBytesPerRead = 64_MB)
    {
        auto config = New<TPartitionReaderConfig>();
        config->ChunkSessionReaderConfig = New<TDistributedChunkSessionReaderConfig>();
        config->Codec = NCompression::ECodec::Lz4;
        config->RowBufferStartChunkSize = 4_KB;
        config->MaxBytesPerRead = maxBytesPerRead;
        return config;
    }

    // Builds a TChunkReadResult containing one record with the given header
    // and rows whose values are (int64 k, int64 v).
    TChunkReadResult MakeSingleRecordResult(
        i32 mapperId,
        i64 startRow,
        const std::vector<std::pair<i64, i64>>& rows,
        bool finished = false)
    {
        TShuffleRecordBuilder builder(mapperId, startRow);
        for (auto& [k, v] : rows) {
            TUnversionedRowBuilder rowBuilder;
            rowBuilder.AddValue(MakeUnversionedInt64Value(k, /*id*/ 0));
            rowBuilder.AddValue(MakeUnversionedInt64Value(v, /*id*/ 1));
            builder.AddRow(rowBuilder.GetRow());
        }
        auto recordOpt = builder.FlushRecord();
        YT_VERIFY(recordOpt);

        auto wireRefs = CompressShuffleRecord(*recordOpt, NCompression::ECodec::Lz4);
        auto wire = MergeRefsToRef<TDefaultBlobTag>(wireRefs);

        TChunkReadResult result;
        result.Records.push_back(std::move(wire));
        result.Finished = finished;
        return result;
    }

    // Builds a wire blob with a valid 16-byte header (so ReadShuffleRecordHeader
    // succeeds for cheap-peek filtering) and a deliberately-corrupt payload
    // body (so any actual Lz4 decompression would throw). Used to prove the
    // filter runs BEFORE decompression: if the reader peeked-then-decompressed
    // unconditionally, a rejected blob would still trigger FailReader.
    TSharedRef MakeRecordWithValidHeaderCorruptPayload(
        i32 mapperId,
        i64 startRow,
        i32 rowCount)
    {
        TRecordHeader header{
            .RowCount = rowCount,
            .MapperId = mapperId,
            .StartRow = startRow,
        };
        std::vector<char> bytes(sizeof(header));
        std::memcpy(bytes.data(), &header, sizeof(header));
        // 64 bytes of garbage that won't decompress as Lz4.
        bytes.insert(bytes.end(), 64, 'X');
        return TSharedRef::FromString(std::string(bytes.begin(), bytes.end()));
    }

    // Wait for all pending invoker actions to drain.
    void DrainInvoker()
    {
        auto barrier = NewPromise<void>();
        Invoker()->Invoke(BIND([barrier] () mutable { barrier.Set(); }));
        WaitFor(barrier.ToFuture())
            .ThrowOnError();
    }

    // Release the reader<->mock retention cycle at end of a test that left
    // mock reads pending. The cycle is:
    //   mock.PendingPromise -> chunk session callback -> MakeStrong(reader) -> reader
    //   reader.ChunkStates_ -> mock
    // Firing a terminal {Records=[], Finished=true} fulfils the promise, the
    // callback fires (and drops its strong ref to the reader), and both ends
    // of the cycle collapse. LSan reports the otherwise-unbreakable cycle as
    // a leak under release-mode ASan.
    void FlushPendingMockRead(const TMockChunkSessionReaderPtr& mock)
    {
        if (!mock->HasPendingRead()) {
            return;
        }
        TChunkReadResult terminal;
        terminal.Finished = true;
        mock->SetNextReadResult(std::move(terminal));
        DrainInvoker();
    }

    TActionQueuePtr ActionQueue_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, SingleChunkHappyPath)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(),
        createSessionReader,
        Invoker());

    auto chunkId = TChunkId(1, 2, 3, 4);
    reader->AddChunk(chunkId, /*replicas*/ {}, /*start*/ 0, /*end*/ std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto readFuture = reader->Read();

    // Fire one chunk session batch containing two records, terminal.
    DrainInvoker();
    ASSERT_TRUE(mock->HasPendingRead());

    TChunkReadResult result;
    auto record1 = MakeSingleRecordResult(
        /*mapperId*/ 7,
        /*startRow*/ 0,
        {{10, 100}, {11, 101}},
        /*finished*/ false);
    result.Records.push_back(std::move(record1.Records[0]));
    result.Finished = true;
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.MapperId, 7);
    EXPECT_EQ(batch->Records[0].Header.StartRow, 0);
    EXPECT_EQ(batch->Records[0].Header.RowCount, 2);
    EXPECT_EQ(std::ssize(batch->Records[0].Rows), 2);
    EXPECT_EQ(batch->Records[0].Rows[0][0].Data.Int64, 10);
    EXPECT_EQ(batch->Records[0].Rows[0][1].Data.Int64, 100);
    EXPECT_EQ(batch->Records[0].Rows[1][0].Data.Int64, 11);
    EXPECT_EQ(batch->Records[0].Rows[1][1].Data.Int64, 101);
}

TEST_F(TPartitionReaderTest, AppendsIdentityValues)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(),
        createSessionReader,
        Invoker(),
        {},
        TIdentityColumnIds{
            .MapperId = 10,
            .RowId = 11,
        });

    reader->AddChunk(TChunkId(1, 2, 3, 4), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    ASSERT_TRUE(mock->HasPendingRead());

    auto result = MakeSingleRecordResult(
        /*mapperId*/ 7,
        /*startRow*/ 100,
        {{10, 1000}, {11, 1100}},
        /*finished*/ true);
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(batch->Records), 1);
    ASSERT_EQ(std::ssize(batch->Records[0].Rows), 2);
    for (int index = 0; index < 2; ++index) {
        auto row = batch->Records[0].Rows[index];
        ASSERT_EQ(row.GetCount(), 4u);
        EXPECT_EQ(row[2].Id, 10u);
        EXPECT_EQ(row[2].Data.Int64, 7);
        EXPECT_EQ(row[3].Id, 11u);
        EXPECT_EQ(row[3].Data.Int64, 100 + index);
    }
}

TEST_F(TPartitionReaderTest, IdentityPreservingSortConsumesExtendedRows)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };
    TIdentityColumnIds identityColumnIds{
        .MapperId = 10,
        .RowId = 11,
    };
    auto partitionReader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(),
        createSessionReader,
        /*invoker*/ Invoker(),
        /*recordHeaderFilter*/ {},
        identityColumnIds);
    auto sortReader = CreateSortReaderForTesting(
        New<TSortReaderConfig>(),
        std::move(partitionReader),
        TComparator({ESortOrder::Ascending}),
        identityColumnIds,
        /*invoker*/ Invoker(),
        /*sortInvoker*/ Invoker());

    sortReader->AddChunk(
        TChunkId(1, 2, 3, 4),
        /*replicas*/ {},
        /*startRecordIndex*/ 0,
        /*rangeEndRecordIndex*/ std::nullopt);
    sortReader->SetNoMoreChunks();
    auto readFuture = sortReader->Read();
    DrainInvoker();
    ASSERT_TRUE(mock->HasPendingRead());

    const i32 mapperId = 7;
    const i64 startRowIndex = 100;
    mock->SetNextReadResult(MakeSingleRecordResult(
        mapperId,
        startRowIndex,
        {{2, 20}, {1, 10}},
        /*finished*/ true));

    auto rows = WaitFor(readFuture)
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(rows), 2);
    EXPECT_EQ(rows[0], MakeUnversionedOwningRow(1, 10, mapperId, startRowIndex + 1));
    EXPECT_EQ(rows[1], MakeUnversionedOwningRow(2, 20, mapperId, startRowIndex));
    for (const auto& row : rows) {
        ASSERT_EQ(row.GetCount(), 4u);
        EXPECT_EQ(row[2].Id, identityColumnIds.MapperId);
        EXPECT_EQ(row[3].Id, identityColumnIds.RowId);
    }
}

struct TIdentityColumnValidationTestCase
{
    TIdentityColumnIds IdentityColumnIds;
    std::string ExpectedError;
};

TEST_PI(
    TPartitionReaderTest,
    IdentityColumnValidationRejectsConflict,
    ::testing::Values(
        TIdentityColumnValidationTestCase{
            .IdentityColumnIds = {
                .MapperId = 1,
                .RowId = 11,
            },
            .ExpectedError = "mapper identity column ID 1",
        },
        TIdentityColumnValidationTestCase{
            .IdentityColumnIds = {
                .MapperId = 10,
                .RowId = 1,
            },
            .ExpectedError = "row identity column ID 1",
        }),
    TIdentityColumnValidationTestCase)
{
    const auto& testCase = GetParam();
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };
    auto config = MakeConfig();
    config->ValidateIdentityColumnIds = true;
    auto reader = CreatePushBasedPartitionReaderForTesting(
        std::move(config),
        createSessionReader,
        Invoker(),
        {},
        testCase.IdentityColumnIds);

    reader->AddChunk(TChunkId(1, 2, 3, 4), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    mock->SetNextReadResult(MakeSingleRecordResult(
        /*mapperId*/ 7,
        /*startRow*/ 100,
        {{10, 1000}},
        /*finished*/ true));

    auto error = WaitFor(readFuture);
    ASSERT_FALSE(error.IsOK());
    EXPECT_THAT(error.GetMessage(), ::testing::HasSubstr(testCase.ExpectedError));
    EXPECT_FALSE(WaitFor(reader->Read()).IsOK());
}

TEST_F(TPartitionReaderTest, MultiChunkStagedCoalescing)
{
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int callCount = 0;
    auto createSessionReader = [&] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return (callCount++ == 0) ? mock1 : mock2;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    // Stage BOTH results before issuing Read so MaybeResolveRead drains both.
    auto r1 = MakeSingleRecordResult(/*mapperId*/ 1, /*startRow*/ 0, {{10, 100}}, /*finished*/ true);
    auto r2 = MakeSingleRecordResult(/*mapperId*/ 2, /*startRow*/ 0, {{20, 200}}, /*finished*/ true);
    mock1->SetNextReadResult(std::move(r1));
    mock2->SetNextReadResult(std::move(r2));
    DrainInvoker();

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    EXPECT_EQ(std::ssize(batch->Records), 2);

    std::vector<i32> mapperIds;
    for (auto& r : batch->Records) {
        mapperIds.push_back(r.Header.MapperId);
    }
    std::sort(mapperIds.begin(), mapperIds.end());
    EXPECT_EQ(mapperIds, std::vector<i32>({1, 2}));
}

TEST_F(TPartitionReaderTest, PendingReadResolvesOnFirstResult)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    EXPECT_FALSE(readFuture.IsSet());

    // Now fire the result.
    auto result = MakeSingleRecordResult(/*mapperId*/ 1, 0, {{42, 4200}}, /*finished*/ false);
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    EXPECT_FALSE(batch->Finished);
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.MapperId, 1);

    FlushPendingMockRead(mock);
}

TEST_F(TPartitionReaderTest, DynamicAddChunk)
{
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int n = 0;
    auto createSessionReader = [&] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return (n++ == 0) ? mock1 : mock2;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());

    // Phase 1: chunk 1 only, drain its record.
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();
    auto r1 = MakeSingleRecordResult(/*mapperId*/ 1, 0, {{10, 100}}, /*finished*/ true);
    mock1->SetNextReadResult(std::move(r1));

    auto batch1 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_FALSE(batch1->Finished); // NoMoreChunks not set yet.
    ASSERT_EQ(std::ssize(batch1->Records), 1);
    EXPECT_EQ(batch1->Records[0].Header.MapperId, 1);

    // Phase 2: AddChunk(C2) + SetNoMoreChunks + drain.
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto r2 = MakeSingleRecordResult(/*mapperId*/ 2, 0, {{20, 200}}, /*finished*/ true);
    mock2->SetNextReadResult(std::move(r2));

    auto batch2 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch2->Finished);
    ASSERT_EQ(std::ssize(batch2->Records), 1);
    EXPECT_EQ(batch2->Records[0].Header.MapperId, 2);
}

TEST_F(TPartitionReaderTest, FinalNonEmptyBatchCarriesFinished)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto result = MakeSingleRecordResult(/*mapperId*/ 7, 0, {{1, 10}, {2, 20}}, /*finished*/ true);
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    EXPECT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.RowCount, 2);
}

TEST_F(TPartitionReaderTest, ReadsAfterFinishedReturnEmptyFinished)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();
    auto r = MakeSingleRecordResult(7, 0, {{1, 10}}, /*finished*/ true);
    mock->SetNextReadResult(std::move(r));

    auto batch1 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch1->Finished);

    auto batch2 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch2->Finished);
    EXPECT_TRUE(batch2->Records.empty());

    auto batch3 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch3->Finished);
    EXPECT_TRUE(batch3->Records.empty());
}

TEST_F(TPartitionReaderTest, MaxBytesPerReadDefersExtraChunks)
{
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int n = 0;
    auto createSessionReader = [&] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return (n++ == 0) ? mock1 : mock2;
    };

    // Cap at 1 byte so any chunk session result trips it.
    auto config = MakeConfig(/*maxBytesPerRead*/ 1);
    auto reader = CreatePushBasedPartitionReaderForTesting(config, createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    DrainInvoker();

    // Stage both chunks ready, each one large enough to exceed the cap on its own.
    auto r1 = MakeSingleRecordResult(/*mapperId*/ 1, 0, {{1, 1}}, /*finished*/ false);
    auto r2 = MakeSingleRecordResult(/*mapperId*/ 2, 0, {{2, 2}}, /*finished*/ false);
    mock1->SetNextReadResult(std::move(r1));
    mock2->SetNextReadResult(std::move(r2));
    DrainInvoker();

    // First Read drains one chunk only (cap hit after the first ready result).
    auto batch1 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_FALSE(batch1->Finished);
    EXPECT_EQ(std::ssize(batch1->Records), 1);

    // The other chunk's records appear on the next Read.
    auto batch2 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_FALSE(batch2->Finished);
    EXPECT_EQ(std::ssize(batch2->Records), 1);

    // Drained record should differ.
    EXPECT_NE(batch1->Records[0].Header.MapperId, batch2->Records[0].Header.MapperId);

    FlushPendingMockRead(mock1);
    FlushPendingMockRead(mock2);
}

TEST_F(TPartitionReaderTest, MaxBytesPerReadSplitsSingleChunkResult)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(/*maxBytesPerRead*/ 1),
        createSessionReader,
        Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    TChunkReadResult result;
    for (int mapperId = 0; mapperId < 3; ++mapperId) {
        auto record = MakeSingleRecordResult(
            mapperId,
            /*startRow*/ 0,
            {{mapperId, mapperId}},
            /*finished*/ false);
        result.Records.push_back(std::move(record.Records[0]));
    }
    result.Finished = true;
    mock->SetNextReadResult(std::move(result));

    for (int mapperId = 0; mapperId < 3; ++mapperId) {
        auto batch = WaitFor(reader->Read())
            .ValueOrThrow();
        ASSERT_EQ(std::ssize(batch->Records), 1);
        EXPECT_EQ(batch->Records[0].Header.MapperId, mapperId);
        EXPECT_EQ(batch->Finished, mapperId == 2);
    }
}

TEST_F(TPartitionReaderTest, DecompressionFailureFailsReader)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();

    // Deliberately malformed: a single random blob that won't decompress as Lz4 shuffle record.
    TChunkReadResult result;
    result.Records.push_back(TSharedRef::FromString(std::string(64, 'X')));
    result.Finished = false;
    mock->SetNextReadResult(std::move(result));

    auto err = WaitFor(reader->Read());
    EXPECT_FALSE(err.IsOK());

    // Subsequent Reads return the same error — i.e. the first failure
    // installed a sticky terminal error rather than being a one-off.
    auto err2 = WaitFor(reader->Read());
    EXPECT_FALSE(err2.IsOK());
}

TEST_F(TPartitionReaderTest, HeaderFilterDropsRejectedRecords)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto filter = [] (const TRecordHeader& header) {
        return header.MapperId == 7;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker(), filter);
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    // Three blobs with mapperIds {0, 7, 3}. The filter accepts only mapperId 7.
    // Rejected blobs have valid 16-byte headers but DELIBERATELY CORRUPT
    // payloads — if the reader decompressed before filtering, the bad payload
    // would throw and propagate to FailReader, surfacing a TerminalError_
    // here instead of a clean batch.
    TChunkReadResult result;
    result.Records.push_back(MakeRecordWithValidHeaderCorruptPayload(
        /*mapperId*/ 0, /*startRow*/ 0, /*rowCount*/ 1));
    auto accepted = MakeSingleRecordResult(/*mapperId*/ 7, 0, {{20, 200}}, /*finished*/ false);
    result.Records.push_back(std::move(accepted.Records[0]));
    result.Records.push_back(MakeRecordWithValidHeaderCorruptPayload(
        /*mapperId*/ 3, /*startRow*/ 0, /*rowCount*/ 1));
    result.Finished = true;
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.MapperId, 7);
}

TEST_F(TPartitionReaderTest, DropsDuplicateBeforeDecompression)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(),
        createSessionReader,
        Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto result = MakeSingleRecordResult(
        /*mapperId*/ 7,
        /*startRow*/ 10,
        {{20, 200}},
        /*finished*/ false);
    result.Records.push_back(MakeRecordWithValidHeaderCorruptPayload(
        /*mapperId*/ 7,
        /*startRow*/ 10,
        /*rowCount*/ 1));
    result.Finished = true;
    mock->SetNextReadResult(std::move(result));

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.MapperId, 7);
    EXPECT_EQ(batch->Records[0].Header.StartRow, 10);
}

TEST_F(TPartitionReaderTest, ReleasesHeaderFilterAfterFinish)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId,
        TChunkReplicaList,
        i64,
        std::optional<i64>)
    {
        return mock;
    };

    auto filterState = std::make_shared<int>();
    std::weak_ptr<int> weakFilterState = filterState;
    TRecordHeaderFilter filter = [filterState] (const TRecordHeader&) {
        return true;
    };
    filterState.reset();

    auto reader = CreatePushBasedPartitionReaderForTesting(
        MakeConfig(),
        createSessionReader,
        Invoker(),
        std::move(filter));
    filter = {};
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    mock->SetNextReadResult(MakeSingleRecordResult(
        /*mapperId*/ 7,
        /*startRow*/ 0,
        {{1, 10}},
        /*finished*/ true));

    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    EXPECT_TRUE(weakFilterState.expired());
}

TEST_F(TPartitionReaderTest, HeaderFilterRejectAllRespectsMaxBytes)
{
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int n = 0;
    auto createSessionReader = [&] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return (n++ == 0) ? mock1 : mock2;
    };

    auto filter = [] (const TRecordHeader&) { return false; };
    auto config = MakeConfig(/*maxBytesPerRead*/ 1);
    auto reader = CreatePushBasedPartitionReaderForTesting(config, createSessionReader, Invoker(), filter);

    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    DrainInvoker();

    auto r1 = MakeSingleRecordResult(0, 0, {{10, 100}}, /*finished*/ false);
    auto r2 = MakeSingleRecordResult(1, 0, {{20, 200}}, /*finished*/ false);
    mock1->SetNextReadResult(std::move(r1));
    mock2->SetNextReadResult(std::move(r2));
    DrainInvoker();

    // First Read drains exactly one chunk (cap honored), batch is empty (all filtered) AND not Finished.
    auto batch1 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_FALSE(batch1->Finished);
    EXPECT_TRUE(batch1->Records.empty());

    // Second Read drains the OTHER chunk — also empty, not Finished.
    auto batch2 = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_FALSE(batch2->Finished);
    EXPECT_TRUE(batch2->Records.empty());

    FlushPendingMockRead(mock1);
    FlushPendingMockRead(mock2);
}

TEST_F(TPartitionReaderTest, ChunkSessionErrorPropagatesAndPersists)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();

    auto readFuture = reader->Read();
    mock->FailNextRead(TError("simulated chunk session failure"));

    auto err = WaitFor(readFuture);
    EXPECT_FALSE(err.IsOK());
    EXPECT_THAT(err.GetMessage(), ::testing::HasSubstr("simulated chunk session failure"));

    auto err2 = WaitFor(reader->Read());
    EXPECT_FALSE(err2.IsOK());

    // SetAllWritersFinished must have been called on the surviving chunk session reader.
    EXPECT_TRUE(mock->WasSetAllWritersFinishedCalled());
}

TEST_F(TPartitionReaderTest, CancelingPendingReadCancelsReader)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    ASSERT_FALSE(readFuture.IsSet());

    ASSERT_TRUE(readFuture.Cancel(TError("test cancellation")));
    auto error = WaitFor(readFuture.WithTimeout(TDuration::Seconds(1)));
    ASSERT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled).has_value());
    EXPECT_THAT(error.GetMessage(), ::testing::HasSubstr("Partition reader canceled"));

    DrainInvoker();
    EXPECT_TRUE(mock->WasSetAllWritersFinishedCalled());
    EXPECT_TRUE(mock->WasReadCanceled());

    auto nextError = WaitFor(reader->Read());
    EXPECT_TRUE(nextError.FindMatching(NYT::EErrorCode::Canceled).has_value());

    FlushPendingMockRead(mock);
}

TEST_F(TPartitionReaderTest, EmptyTerminalChunkSessionResultUnblocksReadAfterSeal)
{
    // Chunk session can legitimately return {Records=[], Finished=true}
    // when the cursor is already at the effective end (see
    // distributed_chunk_session_reader.cpp:210-220). An empty terminal
    // must still mark the chunk finished; otherwise a sealed partition
    // with a pending Read would hang.
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();
    ASSERT_FALSE(readFuture.IsSet());
    ASSERT_TRUE(mock->HasPendingRead());

    TChunkReadResult emptyTerminal;
    emptyTerminal.Finished = true;
    mock->SetNextReadResult(std::move(emptyTerminal));

    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    EXPECT_TRUE(batch->Records.empty());
}

TEST_F(TPartitionReaderTest, EmptyTerminalChunkSessionResultUnblocksReadBeforeSeal)
{
    // Empty terminal arrives BEFORE SetNoMoreChunks. The reader must still
    // consume the result (advancing chunk-finished state) so that the
    // subsequent SetNoMoreChunks can resolve the pending Read.
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    DrainInvoker();

    auto readFuture = reader->Read();
    DrainInvoker();

    TChunkReadResult emptyTerminal;
    emptyTerminal.Finished = true;
    mock->SetNextReadResult(std::move(emptyTerminal));
    DrainInvoker();

    // Read may resolve eagerly with an empty non-finished batch (matches the
    // existing HeaderFilterRejectAllRespectsMaxBytes contract: empty +
    // not-Finished is valid forward progress).
    if (auto ready = readFuture.TryGet()) {
        auto batch = ready->ValueOrThrow();
        EXPECT_FALSE(batch->Finished);
        EXPECT_TRUE(batch->Records.empty());
        // Issue a second Read; it should remain pending until SetNoMoreChunks.
        readFuture = reader->Read();
        DrainInvoker();
    }

    // Now seal — the pending Read must resolve.
    reader->SetNoMoreChunks();
    auto batch = WaitFor(readFuture)
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    EXPECT_TRUE(batch->Records.empty());
}

TEST_F(TPartitionReaderTest, EmptyTerminalOnOneChunkOtherStillActive)
{
    // One chunk's chunk session returns {[], Finished=true} while a sibling
    // chunk still has records to deliver. The pending Read must NOT resolve
    // empty just because of the first chunk; it must wait for the active one.
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int n = 0;
    auto createSessionReader = [&] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return (n++ == 0) ? mock1 : mock2;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    reader->SetNoMoreChunks();
    DrainInvoker();

    // Empty terminal on chunk 1.
    TChunkReadResult empty;
    empty.Finished = true;
    mock1->SetNextReadResult(std::move(empty));
    DrainInvoker();

    // Real records on chunk 2.
    auto r2 = MakeSingleRecordResult(/*mapperId*/ 9, /*startRow*/ 0, {{42, 4242}}, /*finished*/ true);
    mock2->SetNextReadResult(std::move(r2));

    auto batch = WaitFor(reader->Read())
        .ValueOrThrow();
    EXPECT_TRUE(batch->Finished);
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Header.MapperId, 9);
}

TEST_F(TPartitionReaderTest, SetNoMoreChunksPropagatesSetAllWritersFinished)
{
    // SetNoMoreChunks must call SetAllWritersFinished on all live (not-yet-
    // finished) chunk session readers, so paused active-phase polls wind down.
    auto mock1 = New<TMockChunkSessionReader>();
    auto mock2 = New<TMockChunkSessionReader>();
    int n = 0;
    auto createSessionReader = [&] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return (n++ == 0) ? mock1 : mock2;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    reader->AddChunk(TChunkId(2, 2, 2, 2), {}, 0, std::nullopt);
    DrainInvoker();
    EXPECT_FALSE(mock1->WasSetAllWritersFinishedCalled());
    EXPECT_FALSE(mock2->WasSetAllWritersFinishedCalled());

    reader->SetNoMoreChunks();
    DrainInvoker();

    EXPECT_TRUE(mock1->WasSetAllWritersFinishedCalled());
    EXPECT_TRUE(mock2->WasSetAllWritersFinishedCalled());

    FlushPendingMockRead(mock1);
    FlushPendingMockRead(mock2);
}

TEST_F(TPartitionReaderTest, TerminalErrorSilencesLaterAddChunkContractViolations)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
    auto chunkId = TChunkId(1, 1, 1, 1);
    reader->AddChunk(chunkId, {}, 0, std::nullopt);
    DrainInvoker();

    // Trigger terminal error.
    mock->FailNextRead(TError("simulated chunk session failure"));
    auto err = WaitFor(reader->Read());
    EXPECT_FALSE(err.IsOK());

    // Stale AddChunk racing after FailReader — even with a DUPLICATE chunkId
    // (which would otherwise YT_VERIFY) — must be silently ignored.
    reader->AddChunk(chunkId, {}, 0, std::nullopt);
    DrainInvoker();
    // Process did not abort: subsequent Read still returns the terminal error.
    auto err2 = WaitFor(reader->Read());
    EXPECT_FALSE(err2.IsOK());
}

////////////////////////////////////////////////////////////////////////////////
// Death tests run in a separate fixture using GetSyncInvoker() — no
// TActionQueue is started, so no threads exist when EXPECT_DEATH forks.
// Avoids gtest's "death tests use fork() in a threaded context" warning
// and the deadlock risk that comes with it.

class TPartitionReaderDeathTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // YT process has background threads (logger, profiler, etc.) running
        // by the time tests start; fork-based death tests deadlock-risk on
        // those. "threadsafe" mode re-execs the test binary instead — slower
        // but safe.
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }

    TPartitionReaderConfigPtr MakeConfig()
    {
        auto config = New<TPartitionReaderConfig>();
        config->ChunkSessionReaderConfig = New<TDistributedChunkSessionReaderConfig>();
        config->Codec = NCompression::ECodec::Lz4;
        config->RowBufferStartChunkSize = 4_KB;
        config->MaxBytesPerRead = 64_MB;
        return config;
    }

    IPushBasedPartitionReaderPtr MakeReader()
    {
        auto mock = New<TMockChunkSessionReader>();
        auto createSessionReader = [mock] (
            TChunkId, TChunkReplicaList, i64, std::optional<i64>)
        {
            return mock;
        };
        return CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, GetSyncInvoker());
    }
};

// YT_VERIFY's abort prints the stringized expression (`#expr`); match per-
// test against the actual guard predicate so an unrelated abort can't satisfy
// the death expectation. See library/cpp/yt/assert/assert.h:68.

TEST_F(TPartitionReaderDeathTest, AddChunkAfterSetNoMoreChunks)
{
    EXPECT_DEATH({
        auto reader = MakeReader();
        reader->SetNoMoreChunks();
        reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
    }, "!NoMoreChunks_");
}

TEST_F(TPartitionReaderDeathTest, DuplicateChunkId)
{
    EXPECT_DEATH({
        auto reader = MakeReader();
        auto id = TChunkId(1, 1, 1, 1);
        reader->AddChunk(id, {}, 0, std::nullopt);
        reader->AddChunk(id, {}, 0, std::nullopt);
    }, "ChunkStates_\\.contains");
}

TEST_F(TPartitionReaderDeathTest, OverlappingReads)
{
    // Y_UNUSED is safe here: the second Read posts a closure that aborts via
    // YT_VERIFY before either future could be subscribed to; in threadsafe
    // death-test mode the whole binary re-execs, so no future leaks.
    EXPECT_DEATH({
        auto reader = MakeReader();
        reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
        Y_UNUSED(reader->Read());
        Y_UNUSED(reader->Read());
    }, "!PendingReadPromise_");
}

TEST_F(TPartitionReaderTest, LifetimePendingReadBreaksOnDrop)
{
    // Chunk session subscriptions hold a weak self-ref, so dropping the
    // external handle while a Read() is pending releases the reader and
    // resolves the caller's future with broken-promise.
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    TFuture<TShuffleReadBatchPtr> readFuture;
    {
        auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
        reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
        reader->SetNoMoreChunks();
        DrainInvoker();
        readFuture = reader->Read();
        DrainInvoker();
        // Drop reader handle while the mock's chunk session read is still pending.
    }

    auto err = WaitFor(readFuture);
    EXPECT_FALSE(err.IsOK());

    // Fire the mock's pending chunk session promise so the test exits cleanly; the
    // weak subscriber locks, finds the reader gone, and bails.
    FlushPendingMockRead(mock);
}

TEST_F(TPartitionReaderTest, LifetimeBrokenPromiseWhenNoChunks)
{
    auto createSessionReader = [] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>) -> IDistributedChunkSessionReaderPtr
    {
        // Should never be called in this test. Return type is explicit
        // because YT_ABORT() is [[noreturn]] — auto-deduction yields void.
        YT_ABORT();
    };

    TFuture<TShuffleReadBatchPtr> readFuture;
    {
        auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
        readFuture = reader->Read();
        // Ensure the Read closure has dispatched (and released its MakeStrong) before drop.
        DrainInvoker();
    }

    auto err = WaitFor(readFuture);
    EXPECT_FALSE(err.IsOK());
}

TEST_F(TPartitionReaderTest, LifetimeReleasedOnHandleDropEvenWithPendingChunkSessionRead)
{
    // Chunk session subscriptions hold a weak self-ref, so dropping the external handle
    // releases the reader immediately even when chunk session polling is still in
    // flight. Without this, the reader would leak until process exit any
    // time a caller forgot to call SetNoMoreChunks().
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    TWeakPtr<IPushBasedPartitionReader> weakReader;
    {
        auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
        weakReader = reader;
        reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
        DrainInvoker();
        // Drop handle WITHOUT SetNoMoreChunks.
    }

    EXPECT_EQ(weakReader.Lock(), nullptr);

    // Fire the mock's pending chunk session promise so the subscriber chain unwinds
    // and the test exits cleanly.
    FlushPendingMockRead(mock);
}

TEST_F(TPartitionReaderTest, LifetimeBatchOutlivesReader)
{
    auto mock = New<TMockChunkSessionReader>();
    auto createSessionReader = [mock] (
        TChunkId, TChunkReplicaList, i64, std::optional<i64>)
    {
        return mock;
    };

    TShuffleReadBatchPtr batch;
    {
        auto reader = CreatePushBasedPartitionReaderForTesting(MakeConfig(), createSessionReader, Invoker());
        reader->AddChunk(TChunkId(1, 1, 1, 1), {}, 0, std::nullopt);
        reader->SetNoMoreChunks();
        DrainInvoker();

        auto result = MakeSingleRecordResult(7, 0, {{42, 4242}}, /*finished*/ true);
        mock->SetNextReadResult(std::move(result));
        batch = WaitFor(reader->Read())
            .ValueOrThrow();
    }

    // Reader gone; batch still valid.
    ASSERT_EQ(std::ssize(batch->Records), 1);
    EXPECT_EQ(batch->Records[0].Rows[0][0].Data.Int64, 42);
    EXPECT_EQ(batch->Records[0].Rows[0][1].Data.Int64, 4242);
}

} // namespace
} // namespace NYT::NPushBasedShuffleClient
