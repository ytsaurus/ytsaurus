#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/partition_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/session_provider.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sort_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/sorted_merging_reader.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <yt/yt/core/misc/finally.h>

#include <algorithm>
#include <csignal>
#include <iterator>
#include <set>
#include <utility>
#include <vector>

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;
using namespace NPushBasedShuffleClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static int GetPidByPort(int port)
{
    auto command = Format("ss -tlnp 'sport = :%v'", port);
    FILE* pipe = popen(command.c_str(), "r");
    YT_VERIFY(pipe);

    char buffer[256];
    std::string output;
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }
    pclose(pipe);

    auto pidPos = output.find("pid=");
    YT_VERIFY(pidPos != std::string::npos);

    auto pidStart = pidPos + 4;
    auto pidEnd = output.find(',', pidStart);
    YT_VERIFY(pidEnd != std::string::npos);

    return std::stoi(output.substr(pidStart, pidEnd - pidStart));
}

// TODO(apollo1321): extract PauseProcess + GetPidByPort to test_base/. The
// helper is duplicated verbatim from test_distributed_chunk_sessions.cpp; a
// third C++ test will likely want it too.
[[nodiscard]] static auto PauseProcess(const std::string& dataNode)
{
    auto colonPos = dataNode.rfind(':');
    int port = std::stoi(dataNode.substr(colonPos + 1));
    int pid = GetPidByPort(port);

    YT_VERIFY(kill(pid, SIGSTOP) == 0);

    return Finally([pid] {
        YT_VERIFY(kill(pid, SIGCONT) == 0);
    });
}

////////////////////////////////////////////////////////////////////////////////

class TSortReaderRowBatchAdapter
    : public IRowBatchReader
{
public:
    TSortReaderRowBatchAdapter(
        ISortReaderPtr reader,
        TNameTablePtr nameTable)
        : Reader_(std::move(reader))
        , NameTable_(std::move(nameTable))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        if (Drained_) {
            return nullptr;
        }
        if (!PendingRows_) {
            PendingRows_ = Reader_->Read();
        }
        if (!PendingRows_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        auto rows = PendingRows_.GetOrCrash()
            .ValueOrThrow();
        PendingRows_.Reset();
        if (rows.empty()) {
            Drained_ = true;
            return nullptr;
        }

        return CreateBatchFromUnversionedRows(std::move(rows));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return PendingRows_
            ? PendingRows_.As<void>()
            : OKFuture;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const ISortReaderPtr Reader_;
    const TNameTablePtr NameTable_;

    TFuture<TSharedRange<TUnversionedRow>> PendingRows_;
    bool Drained_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionReaderTest
    : public TApiTestBase
{
protected:
    static constexpr int KeyColumnId = 0;
    static constexpr int WriterIdColumnId = 1;
    static constexpr int RowIdColumnId = 2;
    static constexpr int PayloadColumnId = 3;

    NNative::IClientPtr NativeClient_;
    NNative::IConnectionPtr NativeConnection_;
    ISuspendableActionQueuePtr ActionQueue_;
    ITransactionPtr Transaction_;
    TDistributedChunkSessionControllerConfigPtr ControllerConfig_;
    TDistributedChunkSessionPoolConfigPtr PoolConfig_;
    TJournalChunkWriterConfigPtr WriterConfig_;
    TJournalChunkWriterOptionsPtr WriterOptions_;

    // Chunk session may yield uncommitted records (records read past a transient
    // commit horizon that later didn't reach quorum); the reader passes
    // those through. Tests that exercise session rotation / failover
    // therefore assert readSet ⊇ writtenSet rather than strict equality.
    template <class TSet>
    static void ExpectSetIncludes(const TSet& readSet, const TSet& writtenSet)
    {
        std::vector<typename TSet::value_type> missing;
        std::set_difference(
            writtenSet.begin(), writtenSet.end(),
            readSet.begin(), readSet.end(),
            std::back_inserter(missing));
        EXPECT_TRUE(missing.empty()) << "Missing " << missing.size() << " written record(s) in readSet";
    }

    // For paths where the writer cannot produce extras (single writer + single
    // session + no induced failure), assert exact equality so a regression
    // that adds spurious records is caught instead of being masked by ⊇.
    template <class TSet>
    static void ExpectSetEquals(const TSet& readSet, const TSet& writtenSet)
    {
        std::vector<typename TSet::value_type> missing;
        std::set_difference(
            writtenSet.begin(), writtenSet.end(),
            readSet.begin(), readSet.end(),
            std::back_inserter(missing));
        std::vector<typename TSet::value_type> extra;
        std::set_difference(
            readSet.begin(), readSet.end(),
            writtenSet.begin(), writtenSet.end(),
            std::back_inserter(extra));
        EXPECT_TRUE(missing.empty()) << "Missing " << missing.size() << " written record(s) in readSet";
        EXPECT_TRUE(extra.empty()) << "Unexpected " << extra.size() << " extra record(s) in readSet";
    }

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        NativeConnection_ = NativeClient_->GetNativeConnection();

        ActionQueue_ = CreateSuspendableActionQueue("TestShuffleReader");

        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        ControllerConfig_ = New<TDistributedChunkSessionControllerConfig>();
        ControllerConfig_->Account = "intermediate";
        ControllerConfig_->SessionPingPeriod = TDuration::MilliSeconds(100);
        ControllerConfig_->SessionTimeout = TDuration::Seconds(5);

        PoolConfig_ = New<TDistributedChunkSessionPoolConfig>();

        WriterConfig_ = New<TJournalChunkWriterConfig>();
        WriterOptions_ = New<TJournalChunkWriterOptions>();
        WriterOptions_->ReplicationFactor = 3;
        WriterOptions_->ReadQuorum = 2;
        WriterOptions_->WriteQuorum = 2;
    }

    void TearDown() override
    {
        ActionQueue_->Shutdown(/*graceful*/ true);
    }

    struct TWrittenRecord
    {
        i32 WriterId = 0;
        i64 RowId = 0;
        i64 Key = 0;
        i64 Value = 0;

        auto operator<=>(const TWrittenRecord&) const = default;
    };

    struct TWriterContext
    {
        IDistributedChunkSessionPoolPtr Pool;
        IPushBasedShuffleWriterPtr Writer;
        TRowBufferPtr RowBuffer;
        std::vector<TWrittenRecord> WrittenRecords;
        int Slot = 0;
    };

    TWriterContext MakeWriter(
        i32 writerId,
        IPartitionerPtr partitioner = nullptr)
    {
        TWriterContext ctx;
        ctx.RowBuffer = New<TRowBuffer>();
        ctx.Pool = CreateDistributedChunkSessionPool(
            NativeClient_,
            PoolConfig_,
            ControllerConfig_,
            Transaction_->GetId(),
            WriterOptions_,
            WriterConfig_,
            ActionQueue_->GetInvoker());

        auto provider = CreateDirectPartitionWriteSessionProvider(
            ctx.Pool,
            std::vector<int>{ctx.Slot});

        auto writerConfig = New<TShuffleWriterConfig>();
        writerConfig->MemoryBudget = 16_MB;
        writerConfig->Codec = NCompression::ECodec::Lz4;

        if (!partitioner) {
            partitioner = CreateColumnBasedPartitioner(
                /*partitionCount*/ 1,
                /*columnId*/ 0);
        }

        ctx.Writer = CreatePushBasedShuffleWriter(
            writerConfig,
            provider,
            std::move(partitioner),
            NativeConnection_,
            writerId,
            ActionQueue_->GetInvoker());

        return ctx;
    }

    TWriterContext MakeSinglePartitionSortPipelineWriter(i32 writerId)
    {
        std::vector<TOwningKeyBound> partitionLowerBounds{
            TOwningKeyBound::MakeUniversal(/*isUpper*/ false),
        };
        return MakeWriter(
            writerId,
            CreateOrderedPartitioner(
                std::move(partitionLowerBounds),
                TComparator({ESortOrder::Ascending})));
    }

    TWriterContext MakeTightBudgetWriter(i32 writerId)
    {
        TWriterContext ctx;
        ctx.RowBuffer = New<TRowBuffer>();
        ctx.Pool = CreateDistributedChunkSessionPool(
            NativeClient_,
            PoolConfig_,
            ControllerConfig_,
            Transaction_->GetId(),
            WriterOptions_,
            WriterConfig_,
            ActionQueue_->GetInvoker());

        auto provider = CreateDirectPartitionWriteSessionProvider(
            ctx.Pool,
            std::vector<int>{ctx.Slot});

        auto writerConfig = New<TShuffleWriterConfig>();
        writerConfig->MemoryBudget = 1_MB;
        writerConfig->BuildersBudgetFraction = 0.01;
        writerConfig->Codec = NCompression::ECodec::Lz4;

        auto partitioner = CreateColumnBasedPartitioner(1, 0);

        ctx.Writer = CreatePushBasedShuffleWriter(
            writerConfig,
            provider,
            partitioner,
            NativeConnection_,
            writerId,
            ActionQueue_->GetInvoker());
        return ctx;
    }

    void WriteRecords(TWriterContext& ctx, int count, i32 writerIdToTrack)
    {
        std::vector<TUnversionedRow> rows;
        for (int i = 0; i < count; ++i) {
            TUnversionedRowBuilder rb;
            rb.AddValue(MakeUnversionedInt64Value(0, /*id*/ 0));
            rb.AddValue(MakeUnversionedInt64Value(i, /*id*/ 1));
            rb.AddValue(MakeUnversionedInt64Value(i * 1000, /*id*/ 2));
            rows.push_back(ctx.RowBuffer->CaptureRow(rb.GetRow()));
            ctx.WrittenRecords.push_back({
                writerIdToTrack,
                static_cast<i64>(ctx.WrittenRecords.size()),
                static_cast<i64>(i),
                static_cast<i64>(i * 1000),
            });
        }
        WaitFor(ctx.Writer->Write(TRange<TUnversionedRow>(rows.data(), rows.size())))
            .ThrowOnError();
    }

    void WriteSortPipelineRows(
        TWriterContext* context,
        const std::vector<std::pair<i64, i64>>& keyPayloadPairs)
    {
        YT_VERIFY(context);

        std::vector<TUnversionedRow> rows;
        rows.reserve(keyPayloadPairs.size());
        for (const auto& [key, payload] : keyPayloadPairs) {
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(key, KeyColumnId));
            builder.AddValue(MakeUnversionedInt64Value(payload, PayloadColumnId));
            rows.push_back(context->RowBuffer->CaptureRow(builder.GetRow()));
        }
        WaitFor(context->Writer->Write(TRange(rows)))
            .ThrowOnError();
    }

    void CloseAndFinalize(TWriterContext& ctx)
    {
        WaitFor(ctx.Writer->Close())
            .ThrowOnError();
        ctx.Pool->FinalizeSlot(ctx.Slot);
    }

    std::vector<TSlotChunkInfo> GetChunkInfos(TWriterContext& ctx)
    {
        return WaitFor(ctx.Pool->GetSlotChunks(ctx.Slot))
            .ValueOrThrow();
    }

    // Poll GetSlotChunks until a chunk appears or we time out.
    // Useful for mid-stream tests where the first session is being created asynchronously.
    std::vector<TSlotChunkInfo> WaitForFirstChunk(TWriterContext& ctx, TDuration timeout = TDuration::Seconds(30))
    {
        auto deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            auto chunks = GetChunkInfos(ctx);
            if (!chunks.empty()) {
                return chunks;
            }
            Sleep(TDuration::MilliSeconds(50));
        }
        return GetChunkInfos(ctx);
    }

    TPartitionReaderConfigPtr MakePartitionReaderConfig()
    {
        auto config = New<TPartitionReaderConfig>();
        config->ChunkSessionReaderConfig = New<TDistributedChunkSessionReaderConfig>();
        // Tighter knobs than the production defaults so the data-node-failure
        // test doesn't spend ~150s exhausting retries against a paused replica.
        config->ChunkSessionReaderConfig->ProbeTimeout = TDuration::Seconds(1);
        config->ChunkSessionReaderConfig->PollInterval = TDuration::MilliSeconds(100);
        config->ChunkSessionReaderConfig->RefreshTimeout = TDuration::Seconds(2);
        config->ChunkSessionReaderConfig->ErrorBackoff.MinBackoff = TDuration::MilliSeconds(50);
        // Tight upper bound so DataNodeFailureDuringRead doesn't spend most of
        // its wall time inside a single backoff sleep. Once IPushBasedShuffle-
        // Reader exposes statistics we can switch that test to assert
        // ErrorAttemptCount > 0 rather than relying on timing.
        config->ChunkSessionReaderConfig->ErrorBackoff.MaxBackoff = TDuration::MilliSeconds(250);
        config->Codec = NCompression::ECodec::Lz4;
        config->RowBufferStartChunkSize = 64_KB;
        config->MaxBytesPerRead = 64_MB;

        return config;
    }

    IPushBasedPartitionReaderPtr MakeReader()
    {
        return CreatePushBasedPartitionReader(
            MakePartitionReaderConfig(),
            NativeClient_,
            New<TChunkReaderHost>(NativeClient_),
            /*readQuorum*/ 2,
            ActionQueue_->GetInvoker());
    }

    ISortReaderPtr MakeIdentityPreservingSortReader(
        const std::vector<TSlotChunkInfo>& chunks,
        TIdentityColumnIds identityColumnIds)
    {
        auto sortConfig = New<TSortReaderConfig>();
        sortConfig->BucketRowCount = 2;
        sortConfig->MaxRowsPerRead = 2;

        auto reader = CreateSortReader(
            sortConfig,
            MakePartitionReaderConfig(),
            NativeClient_,
            New<TChunkReaderHost>(NativeClient_),
            /*readQuorum*/ 2,
            TComparator({ESortOrder::Ascending}),
            identityColumnIds,
            ActionQueue_->GetInvoker(),
            ActionQueue_->GetInvoker());
        for (const auto& chunk : chunks) {
            reader->AddChunk(
                chunk.ChunkId,
                chunk.Replicas,
                /*startRecordIndex*/ 0,
                /*rangeEndRecordIndex*/ std::nullopt);
        }
        reader->SetNoMoreChunks();
        return reader;
    }

    std::vector<NChunkClient::NProto::TChunkSpec> WriteIntermediateChunks(
        const ISortReaderPtr& reader,
        const TTableSchemaPtr& schema,
        const TNameTablePtr& nameTable)
    {
        auto options = New<NTableClient::TTableWriterOptions>();
        options->Account = "intermediate";

        auto writer = CreateSchemalessMultiChunkWriter(
            New<NTableClient::TTableWriterConfig>(),
            options,
            nameTable,
            schema,
            /*lastKey*/ {},
            NativeClient_,
            /*localHostName*/ {},
            NObjectClient::CellTagFromId(Transaction_->GetId()),
            Transaction_->GetId(),
            NullTableSchemaId,
            /*dataSink*/ {},
            /*writeBlocksOptions*/ {});

        PipeReaderToWriter(
            New<TSortReaderRowBatchAdapter>(reader, nameTable),
            writer,
            /*options*/ {});

        auto chunkSpecs = writer->GetWrittenChunkSpecs();
        YT_VERIFY(std::ssize(chunkSpecs) == 1);
        chunkSpecs[0].set_use_new_override_semantics(true);
        chunkSpecs[0].set_row_count_override(
            writer->GetDataStatistics().row_count());
        return chunkSpecs;
    }

    ISchemalessMultiChunkReaderPtr MakeIntermediateReader(
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        const TTableSchemaPtr& schema,
        const TNameTablePtr& nameTable)
    {
        auto sourceDirectory = New<TDataSourceDirectory>();
        auto& source = sourceDirectory->DataSources().emplace_back(New<TDataSource>());
        source->Schema() = schema;

        std::vector<TDataSliceDescriptor> dataSlices;
        dataSlices.reserve(chunkSpecs.size());
        for (const auto& chunkSpec : chunkSpecs) {
            dataSlices.emplace_back(chunkSpec);
        }

        return CreateSchemalessSequentialMultiReader(
            New<NTableClient::TTableReaderConfig>(),
            New<NTableClient::TTableReaderOptions>(),
            New<TMultiChunkReaderHost>(New<TChunkReaderHost>(NativeClient_)),
            sourceDirectory,
            dataSlices,
            /*hintKeyPrefixes*/ std::nullopt,
            nameTable,
            /*chunkReadOptions*/ {},
            TReaderInterruptionOptions::NonInterruptible());
    }

    std::vector<std::pair<i64, i64>> DrainMergedReader(
        const ISchemalessMultiChunkReaderPtr& reader)
    {
        std::vector<std::pair<i64, i64>> keyPayloadPairs;
        TRowBatchReadOptions readOptions{
            .MaxRowsPerRead = 1,
        };
        while (auto batch = reader->Read(readOptions)) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (const auto& row : batch->MaterializeRows()) {
                EXPECT_EQ(row.GetCount(), 2u);
                if (row.GetCount() != 2) {
                    continue;
                }
                EXPECT_EQ(row[0].Id, KeyColumnId);
                EXPECT_EQ(row[1].Id, PayloadColumnId);
                keyPayloadPairs.emplace_back(
                    row[0].Data.Int64,
                    row[1].Data.Int64);
            }
        }
        return keyPayloadPairs;
    }

    std::vector<TWrittenRecord> DrainReader(const IPushBasedPartitionReaderPtr& reader)
    {
        std::vector<TWrittenRecord> out;
        while (true) {
            auto batch = WaitFor(reader->Read())
                .ValueOrThrow();
            for (auto& record : batch->Records) {
                for (int i = 0; i < record.Header.RowCount; ++i) {
                    auto row = record.Rows[i];
                    out.push_back({
                        record.Header.WriterId,
                        record.Header.StartRow + i,
                        row[1].Data.Int64,
                        row[2].Data.Int64,
                    });
                }
            }
            if (batch->Finished) {
                break;
            }
        }
        return out;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, RoundTripHappyPath)
{
    auto writerCtx = MakeWriter(/*writerId*/ 42);
    constexpr int M = 200;
    WriteRecords(writerCtx, M, /*writerIdToTrack*/ 42);
    CloseAndFinalize(writerCtx);

    auto chunks = GetChunkInfos(writerCtx);
    ASSERT_FALSE(chunks.empty());

    auto reader = MakeReader();
    for (auto& info : chunks) {
        reader->AddChunk(info.ChunkId, info.Replicas, /*start*/ 0, /*end*/ std::nullopt);
    }
    reader->SetNoMoreChunks();

    auto readRecords = DrainReader(reader);

    std::set<TWrittenRecord> writtenSet, readSet;
    for (auto& r : writerCtx.WrittenRecords) {
        writtenSet.insert(r);
    }
    for (auto& r : readRecords) {
        readSet.insert(r);
    }
    // Single writer + single session + no failure injection — no path that
    // can produce chunk session duplicates here, so assert exact equality.
    ExpectSetEquals(readSet, writtenSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, MultiChunkReader)
{
    constexpr int NumWriters = 3;
    constexpr int RecordsPerWriter = 100;
    std::vector<TWriterContext> writerCtxs;
    for (int i = 0; i < NumWriters; ++i) {
        auto ctx = MakeWriter(/*writerId*/ i);
        WriteRecords(ctx, RecordsPerWriter, /*writerIdToTrack*/ i);
        CloseAndFinalize(ctx);
        writerCtxs.push_back(std::move(ctx));
    }

    std::vector<TSlotChunkInfo> allChunks;
    std::set<TWrittenRecord> writtenSet;
    for (auto& ctx : writerCtxs) {
        auto chunks = GetChunkInfos(ctx);
        for (auto& c : chunks) {
            allChunks.push_back(c);
        }
        for (auto& r : ctx.WrittenRecords) {
            writtenSet.insert(r);
        }
    }
    ASSERT_GE(std::ssize(allChunks), NumWriters);

    auto reader = MakeReader();
    for (auto& info : allChunks) {
        reader->AddChunk(info.ChunkId, info.Replicas, 0, std::nullopt);
    }
    reader->SetNoMoreChunks();

    auto readRecords = DrainReader(reader);
    std::set<TWrittenRecord> readSet;
    for (auto& r : readRecords) {
        readSet.insert(r);
    }
    ExpectSetIncludes(readSet, writtenSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, MultipleIntermediateSortersRoundTripThroughSortedMerge)
{
    const std::vector<std::pair<i64, i64>> retriedRows{
        {3, 103},
        {1, 101},
        {2, 102},
    };

    auto originalWriter = MakeSinglePartitionSortPipelineWriter(/*writerId*/ 1);
    WriteSortPipelineRows(&originalWriter, retriedRows);
    CloseAndFinalize(originalWriter);

    auto retryWriter = MakeSinglePartitionSortPipelineWriter(/*writerId*/ 1);
    WriteSortPipelineRows(&retryWriter, retriedRows);
    CloseAndFinalize(retryWriter);

    auto secondWriter = MakeSinglePartitionSortPipelineWriter(/*writerId*/ 2);
    WriteSortPipelineRows(&secondWriter, {
        {2, 202},
        {0, 200},
        {4, 204},
    });
    CloseAndFinalize(secondWriter);

    auto invalidWriter = MakeSinglePartitionSortPipelineWriter(/*writerId*/ 99);
    WriteSortPipelineRows(&invalidWriter, {
        {0, 900},
        {5, 905},
    });
    CloseAndFinalize(invalidWriter);

    auto originalChunks = GetChunkInfos(originalWriter);
    auto retryChunks = GetChunkInfos(retryWriter);
    auto secondWriterChunks = GetChunkInfos(secondWriter);
    auto invalidWriterChunks = GetChunkInfos(invalidWriter);
    ASSERT_FALSE(originalChunks.empty());
    ASSERT_FALSE(retryChunks.empty());
    ASSERT_FALSE(secondWriterChunks.empty());
    ASSERT_FALSE(invalidWriterChunks.empty());

    std::vector<TSlotChunkInfo> firstSorterChunks = originalChunks;
    firstSorterChunks.insert(
        firstSorterChunks.end(),
        invalidWriterChunks.begin(),
        invalidWriterChunks.end());
    std::vector<TSlotChunkInfo> secondSorterChunks = retryChunks;
    secondSorterChunks.insert(
        secondSorterChunks.end(),
        secondWriterChunks.begin(),
        secondWriterChunks.end());

    const TIdentityColumnIds identityColumnIds{
        .WriterId = WriterIdColumnId,
        .RowId = RowIdColumnId,
    };
    auto intermediateSchema = New<TTableSchema>(std::vector{
        TColumnSchema("key", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("writer_id", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("row_id", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("payload", EValueType::Int64),
    });
    auto intermediateNameTable = TNameTable::FromSchema(*intermediateSchema);

    auto firstIntermediateChunks = WriteIntermediateChunks(
        MakeIdentityPreservingSortReader(firstSorterChunks, identityColumnIds),
        intermediateSchema,
        intermediateNameTable);
    auto secondIntermediateChunks = WriteIntermediateChunks(
        MakeIdentityPreservingSortReader(secondSorterChunks, identityColumnIds),
        intermediateSchema,
        intermediateNameTable);

    std::vector<ISchemalessMultiChunkReaderPtr> intermediateReaders{
        MakeIntermediateReader(
            firstIntermediateChunks,
            intermediateSchema,
            intermediateNameTable),
        MakeIntermediateReader(
            secondIntermediateChunks,
            intermediateSchema,
            intermediateNameTable),
    };
    auto mergedReader = CreateIdentityAwareSortedMergingReader(
        intermediateReaders,
        intermediateSchema->ToComparator(),
        identityColumnIds,
        TValidWriterIds{1, 2});

    EXPECT_EQ(
        DrainMergedReader(mergedReader),
        (std::vector<std::pair<i64, i64>>{
            {0, 200},
            {1, 101},
            {2, 102},
            {2, 202},
            {3, 103},
            {4, 204},
        }));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, DynamicAddChunk)
{
    auto ctx1 = MakeWriter(/*writerId*/ 1);
    WriteRecords(ctx1, /*count*/ 50, 1);
    CloseAndFinalize(ctx1);
    auto chunks1 = GetChunkInfos(ctx1);
    ASSERT_FALSE(chunks1.empty());

    auto ctx2 = MakeWriter(/*writerId*/ 2);
    WriteRecords(ctx2, /*count*/ 50, 2);
    CloseAndFinalize(ctx2);
    auto chunks2 = GetChunkInfos(ctx2);
    ASSERT_FALSE(chunks2.empty());

    auto reader = MakeReader();
    for (auto& c : chunks1) {
        reader->AddChunk(c.ChunkId, c.Replicas, 0, std::nullopt);
    }

    std::vector<TWrittenRecord> phase1Records;
    while (std::ssize(phase1Records) < std::ssize(ctx1.WrittenRecords)) {
        auto batch = WaitFor(reader->Read())
            .ValueOrThrow();
        for (auto& record : batch->Records) {
            for (int i = 0; i < record.Header.RowCount; ++i) {
                auto row = record.Rows[i];
                phase1Records.push_back({
                    record.Header.WriterId,
                    record.Header.StartRow + i,
                    row[1].Data.Int64,
                    row[2].Data.Int64,
                });
            }
        }
    }
    EXPECT_EQ(std::ssize(phase1Records), std::ssize(ctx1.WrittenRecords));

    for (auto& c : chunks2) {
        reader->AddChunk(c.ChunkId, c.Replicas, 0, std::nullopt);
    }
    reader->SetNoMoreChunks();

    auto allRecords = phase1Records;
    while (true) {
        auto batch = WaitFor(reader->Read())
            .ValueOrThrow();
        for (auto& record : batch->Records) {
            for (int i = 0; i < record.Header.RowCount; ++i) {
                auto row = record.Rows[i];
                allRecords.push_back({
                    record.Header.WriterId,
                    record.Header.StartRow + i,
                    row[1].Data.Int64,
                    row[2].Data.Int64,
                });
            }
        }
        if (batch->Finished) {
            break;
        }
    }

    std::set<TWrittenRecord> writtenSet, readSet;
    for (auto& r : ctx1.WrittenRecords) {
        writtenSet.insert(r);
    }
    for (auto& r : ctx2.WrittenRecords) {
        writtenSet.insert(r);
    }
    for (auto& r : allRecords) {
        readSet.insert(r);
    }
    ExpectSetIncludes(readSet, writtenSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, ReadWhileWriting)
{
    auto ctx = MakeTightBudgetWriter(/*writerId*/ 5);
    WriteRecords(ctx, /*count*/ 30, /*writerIdToTrack*/ 5);

    auto chunksMid = WaitForFirstChunk(ctx);
    ASSERT_FALSE(chunksMid.empty());

    auto reader = MakeReader();
    reader->AddChunk(chunksMid[0].ChunkId, chunksMid[0].Replicas, 0, std::nullopt);

    auto collectFuture = BIND([reader] {
        std::vector<TWrittenRecord> out;
        while (true) {
            auto batch = WaitFor(reader->Read())
                .ValueOrThrow();
            for (auto& record : batch->Records) {
                for (int i = 0; i < record.Header.RowCount; ++i) {
                    auto row = record.Rows[i];
                    out.push_back({
                        record.Header.WriterId,
                        record.Header.StartRow + i,
                        row[1].Data.Int64,
                        row[2].Data.Int64,
                    });
                }
            }
            if (batch->Finished) {
                break;
            }
        }
        return out;
    })
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run();

    WriteRecords(ctx, /*count*/ 30, /*writerIdToTrack*/ 5);
    CloseAndFinalize(ctx);
    reader->SetNoMoreChunks();

    auto readRecords = WaitFor(collectFuture)
        .ValueOrThrow();

    std::set<TWrittenRecord> writtenSet, readSet;
    for (auto& r : ctx.WrittenRecords) {
        writtenSet.insert(r);
    }
    for (auto& r : readRecords) {
        readSet.insert(r);
    }
    // Single writer + single session + no failure injection — `Close` blocks
    // until write-quorum acks, so there's no way for the reader to observe a
    // record that isn't in writtenSet. Assert exact equality.
    ExpectSetEquals(readSet, writtenSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionReaderTest, DataNodeFailureDuringRead)
{
    auto ctx = MakeTightBudgetWriter(/*writerId*/ 9);
    WriteRecords(ctx, /*count*/ 30, 9);

    auto chunks = WaitForFirstChunk(ctx);
    ASSERT_FALSE(chunks.empty());
    auto chunkId = chunks[0].ChunkId;

    auto reader = MakeReader();
    reader->AddChunk(chunkId, chunks[0].Replicas, 0, std::nullopt);

    auto collectFuture = BIND([reader] {
        std::vector<TWrittenRecord> out;
        while (true) {
            auto batch = WaitFor(reader->Read())
                .ValueOrThrow();
            for (auto& record : batch->Records) {
                for (int i = 0; i < record.Header.RowCount; ++i) {
                    auto row = record.Rows[i];
                    out.push_back({
                        record.Header.WriterId,
                        record.Header.StartRow + i,
                        row[1].Data.Int64,
                        row[2].Data.Int64,
                    });
                }
            }
            if (batch->Finished) {
                break;
            }
        }
        return out;
    })
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run();

    auto sessionDesc = WaitFor(ctx.Pool->GetSession(ctx.Slot))
        .ValueOrThrow();
    auto replicaStrings = ConvertTo<std::vector<std::string>>(
        WaitFor(NativeClient_->GetNode(Format("#%v/@stored_replicas", chunkId)))
            .ValueOrThrow());
    std::string victimAddress;
    for (auto& addr : replicaStrings) {
        if (addr != sessionDesc.SequencerNode.GetDefaultAddress()) {
            victimAddress = addr;
            break;
        }
    }
    ASSERT_FALSE(victimAddress.empty());
    auto pauseGuard = PauseProcess(victimAddress);

    WriteRecords(ctx, /*count*/ 30, 9);
    CloseAndFinalize(ctx);
    reader->SetNoMoreChunks();

    auto readRecords = WaitFor(collectFuture)
        .ValueOrThrow();

    std::set<TWrittenRecord> writtenSet, readSet;
    for (auto& r : ctx.WrittenRecords) {
        writtenSet.insert(r);
    }
    for (auto& r : readRecords) {
        readSet.insert(r);
    }
    ExpectSetIncludes(readSet, writtenSet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
