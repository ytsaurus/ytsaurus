#include <yt/yt/flow/library/cpp/connectors/static_table/arrival_order_table_sink.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow::NStaticTableConnector {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace testing;

const NLogging::TLogger Logger("ArrivalOrderTableSinkTest");
const TStreamId StreamId("test");
const TDuration TableTtl = TDuration::Days(7);
constexpr auto MessageSystemTimestamp = TSystemTimestamp(1700000000);
const NYPath::TYPath OutputDirectory("//tmp/output");
const TPartitionId PartitionId(TGuid::Create());

NYPath::TRichYPath MakePipelinePath()
{
    NYPath::TRichYPath path("//pipeline");
    path.SetCluster("pipeline");
    return path;
}

const NYPath::TRichYPath PipelinePath = MakePipelinePath();

std::string SerializeSourceKey(ui64 sourceKey)
{
    return ConvertToYsonString(MakeKey(sourceKey).Underlying(), NYson::EYsonFormat::Text).ToString();
}

TArrivalOrderTableSinkProgressPtr MakeProgress(
    TInstant nextTableTimestamp,
    std::optional<i64> persistedThroughId = {},
    ui64 sourceKey = 0,
    TSystemTimestamp systemTimestamp = MessageSystemTimestamp)
{
    auto progress = New<TArrivalOrderTableSinkProgress>();
    progress->Owner->PipelinePath = PipelinePath;
    progress->Owner->ComputationId = TComputationId("reader");
    progress->Owner->SinkId = TSinkId("main");
    progress->NextTableTimestamp = nextTableTimestamp;
    if (persistedThroughId) {
        auto partitionProgress = New<TArrivalOrderTableSinkPartitionProgress>();
        partitionProgress->SystemTimestamp = systemTimestamp;
        partitionProgress->MessageId = TMessageId(LexicographicallySerialize(*persistedThroughId));
        progress->Partitions[SerializeSourceKey(sourceKey)] = std::move(partitionProgress);
    }
    return progress;
}

class TRecordingClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TRecordingClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        RequestedClusters.push_back(ToString(clusterUrl));
        return Client_;
    }

    std::vector<std::string> RequestedClusters;

private:
    const IClientPtr Client_;
};

class TTestTableWriter
    : public ITableWriter
{
public:
    explicit TTestTableWriter(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
        , NameTable_(New<TNameTable>())
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        RowCount += rows.Size();
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return OKFuture;
    }

    TFuture<void> Close() override
    {
        return OKFuture;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    i64 RowCount = 0;

private:
    const TTableSchemaPtr Schema_;
    const TNameTablePtr NameTable_;
};

struct TSinkOptions
{
    TDuration TablePeriod = TDuration::Minutes(5);
    i64 MaxRowCount = 1;
    i64 MaxDataWeight = 1_GB;
    TDuration RetryBackoff = TDuration::MilliSeconds(1);
    bool UseDataWeightColumn = true;
    std::string DataWeightColumnName = "data_weight";
};

struct TBatchObservation
{
    NYPath::TYPath TablePath;
    TInstant TableTimestamp;
    TInstant ExpirationTime;
    TArrivalOrderTableSinkProgressPtr PublishedProgress;
    TIntrusivePtr<TTestTableWriter> Writer;
};

struct TCommitExpectation
{
    bool FailCommit = false;
    std::function<void()> StartHook;
    std::function<TFuture<NApi::TTransactionCommitResult>()> CommitAction;
    bool ExpectAbort = false;
};

class TArrivalOrderTableSinkTest
    : public ::testing::Test
{
protected:
    IThreadPoolPtr ThreadPool = CreateThreadPool(1, "ArrivalOrder");
    TTableSchemaPtr Schema = New<TTableSchema>(std::vector{
        TColumnSchema("data_weight", EValueType::Int64),
    });
    TComputationStreamSpecStoragePtr StreamSpecStorage = MakeStreamSpecStorage();
    TIntrusivePtr<StrictMock<TMockClient>> Client = New<StrictMock<TMockClient>>();
    TIntrusivePtr<TRecordingClientsCache> ClientsCache = New<TRecordingClientsCache>(Client);
    IStatusProfilerPtr StatusProfiler = CreateSyncStatusProfiler();
    std::deque<ITransactionPtr> Transactions;
    std::deque<std::function<void()>> TransactionStartHooks;
    TWatermarksPtr Watermarks = New<TWatermarks>();
    TWatermarkStatePtr WatermarkState = MakeWatermarkState();
    // A reference into the shared state: assignments are visible to the sink at once.
    TSystemTimestamp& SystemWatermark = Watermarks->SystemWatermark;
    TArrivalOrderTableSinkProgressPtr CreatedInitialProgress;

    void SetUp() override
    {
        EXPECT_CALL(*Client, StartTransaction(NTransactionClient::ETransactionType::Master, _))
            .Times(AnyNumber())
            .WillRepeatedly([this] (auto, const auto&) {
                YT_VERIFY(!Transactions.empty());
                YT_VERIFY(!TransactionStartHooks.empty());
                auto transaction = Transactions.front();
                Transactions.pop_front();
                auto hook = std::move(TransactionStartHooks.front());
                TransactionStartHooks.pop_front();
                if (hook) {
                    hook();
                }
                return MakeFuture<ITransactionPtr>(std::move(transaction));
            });
    }

    void TearDown() override
    {
        EXPECT_TRUE(Transactions.empty());
        EXPECT_TRUE(TransactionStartHooks.empty());
    }

    void ExpectProgressLock(const TIntrusivePtr<StrictMock<TMockTransaction>>& transaction)
    {
        const auto lockId = NCypressClient::TLockId(TGuid::Create());
        EXPECT_CALL(*transaction, LockNode(OutputDirectory, NCypressClient::ELockMode::Shared, _))
            .WillOnce([lockId] (auto, auto, const TLockNodeOptions& options) {
                EXPECT_EQ("progress", options.AttributeKey);
                EXPECT_TRUE(options.Waitable);
                return MakeFuture<TLockNodeResult>(TLockNodeResult{.LockId = lockId});
            });
        EXPECT_CALL(*transaction, GetNode(Format("#%v/@state", lockId), _))
            .WillOnce(Return(MakeFuture(NYson::ConvertToYsonString(std::string("acquired")))));
    }

    void QueueTransaction(ITransactionPtr transaction, std::function<void()> startHook = {})
    {
        Transactions.push_back(std::move(transaction));
        TransactionStartHooks.push_back(std::move(startHook));
    }

    TWatermarkStatePtr MakeWatermarkState()
    {
        auto state = New<TWatermarkState>();
        state->Streams[StreamId] = Watermarks;
        return state;
    }

    TComputationStreamSpecStoragePtr MakeStreamSpecStorage()
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = Schema;
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
        specs[StreamId][TStreamSpecId(1)] = streamSpec;
        return New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(specs),
            New<TTableSchema>(),
            /*evaluatorCache*/ nullptr);
    }

    TArrivalOrderTableSinkPtr MakeSink(const TSinkOptions& options, std::optional<ui64> sourceKey)
    {
        auto spec = New<TSinkSpec>();
        spec->SinkClassName = TypeName<TArrivalOrderTableSink>();
        spec->InputStreamIds = {StreamId};
        spec->Parameters->AddChild("output_directory", ConvertToNode("<cluster=output>//tmp/output"));
        spec->Parameters->AddChild("table_period", ConvertToNode(options.TablePeriod));
        spec->Parameters->AddChild("table_ttl", ConvertToNode(TableTtl));
        if (options.UseDataWeightColumn) {
            spec->Parameters->AddChild("data_weight_column", ConvertToNode(options.DataWeightColumnName));
        }

        auto dynamicSpec = New<TDynamicSinkSpec>();
        dynamicSpec->Parameters->AddChild("max_row_count", ConvertToNode(options.MaxRowCount));
        dynamicSpec->Parameters->AddChild("max_data_weight", ConvertToNode(options.MaxDataWeight));
        dynamicSpec->Parameters->AddChild("retry_backoff", ConvertToNode(options.RetryBackoff));

        auto context = New<TSinkContext>();
        context->Logger = Logger;
        context->ClientsCache = ClientsCache;
        context->PipelinePath = PipelinePath;
        context->StreamSpecStorage = StreamSpecStorage;
        context->SinkSpec = std::move(spec);
        context->Partition = New<TPartition>();
        context->Partition->PartitionId = PartitionId;
        if (sourceKey) {
            context->Partition->SourceKey = MakeKey(*sourceKey);
        }
        context->Partition->ComputationId = TComputationId("reader");
        context->SinkId = TSinkId("main");
        context->PoolInvoker = ThreadPool->GetInvoker();
        context->StatusProfiler = StatusProfiler;

        auto dynamicContext = New<TDynamicSinkContext>();
        dynamicContext->DynamicSinkSpec = std::move(dynamicSpec);
        auto sink = New<TArrivalOrderTableSink>(std::move(context), std::move(dynamicContext));
        sink->UpdateWatermarkState(WatermarkState);
        return sink;
    }

    TOutputMessageConstPtr MakeMessage(i64 id, std::optional<i64> dataWeight = 1)
    {
        TMessageBuilder builder(StreamId, Schema);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        if (dataWeight) {
            builder.Payload().Set<i64>(*dataWeight, "data_weight");
        } else {
            builder.Payload().SetValue(MakeUnversionedNullValue(), "data_weight");
        }
        return New<TOutputMessage>(builder.Finish(), StreamSpecStorage);
    }

    std::shared_ptr<std::atomic<bool>> Distribute(
        const TArrivalOrderTableSinkPtr& sink,
        i64 id,
        std::optional<i64> dataWeight = 1)
    {
        auto callbackFired = std::make_shared<std::atomic<bool>>(false);
        sink->Distribute(
            MakeMessage(id, dataWeight),
            TOnDistributedCallback::FromCallback([callbackFired] {
                callbackFired->store(true);
            }));
        return callbackFired;
    }

    void ExpectInitialization(
        const TArrivalOrderTableSinkProgressPtr& storedProgress = nullptr,
        std::function<void()> startHook = {},
        std::vector<TInstant> existingTableTimestamps = {})
    {
        auto transaction = New<StrictMock<TMockTransaction>>();
        QueueTransaction(transaction, std::move(startHook));

        InSequence sequence;
        EXPECT_CALL(*transaction, CreateNode(OutputDirectory, NObjectClient::EObjectType::MapNode, _))
            .WillOnce([] (auto, auto, const TCreateNodeOptions& options) {
                // The whole seeding scheme rests on these: an existing directory is left
                // untouched, and its attributes are not applied by the create.
                EXPECT_TRUE(options.Recursive);
                EXPECT_TRUE(options.IgnoreExisting);
                return MakeFuture<NCypressClient::TNodeId>(NCypressClient::TNodeId(TGuid::Create()));
            });
        ExpectProgressLock(transaction);
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce([storedProgress] {
                return storedProgress
                    ? MakeFuture(NYson::ConvertToYsonString(storedProgress))
                    : MakeFuture<NYson::TYsonString>(TError(NYTree::EErrorCode::ResolveError, "No such attribute"));
            });
        if (!storedProgress) {
            // A directory that predates the sink has no progress attribute, so it is seeded here.
            EXPECT_CALL(*transaction, ListNode(OutputDirectory, _))
                .WillOnce([existingTableTimestamps] (const auto&, const auto&) {
                    return MakeFuture(BuildYsonStringFluently()
                            .DoListFor(existingTableTimestamps, [] (auto fluent, TInstant timestamp) {
                                fluent.Item()
                                    .BeginAttributes()
                                    .Item("table_timestamp")
                                    .Value(timestamp)
                                    .EndAttributes()
                                    .Value("table");
                            }));
                });
            EXPECT_CALL(*transaction, SetNode(OutputDirectory + "/@progress", _, _))
                .WillOnce([this] (const auto&, const NYson::TYsonString& value, const auto&) {
                    CreatedInitialProgress = ConvertTo<TArrivalOrderTableSinkProgressPtr>(value);
                    return OKFuture;
                });
        }
        EXPECT_CALL(*transaction, Commit(_))
            .WillOnce([] (const TTransactionCommitOptions& options) {
                EXPECT_TRUE(options.PrerequisiteTransactionIds.empty());
                return MakeFuture(TTransactionCommitResult{});
            });
    }

    TArrivalOrderTableSinkPtr InitializeSink(
        const TSinkOptions& options,
        const TArrivalOrderTableSinkProgressPtr& storedProgress = nullptr,
        std::optional<ui64> sourceKey = 0)
    {
        auto sink = MakeSink(options, sourceKey);
        ExpectInitialization(storedProgress);
        sink->Init(nullptr);
        // The external state is read on the first #Commit(), so drive one to leave the sink ready.
        sink->Commit();
        EXPECT_FALSE(ClientsCache->RequestedClusters.empty());
        EXPECT_EQ("output", ClientsCache->RequestedClusters.back());
        return sink;
    }

    void ExpectCoveredBatch(const TArrivalOrderTableSinkProgressPtr& progress)
    {
        auto transaction = New<StrictMock<TMockTransaction>>();
        QueueTransaction(transaction);

        InSequence sequence;
        ExpectProgressLock(transaction);
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce([progress] {
                return MakeFuture(NYson::ConvertToYsonString(progress));
            });
        // Committed rather than aborted: the read may have seeded the progress attribute.
        EXPECT_CALL(*transaction, Commit(_))
            .WillOnce(Return(MakeFuture(TTransactionCommitResult{})));
    }

    std::shared_ptr<TBatchObservation> ExpectCommittedBatch(
        const TArrivalOrderTableSinkProgressPtr& progress,
        i64 expectedRowCount,
        TCommitExpectation expectation = {})
    {
        const auto failCommit = expectation.FailCommit;
        auto transaction = New<StrictMock<TMockTransaction>>();
        QueueTransaction(transaction, std::move(expectation.StartHook));
        auto observation = std::make_shared<TBatchObservation>();
        if (expectedRowCount > 0) {
            observation->Writer = New<TTestTableWriter>(Schema);
        }

        InSequence sequence;
        ExpectProgressLock(transaction);
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce([progress] {
                return MakeFuture(NYson::ConvertToYsonString(progress));
            });
        EXPECT_CALL(*transaction, CreateNode(_, NObjectClient::EObjectType::Table, _))
            .WillOnce([this, observation] (
                const NYPath::TYPath& path,
                auto,
                const TCreateNodeOptions& options) {
                observation->TablePath = path;
                observation->TableTimestamp = options.Attributes->Get<TInstant>("table_timestamp");
                observation->ExpirationTime = options.Attributes->Get<TInstant>("expiration_time");
                EXPECT_EQ(*Schema, *options.Attributes->Get<TTableSchemaPtr>("schema"));
                EXPECT_LE(observation->TableTimestamp + TableTtl, observation->ExpirationTime);
                EXPECT_LE(
                    observation->ExpirationTime,
                    std::max(observation->TableTimestamp, TInstant::Now()) + TableTtl);
                return MakeFuture<NCypressClient::TNodeId>(NCypressClient::TNodeId(TGuid::Create()));
            });
        if (expectedRowCount > 0) {
            EXPECT_CALL(*transaction, CreateTableWriter(_, _))
                .WillOnce(Return(MakeFuture<ITableWriterPtr>(observation->Writer)));
        }
        EXPECT_CALL(*transaction, SetNode(OutputDirectory + "/@progress", _, _))
            .WillOnce([observation] (const auto&, const NYson::TYsonString& value, const auto&) {
                observation->PublishedProgress = ConvertTo<TArrivalOrderTableSinkProgressPtr>(value);
                return OKFuture;
            });
        EXPECT_CALL(*transaction, Commit(_))
            .WillOnce([failCommit, commitAction = std::move(expectation.CommitAction)] (
                const TTransactionCommitOptions& options) mutable {
                EXPECT_TRUE(options.PrerequisiteTransactionIds.empty());
                if (commitAction) {
                    return commitAction();
                }
                return failCommit
                    ? MakeFuture<TTransactionCommitResult>(TError("Injected commit failure"))
                    : MakeFuture(TTransactionCommitResult{});
            });
        if (failCommit || expectation.ExpectAbort) {
            EXPECT_CALL(*transaction, Abort(_))
                .WillOnce(Return(OKFuture));
        }
        return observation;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TArrivalOrderTableSinkTest, ZeroWatermarkDoesNotCreateEmptyTableForOldProgress)
{
    TSinkOptions options;
    auto sink = InitializeSink(options, MakeProgress(TInstant::Zero()));

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
}

TEST_F(TArrivalOrderTableSinkTest, WatermarkMustStrictlyPassSlotEndToCreateEmptyTable)
{
    TSinkOptions options;
    const auto tableTimestamp = TInstant::Now() - TDuration::Hours(1);
    const auto slotEnd = tableTimestamp + options.TablePeriod;
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(options, progress);

    SystemWatermark = TSystemTimestamp(slotEnd.Seconds() - 1);
    sink->Sync(nullptr);
    sink->Commit();

    SystemWatermark = TSystemTimestamp(slotEnd.Seconds());
    sink->Sync(nullptr);
    sink->Commit();

    SystemWatermark = TSystemTimestamp(slotEnd.Seconds() + 1);
    auto batch = ExpectCommittedBatch(progress, 0);
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);

    EXPECT_EQ(tableTimestamp, batch->TableTimestamp);
}

TEST_F(TArrivalOrderTableSinkTest, SourceWideWatermarkWaitsForSlowPartitionBeforeEmptyTable)
{
    const auto makePartition = [] (TSystemTimestamp watermark) {
        auto partition = New<TNodeTraverseData>();
        partition->ReportTime = watermark;
        partition->Streams[StreamId] = MakeCompletedStreamTraverseData(1, watermark);
        return partition;
    };
    const auto mergePartitions = [&] (TSystemTimestamp first, TSystemTimestamp second) {
        auto merged = MergeNodeTraverseData({makePartition(first), makePartition(second)});
        return merged->Streams.at(StreamId)->SystemWatermark;
    };

    TSinkOptions options;
    const auto tableTimestamp = TInstant::Now() - TDuration::Hours(1);
    const auto slotEnd = (tableTimestamp + options.TablePeriod).Seconds();
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(options, progress);

    SystemWatermark = mergePartitions(TSystemTimestamp(slotEnd + 10), TSystemTimestamp(slotEnd));
    sink->Sync(nullptr);
    sink->Commit();

    SystemWatermark = mergePartitions(TSystemTimestamp(slotEnd + 10), TSystemTimestamp(slotEnd + 1));
    auto batch = ExpectCommittedBatch(progress, 0);
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);

    EXPECT_EQ(tableTimestamp, batch->TableTimestamp);
}

TEST_F(TArrivalOrderTableSinkTest, InfiniteWatermarkDoesNotCreateFutureEmptyTable)
{
    TSinkOptions options;
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    SystemWatermark = InfinitySystemTimestamp;
    auto sink = InitializeSink(options, MakeProgress(tableTimestamp));

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
}

TEST_F(TArrivalOrderTableSinkTest, MessageBeforeWatermarkPassesFillsCurrentSlot)
{
    TSinkOptions options{
        .MaxRowCount = 10,
    };
    const auto tableTimestamp = TInstant::Now() - TDuration::Hours(1);
    const auto slotEnd = tableTimestamp + options.TablePeriod;
    auto progress = MakeProgress(tableTimestamp);
    SystemWatermark = TSystemTimestamp(slotEnd.Seconds());
    auto sink = InitializeSink(options, progress);

    auto callback = Distribute(sink, 1);
    SystemWatermark = TSystemTimestamp(slotEnd.Seconds() + 1);
    auto batch = ExpectCommittedBatch(progress, 1);
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);

    EXPECT_EQ(tableTimestamp, batch->TableTimestamp);
    EXPECT_EQ(1, batch->Writer->RowCount);
    EXPECT_FALSE(callback->load());
    sink->Commit();
    EXPECT_TRUE(callback->load());
}

TEST_F(TArrivalOrderTableSinkTest, NegativeDataWeightThrows)
{
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);

    EXPECT_THROW(Distribute(sink, 1, -1), TErrorException);
}

TEST_F(TArrivalOrderTableSinkTest, DataWeightFallsBackToMessageByteSize)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    // Both without the column and with a null value, the weight is the message byte size,
    // which exceeds this limit and must seal the batch immediately.
    for (const auto useColumn : {false, true}) {
        auto progress = MakeProgress(tableTimestamp);
        TSinkOptions options{
            .MaxRowCount = 10,
            .MaxDataWeight = 1,
            .UseDataWeightColumn = useColumn,
        };
        auto sink = InitializeSink(options, progress);
        auto callback = Distribute(sink, 1, useColumn ? std::optional<i64>() : std::optional<i64>(1));
        auto batch = ExpectCommittedBatch(progress, 1);
        sink->Sync(nullptr);
        sink->Commit();
        sink->Sync(nullptr);
        sink->Commit();
        EXPECT_TRUE(callback->load());
        EXPECT_EQ(1, batch->Writer->RowCount);
    }
}

TEST_F(TArrivalOrderTableSinkTest, AmbiguousReplayWithFullFrontierAcknowledgesWithoutDuplicateTable)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options{
        .MaxRowCount = 10,
    };
    auto sink = InitializeSink(options, MakeProgress(tableTimestamp, 10));

    std::vector<std::shared_ptr<std::atomic<bool>>> callbacks;
    for (i64 id = 11; id <= 20; ++id) {
        callbacks.push_back(Distribute(sink, id));
    }

    ExpectCoveredBatch(MakeProgress(tableTimestamp + options.TablePeriod, 20));
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_NO_THROW(sink->Sync(nullptr));
    for (const auto& callback : callbacks) {
        EXPECT_FALSE(callback->load());
    }

    sink->Commit();
    for (const auto& callback : callbacks) {
        EXPECT_TRUE(callback->load());
    }
}

TEST_F(TArrivalOrderTableSinkTest, AmbiguousReplayWithPartialFrontierWritesOnlyUncoveredSuffix)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options{
        .MaxRowCount = 10,
    };
    auto sink = InitializeSink(options, MakeProgress(tableTimestamp, 10));

    std::vector<std::shared_ptr<std::atomic<bool>>> callbacks;
    for (i64 id = 11; id <= 20; ++id) {
        callbacks.push_back(Distribute(sink, id));
    }

    auto partialProgress = MakeProgress(tableTimestamp + options.TablePeriod, 15);
    auto batch = ExpectCommittedBatch(partialProgress, 5);
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_NO_THROW(sink->Sync(nullptr));
    for (const auto& callback : callbacks) {
        EXPECT_FALSE(callback->load());
    }

    sink->Commit();
    for (const auto& callback : callbacks) {
        EXPECT_TRUE(callback->load());
    }
    EXPECT_EQ(tableTimestamp + options.TablePeriod, batch->TableTimestamp);
    EXPECT_EQ(5, batch->Writer->RowCount);
    EXPECT_EQ(
        TMessageId(LexicographicallySerialize(i64{20})),
        batch->PublishedProgress->Partitions.at(SerializeSourceKey(0))->MessageId);
}

TEST_F(TArrivalOrderTableSinkTest, AmbiguousOriginallyEmptyBatchNeedsOnlyTimestampAdvance)
{
    TSinkOptions options;
    const auto tableTimestamp = TInstant::Zero();
    SystemWatermark = InfinitySystemTimestamp;
    auto sink = InitializeSink(options, MakeProgress(tableTimestamp));

    sink->Sync(nullptr);
    ExpectCoveredBatch(MakeProgress(tableTimestamp + options.TablePeriod));
    sink->Commit();
    EXPECT_NO_THROW(sink->Sync(nullptr));
}

TEST_F(TArrivalOrderTableSinkTest, CommitFailureAbortsAndRetryUsesStableTablePath)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options;
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(options, progress);
    auto callback = Distribute(sink, 1);

    auto failedAttempt = ExpectCommittedBatch(progress, 1, {.FailCommit = true});
    auto successfulAttempt = ExpectCommittedBatch(
        progress,
        1,
        {
            .StartHook = [this] {
                EXPECT_TRUE(StatusProfiler->GetStatus().Errors.contains("/retry"));
            },
        });

    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_TRUE(StatusProfiler->GetStatus().Errors.empty());
    EXPECT_NO_THROW(sink->Sync(nullptr));
    EXPECT_FALSE(callback->load());
    EXPECT_EQ(failedAttempt->TablePath, successfulAttempt->TablePath);
    EXPECT_EQ(1, failedAttempt->Writer->RowCount);
    EXPECT_EQ(1, successfulAttempt->Writer->RowCount);

    sink->Commit();
    EXPECT_TRUE(callback->load());
}

TEST_F(TArrivalOrderTableSinkTest, CommitRetryStopsOnFiberCancellationAndReleasesSink)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(TSinkOptions{}, progress);
    auto callback = Distribute(sink, 1);
    sink->Sync(nullptr);

    auto commitStarted = NewPromise<void>();
    auto blockedCommit = NewPromise<TTransactionCommitResult>();
    ExpectCommittedBatch(
        progress,
        1,
        {
            .CommitAction = [commitStarted, blockedCommit] () mutable {
                commitStarted.Set();
                return blockedCommit.ToFuture();
            },
            .ExpectAbort = true,
        });

    auto cancelableContext = New<TCancelableContext>();
    auto weakSink = MakeWeak(sink);
    auto commitFuture = BIND([sink] {
        sink->Commit();
    })
        .AsyncVia(cancelableContext->CreateInvoker(ThreadPool->GetInvoker()))
        .Run();
    sink.Reset();

    WaitFor(commitStarted.ToFuture()).ThrowOnError();
    cancelableContext->Cancel(TError(NYT::EErrorCode::Canceled, "Job stopped"));
    auto result = WaitFor(commitFuture);
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Canceled, result.GetCode());
    commitFuture.Reset();

    // |commitFuture| may complete before the callback frame releases its captures.
    // Drain the same single-thread invoker before checking |weakSink|.
    WaitFor(BIND([] {
    })
            .AsyncVia(ThreadPool->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_FALSE(weakSink.Lock());
    EXPECT_FALSE(callback->load());
}

TEST_F(TArrivalOrderTableSinkTest, RowAndByteLimitsAdvanceExactlyOneSlot)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options{
        .MaxRowCount = 2,
        .MaxDataWeight = 10,
    };
    auto firstProgress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(options, firstProgress);

    auto first = Distribute(sink, 1, 1);
    auto second = Distribute(sink, 2, 1);
    auto third = Distribute(sink, 3, 10);

    auto firstBatch = ExpectCommittedBatch(firstProgress, 2);
    auto secondProgress = MakeProgress(tableTimestamp + options.TablePeriod, 2);
    auto secondBatch = ExpectCommittedBatch(secondProgress, 1);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    EXPECT_FALSE(first->load());
    EXPECT_FALSE(second->load());
    EXPECT_FALSE(third->load());

    sink->Commit();
    EXPECT_TRUE(first->load());
    EXPECT_TRUE(second->load());
    EXPECT_FALSE(third->load());

    sink->Sync(nullptr);
    EXPECT_FALSE(third->load());
    sink->Commit();
    EXPECT_TRUE(third->load());

    EXPECT_EQ(tableTimestamp, firstBatch->TableTimestamp);
    EXPECT_EQ(tableTimestamp + options.TablePeriod, secondBatch->TableTimestamp);
    EXPECT_NE(firstBatch->TablePath, secondBatch->TablePath);
}

TEST_F(TArrivalOrderTableSinkTest, SourceKeysShareTableSequenceAndKeepSeparateFrontiers)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options;
    auto initialProgress = MakeProgress(tableTimestamp);

    auto firstSink = InitializeSink(options, initialProgress, 0);
    auto secondSink = MakeSink(options, /*sourceKey*/ 1);
    secondSink->Init(nullptr);

    auto firstCallback = Distribute(firstSink, 1);
    auto firstBatch = ExpectCommittedBatch(initialProgress, 1);
    firstSink->Sync(nullptr);
    firstSink->Commit();
    firstSink->Sync(nullptr);
    firstSink->Commit();
    EXPECT_TRUE(firstCallback->load());

    auto secondCallback = Distribute(secondSink, 1);
    ExpectInitialization(firstBatch->PublishedProgress);
    auto secondBatch = ExpectCommittedBatch(firstBatch->PublishedProgress, 1);
    secondSink->Sync(nullptr);
    secondSink->Commit();
    secondSink->Sync(nullptr);
    secondSink->Commit();
    EXPECT_TRUE(secondCallback->load());

    EXPECT_EQ(tableTimestamp, firstBatch->TableTimestamp);
    EXPECT_EQ(tableTimestamp + options.TablePeriod, secondBatch->TableTimestamp);
    EXPECT_EQ(2u, secondBatch->PublishedProgress->Partitions.size());
    EXPECT_EQ(
        TMessageId(LexicographicallySerialize(i64{1})),
        secondBatch->PublishedProgress->Partitions.at(SerializeSourceKey(0))->MessageId);
    EXPECT_EQ(
        TMessageId(LexicographicallySerialize(i64{1})),
        secondBatch->PublishedProgress->Partitions.at(SerializeSourceKey(1))->MessageId);
}

TEST_F(TArrivalOrderTableSinkTest, OverdueEmptySlotsAreCommittedOnePeriodAtATime)
{
    TSinkOptions options;
    const auto firstTimestamp = TInstant::Zero();
    SystemWatermark = InfinitySystemTimestamp;
    auto firstProgress = MakeProgress(firstTimestamp);
    auto sink = InitializeSink(options, firstProgress);

    auto firstBatch = ExpectCommittedBatch(firstProgress, 0);
    auto secondProgress = MakeProgress(firstTimestamp + options.TablePeriod);
    auto secondBatch = ExpectCommittedBatch(secondProgress, 0);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);

    EXPECT_EQ(firstTimestamp, firstBatch->TableTimestamp);
    EXPECT_EQ(firstTimestamp + options.TablePeriod, secondBatch->TableTimestamp);
    EXPECT_NE(firstBatch->TablePath, secondBatch->TablePath);
    EXPECT_TRUE(firstBatch->PublishedProgress->Partitions.empty());
    EXPECT_TRUE(secondBatch->PublishedProgress->Partitions.empty());
}

TEST_F(TArrivalOrderTableSinkTest, ForeignDirectoryOwnerFailsWithHandoverHint)
{
    auto foreignProgress = MakeProgress(TInstant::Now() + TDuration::Hours(1));
    foreignProgress->Owner->SinkId = TSinkId("other");

    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);
    ExpectInitialization(foreignProgress);
    EXPECT_THROW_WITH_SUBSTRING(
        sink->Commit(),
        "remove its @progress attribute");
}

TEST_F(TArrivalOrderTableSinkTest, ExternalStateIsNotTouchedBeforeFirstCommit)
{
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);
    auto callback = Distribute(sink, 1);

    // No transaction is queued: the master is only touched from #Commit(), after the epoch is durable.
    EXPECT_TRUE(Transactions.empty());
    EXPECT_FALSE(callback->load());

    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto progress = MakeProgress(tableTimestamp);
    ExpectInitialization(progress);
    auto batch = ExpectCommittedBatch(progress, 1);
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_TRUE(callback->load());
    EXPECT_EQ(1, batch->Writer->RowCount);
}

TEST_F(TArrivalOrderTableSinkTest, PartitionsBelowWatermarkAreForgotten)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto progress = MakeProgress(tableTimestamp);
    const auto retiredKey = SerializeSourceKey(42);
    auto retiredProgress = New<TArrivalOrderTableSinkPartitionProgress>();
    retiredProgress->SystemTimestamp = TSystemTimestamp(MessageSystemTimestamp.Underlying() - 1);
    retiredProgress->MessageId = TMessageId(LexicographicallySerialize(i64{7}));
    progress->Partitions[retiredKey] = retiredProgress;

    SystemWatermark = MessageSystemTimestamp;
    auto sink = InitializeSink(TSinkOptions{}, progress);
    auto callback = Distribute(sink, 1);
    auto batch = ExpectCommittedBatch(progress, 1);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_TRUE(callback->load());

    EXPECT_FALSE(batch->PublishedProgress->Partitions.contains(retiredKey));
    EXPECT_EQ(
        MessageSystemTimestamp,
        batch->PublishedProgress->Partitions.at(SerializeSourceKey(0))->SystemTimestamp);
}

TEST_F(TArrivalOrderTableSinkTest, PartitionWithoutSourceKeyIsTrackedByPartitionId)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(TSinkOptions{}, progress, /*sourceKey*/ std::nullopt);
    auto callback = Distribute(sink, 1);
    auto batch = ExpectCommittedBatch(progress, 1);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_TRUE(callback->load());

    EXPECT_EQ(
        TMessageId(LexicographicallySerialize(i64{1})),
        batch->PublishedProgress->Partitions.at(ToString(PartitionId))->MessageId);
}

TEST_F(TArrivalOrderTableSinkTest, FreshDirectoryIsSeededWithOwnedProgress)
{
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);
    const auto before = TInstant::Now();
    ExpectInitialization();
    sink->Commit();

    ASSERT_TRUE(CreatedInitialProgress);
    EXPECT_EQ(PipelinePath, CreatedInitialProgress->Owner->PipelinePath);
    EXPECT_EQ(TComputationId("reader"), CreatedInitialProgress->Owner->ComputationId);
    EXPECT_EQ(TSinkId("main"), CreatedInitialProgress->Owner->SinkId);
    EXPECT_TRUE(CreatedInitialProgress->Partitions.empty());
    EXPECT_LT(before, CreatedInitialProgress->NextTableTimestamp);
    EXPECT_LE(CreatedInitialProgress->NextTableTimestamp, before + TSinkOptions{}.TablePeriod);
}

TEST_F(TArrivalOrderTableSinkTest, EmptySlotCollectsOnlyFrontiersBelowWatermark)
{
    auto progress = MakeProgress(TInstant::Zero(), /*persistedThroughId*/ 7);
    const auto retiredKey = SerializeSourceKey(42);
    const auto liveKey = SerializeSourceKey(43);
    for (const auto& [key, systemTimestamp] : {
            std::pair(retiredKey, TSystemTimestamp(MessageSystemTimestamp.Underlying() - 1)),
            std::pair(liveKey, TSystemTimestamp(MessageSystemTimestamp.Underlying() + 1)),
         }) {
        auto partitionProgress = New<TArrivalOrderTableSinkPartitionProgress>();
        partitionProgress->SystemTimestamp = systemTimestamp;
        partitionProgress->MessageId = TMessageId(LexicographicallySerialize(i64{1}));
        progress->Partitions[key] = std::move(partitionProgress);
    }

    SystemWatermark = MessageSystemTimestamp;
    auto sink = InitializeSink(TSinkOptions{}, progress);
    // No message is distributed: the sink commits an empty slot.
    auto batch = ExpectCommittedBatch(progress, 0);
    sink->Sync(nullptr);
    sink->Commit();

    const auto& partitions = batch->PublishedProgress->Partitions;
    EXPECT_EQ(MessageSystemTimestamp, partitions.at(SerializeSourceKey(0))->SystemTimestamp);
    EXPECT_FALSE(partitions.contains(retiredKey));
    EXPECT_TRUE(partitions.contains(liveKey));
}

TEST_F(TArrivalOrderTableSinkTest, BufferedRequestsAreSplitByLimitsAfterInitialization)
{
    TSinkOptions options{
        .MaxRowCount = 2,
    };
    auto sink = MakeSink(options, /*sourceKey*/ 0);
    sink->Init(nullptr);
    for (i64 id = 1; id <= 4; ++id) {
        Distribute(sink, id);
    }
    EXPECT_TRUE(Transactions.empty());

    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto firstProgress = MakeProgress(tableTimestamp);
    ExpectInitialization(firstProgress);
    auto firstBatch = ExpectCommittedBatch(firstProgress, 2);
    auto secondProgress = MakeProgress(tableTimestamp + options.TablePeriod, 2);
    auto secondBatch = ExpectCommittedBatch(secondProgress, 2);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();

    EXPECT_EQ(2, firstBatch->Writer->RowCount);
    EXPECT_EQ(2, secondBatch->Writer->RowCount);
}

TEST_F(TArrivalOrderTableSinkTest, ForeignOwnerOnCommitPathAbortsAndRetriesWithoutWriting)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(TSinkOptions{.RetryBackoff = TDuration::Hours(1)}, progress);
    auto callback = Distribute(sink, 1);
    sink->Sync(nullptr);

    auto foreignProgress = MakeProgress(tableTimestamp);
    foreignProgress->Owner->SinkId = TSinkId("other");
    auto aborted = NewPromise<void>();
    auto transaction = New<StrictMock<TMockTransaction>>();
    QueueTransaction(transaction);
    {
        InSequence sequence;
        ExpectProgressLock(transaction);
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce([foreignProgress] {
                return MakeFuture(NYson::ConvertToYsonString(foreignProgress));
            });
        // Neither #CreateNode() nor #SetNode() may follow: a StrictMock fails the test if they do.
        EXPECT_CALL(*transaction, Abort(_))
            .WillOnce([aborted] () mutable {
                aborted.Set();
                return OKFuture;
            });
    }

    auto cancelableContext = New<TCancelableContext>();
    auto commitFuture = BIND([sink] {
        sink->Commit();
    })
        .AsyncVia(cancelableContext->CreateInvoker(ThreadPool->GetInvoker()))
        .Run();

    WaitFor(aborted.ToFuture()).ThrowOnError();
    cancelableContext->Cancel(TError(NYT::EErrorCode::Canceled, "Job stopped"));
    EXPECT_FALSE(WaitFor(commitFuture).IsOK());
    EXPECT_FALSE(callback->load());
}

TEST_F(TArrivalOrderTableSinkTest, BufferedRequestsWeightIsCountedOnceAfterInitialization)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    TSinkOptions options{
        .MaxRowCount = 10,
        .MaxDataWeight = 12,
    };
    auto sink = MakeSink(options, /*sourceKey*/ 0);
    sink->Init(nullptr);

    std::vector<std::shared_ptr<std::atomic<bool>>> callbacks;
    for (i64 id = 1; id <= 4; ++id) {
        callbacks.push_back(Distribute(sink, id, /*dataWeight*/ 3));
    }

    // Weights accumulated while buffering must be dropped before the re-feed: double counting
    // would cross the limit after the first re-added request and seal a one-row batch.
    auto progress = MakeProgress(tableTimestamp);
    ExpectInitialization(progress);
    auto batch = ExpectCommittedBatch(progress, 4);
    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();

    EXPECT_EQ(4, batch->Writer->RowCount);
    for (const auto& callback : callbacks) {
        EXPECT_TRUE(callback->load());
    }
}

TEST_F(TArrivalOrderTableSinkTest, CatchUpTableExpirationIsClampedToNow)
{
    // The slot is far older than the TTL: an unclamped expiration would already be in the past
    // and the master would remove the freshly committed table at once.
    const auto tableTimestamp = TInstant::Now() - TDuration::Days(365);
    const auto before = TInstant::Now();
    auto progress = MakeProgress(tableTimestamp);
    auto sink = InitializeSink(TSinkOptions{}, progress);
    auto callback = Distribute(sink, 1);
    auto batch = ExpectCommittedBatch(progress, 1);

    sink->Sync(nullptr);
    sink->Commit();
    sink->Sync(nullptr);
    sink->Commit();
    EXPECT_TRUE(callback->load());
    EXPECT_EQ(tableTimestamp, batch->TableTimestamp);
    EXPECT_GE(batch->ExpirationTime, before + TableTtl);
}

TEST_F(TArrivalOrderTableSinkTest, OwnFrontierBelowWatermarkIsCollected)
{
    // Any entry below the watermark is durably drained, this partition's own included.
    const auto tableTimestamp = TInstant::Zero();
    SystemWatermark = TSystemTimestamp(MessageSystemTimestamp.Underlying() + 1);
    auto progress = MakeProgress(tableTimestamp, /*persistedThroughId*/ 7);
    auto sink = InitializeSink(TSinkOptions{}, progress);
    auto batch = ExpectCommittedBatch(progress, 0);

    sink->Sync(nullptr);
    sink->Commit();

    EXPECT_TRUE(batch->PublishedProgress->Partitions.empty());
}

TEST_F(TArrivalOrderTableSinkTest, MissingDataWeightColumnFailsConstruction)
{
    EXPECT_THROW_WITH_SUBSTRING(
        MakeSink(TSinkOptions{.DataWeightColumnName = "no_such_column"}, /*sourceKey*/ 0),
        "is missing from input stream schema");
}

TEST(TArrivalOrderTableSinkSpecTest, PostprocessorRejectsBrokenConfigurations)
{
    const auto build = [] (TStringBuf patch) {
        return ConvertTo<TArrivalOrderTableSinkParametersPtr>(NYson::TYsonString(Format(R"({output_directory="//tmp/output"; table_ttl="1h"; %v})",
            patch)));
    };

    EXPECT_NO_THROW(build(""));
    EXPECT_THROW_WITH_SUBSTRING(build(R"(table_period="0")"), "must be positive");
    EXPECT_THROW_WITH_SUBSTRING(build(R"(table_period="500ms")"), "whole number of seconds");
    EXPECT_THROW_WITH_SUBSTRING(
        build(R"(table_ttl="365d"; table_period="1s")"),
        "child count limit");
    EXPECT_THROW_WITH_SUBSTRING(
        build(R"(table_name_format="%Y/%m/%d")"),
        "path separators");
    EXPECT_THROW_WITH_SUBSTRING(build(R"(table_name_format="%M")"), "losslessly");
    EXPECT_THROW_WITH_SUBSTRING(build(R"(output_directory="")"), "must not be empty");
}

TEST_F(TArrivalOrderTableSinkTest, SeedingContinuesGridPastExistingTables)
{
    const auto existingTimestamp = TInstant::Now() + TDuration::Days(3);
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);
    ExpectInitialization(
        /*storedProgress*/ nullptr,
        /*startHook*/ {},
        /*existingTableTimestamps*/ {existingTimestamp - TDuration::Hours(1), existingTimestamp});
    sink->Commit();

    ASSERT_TRUE(CreatedInitialProgress);
    EXPECT_EQ(
        existingTimestamp + TSinkOptions{}.TablePeriod,
        CreatedInitialProgress->NextTableTimestamp);
}

TEST_F(TArrivalOrderTableSinkTest, SeedingRefusesDirectoryWithForeignChildren)
{
    auto sink = MakeSink(TSinkOptions{.RetryBackoff = TDuration::Hours(1)}, /*sourceKey*/ 0);
    sink->Init(nullptr);

    auto aborted = NewPromise<void>();
    auto transaction = New<StrictMock<TMockTransaction>>();
    QueueTransaction(transaction);
    {
        InSequence sequence;
        EXPECT_CALL(*transaction, CreateNode(OutputDirectory, NObjectClient::EObjectType::MapNode, _))
            .WillOnce(Return(MakeFuture<NCypressClient::TNodeId>(NCypressClient::TNodeId(TGuid::Create()))));
        ExpectProgressLock(transaction);
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce(Return(MakeFuture<NYson::TYsonString>(
                TError(NYTree::EErrorCode::ResolveError, "No such attribute"))));
        // A child without the table_timestamp attribute aborts the adoption; the StrictMock
        // fails the test if the progress is seeded regardless.
        EXPECT_CALL(*transaction, ListNode(OutputDirectory, _))
            .WillOnce(Return(MakeFuture(NYson::TYsonString(TString("[\"stray\"]")))));
        EXPECT_CALL(*transaction, Abort(_))
            .WillOnce([aborted] () mutable {
                aborted.Set();
                return OKFuture;
            });
    }

    auto cancelableContext = New<TCancelableContext>();
    auto commitFuture = BIND([sink] {
        sink->Commit();
    })
        .AsyncVia(cancelableContext->CreateInvoker(ThreadPool->GetInvoker()))
        .Run();

    WaitFor(aborted.ToFuture()).ThrowOnError();
    // The refusal is reported through the retry error state; that hint is the operator's only signal.
    while (true) {
        const auto errors = StatusProfiler->GetStatus().Errors;
        const auto it = errors.find("/retry");
        if (it != errors.end() &&
            ToString(it->second).find("remove foreign children") != std::string::npos)
        {
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }
    cancelableContext->Cancel(TError(NYT::EErrorCode::Canceled, "Job stopped"));
    EXPECT_FALSE(WaitFor(commitFuture).IsOK());
}

TEST_F(TArrivalOrderTableSinkTest, TransientProgressReadErrorDoesNotSeed)
{
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);

    {
        auto transaction = New<StrictMock<TMockTransaction>>();
        QueueTransaction(transaction);
        InSequence sequence;
        EXPECT_CALL(*transaction, CreateNode(OutputDirectory, NObjectClient::EObjectType::MapNode, _))
            .WillOnce(Return(MakeFuture<NCypressClient::TNodeId>(NCypressClient::TNodeId(TGuid::Create()))));
        ExpectProgressLock(transaction);
        // A transient failure must not be mistaken for a missing attribute: seeding on it would
        // overwrite the live progress. The StrictMock fails the test on any #SetNode().
        EXPECT_CALL(*transaction, GetNode(OutputDirectory + "/@progress", _))
            .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Transient master failure"))));
        EXPECT_CALL(*transaction, Abort(_))
            .WillOnce(Return(OKFuture));
    }
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    ExpectInitialization(MakeProgress(tableTimestamp));
    sink->Commit();

    EXPECT_FALSE(CreatedInitialProgress);
}

TEST_F(TArrivalOrderTableSinkTest, ReplayedMessagesAreAcknowledgedOnInitializationWithoutBurningSlots)
{
    const auto tableTimestamp = TInstant::Now() + TDuration::Hours(1);
    auto sink = MakeSink(TSinkOptions{}, /*sourceKey*/ 0);
    sink->Init(nullptr);

    std::vector<std::shared_ptr<std::atomic<bool>>> callbacks;
    for (i64 id = 1; id <= 3; ++id) {
        callbacks.push_back(Distribute(sink, id));
    }

    // The stored frontier covers the whole replay: the buffered messages must be acknowledged
    // during initialization, without sealing batches or starting a commit.
    ExpectInitialization(MakeProgress(tableTimestamp, /*persistedThroughId*/ 10));
    sink->Commit();
    for (const auto& callback : callbacks) {
        EXPECT_TRUE(callback->load());
    }
    sink->Sync(nullptr);
    sink->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NStaticTableConnector
