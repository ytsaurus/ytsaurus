#include "computation_test_harness.h"

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/cache/public.h>

#include <yt/yt/client/hedging/unittests/mock/cache.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NComputationTesting {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Feeds the computation from #TComputationRunOptions::GetNextBatch and forwards its acknowledgements
//! and outputs to the callbacks of the options.
class TTestComputationRunContext
    : public IComputationRunContext
{
public:
    explicit TTestComputationRunContext(TComputationRunOptions options)
        : Options_(std::move(options))
    { }

    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& /*allowedStreams*/) override
    {
        return MakeFuture(Options_.GetNextBatch ? Options_.GetNextBatch() : std::vector<TInputMessageConstPtr>{});
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        THashMap<TStreamId, TInflightMetricsPtr> metrics;
        for (const auto& streamId : Options_.Spec->InputStreamIds) {
            metrics[streamId] = New<TInflightMetrics>();
        }
        return MakeFuture(std::move(metrics));
    }

    void MarkPersisted(std::span<const TMessageId> messageIds) override
    {
        if (Options_.OnMarkPersisted) {
            Options_.OnMarkPersisted(messageIds);
        }
    }

    void MarkDeduplicated(std::span<const TMessageId> /*messageIds*/) override
    { }

    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> messages,
        std::span<TDistributingTracker> /*trackers*/) override
    {
        if (Options_.OnRegisterOutputMessages) {
            Options_.OnRegisterOutputMessages(messages);
        }
    }

    void Commit() override
    { }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }

private:
    const TComputationRunOptions Options_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr GetTestKeySchema()
{
    static const auto Schema = New<NTableClient::TTableSchema>(std::vector{
        NTableClient::TColumnSchema("key", NTableClient::EValueType::Uint64)
            .SetRequired(true)
            .SetSortOrder(NTableClient::ESortOrder::Ascending),
    });
    return Schema;
}

TInputMessageConstPtr MakeTestInputMessage(const TStreamId& streamId, const TMessageId& messageId, TKey key)
{
    TMessageBuilder builder(streamId, New<NTableClient::TTableSchema>());
    builder.SetMessageId(messageId);
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    return New<TInputMessage>(builder.Finish(), std::move(key));
}

void Serialize(const TUnserializable& /*value*/, NYson::IYsonConsumer* /*consumer*/)
{
    THROW_ERROR_EXCEPTION("Value is not serializable");
}

void Deserialize(TUnserializable& /*value*/, NYTree::INodePtr /*node*/)
{ }

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Returns clients whose transactions call |onCommit| on commit, whose tables are empty and whose
//! timestamp provider yields increasing timestamps starting at 2^32, i.e. a few seconds as system timestamps.
NClient::NCache::IClientsCachePtr CreateMockClientsCache(std::function<void()> onCommit)
{
    using namespace NApi;
    using namespace NTableClient;
    using namespace NTransactionClient;
    using ::testing::_;
    using ::testing::Return;

    auto timestampProvider = New<::testing::NiceMock<TMockTimestampProvider>>();
    auto nextTimestamp = std::make_shared<ui64>(1ULL << 32);
    ON_CALL(*timestampProvider, GenerateTimestamps(_, _))
        .WillByDefault([nextTimestamp] (auto, auto) {
            return MakeFuture(TTimestamp((*nextTimestamp)++));
        });
    auto client = New<::testing::NiceMock<TMockClient>>();
    client->SetTimestampProvider(timestampProvider);
    auto timerSchema = New<TTableSchema>(std::vector{
        TColumnSchema("computation_id", EValueType::String),
        TColumnSchema("key", EValueType::Any),
        TColumnSchema("message_id", EValueType::String),
        TColumnSchema("stream_id", EValueType::String),
        TColumnSchema("system_timestamp", EValueType::Int64),
        TColumnSchema("event_timestamp", EValueType::Int64),
        TColumnSchema("trigger_timestamp", EValueType::Int64),
    });
    ON_CALL(*client, SelectRows(_, _))
        .WillByDefault(Return(MakeFuture(TSelectRowsResult{
            .Rowset = CreateRowset(timerSchema, MakeSharedRange(std::vector<TUnversionedRow>{})),
        })));
    ON_CALL(*client, LookupRows(_, _, _, _))
        .WillByDefault([] (const auto&, const auto&, const auto& keys, const auto&) {
            return MakeFuture(TUnversionedLookupRowsResult{
                .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>(keys.Size()))),
            });
        });
    auto transaction = New<::testing::NiceMock<TMockTransaction>>();
    transaction->StartTimestamp = TTimestamp(1);
    ON_CALL(*transaction, Commit(_))
        .WillByDefault([onCommit = std::move(onCommit)] (const auto&) {
            if (onCommit) {
                onCommit();
            }
            return MakeFuture(TTransactionCommitResult{});
        });
    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));
    auto clientsCache = New<::testing::NiceMock<NClient::NHedging::TMockClientsCache>>();
    ON_CALL(*clientsCache, GetClient(_)).WillByDefault(Return(client));
    return clientsCache;
}

//! Builds the context of a computation of |spec| that runs on |invoker| with |clientsCache|. The input
//! streams of |spec| have an empty schema and its output streams have |outputSchema| unless
//! |outputSchemaOverrides| has another one.
TComputationContextPtr CreateTestComputationContext(
    TComputationSpecPtr spec,
    const NTableClient::TTableSchemaPtr& outputSchema,
    const THashMap<TStreamId, NTableClient::TTableSchemaPtr>& outputSchemaOverrides,
    const IInvokerPtr& invoker,
    NClient::NCache::IClientsCachePtr clientsCache)
{
    auto context = New<TComputationContext>();
    context->ComputationSpec = spec;
    context->ClientsCache = std::move(clientsCache);
    context->PipelinePath = NYPath::TRichYPath("//pipeline");
    context->PipelinePath.SetCluster("test");
    context->Partition = New<TPartition>();
    context->Partition->ComputationId = TComputationId("computation");
    context->Partition->State = EPartitionState::Executing;
    context->Partition->PartitionId = TPartitionId(TGuid::Create());
    context->Job = New<TJob>();
    context->Job->JobId = TJobId(TGuid::Create());
    context->Job->Generation = TUniqueSeqNo(42);
    context->Job->PartitionId = context->Partition->PartitionId;
    context->Job->LeaseId = TLeaseId(TGuid::Create());
    context->SerializedInvoker = invoker;
    context->PoolInvoker = invoker;
    context->Logger = NLogging::TLogger("ComputationTest");
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->LoadThroughputThrottler = New<TLoadThroughputThrottler>("ComputationTest", context->Logger, context->Profiler);

    TStreamLimitUsageStateMap bufferStates;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    int nextStreamSpecId = 1;
    for (const auto& streamId : spec->InputStreamIds) {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = New<NTableClient::TTableSchema>();
        streamSpecs[streamId][TStreamSpecId(nextStreamSpecId++)] = std::move(streamSpec);
    }
    for (const auto& streamId : spec->OutputStreamIds) {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = GetOrDefault(outputSchemaOverrides, streamId, outputSchema);
        streamSpecs[streamId][TStreamSpecId(nextStreamSpecId++)] = std::move(streamSpec);
        bufferStates[streamId] = New<TStreamLimitUsageState>();
    }
    context->PartitionBufferState = CreateDetachedPartitionBufferState(std::move(bufferStates));
    context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        spec->GroupBySchema,
        /*converterCache*/ nullptr);
    context->DistributedThrottlerControllerChannelProvider = [] {
        return NRpc::IChannelPtr{};
    };
    return context;
}

//! Returns a dynamic context that retries an empty batch after 10 ms.
TDynamicComputationContextPtr CreateTestDynamicComputationContext()
{
    auto dynamicContext = New<TDynamicComputationContext>();
    dynamicContext->DynamicComputationSpec = New<TDynamicComputationSpec>();
    dynamicContext->DynamicComputationSpec->EmptyBatchBackoff = TDuration::MilliSeconds(10);
    dynamicContext->DynamicPartitionSpec = NYTree::ConvertTo<TDynamicPartitionSpecPtr>(NYson::TYsonStringBuf("{computation_partition_spec={};}"));
    return dynamicContext;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TError RunComputation(const TComputationRunOptions& options, const TComputationFactory& factory)
{
    auto queue = New<NConcurrency::TActionQueue>("ComputationTest");
    auto context = CreateTestComputationContext(
        options.Spec,
        options.OutputSchema,
        options.OutputSchemaOverrides,
        queue->GetInvoker(),
        CreateMockClientsCache(options.OnCommit));
    auto dynamicContext = CreateTestDynamicComputationContext();
    auto runContext = New<TTestComputationRunContext>(options);
    return NConcurrency::WaitFor(BIND([&] {
        auto computation = factory(context, dynamicContext);
        auto watermarkState = New<TWatermarkState>();
        watermarkState->CurrentTimestamp = TSystemTimestamp(1000);
        computation->UpdateWatermarkState(watermarkState);
        THashMap<TStreamId, TStreamTraverseDataPtr> inputTraverses;
        for (const auto& streamId : options.Spec->InputStreamIds) {
            auto inputTraverse = New<TStreamTraverseData>();
            inputTraverse->State = EStreamState::Active;
            inputTraverse->SystemWatermark = ZeroSystemTimestamp;
            inputTraverse->InflightMetrics = New<TInflightMetrics>();
            inputTraverses[streamId] = std::move(inputTraverse);
        }
        computation->SetInputTraverse(std::move(inputTraverses));
        computation->Run(runContext);
    })
            .AsyncVia(queue->GetInvoker())
            .Run());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NComputationTesting
