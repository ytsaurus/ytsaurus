#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/hedging/unittests/mock/cache.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <atomic>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

const TStreamId InputStreamId("input");
const TStreamId TimerStreamId("timer");
const TStreamId OutputStreamId("output");
const TMessageId InputMessageId("input-1");
const TKey TimerKey = MakeKey(static_cast<ui64>(7));

TTableSchemaPtr GetKeySchema()
{
    static const auto Schema = New<TTableSchema>(std::vector{
        TColumnSchema("key", EValueType::Uint64).SetRequired(true).SetSortOrder(ESortOrder::Ascending),
    });
    return Schema;
}

//! What the computation under test saw; touched from the serialized invoker only.
struct TPersistenceProbe
{
    //! Whether the input message adds a timer.
    bool AddTimer = true;

    bool InputSent = false;
    int Epochs = 0;
    int Firings = 0;
    //! Committed transactions so far.
    int Commits = 0;
    std::optional<int> CommitsWhenInputProcessed;
    std::optional<int> CommitsWhenInputPersisted;
};

std::atomic<TPersistenceProbe*>& GetActiveProbe()
{
    static std::atomic<TPersistenceProbe*> Probe = nullptr;
    return Probe;
}

TPersistenceProbe& GetProbe()
{
    auto* probe = GetActiveProbe().load();
    YT_VERIFY(probe);
    return *probe;
}

class TTimerRegisteringSwiftMap
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::DoProcessMessage;
    using TSwiftMapComputation::DoProcessTimer;
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        auto& probe = GetProbe();
        THROW_ERROR_EXCEPTION_IF(++probe.Epochs > 1000, "Too many epochs");
        if (probe.CommitsWhenInputPersisted && TimerStore_->GetCount() == 0) {
            THROW_ERROR_EXCEPTION("Test finished");
        }
        TSwiftMapComputation::DoProcess(std::move(input), std::move(output));
    }

    void DoProcessMessage(const TInputMessageConstPtr& /*message*/, IOutputCollectorPtr output) override
    {
        auto& probe = GetProbe();
        probe.CommitsWhenInputProcessed = probe.Commits;
        if (probe.AddTimer) {
            output->AddTimer(TimerStreamId, TSystemTimestamp(1));
        }
    }

    void DoProcessTimer(const TInputTimerConstPtr& /*timer*/, IOutputCollectorPtr /*output*/) override
    {
        ++GetProbe().Firings;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTimerRegisteringSwiftMap);

//! Feeds a single input message and records when it is marked persisted.
class TSwiftMapRunContext
    : public IComputationRunContext
{
public:
    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& /*allowedStreams*/) override
    {
        auto& probe = GetProbe();
        std::vector<TInputMessageConstPtr> messages;
        if (!std::exchange(probe.InputSent, true)) {
            TMessageBuilder builder(InputStreamId, New<TTableSchema>());
            builder.SetMessageId(InputMessageId);
            builder.SetSystemTimestamp(TSystemTimestamp(1));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1));
            builder.SetEventTimestamp(TSystemTimestamp(1));
            messages.push_back(New<TInputMessage>(builder.Finish(), TimerKey));
        }
        return MakeFuture(std::move(messages));
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        return MakeFuture(THashMap<TStreamId, TInflightMetricsPtr>{{InputStreamId, New<TInflightMetrics>()}});
    }

    void MarkPersisted(std::span<const TMessageId> messageIds) override
    {
        auto& probe = GetProbe();
        for (const auto& messageId : messageIds) {
            if (messageId == InputMessageId && !probe.CommitsWhenInputPersisted) {
                probe.CommitsWhenInputPersisted = probe.Commits;
            }
        }
    }

    void MarkDeduplicated(std::span<const TMessageId> /*messageIds*/) override
    { }

    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> /*messages*/,
        std::span<TDistributingTracker> /*trackers*/) override
    { }

    void Commit() override
    { }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }
};

class TSwiftMapHarness
{
public:
    const TComputationSpecPtr Spec = New<TComputationSpec>();
    const TDynamicComputationContextPtr Dynamic = New<TDynamicComputationContext>();
    TPersistenceProbe Probe;

    TSwiftMapHarness()
    {
        Spec->ComputationClassName = TypeName<TTimerRegisteringSwiftMap>();
        Spec->GroupBySchema = GetKeySchema();
        Spec->InputStreamIds.insert(InputStreamId);
        Spec->OutputStreamIds.insert(OutputStreamId);
        auto timerSpec = New<TTimerSpec>();
        timerSpec->TimeType = ETimeType::CurrentTime;
        timerSpec->DeduplicateEqualTimestamps = false;
        Spec->TimerStreams[TimerStreamId] = timerSpec;
        Spec->StreamsDependency[TimerStreamId] = {InputStreamId};
        // An output of a Swift computation must not depend on its timers.
        Spec->StreamsDependency[OutputStreamId] = {InputStreamId};
        Spec->Parameters = ConvertTo<IMapNodePtr>(TYsonStringBuf("{}"));
        Dynamic->DynamicComputationSpec = New<TDynamicComputationSpec>();
        Dynamic->DynamicComputationSpec->EmptyBatchBackoff = TDuration::MilliSeconds(10);
        Dynamic->DynamicPartitionSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf("{computation_partition_spec={};}"));
    }

    //! Runs the computation until it throws.
    TError Run()
    {
        auto queue = New<TActionQueue>("SwiftMapTest");
        auto timestampProvider = New<NiceMock<TMockTimestampProvider>>();
        auto nextTimestamp = std::make_shared<ui64>(1ULL << 32);
        ON_CALL(*timestampProvider, GenerateTimestamps(_, _))
            .WillByDefault([nextTimestamp] (auto, auto) {
                return MakeFuture(TTimestamp((*nextTimestamp)++));
            });
        auto client = New<NiceMock<TMockClient>>();
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
        auto transaction = New<NiceMock<TMockTransaction>>();
        transaction->StartTimestamp = TTimestamp(1);
        ON_CALL(*transaction, Commit(_))
            .WillByDefault([this] (const auto&) {
                ++Probe.Commits;
                return MakeFuture(TTransactionCommitResult{});
            });
        ON_CALL(*client, StartTransaction(_, _))
            .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));
        auto clientsCache = New<NiceMock<NClient::NHedging::TMockClientsCache>>();
        ON_CALL(*clientsCache, GetClient(_)).WillByDefault(Return(client));

        auto context = New<TComputationContext>();
        context->ComputationSpec = Spec;
        context->ClientsCache = clientsCache;
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Partition = New<TPartition>();
        context->Partition->ComputationId = TComputationId("swift-map");
        context->Partition->State = EPartitionState::Executing;
        context->Partition->PartitionId = TPartitionId(TGuid::Create());
        context->Job = New<TJob>();
        context->Job->JobId = TJobId(TGuid::Create());
        context->Job->Generation = TUniqueSeqNo(42);
        context->Job->PartitionId = context->Partition->PartitionId;
        context->Job->LeaseId = TLeaseId(TGuid::Create());
        context->SerializedInvoker = queue->GetInvoker();
        context->PoolInvoker = queue->GetInvoker();
        context->Logger = NLogging::TLogger("SwiftMapTest");
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>("SwiftMapTest", context->Logger, context->Profiler);
        context->PartitionBufferState = CreateDetachedPartitionBufferState({{OutputStreamId, New<TStreamLimitUsageState>()}});
        auto inputStreamSpec = New<TStreamSpec>();
        inputStreamSpec->Schema = New<TTableSchema>();
        auto outputStreamSpec = New<TStreamSpec>();
        outputStreamSpec->Schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Int64)});
        context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>{
                {InputStreamId, {{TStreamSpecId(1), inputStreamSpec}}},
                {OutputStreamId, {{TStreamSpecId(2), outputStreamSpec}}},
            }),
            GetKeySchema(),
            /*converterCache*/ nullptr);
        context->DistributedThrottlerControllerChannelProvider = [] {
            return NRpc::IChannelPtr{};
        };

        YT_VERIFY(!GetActiveProbe().exchange(&Probe));
        auto resetProbe = Finally([] {
            GetActiveProbe().store(nullptr);
        });
        return WaitFor(BIND([this, context] {
            auto computation = New<TTimerRegisteringSwiftMap>(context, Dynamic);
            auto watermarkState = New<TWatermarkState>();
            watermarkState->CurrentTimestamp = TSystemTimestamp(1000);
            computation->UpdateWatermarkState(watermarkState);
            auto inputTraverse = New<TStreamTraverseData>();
            inputTraverse->State = EStreamState::Active;
            inputTraverse->SystemWatermark = ZeroSystemTimestamp;
            inputTraverse->InflightMetrics = New<TInflightMetrics>();
            computation->SetInputTraverse({{InputStreamId, std::move(inputTraverse)}});
            static_cast<IComputation*>(computation.Get())->Run(New<TSwiftMapRunContext>());
        })
                .AsyncVia(queue->GetInvoker())
                .Run());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TSwiftMapComputationTest, InputIsPersistedAfterTheCommitThatRegistersItsTimer)
{
    TSwiftMapHarness harness;
    EXPECT_THAT(ToString(harness.Run()), HasSubstr("Test finished"));

    const auto& probe = harness.Probe;
    // An upstream that took the input as persisted before the commit would lose the timer on a restart.
    ASSERT_TRUE(probe.CommitsWhenInputProcessed);
    ASSERT_TRUE(probe.CommitsWhenInputPersisted);
    EXPECT_GT(*probe.CommitsWhenInputPersisted, *probe.CommitsWhenInputProcessed);
    EXPECT_EQ(probe.Firings, 1);
}

TEST(TSwiftMapComputationTest, InputWithoutTimersIsPersistedInItsEpoch)
{
    TSwiftMapHarness harness;
    harness.Probe.AddTimer = false;
    EXPECT_THAT(ToString(harness.Run()), HasSubstr("Test finished"));

    const auto& probe = harness.Probe;
    ASSERT_TRUE(probe.CommitsWhenInputProcessed);
    ASSERT_TRUE(probe.CommitsWhenInputPersisted);
    EXPECT_EQ(*probe.CommitsWhenInputPersisted, *probe.CommitsWhenInputProcessed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
