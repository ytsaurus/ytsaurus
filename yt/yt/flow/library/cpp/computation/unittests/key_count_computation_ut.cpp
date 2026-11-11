#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

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
const TStreamId OutputStreamId("output");

struct TKeyCountProbe
{
    bool GrowingKeyEvidence = false;
    bool CompositeKeys = false;
    bool FailTimestampAfterInput = false;
    bool Blocked = false;
    bool TimerOnly = false;
    bool PreloadCalled = false;
    bool UserCallbackCalled = false;
    int InputBatchIndex = 0;
    int CommitCount = 0;
    std::vector<int> PreloadBatches;
    std::vector<int> UserBatches;
    THashMap<TKey, TInstant> BeforeGrowthReport;
    THashMap<TKey, TInstant> ObservedKeys;
};

std::atomic<TKeyCountProbe*>& GetActiveKeyCountProbe()
{
    static std::atomic<TKeyCountProbe*> Probe = nullptr;
    return Probe;
}

class TKeyCountRunContext
    : public IComputationRunContext
{
public:
    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& allowedStreams) override
    {
        EXPECT_TRUE(allowedStreams.contains(InputStreamId));
        auto* probe = GetActiveKeyCountProbe().load();
        const int batch = probe->InputBatchIndex++;
        const int count = probe->GrowingKeyEvidence ? 400 : 1200;
        std::vector<TInputMessageConstPtr> messages;
        for (int offset = 0; offset < count; ++offset) {
            const int key = probe->GrowingKeyEvidence ? batch * count + offset : offset;
            TMessageBuilder builder(InputStreamId, New<TTableSchema>());
            builder.SetMessageId(TMessageId(ToString(key)));
            builder.SetSystemTimestamp(TSystemTimestamp(1));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1));
            builder.SetEventTimestamp(TSystemTimestamp(1));
            messages.push_back(New<TInputMessage>(builder.Finish(),
                probe->CompositeKeys ? MakeKey("same-prefix", static_cast<ui64>(key)) : MakeKey(static_cast<ui64>(key))));
        }
        return MakeFuture(std::move(messages));
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        auto metrics = New<TInflightMetrics>();
        metrics->Count = 1200;
        return MakeFuture(THashMap<TStreamId, TInflightMetricsPtr>{{InputStreamId, metrics}});
    }

    void RegisterSourceMessages(i64 /*count*/) override
    { }

    void MarkPersisted(std::span<const TMessageId> /*messageIds*/) override
    { }

    void MarkDeduplicated(std::span<const TMessageId> /*messageIds*/) override
    { }

    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> /*messages*/,
        std::span<TDistributingTracker> /*trackers*/) override
    { }

    void Commit() override
    {
        ++GetActiveKeyCountProbe().load()->CommitCount;
    }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }
};

void RecordUserBatch(TUniversalComputationBase* computation)
{
    auto* probe = GetActiveKeyCountProbe().load();
    probe->UserCallbackCalled = true;
    probe->UserBatches.push_back(probe->InputBatchIndex);
    if (probe->GrowingKeyEvidence && probe->InputBatchIndex <= 2) {
        probe->BeforeGrowthReport = computation->GetStatus()->ObservedKeys.value_or(THashMap<TKey, TInstant>{});
        return;
    }
    THROW_ERROR_EXCEPTION("User callback must not run");
}

class TKeyCountExecutionTransform
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcess(IInputContextPtr, IOutputCollectorPtr) override
    {
        RecordUserBatch(this);
    }
};

class TKeyCountExecutionSwiftMap
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcess(IInputContextPtr, IOutputCollectorPtr) override
    {
        RecordUserBatch(this);
    }
};

class TKeyCountBatchComputation
    : public TUniversalComputationBase
{
public:
    using TUniversalComputationBase::TUniversalComputationBase;

private:
    void DoPrepare(const IComputationRunContextPtr&) override
    { }

    void DoExecute(
        const IComputationRunContextPtr& context,
        NTracing::TTraceContextGuard&&) override
    {
        auto* probe = GetActiveKeyCountProbe().load();
        auto iteration = StartRunIteration(context);
        if (probe->Blocked) {
            const auto limits = CheckOutputLimits(GetDynamicSpec(), GetDynamicPartitionSpec());
            EXPECT_TRUE(limits.BlockedByController);
            EXPECT_TRUE(limits.AllowedInputStreams.empty());
            THROW_ERROR_EXCEPTION("Output is blocked");
        }
        std::vector<TInputTimerConstPtr> timers;
        std::vector<TInputVisitConstPtr> visits;
        for (int key = 0; key < 1200; ++key) {
            if (probe->TimerOnly) {
                TTimer timer;
                timer.MessageId = TMessageId(ToString(key));
                timer.StreamId = InputStreamId;
                timer.SystemTimestamp = TSystemTimestamp(1);
                timer.AlignmentTimestamp = TSystemTimestamp(1);
                timer.EventTimestamp = TSystemTimestamp(1);
                timer.TriggerTimestamp = TSystemTimestamp(2);
                timer.Key = MakeKey(static_cast<ui64>(key));
                timer.KeySchema = New<TTableSchema>(std::vector{
                    TColumnSchema("key", EValueType::Uint64).SetRequired(true).SetSortOrder(ESortOrder::Ascending),
                });
                timers.push_back(New<TInputTimer>(std::move(timer)));
            } else {
                TVisit visit;
                visit.MessageId = TMessageId(ToString(key));
                visit.StreamId = InputStreamId;
                visit.SystemTimestamp = TSystemTimestamp(1);
                visit.AlignmentTimestamp = TSystemTimestamp(1);
                visit.EventTimestamp = TSystemTimestamp(1);
                visit.Key = MakeKey(static_cast<ui64>(key));
                visits.push_back(New<TInputVisit>(std::move(visit)));
            }
        }
        ObserveKeyCountInput({}, timers, visits);
        probe->UserCallbackCalled = true;
        THROW_ERROR_EXCEPTION("User callback must not run");
    }

    void ProcessDistributedMessages(
        const IComputationRunContextPtr&,
        std::deque<TOutputMessageConstPtr>&&) override
    { }
};

YT_FLOW_DEFINE_COMPUTATION(TKeyCountExecutionTransform);
YT_FLOW_DEFINE_COMPUTATION(TKeyCountExecutionSwiftMap);
YT_FLOW_DEFINE_COMPUTATION(TKeyCountBatchComputation);

class TKeyCountPreloadTrap
    : public IExternalStateManager
{
public:
    TKeyCountPreloadTrap(TExternalStateManagerContextPtr, TDynamicExternalStateManagerContextPtr)
    { }

    IStateHolderPtr GetState(const TKey&) override
    {
        return New<TStateHolder<i64>>();
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>&) override
    {
        auto* probe = GetActiveKeyCountProbe().load();
        probe->PreloadCalled = true;
        probe->PreloadBatches.push_back(probe->InputBatchIndex);
        return probe->GrowingKeyEvidence && probe->InputBatchIndex <= 2
            ? OKFuture
            : MakeFuture(TError("State preload must not run"));
    }

    TTableSchemaPtr GetKeySchema() const override
    {
        return nullptr;
    }

    void Sync(IRetryableTransactionPtr) override
    { }

    void ValidateStateClass(const std::type_info&) const override
    { }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TKeyCountPreloadTrap);

class TKeyCountComputationHarness
{
public:
    const TComputationSpecPtr Spec = New<TComputationSpec>();
    const TDynamicComputationContextPtr Dynamic = New<TDynamicComputationContext>();
    TKeyCountProbe Probe;
    TPartitioningSpecPtr Partitioning = New<TPartitioningSpec>();

    explicit TKeyCountComputationHarness(bool swift = false)
    {
        Spec->ComputationClassName = swift ? TypeName<TKeyCountExecutionSwiftMap>() : TypeName<TKeyCountExecutionTransform>();
        Spec->InputStreamIds.insert(InputStreamId);
        Partitioning->KeyCountPartitioning->Enable = true;
        Dynamic->DynamicComputationSpec = New<TDynamicComputationSpec>();
        Dynamic->DynamicPartitionSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf("{computation_partition_spec={};}"));
    }

    void AddPreloadTrap()
    {
        auto manager = New<TExternalStateManagerSpec>();
        manager->ExternalStateManagerClassName = TypeName<TKeyCountPreloadTrap>();
        manager->AutoPreload = true;
        Spec->ExternalStateManagers["preload-trap"] = std::move(manager);
    }

    TError Run()
    {
        auto queue = New<TActionQueue>("KeyCountComputationTest");
        auto timestampProvider = New<NiceMock<TMockTimestampProvider>>();
        auto nextTimestamp = std::make_shared<ui64>(1ULL << 32);
        ON_CALL(*timestampProvider, GenerateTimestamps(_, _))
            .WillByDefault([this, nextTimestamp] (auto, auto) {
                if (Probe.FailTimestampAfterInput && Probe.InputBatchIndex > 0) {
                    return MakeFuture<TTimestamp>(TError("Timestamp failed after input"));
                }
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
            .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));
        ON_CALL(*client, StartTransaction(_, _))
            .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));
        auto clientsCache = New<NiceMock<NClient::NHedging::TMockClientsCache>>();
        ON_CALL(*clientsCache, GetClient(_)).WillByDefault(Return(client));
        auto context = New<TComputationContext>();
        Spec->Parameters = ConvertTo<IMapNodePtr>(Partitioning);
        context->ComputationSpec = Spec;
        context->ClientsCache = clientsCache;
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Partition = New<TPartition>();
        context->Partition->ComputationId = TComputationId("key-count");
        context->Partition->State = EPartitionState::Executing;
        context->Partition->PartitionId = TPartitionId(TGuid::Create());
        context->Job = New<TJob>();
        context->Job->JobId = TJobId(TGuid::Create());
        context->Job->Generation = TUniqueSeqNo(42);
        context->Job->PartitionId = context->Partition->PartitionId;
        context->Job->LeaseId = TLeaseId(TGuid::Create());
        context->SerializedInvoker = queue->GetInvoker();
        context->PoolInvoker = queue->GetInvoker();
        context->Logger = NLogging::TLogger("KeyCountComputationTest");
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>("KeyCountComputationTest", context->Logger, context->Profiler);
        context->PartitionBufferState = CreateDetachedPartitionBufferState({{OutputStreamId, New<TStreamLimitUsageState>()}});
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = New<TTableSchema>();
        auto outputStreamSpec = New<TStreamSpec>();
        outputStreamSpec->Schema = New<TTableSchema>();
        context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>{
                {InputStreamId, {{TStreamSpecId(1), streamSpec}}},
                {OutputStreamId, {{TStreamSpecId(2), outputStreamSpec}}},
            }),
            New<TTableSchema>(),
            /*converterCache*/ nullptr);
        context->DistributedThrottlerControllerChannelProvider = [] {
            return NRpc::IChannelPtr{};
        };
        YT_VERIFY(!GetActiveKeyCountProbe().exchange(&Probe));
        auto resetProbe = Finally([] {
            GetActiveKeyCountProbe().store(nullptr);
        });
        return WaitFor(BIND([this, context] {
            TIntrusivePtr<TUniversalComputationBase> computation;
            if (Spec->ComputationClassName == TypeName<TKeyCountBatchComputation>()) {
                computation = New<TKeyCountBatchComputation>(context, Dynamic);
            } else if (Spec->ComputationClassName == TypeName<TKeyCountExecutionSwiftMap>()) {
                computation = New<TKeyCountExecutionSwiftMap>(context, Dynamic);
            } else {
                computation = New<TKeyCountExecutionTransform>(context, Dynamic);
            }
            computation->UpdateWatermarkState(New<TWatermarkState>());
            auto input = New<TStreamTraverseData>();
            input->State = EStreamState::Active;
            input->InflightMetrics = New<TInflightMetrics>();
            computation->SetInputTraverse({{InputStreamId, input}});
            auto captureKeys = Finally([&] {
                Probe.ObservedKeys = computation->GetStatus()->ObservedKeys.value_or(THashMap<TKey, TInstant>{});
            });
            static_cast<IComputation*>(computation.Get())->Run(New<TKeyCountRunContext>());
        })
                .AsyncVia(queue->GetInvoker())
                .Run());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyCountComputationTest, TransformAndSwiftObserveWholeInputBeforeFilteringPreloadOrUserCode)
{
    for (bool swift : {false, true}) {
        TKeyCountComputationHarness harness(swift);
        harness.Probe.CompositeKeys = true;
        harness.AddPreloadTrap();
        harness.Dynamic->DynamicComputationSpec->SkipIfExpression = "true";
        auto error = harness.Run();
        EXPECT_TRUE(error.FindMatching(EErrorCode::PartitionKeyLimitExceeded)) << ToString(error);
        EXPECT_FALSE(harness.Probe.PreloadCalled);
        EXPECT_FALSE(harness.Probe.UserCallbackCalled);
        EXPECT_EQ(harness.Probe.ObservedKeys.size(), 1200u);
        EXPECT_TRUE(harness.Probe.ObservedKeys.contains(MakeKey("same-prefix", 1199u)));
    }
}

TEST(TKeyCountComputationTest, TimestampFailureAfterFetchStillReportsEveryFetchedKey)
{
    TKeyCountComputationHarness harness;
    harness.Probe.FailTimestampAfterInput = true;
    harness.Partitioning->KeyCountPartitioning->MaxKeysPerPartition = 1200;
    const auto error = harness.Run();
    EXPECT_THAT(ToString(error), HasSubstr("Timestamp failed after input"));
    EXPECT_FALSE(harness.Probe.PreloadCalled);
    EXPECT_FALSE(harness.Probe.UserCallbackCalled);
    EXPECT_EQ(harness.Probe.ObservedKeys.size(), 1200u);
}

TEST(TKeyCountComputationTest, LifetimeGrowthFailsOnlyTheViolatingBatchBeforePreloadOrCommit)
{
    for (bool swift : {false, true}) {
        TKeyCountComputationHarness harness(swift);
        harness.Probe.GrowingKeyEvidence = true;
        harness.AddPreloadTrap();
        harness.Partitioning->KeyCountPartitioning->MaxKeysPerPartition = 800;
        const auto error = harness.Run();
        EXPECT_TRUE(error.FindMatching(EErrorCode::PartitionKeyLimitExceeded)) << ToString(error);
        EXPECT_EQ(harness.Probe.InputBatchIndex, 3);
        EXPECT_EQ(harness.Probe.PreloadBatches, (std::vector<int>{1, 2}));
        EXPECT_EQ(harness.Probe.UserBatches, (std::vector<int>{1, 2}));
        EXPECT_EQ(harness.Probe.CommitCount, 2);
        EXPECT_EQ(harness.Probe.BeforeGrowthReport.size(), 800u);
        EXPECT_EQ(harness.Probe.ObservedKeys.size(), 1200u);
    }
}

TEST(TKeyCountComputationTest, OutputBlockedJobDoesNotObserveQueuedInput)
{
    TKeyCountComputationHarness harness;
    harness.Spec->ComputationClassName = TypeName<TKeyCountBatchComputation>();
    harness.Spec->OutputStreamIds.insert(OutputStreamId);
    harness.Spec->StreamsDependency[OutputStreamId] = {InputStreamId};
    harness.Probe.Blocked = true;
    harness.Dynamic->DynamicPartitionSpec->ComputationPartitionSpec->AddChild(
        "blocked_output_streams",
        ConvertToNode(std::vector{OutputStreamId}));
    const auto error = harness.Run();
    EXPECT_THAT(ToString(error), HasSubstr("Output is blocked"));
    EXPECT_TRUE(harness.Probe.ObservedKeys.empty());
    EXPECT_EQ(harness.Probe.InputBatchIndex, 0);
    EXPECT_FALSE(harness.Probe.UserCallbackCalled);
}

TEST(TKeyCountComputationTest, FullTimerOrVisitBatchFailsBeforeUserWork)
{
    for (bool timerOnly : {false, true}) {
        TKeyCountComputationHarness harness;
        harness.Spec->ComputationClassName = TypeName<TKeyCountBatchComputation>();
        harness.Probe.TimerOnly = timerOnly;
        const auto error = harness.Run();
        EXPECT_TRUE(error.FindMatching(EErrorCode::PartitionKeyLimitExceeded)) << ToString(error);
        EXPECT_FALSE(harness.Probe.UserCallbackCalled);
        EXPECT_EQ(harness.Probe.ObservedKeys.size(), 1200u);
    }
}

TEST(TKeyCountComputationTest, DisabledStaticLimitDoesNotObserveOrRejectInput)
{
    for (bool swift : {false, true}) {
        TKeyCountComputationHarness harness(swift);
        harness.Partitioning->KeyCountPartitioning->Enable = false;
        const auto error = harness.Run();
        EXPECT_FALSE(error.FindMatching(EErrorCode::PartitionKeyLimitExceeded)) << ToString(error);
        EXPECT_THAT(ToString(error), HasSubstr("User callback must not run"));
        EXPECT_TRUE(harness.Probe.UserCallbackCalled);
        EXPECT_TRUE(harness.Probe.ObservedKeys.empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
