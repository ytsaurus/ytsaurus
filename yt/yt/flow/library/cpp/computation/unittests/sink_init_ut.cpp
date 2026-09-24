#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <atomic>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

namespace {

const TStreamId OutputStreamId("output");

struct TSinkInitProbe
{
    enum class EEventType
    {
        Construct,
        Init,
        Prepare,
        Distribute,
        Watermark,
        Sync,
        Commit,
    };

    struct TEvent
    {
        EEventType Type;
        TSinkId SinkId;
        const ISink* Instance = nullptr;
        TDynamicSinkSpecPtr DynamicSinkSpec;
    };

    std::vector<TEvent> Events;
    bool FailInit = false;
    bool MismatchedParentKey = false;
    bool DistributeOnPrepare = false;
    bool RunOneEpoch = false;
    bool CaptureProcessingStatus = false;
    std::vector<TComputationStatusPtr> ProcessingStatuses;
};

std::atomic<TSinkInitProbe*>& GetActiveSinkInitProbe()
{
    static std::atomic<TSinkInitProbe*> Probe = nullptr;
    return Probe;
}

class TRecordingSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

class TRecordingSink
    : public ISink
{
public:
    using TSinkController = TRecordingSinkController;

    TRecordingSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext)
        : Context_(std::move(context))
        , DynamicSinkSpec_(dynamicContext->DynamicSinkSpec)
        , Probe_(GetActiveSinkInitProbe().load())
    {
        YT_VERIFY(Probe_);
        Record(TSinkInitProbe::EEventType::Construct);
    }

    void Init(IInitContextPtr /*initContext*/) override
    {
        Record(TSinkInitProbe::EEventType::Init);
        THROW_ERROR_EXCEPTION_IF(Probe_->FailInit, "Recording sink init failed");
    }

    void Distribute(
        const TOutputMessageConstPtr& /*message*/,
        TOnDistributedCallback onDistributed) override
    {
        Record(TSinkInitProbe::EEventType::Distribute);
        onDistributed();
    }

    void Sync(IDynamicTableTransactionPtr /*transaction*/) override
    {
        Record(TSinkInitProbe::EEventType::Sync);
    }

    void Commit() override
    {
        Record(TSinkInitProbe::EEventType::Commit);
    }

    void UpdateWatermarkState(TWatermarkStatePtr /*state*/) override
    {
        Record(TSinkInitProbe::EEventType::Watermark);
    }

private:
    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }

    void Record(TSinkInitProbe::EEventType type)
    {
        Probe_->Events.push_back({
            .Type = type,
            .SinkId = Context_->SinkId,
            .Instance = this,
            .DynamicSinkSpec = DynamicSinkSpec_,
        });
    }

private:
    const TSinkContextPtr Context_;
    const TDynamicSinkSpecPtr DynamicSinkSpec_;
    TSinkInitProbe* const Probe_;
};

YT_FLOW_DEFINE_SINK(TRecordingSink);

class TTestComputationRunContext
    : public IComputationRunContext
{
public:
    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(
        const THashSet<TStreamId>& /*allowedStreams*/) override
    {
        return MakeFuture(std::vector<TInputMessageConstPtr>{});
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        return MakeFuture(THashMap<TStreamId, TInflightMetricsPtr>{});
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
    { }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }
};

class TProductionWiredSinkInitComputation
    : public TUniversalComputationBase
{
public:
    TProductionWiredSinkInitComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext)
        : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
        , Probe_(GetActiveSinkInitProbe().load())
    {
        YT_VERIFY(Probe_);
    }

    void RunForTest(const IComputationRunContextPtr& context)
    {
        Run(context);
    }

private:
    void DoPrepare(const IComputationRunContextPtr& context) override
    {
        Probe_->Events.push_back({
            .Type = TSinkInitProbe::EEventType::Prepare,
        });

        if (Probe_->DistributeOnPrepare) {
            const auto& schema = GetContext()->StreamSpecStorage->GetSchema(OutputStreamId);
            TMessageBuilder builder(OutputStreamId, schema);
            builder.SetMessageId(TMessageId("message"));
            builder.SetSystemTimestamp(TSystemTimestamp(1));
            builder.SetEventTimestamp(TSystemTimestamp(1));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1));
            std::vector<TOutputMessageConstPtr> messages{
                New<TOutputMessage>(builder.Finish(), GetContext()->StreamSpecStorage),
            };
            RegisterOutputMessages(
                context,
                messages,
                Probe_->MismatchedParentKey
                    ? std::optional<TKey>(MakeKey("other-key"))
                    : GetContext()->Partition->SourceKey);
        }

        if (!Probe_->RunOneEpoch) {
            THROW_ERROR_EXCEPTION("Sink init test stopped after DoPrepare");
        }
    }

    void DoExecute(
        const IComputationRunContextPtr& context,
        NTracing::TTraceContextGuard&& /*initTraceContextGuard*/) override
    {
        YT_VERIFY(Probe_->RunOneEpoch);
        if (Probe_->CaptureProcessingStatus) {
            THashMap<TStreamId, TInflightStreamTraverseDataPtr> inflights{
                {OutputStreamId, New<TInflightStreamTraverseData>()},
            };
            for (int epoch = 0; epoch < 2; ++epoch) {
                auto iterGuard = StartRunIteration(context);
                if (epoch == 0) {
                    UpdateStatus(TSystemTimestamp(1), TSystemTimestamp(1), inflights);
                }
                auto transaction = PrepareTransaction(context);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                Commit(context, transaction);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                auto reportTime = TSystemTimestamp(epoch + 2);
                UpdateStatus(reportTime, reportTime, inflights);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                auto detachedStatus = GetStatus();
                detachedStatus->NodeTraverse.Reset();
                detachedStatus->ProcessingObservation.Reset();
                Probe_->ProcessingStatuses.push_back(GetStatus());
                FinishRunIteration();
            }
            THROW_ERROR_EXCEPTION("Processing status test completed");
        }
        auto iterGuard = StartRunIteration(context);
        auto transaction = PrepareTransaction(context);
        Commit(context, transaction);
        FinishRunIteration();
        THROW_ERROR_EXCEPTION("Sink init test stopped after one epoch");
    }

    void ProcessDistributedMessages(
        const IComputationRunContextPtr& /*context*/,
        std::deque<TOutputMessageConstPtr>&& /*messages*/) override
    { }

private:
    TSinkInitProbe* const Probe_;
};

YT_FLOW_DEFINE_COMPUTATION(TProductionWiredSinkInitComputation);

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const IClientPtr Client_;
};

struct TSinkDescription
{
    TSinkId SinkId;
    bool AcceptOutput = false;
};

TComputationSpecPtr MakeSpec(const std::vector<TSinkDescription>& sinks)
{
    auto specYson = Format("{computation_class_name=%Qv;output_stream_ids=[%Qv];}",
        TypeName<TProductionWiredSinkInitComputation>(),
        OutputStreamId);
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    for (const auto& sink : sinks) {
        auto sinkSpec = New<TSinkSpec>();
        sinkSpec->SinkClassName = TypeName<TRecordingSink>();
        if (sink.AcceptOutput) {
            sinkSpec->InputStreamIds.insert(OutputStreamId);
        }
        sinkSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
        spec->Sinks.emplace(sink.SinkId, std::move(sinkSpec));
    }
    return spec;
}

TComputationStreamSpecStoragePtr MakeStreamSpecStorage()
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = New<TTableSchema>();
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[OutputStreamId][TStreamSpecId(1)] = streamSpec;
    return New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(specs)),
        New<TTableSchema>(),
        /*converterCache*/ nullptr);
}

TDynamicComputationContextPtr MakeDynamicContext()
{
    auto context = New<TDynamicComputationContext>();
    context->DynamicComputationSpec = ConvertTo<TDynamicComputationSpecPtr>(TYsonStringBuf("{}"));
    context->DynamicPartitionSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf(
        "{computation_partition_spec={};}"));
    return context;
}

TError RunProductionWiredComputation(
    const TComputationSpecPtr& spec,
    std::optional<TKey> sourceKey,
    TSinkInitProbe* probe,
    bool distributeOnPrepare,
    bool runOneEpoch = false,
    TDynamicComputationContextPtr reconfigureBeforeRun = nullptr)
{
    auto queue = New<TActionQueue>("SinkInitTest");
    auto timestampProvider = New<NiceMock<TMockTimestampProvider>>();

    auto client = New<NiceMock<TMockClient>>();
    client->SetTimestampProvider(timestampProvider);
    auto transaction = New<NiceMock<TMockTransaction>>();
    transaction->StartTimestamp = TTimestamp(1);
    ON_CALL(*transaction, Commit(_))
        .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));
    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("test");
    partition->SourceKey = std::move(sourceKey);
    partition->State = EPartitionState::Executing;

    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->PartitionId = partition->PartitionId;
    job->LeaseId = TLeaseId(TGuid::Create());

    auto streamSpecStorage = MakeStreamSpecStorage();
    TStreamLimitUsageStateMap outputStreamStates{
        {OutputStreamId, New<TStreamLimitUsageState>()},
    };

    auto context = New<TComputationContext>();
    context->ComputationSpec = spec;
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PipelinePath = NYPath::TRichYPath("//pipeline");
    context->PipelinePath.SetCluster("test");
    context->Partition = partition;
    context->Job = job;
    context->SerializedInvoker = queue->GetInvoker();
    context->PoolInvoker = queue->GetInvoker();
    context->Logger = NLogging::TLogger("SinkInitTest");
    context->Profiler = NProfiling::TProfiler();
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->PartitionBufferState = CreateDetachedPartitionBufferState(std::move(outputStreamStates));
    context->StreamSpecStorage = streamSpecStorage;
    context->DistributedThrottlerControllerChannelProvider = [] {
        return NRpc::IChannelPtr{};
    };

    auto dynamicContext = MakeDynamicContext();

    auto runContext = New<TTestComputationRunContext>();
    probe->DistributeOnPrepare = distributeOnPrepare;
    probe->RunOneEpoch = runOneEpoch;
    YT_VERIFY(!GetActiveSinkInitProbe().exchange(probe));
    auto resetProbe = Finally([] {
        GetActiveSinkInitProbe().store(nullptr);
    });
    return WaitFor(BIND([
        context = std::move(context),
        dynamicContext = std::move(dynamicContext),
        runContext = std::move(runContext),
        reconfigureBeforeRun = std::move(reconfigureBeforeRun)
    ] {
        auto computation = New<TProductionWiredSinkInitComputation>(
            context,
            dynamicContext);
        computation->UpdateWatermarkState(New<TWatermarkState>());
        computation->SetInputTraverse({});
        if (reconfigureBeforeRun) {
            computation->Reconfigure(reconfigureBeforeRun);
        }
        computation->RunForTest(runContext);
    })
            .AsyncVia(queue->GetInvoker())
            .Run());
}

std::vector<TSinkInitProbe::TEvent> GetEvents(
    const TSinkInitProbe& probe,
    TSinkInitProbe::EEventType type)
{
    std::vector<TSinkInitProbe::TEvent> result;
    for (const auto& event : probe.Events) {
        if (event.Type == type) {
            result.push_back(event);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TComputationStatusPublicationTest, PublishesCommittedObservationWithTraverse)
{
    TSinkInitProbe probe;
    probe.CaptureProcessingStatus = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Processing status test completed");
    ASSERT_EQ(probe.ProcessingStatuses.size(), 8u);
    const auto& initial = probe.ProcessingStatuses[0];
    ASSERT_TRUE(initial->NodeTraverse);
    EXPECT_EQ(initial->NodeTraverse->ReportTime, TSystemTimestamp(1));
    EXPECT_FALSE(initial->ProcessingObservation);

    for (int epoch = 0; epoch < 2; ++epoch) {
        for (int index = 2; index <= 3; ++index) {
            const auto& published = probe.ProcessingStatuses[4 * epoch + index];
            ASSERT_TRUE(published->NodeTraverse);
            ASSERT_TRUE(published->ProcessingObservation);
            EXPECT_EQ(published->NodeTraverse->ReportTime, TSystemTimestamp(epoch + 2));
            EXPECT_EQ(published->NodeTraverse->IterationCycle, epoch);
            EXPECT_EQ(published->ProcessingObservation->Sequence, epoch + 1);
        }
    }
    for (int epoch = 0; epoch < 2; ++epoch) {
        const auto& committed = probe.ProcessingStatuses[4 * epoch + 1];
        ASSERT_TRUE(committed->ProcessingObservation);
        EXPECT_EQ(committed->ProcessingObservation->Sequence, epoch + 1);
        EXPECT_EQ(committed->NodeTraverse, probe.ProcessingStatuses[4 * epoch]->NodeTraverse);
    }
    const auto& pending = probe.ProcessingStatuses[4];
    EXPECT_EQ(pending->NodeTraverse, probe.ProcessingStatuses[2]->NodeTraverse);
    EXPECT_EQ(pending->ProcessingObservation, probe.ProcessingStatuses[2]->ProcessingObservation);
}

TEST(TSinkInitTest, RunsPartitionSinkThroughFirstEpoch)
{
    using enum TSinkInitProbe::EEventType;

    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    std::vector<TSinkInitProbe::EEventType> eventTypes;
    for (const auto& event : probe.Events) {
        eventTypes.push_back(event.Type);
    }
    EXPECT_THAT(eventTypes, ElementsAre(Construct, Watermark, Init, Prepare, Distribute, Watermark, Sync, Commit));
    const auto constructions = GetEvents(probe, Construct);
    ASSERT_EQ(constructions.size(), 1u);
    for (const auto& event : probe.Events) {
        if (event.Instance) {
            EXPECT_EQ(event.Instance, constructions[0].Instance);
        }
    }
}

TEST(TSinkInitTest, AppliesPendingDynamicSpecBeforeSinkConstruction)
{
    const TSinkId sinkId("ordinary");
    auto reconfiguredContext = MakeDynamicContext();
    reconfiguredContext->SpecGeneration = 1;
    auto reconfiguredSinkSpec = New<TDynamicSinkSpec>();
    reconfiguredSinkSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    reconfiguredContext->DynamicComputationSpec->Sinks.emplace(sinkId, reconfiguredSinkSpec);

    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({{sinkId, false}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false,
        /*runOneEpoch*/ false,
        std::move(reconfiguredContext));

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after DoPrepare");
    const auto constructions = GetEvents(probe, TSinkInitProbe::EEventType::Construct);
    ASSERT_EQ(constructions.size(), 1u);
    EXPECT_EQ(constructions[0].DynamicSinkSpec, reconfiguredSinkSpec);
}

TEST(TSinkInitTest, InitializesEveryKeyedSinkBeforePrepareAndReusesMatchingSink)
{
    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({
            {TSinkId("ordinary"), true},
            {TSinkId("unused"), false},
        }),
        MakeKey("source-key"),
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    const auto constructions = GetEvents(probe, TSinkInitProbe::EEventType::Construct);
    const auto inits = GetEvents(probe, TSinkInitProbe::EEventType::Init);
    const auto prepares = GetEvents(probe, TSinkInitProbe::EEventType::Prepare);
    const auto distributions = GetEvents(probe, TSinkInitProbe::EEventType::Distribute);
    const auto syncs = GetEvents(probe, TSinkInitProbe::EEventType::Sync);
    const auto commits = GetEvents(probe, TSinkInitProbe::EEventType::Commit);
    ASSERT_EQ(constructions.size(), 2u);
    ASSERT_EQ(inits.size(), 2u);
    ASSERT_EQ(prepares.size(), 1u);
    ASSERT_EQ(distributions.size(), 1u);
    ASSERT_EQ(syncs.size(), 2u);
    ASSERT_EQ(commits.size(), 2u);

    auto prepare = std::find_if(probe.Events.begin(), probe.Events.end(), [] (const auto& event) {
        return event.Type == TSinkInitProbe::EEventType::Prepare;
    });
    ASSERT_NE(prepare, probe.Events.end());
    EXPECT_EQ(std::count_if(probe.Events.begin(), prepare, [] (const auto& event) {
        return event.Type == TSinkInitProbe::EEventType::Init;
    }),
        2);

    auto matchingConstruction = std::find_if(constructions.begin(), constructions.end(), [&] (const auto& event) {
        return event.SinkId == distributions[0].SinkId;
    });
    ASSERT_NE(matchingConstruction, constructions.end());
    EXPECT_EQ(matchingConstruction->Instance, distributions[0].Instance);
}

TEST(TSinkInitTest, InitFailurePreventsPrepare)
{
    TSinkInitProbe probe;
    probe.FailInit = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("unused"), false}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false);

    EXPECT_EQ(error.GetMessage(), "Recording sink init failed");
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Init).size(), 1u);
    EXPECT_TRUE(GetEvents(probe, TSinkInitProbe::EEventType::Prepare).empty());
}

TEST(TSinkInitTest, RejectsMismatchingParentKeyBeforeDistribution)
{
    TSinkInitProbe probe;
    probe.MismatchedParentKey = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        MakeKey("source-key"),
        &probe,
        /*distributeOnPrepare*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink parent key does not match partition source key");
    EXPECT_TRUE(GetEvents(probe, TSinkInitProbe::EEventType::Distribute).empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
