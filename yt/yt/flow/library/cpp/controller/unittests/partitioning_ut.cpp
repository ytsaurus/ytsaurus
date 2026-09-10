#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/controller/job_manager.h>
#include <yt/yt/flow/library/cpp/controller/state_manager.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/random/source.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/partitioning/partitioning_coordinator.h>

#include <util/system/type_name.h>

#include <atomic>

namespace NYT::NFlow {

// using namespace NLogging;
using namespace NController;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Target-queue partition count reported by the test sink controller below. A negative value means
//! that the count is not available yet.
std::atomic<i64> SinkChannelCountForTest{5};

class TChannelCountSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        auto count = SinkChannelCountForTest.load();
        return count >= 0 ? std::optional(count) : std::nullopt;
    }
};

class TChannelCountSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TChannelCountSinkController;

    using TOrderedBatchingAsyncSinkBase::TOrderedBatchingAsyncSinkBase;

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& /*messages*/, i64 /*seqNo*/) override
    {
        return OKFuture;
    }
};

YT_FLOW_DEFINE_SINK(TChannelCountSink);

////////////////////////////////////////////////////////////////////////////////

//! A second, independently controllable sink, used to verify that a reshard of a non-widest sink is
//! still detected (the widest-sink count would not change in that case).
std::atomic<i64> SecondSinkChannelCountForTest{3};

class TSecondChannelCountSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        auto count = SecondSinkChannelCountForTest.load();
        return count >= 0 ? std::optional(count) : std::nullopt;
    }
};

class TSecondChannelCountSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TSecondChannelCountSinkController;

    using TOrderedBatchingAsyncSinkBase::TOrderedBatchingAsyncSinkBase;

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& /*messages*/, i64 /*seqNo*/) override
    {
        return OKFuture;
    }
};

YT_FLOW_DEFINE_SINK(TSecondChannelCountSink);

////////////////////////////////////////////////////////////////////////////////

class TNullChannelCountSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return std::nullopt;
    }
};

class TNullChannelCountSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TNullChannelCountSinkController;

    using TOrderedBatchingAsyncSinkBase::TOrderedBatchingAsyncSinkBase;

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& /*messages*/, i64 /*seqNo*/) override
    {
        return OKFuture;
    }
};

YT_FLOW_DEFINE_SINK(TNullChannelCountSink);

////////////////////////////////////////////////////////////////////////////////

struct TStatefulSourceTestState
{
    std::vector<std::string> Events;
    int ListKeysCallCount = 0;
    int GetGroupCallCount = 0;
    THashMap<TStreamId, THashMap<TKey, TExtendedSourcePartitionStatusPtr>> ReceivedPartitionStatuses;
    THashSet<TStreamId> OmittedSourceStreams;
    THashSet<std::string> SuppressedGroups;
    TSuppressedAvailabilityGroupsBySource SuppressedGroupsBySource;

    void ResetObservation()
    {
        Events.clear();
        ListKeysCallCount = 0;
        GetGroupCallCount = 0;
        ReceivedPartitionStatuses.clear();
        SuppressedGroups.clear();
        SuppressedGroupsBySource.clear();
    }
};

TStatefulSourceTestState StatefulSourceTestState;

class TStatefulPartitioningSourceController
    : public TRandomSourceController
{
public:
    using TRandomSourceController::TRandomSourceController;

    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) override
    {
        StatefulSourceTestState.Events.push_back("statuses");
        StatefulSourceTestState.ReceivedPartitionStatuses[GetGlobalStreamId()] = statuses;
    }

    void ProcessSuppressedGroups(const THashSet<std::string>& groups) override
    {
        StatefulSourceTestState.Events.push_back("suppressed_groups");
        StatefulSourceTestState.SuppressedGroups = groups;
        StatefulSourceTestState.SuppressedGroupsBySource[GetContext()->SourceStreamId] = groups;
    }

    std::optional<THashMap<TKey, IMapNodePtr>> ListKeys() override
    {
        StatefulSourceTestState.Events.push_back("list_keys");
        ++StatefulSourceTestState.ListKeysCallCount;

        const auto globalStreamId = GetGlobalStreamId();
        if (StatefulSourceTestState.OmittedSourceStreams.contains(globalStreamId)) {
            return THashMap<TKey, IMapNodePtr>{};
        }

        auto activeSourceSpec = GetEphemeralNodeFactory()->CreateMap();
        const auto* receivedStatuses =
            StatefulSourceTestState.ReceivedPartitionStatuses.FindPtr(globalStreamId);
        activeSourceSpec->AddChild(
            "marker",
            ConvertToNode(receivedStatuses && !receivedStatuses->empty() ? "after_status" : "initial"));
        return THashMap<TKey, IMapNodePtr>{{MakeKey(0), std::move(activeSourceSpec)}};
    }

    std::string GetGroup(const TKey& /*key*/) override
    {
        StatefulSourceTestState.Events.push_back("get_group");
        ++StatefulSourceTestState.GetGroupCallCount;
        return "group";
    }

private:
    TStreamId GetGlobalStreamId() const
    {
        const auto& context = GetContext();
        return MakeGlobalStreamId(
            context->ComputationId,
            context->SourceStreamId,
            context->ComputationSpec);
    }
};

class TStatefulPartitioningSource
    : public TRandomSource
{
public:
    using TSourceController = TStatefulPartitioningSourceController;
    using TRandomSource::TRandomSource;
};

YT_FLOW_DEFINE_SOURCE(TStatefulPartitioningSource);

////////////////////////////////////////////////////////////////////////////////

//! Fake storage handler. Enough for this test since there's no recovery after restart.
class TStorageHandler : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

constexpr auto InitialTimestamp = TSystemTimestamp(1'784'633'264);

class TPartitioning
    : public ::testing::Test
{
public:
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>();
    TPersistedStateControlPtr<std::string> PersistedControl;
    TFlowViewPtr FlowView;
    IJobManagerPtr JobManager;
    TPipelineSpecPtr Spec;
    TDynamicPipelineSpecPtr DynamicSpec;
    TComputationId ComputationId = "computation";
    TComputationId SecondComputationId = "second_computation";
    TIntrusivePtr<TFakeVersionProvider> VersionProvider = New<TFakeVersionProvider>(InitialTimestamp.Underlying());

    void Prepare(
        ssize_t numWorkers,
        bool withSink = false,
        bool withSecondSink = false,
        bool withSecondComputation = false,
        bool withStatefulSource = false,
        bool withNonUintKey = false,
        bool withSecondStatefulSource = false,
        bool withNullChannelCountSink = false)
    {
        Spec = New<TPipelineSpec>();
        DynamicSpec = New<TDynamicPipelineSpec>();

        Spec->Computations[ComputationId] = New<TComputationSpec>();
        Spec->Computations[ComputationId]->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
        Spec->Computations[ComputationId]->GroupBySchema = New<NTableClient::TTableSchema>(
            std::vector<NTableClient::TColumnSchema>{withNonUintKey
                    ? NTableClient::TColumnSchema("key", NTableClient::EValueType::String).SetRequired(true)
                    : NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
        if (withNonUintKey) {
            Spec->Computations[ComputationId]->ExperimentalEnableNonUintKey = true;
        }
        if (withStatefulSource) {
            auto addSource = [&] (const TStreamId& streamId) {
                auto sourceSpec = New<TSourceSpec>();
                sourceSpec->SourceClassName = TypeName<TStatefulPartitioningSource>();
                Spec->Computations[ComputationId]->SourceStreams[streamId] = std::move(sourceSpec);
            };
            addSource("source_stream");
            if (withSecondStatefulSource) {
                addSource("second_source_stream");
            }
        } else {
            Spec->Computations[ComputationId]->InputStreamIds.insert("input_stream");
        }
        Spec->Computations[ComputationId]->OutputStreamIds.insert("output_stream");
        Spec->Computations[ComputationId]->TimerStreams["timer_stream"] = New<TTimerSpec>();
        if (withSink) {
            Spec->Computations[ComputationId]->Sinks["sink"] = New<TSinkSpec>();
            Spec->Computations[ComputationId]->Sinks["sink"]->SinkClassName = TypeName<TChannelCountSink>();
        }
        if (withSecondSink) {
            Spec->Computations[ComputationId]->Sinks["sink_b"] = New<TSinkSpec>();
            Spec->Computations[ComputationId]->Sinks["sink_b"]->SinkClassName = TypeName<TSecondChannelCountSink>();
        }
        if (withNullChannelCountSink) {
            Spec->Computations[ComputationId]->Sinks["sink_without_topology"] = New<TSinkSpec>();
            Spec->Computations[ComputationId]->Sinks["sink_without_topology"]->SinkClassName =
                TypeName<TNullChannelCountSink>();
        }

        DynamicSpec->JobManager->AsyncBalancing = false;
        DynamicSpec->Computations[ComputationId] = New<TDynamicComputationSpec>();
        DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
            TYsonString(TStringBuf(R""""(
                {
                    "partition_count_double_delay" = 0;
                    "partition_count_half_delay" = 0;
                }
            )"""")));
        if (withStatefulSource) {
            DynamicSpec->Computations[ComputationId]->SourceStreams["source_stream"] = New<TDynamicSourceSpec>();
            if (withSecondStatefulSource) {
                DynamicSpec->Computations[ComputationId]->SourceStreams["second_source_stream"] =
                    New<TDynamicSourceSpec>();
            }
        }
        if (withSink) {
            // Pin the partition count so the sink's channel count is the only thing that can drive
            // a recreation.
            DynamicSpec->Computations[ComputationId]->Parameters->AddChild("desired_partition_count", ConvertToNode(10));
        }

        auto streamSpec = New<TStreamSpec>();
        streamSpec->ClassName = "FakeClassName";
        streamSpec->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
            [{name="value"; type="string";};]
        )"""")));
        Spec->Streams["output_stream"] = streamSpec;
        if (withSecondComputation) {
            auto secondComputationSpec = CloneYsonStruct(Spec->Computations[ComputationId]);
            secondComputationSpec->OutputStreamIds = {"second_output_stream"};
            Spec->Computations[SecondComputationId] = std::move(secondComputationSpec);
            DynamicSpec->Computations[SecondComputationId] = CloneYsonStruct(
                DynamicSpec->Computations[ComputationId]);
            Spec->Streams["second_output_stream"] = CloneYsonStruct(streamSpec);
        }

        FlowView->State->ExecutionSpec->PipelineSpec->TrySetValue(Spec, VersionProvider);
        FlowView->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(DynamicSpec, VersionProvider);
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(Spec), VersionProvider);
        FlowView->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, VersionProvider);
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 0u);
        auto context = New<TJobManagerContext>();
        context->Invoker = GetCurrentInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->VersionProvider = VersionProvider;
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->VersionProvider = VersionProvider;
        JobManager = CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
        FlowView->CurrentSpec->TrySetValue(Spec, VersionProvider);

        FlowView->State->StartMutation();
        for (ssize_t i = 0; i < numWorkers; i++) {
            auto worker = New<NFlow::TWorker>();
            worker->RpcAddress = Format("worker-%v.net:81", i);
            worker->MonitoringAddress = Format("worker-%v.net:80", i);
            worker->IncarnationId = TIncarnationId(TGuid::Create());
            FlowView->State->Workers[worker->RpcAddress] = worker;
        }
        FlowView->State->CommitMutation();
    }

    //! Persist controller state (as the real controller does via SyncJobManagerState) and rebuild
    //! the job manager from it, emulating leader failover / static-spec change.
    void RecreateJobManager(bool syncState = true)
    {
        if (syncState) {
            FlowView->State->JobManagerState = JobManager->GetState();
        }
        auto context = New<TJobManagerContext>();
        context->Invoker = GetCurrentInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->VersionProvider = VersionProvider;
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->VersionProvider = VersionProvider;
        JobManager = CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
    }

    void SetFeedback(double cpuUsage, double memUsage, double messagesPerSecond, double bytesPerSecond)
    {
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            SetPartitionFeedback(partitionId, cpuUsage, memUsage, messagesPerSecond, bytesPerSecond);
        }
    }

    void SetComputationFeedback(
        const TComputationId& computationId,
        double cpuUsage,
        double memUsage,
        double messagesPerSecond,
        double bytesPerSecond)
    {
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            if (partition->ComputationId == computationId) {
                SetPartitionFeedback(partitionId, cpuUsage, memUsage, messagesPerSecond, bytesPerSecond);
            }
        }
    }

    void SetPartitionFeedback(
        const TPartitionId& partitionId,
        double cpuUsage,
        double memUsage,
        double messagesPerSecond,
        double bytesPerSecond)
    {
        auto& partitionJobStatus = FlowView->Feedback->PartitionJobStatuses[partitionId];
        partitionJobStatus = New<TPartitionJobStatus>();
        auto& jobStatus = partitionJobStatus->CurrentJobStatus;
        jobStatus = New<TJobStatus>();

        jobStatus->PerformanceMetrics->CpuUsage10m = cpuUsage;
        jobStatus->PerformanceMetrics->MemoryUsage10m = memUsage;

        auto metrics = New<TNodeInputMetrics>();
        metrics->Global.MessagesPerSecond = messagesPerSecond;
        metrics->Global.BytesPerSecond = bytesPerSecond;
        jobStatus->InputMetrics = std::move(metrics);
    }

    ssize_t GetExecutingPartitionCount(const TComputationId& computationId) const
    {
        ssize_t result = 0;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            if (partition->ComputationId == computationId && partition->State == EPartitionState::Executing) {
                ++result;
            }
        }
        return result;
    }

    void SetUp() override
    {
        Reset();
    }

    void Reset()
    {
        FlowView = New<TFlowView>();
        PersistedControl = New<TPersistedStateControl<std::string>>(StorageHandler);
        FlowView->State->AttachToControl(PersistedControl);
        PersistedControl->Recover();
        FlowView->State->CurrentTimestamp = InitialTimestamp;
        JobManager = nullptr;
    }

    void AdvanceClock()
    {
        ++FlowView->State->CurrentTimestamp.Underlying();
    }

    void RunPartitioning()
    {
        JobManager->BeginIteration();
        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);
        FlowView->State->CommitMutation();
        JobManager->Commit(FlowView);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitioning, FirstPartitioning)
{
    Prepare(1);
    RunPartitioning();
    ui64 minCount = NPartitioning::TPartitioningCoordinator::DefaultMinInputPartitionCount;
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), minCount);

    Reset();
    Prepare(10);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
}

TEST_F(TPartitioning, MalformedPartitionIsInterruptedWithoutBlockingRangeLookup)
{
    Prepare(1);

    auto makeMalformedPartition = [&] (EPartitionState state) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
        partition->ComputationId = ComputationId;
        partition->State = state;
        partition->StateEpoch = FlowView->State->ExecutionSpec->GetEpoch();
        partition->StateTimestamp = TInstant::Now();
        return partition;
    };
    auto malformedPartition = makeMalformedPartition(EPartitionState::Executing);
    auto restoredMalformedPartition = makeMalformedPartition(EPartitionState::Interrupting);

    JobManager->BeginIteration();
    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->CreatePartition(malformedPartition);
    FlowView->State->ExecutionSpec->Layout->CreatePartition(restoredMalformedPartition);
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    JobManager->Commit(FlowView);

    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(malformedPartition->PartitionId)->State,
        EPartitionState::Interrupting);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(restoredMalformedPartition->PartitionId)->State,
        EPartitionState::Interrupting);

    RunPartitioning();

    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(malformedPartition->PartitionId)->State,
        EPartitionState::Interrupting);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(restoredMalformedPartition->PartitionId)->State,
        EPartitionState::Interrupting);
}

TEST_F(TPartitioning, RangePartitioningErasesRetiringSourceMarker)
{
    const auto partitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    auto coordinatorState = New<NPartitioning::TPartitioningCoordinatorState>();
    auto computationState = New<NPartitioning::TComputationPartitioningState>();
    computationState->RetiringSourcePartitions.insert(partitionId);
    coordinatorState->Computations[ComputationId] = std::move(computationState);
    FlowView->State->JobManagerState->Computations[TComputationId(TStateManager::PartitioningStateComputationId)]["/v1"] =
        ConvertToYsonString(coordinatorState);

    Prepare(1);

    const auto preparedState = JobManager->GetState();
    const auto preparedCoordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        preparedState->Computations.at(TComputationId(TStateManager::PartitioningStateComputationId)).at("/v1"));
    ASSERT_TRUE(
        preparedCoordinatorState->Computations.at(ComputationId)->RetiringSourcePartitions.contains(partitionId));

    auto sourcePartition = New<TPartition>();
    sourcePartition->PartitionId = partitionId;
    sourcePartition->ComputationId = ComputationId;
    sourcePartition->SourceKey = MakeKey(0);
    sourcePartition->State = EPartitionState::Executing;
    sourcePartition->StateEpoch = FlowView->State->ExecutionSpec->GetEpoch();
    sourcePartition->StateTimestamp = TInstant::Now();
    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->CreatePartition(sourcePartition);
    FlowView->State->CommitMutation();

    RunPartitioning();

    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId)->State,
        EPartitionState::Interrupting);
    const auto persistedState = JobManager->GetState();
    const auto restoredCoordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        persistedState->Computations.at(TComputationId(TStateManager::PartitioningStateComputationId)).at("/v1"));
    EXPECT_TRUE(
        restoredCoordinatorState->Computations.at(ComputationId)->RetiringSourcePartitions.empty());
}

TEST_F(TPartitioning, RepartitioningCooldownIsSharedAcrossComputations)
{
    Prepare(10, /*withSink*/ false, /*withSecondSink*/ false, /*withSecondComputation*/ true);

    RunPartitioning();
    ASSERT_EQ(GetExecutingPartitionCount(ComputationId), 10);
    ASSERT_EQ(GetExecutingPartitionCount(SecondComputationId), 10);

    DynamicSpec->Computations[ComputationId]->Parameters->AddChild(
        "desired_partition_count",
        ConvertToNode(20));
    DynamicSpec->Computations[SecondComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 1200000;
                "partition_count_half_delay" = 0;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    FlowView->State->CurrentTimestamp.Underlying() += TDuration::Minutes(11).Seconds();
    SetComputationFeedback(
        SecondComputationId,
        NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionCpuLoad * 2,
        0,
        0,
        0);

    for (int iteration = 0; iteration < 2; ++iteration) {
        JobManager->BeginIteration();
        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);
        FlowView->State->CommitMutation();
        JobManager->Commit(FlowView);
    }

    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 20);
    EXPECT_EQ(GetExecutingPartitionCount(SecondComputationId), 20);
}

TEST_F(TPartitioning, LiveSourceSuppressionPrecedesSingleSnapshotAndUpdatesActiveSpec)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);

    const auto& [partitionId, partition] = *FlowView->State->ExecutionSpec->Layout->Partitions.begin();
    ASSERT_TRUE(partition->SourceKey);
    auto watermarkGenerator = New<TWatermarkGeneratorSpec>();
    watermarkGenerator->UnavailablePartitionGroups = New<TUnavailablePartitionGroupsSpec>();
    watermarkGenerator->UnavailablePartitionGroups->MaxUnavailableGroups = 1;
    watermarkGenerator->UnavailablePartitionGroups->MinAvailableGroups = 0;
    Spec->Computations[ComputationId]->WatermarkStrategy->WatermarkGenerator =
        std::move(watermarkGenerator);

    auto jobStatus = New<TPartitionJobStatus>();
    jobStatus->LastPartitionStatus = GetEphemeralNodeFactory()->CreateMap();
    jobStatus->LastPartitionStatus->AddChild(
        "active_source_status",
        GetEphemeralNodeFactory()->CreateMap());
    jobStatus->LastTraverseData = MakeCompletedPartitionTraverseData(
        FlowView->State->ExecutionSpec->GetEpoch(),
        FlowView->State->CurrentTimestamp,
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->GetValue()->Computations.at(ComputationId));
    jobStatus->LastTraverseData->Node->Streams.at("source_stream")->InflightMetrics->UnavailableTimestamp =
        FlowView->State->CurrentTimestamp;
    FlowView->Feedback->PartitionJobStatuses[partitionId] = std::move(jobStatus);

    FlowView->State->StartMutation();
    JobManager->AggregateTraverseData(FlowView);
    FlowView->State->CommitMutation();

    auto staleTraverse = CloneYsonStruct(
        FlowView->Feedback->PartitionJobStatuses.at(partitionId)->LastTraverseData);
    staleTraverse->Node->ReportTime = TSystemTimestamp(0);
    staleTraverse->Node->Streams.at("source_stream")->InflightMetrics->UnavailableTimestamp.reset();
    FlowView->Feedback->PartitionJobStatuses.at(partitionId)->LastTraverseData = std::move(staleTraverse);
    FlowView->State->StartMutation();
    JobManager->AggregateTraverseData(FlowView);
    FlowView->State->CommitMutation();

    StatefulSourceTestState.ResetObservation();
    RunPartitioning();

    EXPECT_EQ(
        StatefulSourceTestState.Events,
        (std::vector<std::string>{"statuses", "suppressed_groups", "list_keys", "get_group"}));
    EXPECT_EQ(StatefulSourceTestState.SuppressedGroups, THashSet<std::string>{"group"});
    EXPECT_EQ(StatefulSourceTestState.ListKeysCallCount, 1);
    EXPECT_EQ(StatefulSourceTestState.GetGroupCallCount, 1);
    const auto& dynamicPartitionSpec = FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    ASSERT_TRUE(dynamicPartitionSpec);
    const auto activeSourceSpec = dynamicPartitionSpec->ComputationPartitionSpec->GetChildOrThrow("active_source")->AsMap();
    EXPECT_EQ(activeSourceSpec->GetChildValueOrThrow<std::string>("marker"), "after_status");
    EXPECT_TRUE(dynamicPartitionSpec->ComputationPartitionSpec->GetChildValueOrThrow<bool>(
        "availability_group_unavailable"));

    const auto persistedState = JobManager->GetState();
    auto restoredState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        persistedState->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_TRUE(restoredState->SuppressedAvailabilityGroups.empty());
    EXPECT_EQ(
        restoredState->SuppressedAvailabilityGroupsBySource.at("source_stream"),
        THashSet<std::string>{"group"});
}

TEST_F(TPartitioning, SourcePartitionStatusesAreDemultiplexedExactly)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true,
        /*withNonUintKey*/ false,
        /*withSecondStatefulSource*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 2u);

    struct TExpectedStatus
    {
        TKey LocalKey;
        EPartitionState PartitionState;
        std::string Marker;
    };

    THashMap<TStreamId, TExpectedStatus> expectedStatuses;
    std::vector<std::pair<TPartitionId, TKey>> sourcePartitions;
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        ASSERT_TRUE(partition->SourceKey);
        sourcePartitions.emplace_back(partitionId, *partition->SourceKey);
    }

    const auto& computationSpec = Spec->Computations.at(ComputationId);
    FlowView->State->StartMutation();
    for (const auto& [partitionId, sourceKey] : sourcePartitions) {
        auto [localStreamId, localKey] = SplitUniversalPartitionKey(sourceKey);
        const auto globalStreamId = MakeGlobalStreamId(ComputationId, localStreamId, computationSpec);
        const auto partitionState = localStreamId == TStreamId("source_stream")
            ? EPartitionState::Completing
            : EPartitionState::Completed;
        const auto marker = Format("status-for-%v", globalStreamId);

        FlowView->State->ExecutionSpec->Layout->UpdatePartition(
            partitionId,
            partitionState,
            FlowView->State->ExecutionSpec->GetEpoch(),
            TInstant::Now());

        auto activeSourceStatus = GetEphemeralNodeFactory()->CreateMap();
        activeSourceStatus->AddChild("marker", ConvertToNode(marker));
        auto partitioningStatus = New<TComputationPartitionStatus>();
        partitioningStatus->ActiveSourceStatus = std::move(activeSourceStatus);
        auto jobStatus = New<TPartitionJobStatus>();
        jobStatus->LastPartitionStatus = ConvertTo<IMapNodePtr>(partitioningStatus);
        FlowView->Feedback->PartitionJobStatuses[partitionId] = std::move(jobStatus);

        EmplaceOrCrash(
            expectedStatuses,
            globalStreamId,
            TExpectedStatus{
                .LocalKey = std::move(localKey),
                .PartitionState = partitionState,
                .Marker = marker,
            });
    }
    FlowView->State->CommitMutation();

    StatefulSourceTestState.ResetObservation();
    RunPartitioning();

    ASSERT_EQ(StatefulSourceTestState.ReceivedPartitionStatuses.size(), 2u);
    for (const auto& [globalStreamId, expected] : expectedStatuses) {
        const auto& statuses = StatefulSourceTestState.ReceivedPartitionStatuses.at(globalStreamId);
        ASSERT_EQ(statuses.size(), 1u);
        const auto& status = statuses.at(expected.LocalKey);
        ASSERT_TRUE(status);
        EXPECT_EQ(status->PartitionState, expected.PartitionState);
        ASSERT_TRUE(status->PartitionStatus);
        EXPECT_EQ(
            status->PartitionStatus->GetChildValueOrThrow<std::string>("marker"),
            expected.Marker);
    }
}

TEST_F(TPartitioning, SourceDynamicSpecContainsBlockedOutputStreams)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto& [partitionId, partition] = *FlowView->State->ExecutionSpec->Layout->Partitions.begin();

    auto interruptingPartition = CloneYsonStruct(partition);
    interruptingPartition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    interruptingPartition->CurrentJobId.reset();
    interruptingPartition->State = EPartitionState::Interrupting;
    interruptingPartition->StateEpoch = FlowView->State->ExecutionSpec->GetEpoch();
    interruptingPartition->StateTimestamp = TInstant::Now();
    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->CreatePartition(std::move(interruptingPartition));
    FlowView->State->CommitMutation();

    RunPartitioning();

    const auto& dynamicPartitionSpec =
        FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    ASSERT_TRUE(dynamicPartitionSpec->ComputationPartitionSpec);
    auto partitioningSpec = ConvertTo<IComputation::TDynamicPartitionSpecPtr>(
        dynamicPartitionSpec->ComputationPartitionSpec);
    EXPECT_EQ(
        partitioningSpec->BlockedOutputStreams,
        THashSet<TStreamId>{TStreamId("output_stream")});
    EXPECT_EQ(
        ConvertTo<THashSet<TStreamId>>(
            dynamicPartitionSpec->ComputationPartitionSpec->GetChildOrThrow("blocked_output_streams")),
        THashSet<TStreamId>{TStreamId("output_stream")});
}

TEST_F(TPartitioning, DisappearedSourcePartitionCompletesThenIsRemoved)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto partitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;
    const auto& partition = FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId);
    ASSERT_EQ(partition->State, EPartitionState::Executing);

    const auto sourceStreamId = MakeGlobalStreamId(
        ComputationId,
        TStreamId("source_stream"),
        Spec->Computations.at(ComputationId));
    StatefulSourceTestState.OmittedSourceStreams.insert(sourceStreamId);
    RunPartitioning();

    const auto& completingPartition =
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId);
    EXPECT_EQ(completingPartition->State, EPartitionState::Completing);
    const auto& dynamicPartitionSpec =
        FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    ASSERT_TRUE(dynamicPartitionSpec->ComputationPartitionSpec);
    auto partitioningSpec = ConvertTo<IComputation::TDynamicPartitionSpecPtr>(
        dynamicPartitionSpec->ComputationPartitionSpec);
    ASSERT_TRUE(partitioningSpec->ActiveSource);
    EXPECT_TRUE(partitioningSpec->ActiveSource->GetChildren().empty());

    RecreateJobManager();
    StatefulSourceTestState.OmittedSourceStreams.erase(sourceStreamId);
    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId)->State,
        EPartitionState::Completing);
    const auto& reappearedDynamicPartitionSpec =
        FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    auto reappearedPartitioningSpec = ConvertTo<IComputation::TDynamicPartitionSpecPtr>(
        reappearedDynamicPartitionSpec->ComputationPartitionSpec);
    ASSERT_TRUE(reappearedPartitioningSpec->ActiveSource);
    EXPECT_TRUE(reappearedPartitioningSpec->ActiveSource->GetChildren().empty());

    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->UpdatePartition(
        partitionId,
        EPartitionState::Completed,
        FlowView->State->ExecutionSpec->GetEpoch(),
        TInstant::Now());
    FlowView->State->CommitMutation();

    auto failedFlowView = FlowView->CopyPtr();
    failedFlowView->State = failedFlowView->State->Clone();
    failedFlowView->EphemeralState = CloneYsonStruct(failedFlowView->EphemeralState);
    JobManager->BeginIteration();
    failedFlowView->State->StartMutation();
    JobManager->DoPartitioning(failedFlowView);

    ASSERT_EQ(failedFlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_FALSE(failedFlowView->State->ExecutionSpec->Layout->Partitions.contains(partitionId));

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_FALSE(FlowView->State->ExecutionSpec->Layout->Partitions.contains(partitionId));
    const auto successorPartitionId =
        FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(successorPartitionId)->State,
        EPartitionState::Executing);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_TRUE(FlowView->State->ExecutionSpec->Layout->Partitions.contains(successorPartitionId));
}

TEST_F(TPartitioning, CompletedSourcePartitionIsNotRecreatedWhileExpected)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto partitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;

    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->UpdatePartition(
        partitionId,
        EPartitionState::Completing,
        FlowView->State->ExecutionSpec->GetEpoch(),
        TInstant::Now());
    FlowView->State->CommitMutation();
    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId)->State,
        EPartitionState::Completing);

    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->UpdatePartition(
        partitionId,
        EPartitionState::Completed,
        FlowView->State->ExecutionSpec->GetEpoch(),
        TInstant::Now());
    FlowView->State->CommitMutation();
    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId)->State,
        EPartitionState::Completed);

    RecreateJobManager();
    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId)->State,
        EPartitionState::Completed);
}

TEST_F(TPartitioning, SourcePartitioningIgnoresDesiredPartitionCount)
{
    StatefulSourceTestState = {};
    Prepare(
        20,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    DynamicSpec->Computations[ComputationId]->Parameters->AddChild(
        "desired_partition_count",
        ConvertToNode(20));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto partitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;

    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_TRUE(FlowView->State->ExecutionSpec->Layout->Partitions.contains(partitionId));
}

TEST_F(TPartitioning, SourceTraverseIgnoresStaleRangePartition)
{
    StatefulSourceTestState = {};
    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();

    auto& partitions = FlowView->State->ExecutionSpec->Layout->Partitions;
    ASSERT_EQ(partitions.size(), 1u);
    const auto sourcePartitionId = partitions.begin()->first;
    ASSERT_TRUE(partitions.begin()->second->SourceKey);

    auto stalePartition = New<TPartition>();
    stalePartition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    stalePartition->ComputationId = ComputationId;
    stalePartition->State = EPartitionState::Interrupting;
    stalePartition->StateEpoch = FlowView->State->ExecutionSpec->GetEpoch();
    stalePartition->StateTimestamp = TInstant::Seconds(FlowView->State->CurrentTimestamp.Underlying());
    const auto [lowerKey, upperKey] = UniversalKeyRange();
    stalePartition->LowerKey = lowerKey;
    stalePartition->UpperKey = upperKey;

    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->Layout->CreatePartition(stalePartition);
    FlowView->State->CommitMutation();

    auto setTraverseData = [&] (const TPartitionId& partitionId, TSystemTimestamp watermark) {
        auto node = New<TNodeTraverseData>();
        node->ReportTime = watermark;
        for (const auto& streamId : {
                TStreamId("source_stream"),
                TStreamId("output_stream"),
                TStreamId("timer_stream")})
        {
            auto stream = New<TStreamTraverseData>();
            stream->Epoch = FlowView->State->ExecutionSpec->GetEpoch();
            stream->State = EStreamState::Active;
            stream->SystemWatermark = watermark;
            stream->EventWatermark = watermark;
            stream->InflightMetrics = New<TInflightMetrics>();
            node->Streams[streamId] = std::move(stream);
        }
        auto fromPartitionTraverseData = New<TFromPartitionTraverseData>();
        fromPartitionTraverseData->Node = std::move(node);
        auto partitionStatus = New<TPartitionJobStatus>();
        partitionStatus->LastTraverseData = std::move(fromPartitionTraverseData);
        FlowView->Feedback->PartitionJobStatuses[partitionId] = std::move(partitionStatus);
    };
    const auto sourceWatermark = TSystemTimestamp(100);
    setTraverseData(sourcePartitionId, sourceWatermark);
    setTraverseData(stalePartition->PartitionId, TSystemTimestamp(10));

    JobManager->AggregateTraverseData(FlowView);

    const auto& computationTraverse = FlowView->State->TraverseData->Computations.at(ComputationId);
    EXPECT_EQ(computationTraverse->Streams.at("source_stream")->EventWatermark, sourceWatermark);
    EXPECT_EQ(computationTraverse->Streams.at("output_stream")->EventWatermark, sourceWatermark);
}

TEST_F(TPartitioning, MaxPartitionCountLimit)
{
    Prepare(30);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
                "max_partition_count" = 25;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 25u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 25u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
}

TEST_F(TPartitioning, VeryLowMax)
{
    Prepare(30);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
                "max_partition_count" = 1;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
}

TEST_F(TPartitioning, VeryHighMin)
{
    Prepare(30);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
                "min_partition_count" = 30000;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30000u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30000u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
}

TEST_F(TPartitioning, WrongMinMax)
{
    Prepare(10);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
                "min_partition_count" = 100;
                "max_partition_count" = 10;
            }
        )"""")));
    EXPECT_THROW(JobManager->Reconfigure(DynamicSpec), TErrorException);
}

TEST_F(TPartitioning, Repartitioning)
{
    Prepare(10);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    AdvanceClock();

    double maxCpuUsage = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 2, 0, 0, 0);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30u); // 10 old and 20 new.

    AdvanceClock();

    double maxMemUsage = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionMemoryUsed;
    SetFeedback(maxCpuUsage, maxMemUsage * 2, 0, 0);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 70u); // 30 old and 40 new.

    AdvanceClock();

    double maxMessages = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionMessagesPerSecond;
    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages * 2, 0);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 150u); // 70 old and 80 new.

    AdvanceClock();

    double maxBytes = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionBytesPerSecond;
    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages, maxBytes * 2);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 310u); // 150 old and 160 new.

    AdvanceClock();

    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages, maxBytes);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 310u);
}

TEST_F(TPartitioning, FirstPassAfterFailoverKeepsTheCooldown)
{
    Prepare(10);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    RecreateJobManager();

    double maxCpuUsage = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 2, 0, 0, 0);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    AdvanceClock();
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30u);
}

TEST_F(TPartitioning, ResumeRearmsTheCooldown)
{
    Prepare(10);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    double maxCpuUsage = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 2, 0, 0, 0);

    AdvanceClock();
    VersionProvider->SetUnixTime(FlowView->State->CurrentTimestamp.Underlying());
    JobManager->BeginIteration();
    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Paused, VersionProvider);
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    JobManager->Commit(FlowView);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    AdvanceClock();
    VersionProvider->SetUnixTime(FlowView->State->CurrentTimestamp.Underlying());
    JobManager->BeginIteration();
    FlowView->State->StartMutation();
    FlowView->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, VersionProvider);
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    JobManager->Commit(FlowView);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    AdvanceClock();
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30u);
}

TEST_F(TPartitioning, RecreateOnSinkChannelCountChange)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    // desired_partition_count is pinned, so the proposed count never changes: the only trigger for a
    // recreation here is a change in the sink's target-queue partition count (YTFLOW-572).
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // No channel-count change: a subsequent partitioning must not recreate anything.
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);

    // The target queue was resharded: partitions must be recreated so fresh producer ids are
    // generated (10 old partitions interrupted + 10 new).
    SinkChannelCountForTest = 7;
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, SinkTopologyChangeUsesFreshProposedPartitionCount)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    RunPartitioning();
    ASSERT_EQ(GetExecutingPartitionCount(ComputationId), 10);

    // Unpin the count and keep the ordinary growth cooldown closed. The reshard still forces one
    // recreation, which must use the fresh sink-based proposal (7 channels * multiplier 3).
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 1200000;
                "partition_count_half_delay" = 0;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);
    AdvanceClock();
    SinkChannelCountForTest = 7;

    RunPartitioning();

    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 21);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 31u);
}

TEST_F(TPartitioning, DelayedFirstSinkChannelCountDoesNotRecreate)
{
    SinkChannelCountForTest = -1;
    Prepare(10, /*withSink*/ true);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    SinkChannelCountForTest = 5;
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);

    const auto stateAfterFirstSinkChannelCount = JobManager->GetState();
    auto coordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        stateAfterFirstSinkChannelCount->Computations.at(
            TComputationId(TStateManager::PartitioningStateComputationId))
            .at("/v1"));
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        stateAfterFirstSinkChannelCount->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_NE(computationState->SinkChannelCounts->GetVersion(), TVersion(0));
    EXPECT_EQ(
        coordinatorState->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion,
        computationState->SinkChannelCounts->GetVersion());

    SinkChannelCountForTest = 7;
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, UsesLastKnownWidestSinkCountWhenAnotherSinkChanges)
{
    SinkChannelCountForTest = 7;
    SecondSinkChannelCountForTest = 3;
    Prepare(10, /*withSink*/ true, /*withSecondSink*/ true);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();
    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 21);

    SinkChannelCountForTest = -1;
    SecondSinkChannelCountForTest = 4;
    RunPartitioning();
    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 21);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 42u);

    const auto state = JobManager->GetState();
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        state->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_EQ(
        computationState->SinkChannelCounts->GetValue(),
        (THashMap<TSinkId, i64>{{TSinkId("sink"), 7}, {TSinkId("sink_b"), 4}}));
}

TEST_F(TPartitioning, LateFirstSinkChannelCountTriggersRecreation)
{
    SinkChannelCountForTest = 5;
    SecondSinkChannelCountForTest = -1;
    Prepare(10, /*withSink*/ true, /*withSecondSink*/ true);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    const auto initialState = JobManager->GetState();
    const auto initialComputationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        initialState->Computations.at(ComputationId).at("/partitioning/v0"));
    const auto initialSinkTopologyVersion = initialComputationState->SinkChannelCounts->GetVersion();

    SecondSinkChannelCountForTest = 3;
    RunPartitioning();

    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 10);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
    const auto state = JobManager->GetState();
    auto coordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        state->Computations.at(TComputationId(TStateManager::PartitioningStateComputationId)).at("/v1"));
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        state->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_NE(computationState->SinkChannelCounts->GetVersion(), initialSinkTopologyVersion);
    EXPECT_EQ(
        coordinatorState->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion,
        computationState->SinkChannelCounts->GetVersion());
    EXPECT_EQ(
        computationState->SinkChannelCounts->GetValue(),
        (THashMap<TSinkId, i64>{{TSinkId("sink"), 5}, {TSinkId("sink_b"), 3}}));
}

TEST_F(TPartitioning, NullSinkChannelCountDoesNotBlockSinkTopologyVersioning)
{
    SinkChannelCountForTest = 5;
    Prepare(
        10,
        /*withSink*/ true,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ false,
        /*withNonUintKey*/ false,
        /*withSecondStatefulSource*/ false,
        /*withNullChannelCountSink*/ true);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    const auto initialState = JobManager->GetState();
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        initialState->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_NE(computationState->SinkChannelCounts->GetVersion(), TVersion(0));
    EXPECT_EQ(
        computationState->SinkChannelCounts->GetValue(),
        (THashMap<TSinkId, i64>{{TSinkId("sink"), 5}}));

    SinkChannelCountForTest = 7;
    RunPartitioning();

    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, UsesAvailableSinkCountForSizing)
{
    SinkChannelCountForTest = 7;
    SecondSinkChannelCountForTest = -1;
    Prepare(10, /*withSink*/ true, /*withSecondSink*/ true);
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 0;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();

    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 21);
    const auto state = JobManager->GetState();
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        state->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_NE(computationState->SinkChannelCounts->GetVersion(), TVersion(0));
    EXPECT_EQ(
        computationState->SinkChannelCounts->GetValue(),
        (THashMap<TSinkId, i64>{{TSinkId("sink"), 7}}));
}

TEST_F(TPartitioning, PersistSinkTopologyAcrossRecreation)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // The target queue is resharded AND the job manager is recreated (leader failover / static-spec
    // change) before the next partitioning. The controller's topology version and the coordinator's
    // applied version are restored independently, so the change still forces producer-id regeneration.
    SinkChannelCountForTest = 7;
    RecreateJobManager();

    RunPartitioning();
    // In-memory-only tracking would miss this; persisted versions recreate 10 interrupted + 10 new.
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, PersistsSinkTopologyAcknowledgementWithRecreation)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    SinkChannelCountForTest = 7;
    JobManager->BeginIteration();
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->JobManagerState = JobManager->GetState();
    FlowView->State->CommitMutation();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);

    RecreateJobManager(/*syncState*/ false);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
}

TEST_F(TPartitioning, InitializesMissingAppliedSinkTopologyVersionWithoutRecreation)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    auto persistedState = JobManager->GetState();
    persistedState->Computations.erase(TComputationId(TStateManager::PartitioningStateComputationId));
    FlowView->State->JobManagerState = std::move(persistedState);

    RecreateJobManager(/*syncState*/ false);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);

    const auto restoredState = JobManager->GetState();
    const auto& coordinatorDomain = restoredState->Computations.at(
        TComputationId(TStateManager::PartitioningStateComputationId));
    auto coordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        coordinatorDomain.at("/v1"));
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        restoredState->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_NE(computationState->SinkChannelCounts->GetVersion(), TVersion(0));
    EXPECT_EQ(
        coordinatorState->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion,
        computationState->SinkChannelCounts->GetVersion());
}

TEST_F(TPartitioning, LoadsLegacySinkTopologyStateWithoutAutomaticMigration)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);
    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    auto persistedState = JobManager->GetState();
    persistedState->Computations[ComputationId]["/partitioning/v0"] = TYsonString(TStringBuf(R""""(
        {
            "last_sink_channel_counts" = {
                "sink" = 5;
            };
        }
    )""""));
    persistedState->Computations.erase(TComputationId(TStateManager::PartitioningStateComputationId));
    FlowView->State->JobManagerState = std::move(persistedState);

    SinkChannelCountForTest = 7;
    RecreateJobManager(/*syncState*/ false);
    RunPartitioning();

    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
    const auto upgradedState = JobManager->GetState();
    const auto& serializedComputationState =
        upgradedState->Computations.at(ComputationId).at("/partitioning/v0");
    auto computationState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        serializedComputationState);
    EXPECT_EQ(
        computationState->SinkChannelCounts->GetValue(),
        (THashMap<TSinkId, i64>{{TSinkId("sink"), 7}}));
}

TEST_F(TPartitioning, CleansRemovedComputationState)
{
    Prepare(
        10,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ true);

    RunPartitioning();

    auto persistedState = JobManager->GetState();
    Spec->Computations.erase(SecondComputationId);
    DynamicSpec->Computations.erase(SecondComputationId);
    FlowView->State->JobManagerState = std::move(persistedState);
    RecreateJobManager(/*syncState*/ false);

    const auto cleanedState = JobManager->GetState();
    const auto& coordinatorDomain = cleanedState->Computations.at(
        TComputationId(TStateManager::PartitioningStateComputationId));
    auto coordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        coordinatorDomain.at("/v1"));
    EXPECT_TRUE(coordinatorState->Computations.contains(ComputationId));
    EXPECT_FALSE(coordinatorState->Computations.contains(SecondComputationId));
}

TEST_F(TPartitioning, MigratesLegacySuppressedAvailabilityGroupsInComputationState)
{
    StatefulSourceTestState = {};
    auto partitioningState = New<TUniversalComputationControllerPartitioningState>();
    partitioningState->SuppressedAvailabilityGroups.insert("source_stream-group");
    FlowView->State->JobManagerState->Computations[ComputationId]["/partitioning/v0"] =
        ConvertToYsonString(partitioningState);

    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto& partitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;
    const auto& dynamicPartitionSpec =
        FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    ASSERT_TRUE(dynamicPartitionSpec);
    EXPECT_TRUE(dynamicPartitionSpec->ComputationPartitionSpec->GetChildValueOrThrow<bool>(
        "availability_group_unavailable"));

    const auto persistedState = JobManager->GetState();
    auto restoredState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        persistedState->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_TRUE(restoredState->SuppressedAvailabilityGroups.empty());
    EXPECT_EQ(
        restoredState->SuppressedAvailabilityGroupsBySource.at("source_stream"),
        THashSet<std::string>{"group"});
}

TEST_F(TPartitioning, RestoresStructuredSuppressionForSourcePartition)
{
    StatefulSourceTestState = {};
    auto partitioningState = New<TUniversalComputationControllerPartitioningState>();
    partitioningState->SuppressedAvailabilityGroupsBySource = {
        {TStreamId("source_stream"), {"group"}},
    };
    FlowView->State->JobManagerState->Computations[ComputationId]["/partitioning/v0"] =
        ConvertToYsonString(partitioningState);

    Prepare(
        1,
        /*withSink*/ false,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ true);

    RunPartitioning();

    EXPECT_EQ(StatefulSourceTestState.SuppressedGroups, THashSet<std::string>{"group"});
    EXPECT_EQ(
        StatefulSourceTestState.SuppressedGroupsBySource.at("source_stream"),
        THashSet<std::string>{"group"});
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    const auto partitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;
    const auto& dynamicPartitionSpec =
        FlowView->EphemeralState->GetPartitionState(partitionId)->DynamicPartitionSpec;
    ASSERT_TRUE(dynamicPartitionSpec);
    EXPECT_TRUE(dynamicPartitionSpec->ComputationPartitionSpec->GetChildValueOrThrow<bool>(
        "availability_group_unavailable"));

    const auto persistedState = JobManager->GetState();
    auto restoredState = ConvertTo<TUniversalComputationControllerPartitioningStatePtr>(
        persistedState->Computations.at(ComputationId).at("/partitioning/v0"));
    EXPECT_TRUE(restoredState->SuppressedAvailabilityGroups.empty());
    EXPECT_EQ(
        restoredState->SuppressedAvailabilityGroupsBySource.at("source_stream"),
        THashSet<std::string>{"group"});
}

TEST_F(TPartitioning, RecreateOnNonWidestSinkChannelCountChange)
{
    // Sink "sink" is the widest (5), "sink_b" is narrower (3). The producer-id decision must track
    // every sink, not just the widest — otherwise a reshard of "sink_b" (with the max unchanged)
    // would be missed.
    SinkChannelCountForTest = 5;
    SecondSinkChannelCountForTest = 3;
    Prepare(10, /*withSink*/ true, /*withSecondSink*/ true);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // Reshard only the narrower sink, keeping it below the widest so the max is unchanged (5).
    SecondSinkChannelCountForTest = 4;
    RunPartitioning();
    // Tracking only the widest sink would miss this; per-sink tracking detects it and recreates.
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, FailedPersistenceDoesNotAcknowledgeSinkTopologyVersion)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);
    RunPartitioning();
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    const auto initialState = JobManager->GetState();
    const auto initialCoordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        initialState->Computations.at(TComputationId(TStateManager::PartitioningStateComputationId)).at("/v1"));
    const auto initialVersion =
        initialCoordinatorState->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion;
    ASSERT_TRUE(initialVersion);

    SinkChannelCountForTest = 7;
    auto failedFlowView = FlowView->CopyPtr();
    failedFlowView->State = failedFlowView->State->Clone();
    failedFlowView->EphemeralState = CloneYsonStruct(failedFlowView->EphemeralState);
    JobManager->BeginIteration();
    failedFlowView->State->StartMutation();
    JobManager->DoPartitioning(failedFlowView);

    JobManager->BeginIteration();
    const auto stateAfterSkippedIteration = JobManager->GetState();
    const auto coordinatorStateAfterSkippedIteration =
        ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        stateAfterSkippedIteration->Computations.at(
            TComputationId(TStateManager::PartitioningStateComputationId))
            .at("/v1"));
    EXPECT_EQ(
        coordinatorStateAfterSkippedIteration->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion,
        initialVersion);
    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
    const auto committedState = JobManager->GetState();
    const auto committedCoordinatorState = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(
        committedState->Computations.at(TComputationId(TStateManager::PartitioningStateComputationId)).at("/v1"));
    EXPECT_NE(
        committedCoordinatorState->Computations.at(ComputationId)->LastAppliedSinkTopologyVersion,
        initialVersion);
}

TEST_F(TPartitioning, SinkTopologyChangeWaitsForNonUintRangePivots)
{
    SinkChannelCountForTest = 5;
    Prepare(
        10,
        /*withSink*/ true,
        /*withSecondSink*/ false,
        /*withSecondComputation*/ false,
        /*withStatefulSource*/ false,
        /*withNonUintKey*/ true);
    RunPartitioning();
    ASSERT_EQ(GetExecutingPartitionCount(ComputationId), 1);
    const auto initialPartitionId = FlowView->State->ExecutionSpec->Layout->Partitions.begin()->first;

    SinkChannelCountForTest = 7;
    RunPartitioning();

    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);
    ASSERT_EQ(GetExecutingPartitionCount(ComputationId), 1);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(initialPartitionId)->State,
        EPartitionState::Executing);

    SetFeedback(0, 0, 0, 0);
    FlowView->Feedback->PartitionJobStatuses.at(initialPartitionId)
        ->CurrentJobStatus->InputMetrics->Global.Pivots = {
        MakeKey(TStringBuf("a")),
        MakeKey(TStringBuf("b")),
        MakeKey(TStringBuf("c")),
        MakeKey(TStringBuf("d")),
        MakeKey(TStringBuf("e")),
        MakeKey(TStringBuf("f")),
        MakeKey(TStringBuf("g")),
        MakeKey(TStringBuf("h")),
        MakeKey(TStringBuf("i")),
    };
    RunPartitioning();

    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 11u);
    EXPECT_EQ(GetExecutingPartitionCount(ComputationId), 10);
    EXPECT_EQ(
        FlowView->State->ExecutionSpec->Layout->Partitions.at(initialPartitionId)->State,
        EPartitionState::Interrupting);
}

////////////////////////////////////////////////////////////////////////////////

// Directly exercises the peak-hold envelope used to damp partition-count reductions (YTFLOWSUPPORT-113):
// the value grows instantly (attack) but shrinks only with the release time constant.
TEST(TPartitionCountPeakHold, ReleaseEnvelope)
{
    using TController = NPartitioning::TPartitioningCoordinator;

    // Instant attack: any growth returns the target regardless of elapsed time / release half-delay.
    EXPECT_EQ(TController::ApplyPeakHoldRelease(100.0, 200.0, TDuration::Zero(), TDuration::Minutes(200)), 200.0);
    EXPECT_EQ(TController::ApplyPeakHoldRelease(100.0, 200.0, TDuration::Hours(10), TDuration::Minutes(200)), 200.0);

    // Zero release half-delay disables smoothing: reduction is applied immediately (backward compatible).
    EXPECT_EQ(TController::ApplyPeakHoldRelease(200.0, 100.0, TDuration::Hours(1), TDuration::Zero()), 100.0);

    // No time elapsed: reduction is fully damped, the value holds.
    EXPECT_EQ(TController::ApplyPeakHoldRelease(200.0, 100.0, TDuration::Zero(), TDuration::Minutes(200)), 200.0);

    // Release over exactly one half-delay halves the remaining gap: 200 -> 150.
    EXPECT_NEAR(
        TController::ApplyPeakHoldRelease(200.0, 100.0, TDuration::Minutes(200), TDuration::Minutes(200)),
        150.0,
        1e-3);

    // Release over two half-delays quarters the remaining gap: 200 -> 125.
    EXPECT_NEAR(
        TController::ApplyPeakHoldRelease(200.0, 100.0, TDuration::Minutes(400), TDuration::Minutes(200)),
        125.0,
        1e-3);

    // Release over many half-delays approaches the target.
    EXPECT_NEAR(
        TController::ApplyPeakHoldRelease(200.0, 100.0, TDuration::Hours(100), TDuration::Minutes(200)),
        100.0,
        1e-3);

    // Repeated sampling with a lower target decays slowly and never overshoots below the target.
    double value = 500.0;
    for (int i = 0; i < 5; ++i) {
        value = TController::ApplyPeakHoldRelease(value, 250.0, TDuration::Minutes(20), TDuration::Minutes(200));
        EXPECT_GE(value, 250.0);
        EXPECT_LE(value, 500.0);
    }
    EXPECT_GT(value, 250.0); // 5 * 20min is far below the settling time, so still well above the target.
}

////////////////////////////////////////////////////////////////////////////////

// A large partition_count_half_delay must not slow partition growth: the peak-hold attack is instant.
TEST_F(TPartitioning, PartitionCountHalfDelayDoesNotBlockGrowth)
{
    Prepare(10);
    // Growth gate open (double_delay = 0); large half_delay (1h in ms) that must not impede growth.
    DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "partition_count_double_delay" = 0;
                "partition_count_half_delay" = 3600000;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    RunPartitioning();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    AdvanceClock();

    double maxCpuUsage = NPartitioning::TPartitioningCoordinator::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 4, 0, 0, 0);
    RunPartitioning();
    // Attack is instant even with a 1h half_delay: the count grows right away.
    EXPECT_GT(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
