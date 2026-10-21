#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/partitioning/partitioning_coordinator.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/profiling/solomon/registry.h>

namespace NYT::NFlow::NPartitioning {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TRangeController
    : public IComputationController
{
public:
    TPartitioningSpecPtr PartitioningSpec = New<TPartitioningSpec>();
    TVersion SinkTopologyVersion;

    TParametersPtr GetParametersBase() const override
    {
        return {};
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return {};
    }

    void Init(IInitContextPtr) override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>&,
        const TNodeTraverseDataPtr&,
        const TFlowViewPtr&) override
    {
        YT_ABORT();
    }

    TPartitioningTopology DescribePartitioningTopology() override
    {
        return {.Value = TPartitioningTopology::TRange{}};
    }

    TPartitioningDescription DescribePartitioning(const TPartitioningStatus&) override
    {
        return {
            .Value = TPartitioningDescription::TRange{
                .PartitioningSpec = PartitioningSpec,
                .SinkTopologyVersion = SinkTopologyVersion,
            },
        };
    }

    double ComputePartitionWeight(const TPartitionId&, const TFlowViewPtr&) override
    {
        return 1;
    }

    void UpdateWatermarkState(TWatermarkStatePtr) override
    { }
};

DEFINE_REFCOUNTED_TYPE(TRangeController);

class TPartitioningTransactionTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId_{"consumer"};
    const TStreamId Input_{"input"};
    TInstant Now_ = TInstant::Seconds(1'789'400'000);
    TStateManagerMockPtr Storage_ = New<TStateManagerMock>();
    TIntrusivePtr<TRangeController> Controller_ = New<TRangeController>();
    TPipelineSpecPtr Spec_ = New<TPipelineSpec>();
    TFlowViewPtr FlowView_ = New<TFlowView>();
    NProfiling::TSolomonRegistryPtr Registry_ = New<NProfiling::TSolomonRegistry>();
    TPartitioningCoordinatorPtr Coordinator_;
    TJobStatusPtr Job_ = New<TJobStatus>();

    void SetUp() override
    {
        Registry_->SetWindowSize(12);
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds.insert(Input_);
        spec->GroupBySchema = New<NTableClient::TTableSchema>(
            std::vector{
                NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64)
                    .SetRequired(true)
                    .SetSortOrder(NTableClient::ESortOrder::Ascending),
            });
        Spec_->Computations[ComputationId_] = spec;
        Controller_->PartitioningSpec->MinPartitionCount = 1;
        Controller_->PartitioningSpec->MaxPartitionCount = 1;
        Controller_->PartitioningSpec->PartitionCountDoubleDelay = TDuration::Seconds(1);

        auto saved = New<TPartitioningCoordinatorState>();
        auto state = New<TComputationPartitioningState>();
        saved->Computations[ComputationId_] = state;
        Storage_->Set("/v1", ConvertToYsonString(saved));
        CreateCoordinator();

        auto control = New<TPersistedStateControl<std::string>>(New<TNullStorageHandler<std::string>>());
        FlowView_->State->AttachToControl(control);
        control->Recover();
        FlowView_->State->ExecutionSpec->PipelineSpec->TrySetValue(Spec_, TestVersionProvider());
        auto dynamicSpec = New<TDynamicPipelineSpec>();
        dynamicSpec->JobManager->Partitioning->EnableThroughputAutoscaler = true;
        FlowView_->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(dynamicSpec, TestVersionProvider());
        FlowView_->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, TestVersionProvider());
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId_;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->CurrentJobId = TJobId(TGuid::Create());
        partition->LowerKey = MinKey();
        partition->UpperKey = MaxKey();
        partition->State = EPartitionState::Executing;
        FlowView_->State->StartMutation();
        FlowView_->State->ExecutionSpec->Layout->Partitions.emplace(partition->PartitionId, partition);
        auto job = New<TJob>();
        job->JobId = *partition->CurrentJobId;
        job->PartitionId = partition->PartitionId;
        FlowView_->State->ExecutionSpec->Layout->Jobs.emplace(job->JobId, job);
        FlowView_->State->CommitMutation();
        Job_->JobId = *partition->CurrentJobId;
        Job_->InitedTime = Now_;
        Job_->FromPartitionTraverseData = New<TFromPartitionTraverseData>();
        auto stream = New<TStreamTraverseData>();
        stream->InflightMetrics = New<TInflightMetrics>();
        stream->InflightMetrics->ReadyCount = 0;
        stream->InflightMetrics->OfferedCountPerSec = 10;
        stream->InflightMetrics->ProcessedCountPerSec = 10;
        Job_->FromPartitionTraverseData->Node->Streams[Input_] = stream;
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobId = partition->CurrentJobId;
        status->CurrentJobStatus = Job_;
        FlowView_->Feedback->PartitionJobStatuses[partition->PartitionId] = status;

        for (int minute = 0; minute <= 6; ++minute) {
            Publish(200);
            Coordinator_->BeginIteration(FlowView_);
            RunPass();
            Coordinator_->Commit();
            Now_ += TDuration::Minutes(1);
        }
        ASSERT_EQ(ReadState()->ThroughputAutoscaler.ObservedPartitionCount, 1);
    }

    void CreateCoordinator()
    {
        Coordinator_ = New<TPartitioningCoordinator>(
            THashMap<TComputationId, IComputationControllerPtr>{{ComputationId_, Controller_}},
            Spec_,
            Storage_->CreateContext(),
            NLogging::TLogger("test"),
            NLogging::TLogger("test"),
            NProfiling::TProfiler(Registry_, "/partitioning"));
    }

    double ReadSensor(TStringBuf name, NProfiling::TTagList tags = {})
    {
        tags.emplace_back("computation_id", ComputationId_.Underlying());
        return ConvertTo<double>(BuildYsonStringFluently().Do([&] (TFluentAny fluent) {
            Registry_->ReadRecentSensorValues(Format("yt%v", name), tags, {}, fluent);
        }));
    }

    TComputationPartitioningStatePtr ReadState()
    {
        Coordinator_->Sync();
        Storage_->Sync();
        return GetOrCrash(ConvertTo<TPartitioningCoordinatorStatePtr>(Storage_->Get("/v1"))->Computations, ComputationId_);
    }

    void Publish(double capacity)
    {
        auto observation = New<TProcessingObservation>();
        observation->Sequence = Job_->ProcessingObservation ? Job_->ProcessingObservation->Sequence + 1 : 1;
        observation->CapturedAt = Now_;
        observation->SpecGeneration = FlowView_->State->ExecutionSpec->GetEpoch();
        const auto& previous = Job_->ProcessingObservation;
        auto elapsed = previous ? Now_ - previous->CapturedAt : TDuration::Seconds(1);
        i64 count = static_cast<i64>(capacity * elapsed.SecondsFloat());
        observation->ProcessedCount = (previous ? previous->ProcessedCount : 0) + count;
        observation->ProcessedByteSize = observation->ProcessedCount * 10;
        observation->ProcessingTime = (previous ? previous->ProcessingTime : TDuration::Zero()) + elapsed;
        Job_->ProcessingObservation = observation;
    }

    void RunPass()
    {
        FlowView_->State->CurrentTimestamp = TSystemTimestamp(Now_.Seconds());
        FlowView_->Feedback->UpdateTime = Now_;
        Job_->Epoch = FlowView_->State->ExecutionSpec->GetEpoch();
        Job_->UpdateTime = Now_;
        for (auto& [_, status] : FlowView_->Feedback->PartitionJobStatuses) {
            status->CurrentJobStatusUpdateTime = Now_;
        }
        FlowView_->State->StartMutation();
        Coordinator_->DoPartitioning(FlowView_);
        FlowView_->State->CommitMutation();
    }
};

TEST_F(TPartitioningTransactionTest, CommittedDemandPeakSurvivesCoordinatorRestart)
{
    const auto saved = ReadState();
    ASSERT_TRUE(saved->ThroughputDemandHistory);
    Coordinator_.Reset();
    CreateCoordinator();
    Job_->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 1;
    Now_ += TDuration::Days(1);
    for (int minute = 0; minute <= 6; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/peak_steady_demand"), 10);
}

TEST_F(TPartitioningTransactionTest, MetricsSurvivePassesAndRetainDecisionCounts)
{
    Job_->PerformanceMetrics = New<TNodePerformanceMetrics>();
    Job_->PerformanceMetrics->CpuUsage10m = 0.2;
    Controller_->PartitioningSpec->DesiredAveragePartitionCpuLoad = 0.1;
    for (int pass = 1; pass <= 2; ++pass) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Registry_->Collect();
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity"), 200);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity_known"), 1);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/criterion_average", {{"criterion", "cpu"}}), 0.2);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/criterion_proposed_partition_count", {{"criterion", "cpu"}}), 2);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/proposed_partition_count"), 1);
        EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/decision_count", {{"criterion", "cpu"}}), pass);
        Now_ += TDuration::Minutes(1);
    }
    auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning;
    config->EnableThroughputAutoscaler = false;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity"), std::exception);
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/decision_count", {{"criterion", "cpu"}}), 3);
}

TEST_F(TPartitioningTransactionTest, ManualPartitionCountRemovesThroughputTelemetry)
{
    Publish(200);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/throughput/capacity_known"), 1);

    Controller_->PartitioningSpec->DesiredPartitionCount = 1;
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity"), std::exception);
    EXPECT_THROW(ReadSensor("/partitioning/throughput/capacity_known"), std::exception);
}

TEST_F(TPartitioningTransactionTest, RepartitionCounterSurvivesCollectionAndCountsOnlyCommits)
{
    Controller_->SinkTopologyVersion = TVersion(42);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Coordinator_->Commit();
    Controller_->SinkTopologyVersion = TVersion(43);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    Registry_->Collect();
    EXPECT_THROW(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), std::exception);
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), 1);
    Coordinator_->Commit();
    Registry_->Collect();
    EXPECT_DOUBLE_EQ(ReadSensor("/partitioning/repartition_count", {{"initiator", "sink_topology"}}), 1);
}

TEST_F(TPartitioningTransactionTest, ThroughputIgnoresLegacyThroughputCriteria)
{
    Controller_->PartitioningSpec->MaxPartitionCount = 64;
    Controller_->PartitioningSpec->DesiredAveragePartitionMessagesPerSecond = 1;
    Controller_->PartitioningSpec->DesiredAveragePartitionBytesPerSecond = 1;
    Job_->InputMetrics = New<TNodeInputMetrics>();
    Job_->InputMetrics->Global.MessagesPerSecond = 1'000'000;
    Job_->InputMetrics->Global.BytesPerSecond = 1'000'000'000;
    for (int minute = 0; minute < 10; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    EXPECT_EQ(FlowView_->State->ExecutionSpec->Layout->Partitions.size(), 1u);
}

TEST_F(TPartitioningTransactionTest, CandidateJobRemovalDoesNotChangeCommittedMeasurement)
{
    Controller_->PartitioningSpec->MaxPartitionCount = 64;
    Job_->FromPartitionTraverseData->Node->Streams[Input_]->InflightMetrics->OfferedCountPerSec = 500;
    for (int minute = 0; minute < 3; ++minute) {
        Publish(200);
        Coordinator_->BeginIteration(FlowView_);
        RunPass();
        Coordinator_->Commit();
        Now_ += TDuration::Minutes(1);
    }
    Publish(200);
    Job_->UpdateTime = Now_;
    FlowView_->Feedback->UpdateTime = Now_;
    auto candidate = FlowView_->CopyPtr();
    candidate->State = candidate->State->Clone();
    Coordinator_->BeginIteration(FlowView_);
    candidate->State->StartMutation();
    auto partition = CloneYsonStruct(candidate->State->ExecutionSpec->Layout->Partitions.begin()->second);
    partition->CurrentJobId.reset();
    partition->StateEpoch = candidate->State->ExecutionSpec->GetEpoch();
    partition->StateTimestamp = Now_;
    candidate->State->ExecutionSpec->Layout->UpdatePartition(partition);
    Coordinator_->DoPartitioning(candidate);
    const auto proposed = ReadState()->ThroughputAutoscaler;
    ASSERT_TRUE(proposed.Anchor);
    EXPECT_EQ(proposed.Anchor->Capacity, 200);

    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    const auto retried = ReadState()->ThroughputAutoscaler;
    ASSERT_TRUE(retried.Anchor);
    EXPECT_EQ(retried.Anchor->Capacity, proposed.Anchor->Capacity);
    EXPECT_TRUE(retried.PendingPartitionCount);
}

TEST_F(TPartitioningTransactionTest, RepeatedPassCannotReplaceRollbackBoundary)
{
    const i64 committed = ReadState()->ThroughputAutoscaler.PipelineResetGeneration;
    auto config = FlowView_->State->ExecutionSpec->DynamicPipelineSpec->GetValue()->JobManager->Partitioning->Throughput;
    config->ResetGeneration = 1;
    Publish(200);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    config->ResetGeneration = 2;
    Now_ += TDuration::Seconds(1);
    Publish(200);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);

    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, committed);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    RunPass();
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    Coordinator_->Commit();
    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->ThroughputAutoscaler.PipelineResetGeneration, 2);
    EXPECT_TRUE(ReadState()->ThroughputAutoscaler.ResetRequested);
}

TEST_F(TPartitioningTransactionTest, RollbackPreservesExistingSinkTopologyBoundary)
{
    Controller_->SinkTopologyVersion = TVersion(42);
    Coordinator_->BeginIteration(FlowView_);
    RunPass();
    EXPECT_EQ(ReadState()->LastAppliedSinkTopologyVersion, TVersion(42));

    Coordinator_->BeginIteration(FlowView_);
    EXPECT_FALSE(ReadState()->LastAppliedSinkTopologyVersion);
    RunPass();
    Coordinator_->Commit();
    Coordinator_->BeginIteration(FlowView_);
    EXPECT_EQ(ReadState()->LastAppliedSinkTopologyVersion, TVersion(42));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NPartitioning
