#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/controller/job_manager.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <util/system/type_name.h>

#include <atomic>

namespace NYT::NFlow {

// using namespace NLogging;
using namespace NController;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Target-queue partition count reported by the test sink controller below. The test mutates it
//! between partitioning cycles to emulate a queue reshard.
std::atomic<i64> SinkChannelCountForTest{5};

class TChannelCountSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return SinkChannelCountForTest.load();
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
        return SecondSinkChannelCountForTest.load();
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

    void Prepare(ssize_t numWorkers, bool withSink = false, bool withSecondSink = false)
    {
        Spec = New<TPipelineSpec>();
        DynamicSpec = New<TDynamicPipelineSpec>();

        Spec->Computations[ComputationId] = New<TComputationSpec>();
        Spec->Computations[ComputationId]->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
        Spec->Computations[ComputationId]->GroupBySchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
            NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
        Spec->Computations[ComputationId]->InputStreamIds.insert("input_stream");
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

        DynamicSpec->JobManager->AsyncBalancing = false;
        DynamicSpec->Computations[ComputationId] = New<TDynamicComputationSpec>();
        DynamicSpec->Computations[ComputationId]->Parameters = ConvertTo<IMapNodePtr>(
            TYsonString(TStringBuf(R""""(
                {
                    "partition_count_double_delay" = 0;
                    "partition_count_half_delay" = 0;
                }
            )"""")));
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

        FlowView->State->ExecutionSpec->PipelineSpec->SetValue(Spec);
        FlowView->State->ExecutionSpec->DynamicPipelineSpec->SetValue(DynamicSpec);
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(Spec));
        FlowView->State->ExecutionSpec->PipelineState->SetValue(EPipelineState::Working);
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 0u);
        auto context = New<TJobManagerContext>();
        context->Invoker = GetCurrentInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->StatusProfiler = CreateStatusProfiler();
        JobManager = CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
        FlowView->CurrentSpec->SetValue(Spec);

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
    void RecreateJobManager()
    {
        FlowView->State->JobManagerState = JobManager->GetState();
        auto context = New<TJobManagerContext>();
        context->Invoker = GetCurrentInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->StatusProfiler = CreateStatusProfiler();
        JobManager = CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
    }

    void SetFeedback(double cpuUsage, double memUsage, double messagesPerSecond, double bytesPerSecond)
    {
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
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
        JobManager = nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitioning, FirstPartitioning)
{
    Prepare(1);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    ui64 minCount = TUniversalComputationController::DefaultMinInputPartitionCount;
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), minCount);

    Reset();
    Prepare(10);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
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

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 25u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
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

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 1u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
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

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30000u);

    // Verify that a subsequent DoPartitioning does not cause any mutations.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
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
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    Sleep(TDuration::MilliSeconds(10));

    double maxCpuUsage = TUniversalComputationController::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 2, 0, 0, 0);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 30u); // 10 old and 20 new.

    Sleep(TDuration::MilliSeconds(10));

    double maxMemUsage = TUniversalComputationController::DefaultDesiredAveragePartitionMemoryUsed;
    SetFeedback(maxCpuUsage, maxMemUsage * 2, 0, 0);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 70u); // 30 old and 40 new.

    Sleep(TDuration::MilliSeconds(10));

    double maxMessages = TUniversalComputationController::DefaultDesiredAveragePartitionMessagesPerSecond;
    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages * 2, 0);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 150u); // 70 old and 80 new.

    Sleep(TDuration::MilliSeconds(10));

    double maxBytes = TUniversalComputationController::DefaultDesiredAveragePartitionBytesPerSecond;
    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages, maxBytes * 2);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 310u); // 150 old and 160 new.

    Sleep(TDuration::MilliSeconds(10));

    SetFeedback(maxCpuUsage, maxMemUsage, maxMessages, maxBytes);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 310u);
}

TEST_F(TPartitioning, RecreateOnSinkChannelCountChange)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    // desired_partition_count is pinned, so the proposed count never changes: the only trigger for a
    // recreation here is a change in the sink's target-queue partition count (YTFLOW-572).
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // No channel-count change: a subsequent partitioning must not recreate anything.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);

    // The target queue was resharded: partitions must be recreated so fresh producer ids are
    // generated (10 old partitions interrupted + 10 new).
    SinkChannelCountForTest = 7;
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, PersistSinkChannelCountAcrossRecreation)
{
    SinkChannelCountForTest = 5;
    Prepare(10, /*withSink*/ true);

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // The target queue is resharded AND the job manager is recreated (leader failover / static-spec
    // change) before the next partitioning. The last channel count is restored from persisted state,
    // so the change is still detected and partitions are recreated to regenerate producer ids.
    SinkChannelCountForTest = 7;
    RecreateJobManager();

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    // In-memory tracking would have lost the previous count on recreation and missed this; with a
    // persisted value the change is detected and partitions are recreated (10 interrupted + 10 new).
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

TEST_F(TPartitioning, RecreateOnNonWidestSinkChannelCountChange)
{
    // Sink "sink" is the widest (5), "sink_b" is narrower (3). The producer-id decision must track
    // every sink, not just the widest — otherwise a reshard of "sink_b" (with the max unchanged)
    // would be missed.
    SinkChannelCountForTest = 5;
    SecondSinkChannelCountForTest = 3;
    Prepare(10, /*withSink*/ true, /*withSecondSink*/ true);

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    // Reshard only the narrower sink, keeping it below the widest so the max is unchanged (5).
    SecondSinkChannelCountForTest = 4;
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    // Tracking only the widest sink would miss this; per-sink tracking detects it and recreates.
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 20u);
}

////////////////////////////////////////////////////////////////////////////////

// Directly exercises the peak-hold envelope used to damp partition-count reductions (YTFLOWSUPPORT-113):
// the value grows instantly (attack) but shrinks only with the release time constant.
TEST(TPartitionCountPeakHold, ReleaseEnvelope)
{
    using TController = TUniversalComputationController;

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

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    EXPECT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);

    Sleep(TDuration::MilliSeconds(10));

    double maxCpuUsage = TUniversalComputationController::DefaultDesiredAveragePartitionCpuLoad;
    SetFeedback(maxCpuUsage * 4, 0, 0, 0);
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();
    // Attack is instant even with a 1h half_delay: the count grows right away.
    EXPECT_GT(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 10u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
