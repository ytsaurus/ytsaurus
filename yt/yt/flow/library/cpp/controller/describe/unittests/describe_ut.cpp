#include <yt/yt/flow/library/cpp/controller/describe/common.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_computation.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_computations.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_partition.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_pipeline.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_worker.h>
#include <yt/yt/flow/library/cpp/controller/describe/describe_workers.h>
#include <yt/yt/flow/library/cpp/controller/describe/fill_graph_limits.h>
#include <yt/yt/flow/library/cpp/controller/describe/pipeline_description_unroll.h>

#include <yt/yt/flow/library/cpp/controller/job_manager.h>

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/describe_traits.h>
#include <yt/yt/flow/library/cpp/common/flow_core_build_info.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/authenticator.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/connectors/random/source.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/matchers.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NDescribe {
namespace {

using namespace NController;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;

using namespace ::testing;
using ::testing::AllOf;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TTestSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TTestSinkController;

    TTestSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext,
        std::shared_ptr<std::vector<std::pair<i64, std::vector<i64>>>> storage = {})
        : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
        , Storage_(std::move(storage))
    { }

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<NYT::NFlow::TOutputMessageConstPtr>& /*messages*/, i64 /*seqNo*/) override
    {
        return OKFuture;
    }

private:
    std::shared_ptr<std::vector<std::pair<i64, std::vector<i64>>>> Storage_;
};

YT_FLOW_DEFINE_SINK(TTestSink);

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr CreateComputationSpec(bool withSource)
{
    auto spec = New<TComputationSpec>();
    spec->GroupBySchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
    if (withSource) {
        spec->ComputationClassName = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
        spec->SourceStreams["Source"] = New<TSourceSpec>();
        spec->SourceStreams["Source"]->SourceClassName = TypeName<TRandomSource>();
    } else {
        spec->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
        spec->InputStreamIds.insert("input_stream");
        spec->TimerStreams["timer_stream"] = New<TTimerSpec>();
    }
    spec->OutputStreamIds.insert("output_stream");
    return spec;
}

TDynamicComputationSpecPtr CreateDynamicComputationSpec(bool withSource, int partitionCount)
{
    auto spec = New<TDynamicComputationSpec>();
    if (withSource) {
        auto& source = spec->SourceStreams["Source"];
        source = New<TDynamicSourceSpec>();
        source->Parameters->AddChild("partition_count", NYTree::ConvertToNode(partitionCount));
    } else {
        spec->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(partitionCount));
    }
    return spec;
}

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

////////////////////////////////////////////////////////////////////////////////

class TDescribeTest
    : public ::testing::Test
{
public:
    struct TComputationPrepareSpec
    {
        int PartitionCount = 0;
        bool WithSource = false;
    };

    std::vector<TComputationPrepareSpec> ComputationPrepareSpecs = {
        {.PartitionCount = 3},
        {.PartitionCount = 3},
        {.PartitionCount = 3, .WithSource = true},
    };

    int WorkerCount = 1;

    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>();
    TPersistedStateControlPtr<std::string> PersistedControl;
    TFlowViewPtr FlowView;
    IJobManagerPtr JobManager;
    IJobDirectoryPtr JobDirectory;
    TPipelineSpecPtr Spec;
    TDynamicPipelineSpecPtr DynamicSpec;
    TPipelineDescription Pipeline;

    void Prepare()
    {
        Pipeline = TPipelineDescription();
        Spec = New<TPipelineSpec>();
        DynamicSpec = New<TDynamicPipelineSpec>();

        // Prevent slow balancer iterations.
        DynamicSpec->JobManager->RebalanceActionMinTime = TDuration::Zero();
        DynamicSpec->JobManager->RebalanceSyncPeriod = TDuration::Zero();

        int totalPartitions = 0;
        for (int i = 0; i < std::ssize(ComputationPrepareSpecs); ++i) {
            const auto& prepareSpec = ComputationPrepareSpecs[i];
            auto computationId = TComputationId(Format("Computation_%v", i + 1));
            Spec->Computations[computationId] = CreateComputationSpec(prepareSpec.WithSource);

            Spec->Computations[computationId]->Sinks[TSinkId(Format("Sink_%v", i + 1))] = New<TSinkSpec>();
            Spec->Computations[computationId]->Sinks[TSinkId(Format("Sink_%v", i + 1))]->SinkClassName = TypeName<TTestSink>();

            DynamicSpec->JobManager->AsyncBalancing = false;
            DynamicSpec->Computations[computationId] = CreateDynamicComputationSpec(prepareSpec.WithSource, prepareSpec.PartitionCount);
            totalPartitions += prepareSpec.PartitionCount;
        }

        auto streamSpec = New<TStreamSpec>();
        streamSpec->ClassName = "FakeClassName";
        streamSpec->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
            [{name="value"; type="string";};]
        )"""")));
        Spec->Streams["output_stream"] = streamSpec;
        Spec->Postprocess();

        FlowView->State->ExecutionSpec->PipelineSpec->SetValue(Spec);
        FlowView->State->ExecutionSpec->DynamicPipelineSpec->SetValue(DynamicSpec);
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(Spec));
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 0u);
        auto context = New<TJobManagerContext>();
        context->Invoker = GetCurrentInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->StatusProfiler = CreateSyncStatusProfiler();
        JobManager = CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
        FlowView->CurrentSpec->SetValue(Spec);

        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);
        FlowView->State->CommitMutation();

        for (int i = 0; i < WorkerCount; ++i) {
            auto worker = New<NFlow::TWorker>();
            worker->RpcAddress = Format("worker-%v.net:81", i + 1);
            worker->MonitoringAddress = Format("worker-%v.net:80", i + 1);
            worker->IncarnationId = TIncarnationId(TGuid::Create());
            FlowView->State->Workers[worker->RpcAddress] = worker;
        }

        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), static_cast<size_t>(totalPartitions));

        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            auto& partitionJobStatus = FlowView->Feedback->PartitionJobStatuses[partitionId];
            partitionJobStatus = New<TPartitionJobStatus>();
            auto& jobStatus = partitionJobStatus->CurrentJobStatus;
            jobStatus = New<TJobStatus>();

            jobStatus->PerformanceMetrics = ConvertTo<TNodePerformanceMetricsPtr>(
                TYsonString(TStringBuf(R""""(
                    {
                        "cpu_usage_10m" = 3.;
                        "cpu_usage_30s" = 2.;
                        "cpu_usage_current" = 1.;
                        "memory_usage_10m" = 3;
                        "memory_usage_30s" = 2;
                        "memory_usage_current" = 1;
                    }
                )"""")));
            if (partition->ComputationId == "Computation_1") {
                jobStatus->PerformanceMetrics->CpuUsage10m = 30;
                jobStatus->PerformanceMetrics->MemoryUsage10m = 20;

                auto metrics = New<TNodeInputMetrics>();
                metrics->Global.MessagesPerSecond = 100;
                metrics->Global.BytesPerSecond = 1000;
                jobStatus->InputMetrics = std::move(metrics);
            }
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

    //! Runs DescribePipeline with .StatusOnly = true and the given controller FlowCoreVersion.
    TPipelineDescription DescribeStatus(std::string controllerFlowCoreVersion = GetBinaryChecksum())
    {
        return DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
            .ControllerFlowCoreVersion = std::move(controllerFlowCoreVersion),
        });
    }

    //! Checks that the YSON-serialized messages contain |text|. Cheap-but-good
    //! enough for substring assertions over a small message list.
    static bool MessagesContain(const std::vector<TMessage>& messages, const std::string& text)
    {
        return ConvertToYsonString(messages).ToString().Contains(text);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TDescribeTest, ValidateSpecs)
{
    Prepare();
    ValidateSpecs(Spec, DynamicSpec, Pipeline.Messages);
    EXPECT_TRUE(Pipeline.Messages.empty())
        << "Messages:" << ConvertToYsonString(Pipeline.Messages).ToString();

    SetNodeByYPath(Spec->Computations["Computation_1"]->Parameters, "/unrecognized_field_1", ConvertTo<INodePtr>(42));
    SetNodeByYPath(DynamicSpec->Computations["Computation_1"]->Parameters, "/unrecognized_field_1", ConvertTo<INodePtr>(42));

    ValidateSpecs(Spec, DynamicSpec, Pipeline.Messages);
    EXPECT_FALSE(Pipeline.Messages.empty());
}

TEST_W(TDescribeTest, GetTopForHighlighting)
{
    EXPECT_THAT(GetTopForHighlighting({{"A", 1}}), ::testing::UnorderedElementsAre());
    EXPECT_THAT(GetTopForHighlighting({{"A", 1}, {"B", 100}}), ::testing::UnorderedElementsAre(TComputationId("B")));
    EXPECT_THAT(GetTopForHighlighting({{"A", 1}, {"B", 1}}), ::testing::UnorderedElementsAre());
    EXPECT_THAT(GetTopForHighlighting({{"A", 100}, {"B", 100}, {"C", 0}, {"D", 0}, {"E", 0}}), ::testing::UnorderedElementsAre(TComputationId("A"), TComputationId("B")));
    EXPECT_THAT(GetTopForHighlighting({{"A", 3}, {"B", 3}, {"C", 1}}), ::testing::UnorderedElementsAre());
}

TEST_W(TDescribeTest, MakeComputationDescriptions)
{
    Prepare();

    Spec->Computations[TComputationId("Computation_1")]->ProcessingFunction = "MyProcessFunction";

    auto makeComputationDescriptions = [&] {
        return MakeComputationDescriptions(FlowView, GetComputationPartitionIntermediateDescriptions(FlowView));
    };

    auto makeExpectedMetrics = [] (double cpuUsage10m, i64 memoryUsage10m) {
        auto metrics = ConvertTo<TNodePerformanceMetricsPtr>(
            TYsonString(TStringBuf(R""""(
                {
                    "cpu_usage_10m" = 9.;
                    "cpu_usage_30s" = 6.;
                    "cpu_usage_current" = 3.;
                    "memory_usage_10m" = 9;
                    "memory_usage_30s" = 6;
                    "memory_usage_current" = 3;
                }
            )"""")));
        metrics->CpuUsage10m = cpuUsage10m;
        metrics->MemoryUsage10m = memoryUsage10m;
        return metrics;
    };

    struct TExpectedComputation
    {
        std::string ClassName;
        std::string ProcessFunction;
        TNodePerformanceMetricsPtr Metrics;
        bool Highlighted = false;
    };

    THashMap<TComputationId, TExpectedComputation> expectedComputations;
    expectedComputations[TComputationId("Computation_1")] = TExpectedComputation{
        .ClassName = "NYT::NFlow::TPassthroughComputation",
        .ProcessFunction = "MyProcessFunction",
        .Metrics = makeExpectedMetrics(/*cpuUsage10m*/ 90, /*memoryUsage10m*/ 60),
        .Highlighted = true,
    };
    expectedComputations[TComputationId("Computation_2")] = TExpectedComputation{
        .ClassName = "NYT::NFlow::TPassthroughComputation",
        .Metrics = makeExpectedMetrics(/*cpuUsage10m*/ 9, /*memoryUsage10m*/ 9),
    };
    expectedComputations[TComputationId("Computation_3")] = TExpectedComputation{
        .ClassName = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation",
        .Metrics = makeExpectedMetrics(/*cpuUsage10m*/ 9, /*memoryUsage10m*/ 9),
    };

    auto computations = makeComputationDescriptions();
    EXPECT_EQ(computations.size(), expectedComputations.size());
    for (const auto& [computationId, expected] : expectedComputations) {
        const auto& computationDescription = GetOrCrash(computations, computationId);

        EXPECT_EQ(computationDescription.PartitionsStats.Count, 3);
        EXPECT_EQ(computationDescription.Status, ELogLevel::Info);
        EXPECT_EQ(computationDescription.ClassName, expected.ClassName);
        EXPECT_EQ(computationDescription.ProcessFunction, expected.ProcessFunction);

        EXPECT_TRUE(*computationDescription.Metrics == *expected.Metrics);
        EXPECT_DOUBLE_EQ(computationDescription.CpuUsage, *expected.Metrics->CpuUsage10m);
        EXPECT_DOUBLE_EQ(computationDescription.MemoryUsage, expected.Metrics->MemoryUsage10m);

        EXPECT_EQ(computationDescription.HighlightCpuUsage, expected.Highlighted);
        EXPECT_EQ(computationDescription.HighlightMemoryUsage, expected.Highlighted);
    }

    {
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus->RetryableErrors["test"] = TError("error");
        }

        computations = makeComputationDescriptions();
        EXPECT_EQ(computations["Computation_1"].Status, ELogLevel::Warning) << ConvertToYsonString(computations["Computation_1"], EYsonFormat::Text).ToString();

        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus->RetryableErrors.clear();
        }

        computations = makeComputationDescriptions();
        EXPECT_EQ(computations["Computation_1"].Status, ELogLevel::Info);
    }

    {
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            auto partitionState = FlowView->EphemeralState->GetPartitionState(partitionId);
            partitionState->PreviousJobFailInstant = TInstant::Seconds(FlowView->State->CurrentTimestamp.Underlying());
            partitionState->PreviousJobFailError = TError("error");
        }

        computations = makeComputationDescriptions();
        EXPECT_EQ(computations["Computation_1"].Status, ELogLevel::Warning) << ConvertToYsonString(computations["Computation_1"], EYsonFormat::Text).ToString();

        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            FlowView->EphemeralState->GetPartitionState(partitionId)->PreviousJobFailError = TError();
        }

        computations = makeComputationDescriptions();
        EXPECT_EQ(computations["Computation_1"].Status, ELogLevel::Info);
    }
}

TEST_W(TDescribeTest, RegisterStreams)
{
    Prepare();
    for (const auto& [computationId, computationSpec] : Spec->Computations)
    {
        auto computationDescription = TPipelineComputationDescription();
        RegisterStreams(Spec, computationId, Pipeline, computationDescription, TDescribeTraitsContext{});
    }
    EXPECT_TRUE(Pipeline.Sources.find("Computation_3/Source") != Pipeline.Sources.end());
    EXPECT_TRUE(Pipeline.Streams.find("input_stream") != Pipeline.Streams.end());
    for (const auto& [computationId, computationSpec] : Spec->Computations) {
        if (computationSpec->TimerStreams.empty()) {
            continue;
        }
        EXPECT_TRUE(Pipeline.Streams.find(TStreamId(Format("%v/timer_stream", computationId))) != Pipeline.Streams.end());
    }
    ASSERT_TRUE(Pipeline.Streams.find("output_stream") != Pipeline.Streams.end());
    ASSERT_GE(Pipeline.Streams.at("output_stream").Messages.size(), 1u);
}

TEST_W(TDescribeTest, FillTopHeavyHittersMessages)
{
    Prepare();
    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto jobDirectory = CreateJobDirectory(converterCache, TLogger("test"));
    jobDirectory->Reconfigure(FlowView->State->ExecutionSpec->Layout, Spec);

    for (auto& [computationId, computationSpec] : Spec->Computations) {
        if (computationId == "Computation_3") {
            continue;
        }
        std::vector<std::tuple<double, TKey, TPartitionId>> topHeavyHitters;

        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            if (partition->ComputationId != computationId) {
                continue;
            }
            FlowView->Feedback->PartitionJobStatuses[partitionId] = New<TPartitionJobStatus>();
            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus = New<TJobStatus>();
        }

        for (int i = 0; i < 1000; ++i) {
            double ratio = RandomNumber<double>();
            TKey key = MakeKey(RandomNumber<ui32>());
            auto route = jobDirectory->FindRouteByKey(computationId, key);
            if (!route) {
                continue;
            }
            auto partitionId = route->PartitionId;
            topHeavyHitters.emplace_back(ratio, key, partitionId);

            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus->InputMetrics->Global.HeavyHitters.emplace_back(ratio, key);
            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus->InputMetrics->Global.MessagesPerSecond = 1000;
        }

        if (topHeavyHitters.empty()) {
            continue;
        }
        int topSize = computationSpec->HeavyHitters.Limit;
        std::partial_sort(topHeavyHitters.begin(), topHeavyHitters.begin() + std::min(topSize, static_cast<int>(topHeavyHitters.size())), topHeavyHitters.end(), std::greater{});
        if (static_cast<int>(topHeavyHitters.size()) > topSize) {
            topHeavyHitters.resize(topSize);
        }
        std::vector<std::string> topHeavyHittersStrings;
        for (auto& tuple : topHeavyHitters) {
            topHeavyHittersStrings.emplace_back(Format("Key=%v, Ratio=%v, PartitionId=%v", std::get<1>(tuple), std::get<0>(tuple), std::get<2>(tuple)));
        }
        TYsonString messageYson = ConvertToYsonString(topHeavyHittersStrings, NYson::EYsonFormat::Text);
        FillTopHeavyHittersMessages(FlowView, GetComputationPartitionIntermediateDescriptions(FlowView), computationId, Pipeline.Computations[computationId]);
        ASSERT_TRUE(Pipeline.Computations[computationId].Messages.size() > 0u);
        EXPECT_TRUE(messageYson == *Pipeline.Computations[computationId].Messages.back().Yson);
    }
}

TEST_W(TDescribeTest, FillStreamTraverseData)
{
    Prepare();
    for (auto& [computationId, computationSpec] : Spec->Computations) {
        FlowView->State->TraverseData->Computations[computationId] = ConvertTo<TNodeTraverseDataPtr>(
            TYsonString(TStringBuf(R""""(
                {
                    "streams" = {
                        "input_stream" = {
                            "inflight_metrics" = {
                                "processed_bytes_per_sec" = 100.;
                                "byte_size" = 100;
                                "count" = 10;
                            }
                        };
                        "output_stream" = {
                            "inflight_metrics" = {
                                "processed_bytes_per_sec" = 200.;
                                "byte_size" = 200;
                                "count" = 20;
                            }
                        }
                    }
                }
            )"""")));
    }
    FillStreamTraverseData(FlowView, Spec, Pipeline);
    EXPECT_TRUE(Pipeline.Streams.find("input_stream") == Pipeline.Streams.end());
    EXPECT_TRUE(Pipeline.Streams.find("Computation_3/input_stream") != Pipeline.Streams.end());
    EXPECT_TRUE(Pipeline.Streams.find("output_stream") != Pipeline.Streams.end());
    EXPECT_TRUE(Pipeline.Streams["output_stream"].BytesPerSecond == 200);
    EXPECT_TRUE(Pipeline.Streams["output_stream"].InflightBytes == 200);
    EXPECT_TRUE(Pipeline.Streams["output_stream"].InflightRows == 20);
}

TEST_W(TDescribeTest, FillComputationPerformanceMetrics)
{
    TExtendedComputationDescription description;
    auto& partitions = description.Partitions;
    for (int i = 1; i <= 3; ++i) {
        auto& partition = partitions.emplace_back();
        partition.PartitionId = TPartitionId(TGuid::Create());
        partition.CpuUsage = i;
        partition.MemoryUsage = 1000 * (4 - i);
        partition.BytesPerSecond = i * 100;
        partition.MessagesPerSecond = i * 10;
    }
    FillComputationPerformanceMetrics(description);
    const auto& performanceMetrics = description.PerformanceMetrics;

    EXPECT_EQ(performanceMetrics.Min.CpuUsage, 1);
    EXPECT_EQ(performanceMetrics.Min.CpuUsageExamplePartition, partitions[0].PartitionId);
    EXPECT_EQ(performanceMetrics.Average.CpuUsage, 2);
    EXPECT_EQ(performanceMetrics.Average.CpuUsageExamplePartition, partitions[1].PartitionId);
    EXPECT_EQ(performanceMetrics.Max.CpuUsage, 3);
    EXPECT_EQ(performanceMetrics.Max.CpuUsageExamplePartition, partitions[2].PartitionId);
    EXPECT_EQ(performanceMetrics.Total.CpuUsage, 6);

    EXPECT_EQ(performanceMetrics.Average.MemoryUsage, 2000);
    EXPECT_EQ(performanceMetrics.Average.MemoryUsageExamplePartition, partitions[1].PartitionId);

    EXPECT_EQ(performanceMetrics.Min.MessagesPerSecond, 10);
    EXPECT_EQ(performanceMetrics.Min.MessagesPerSecondExamplePartition, partitions[0].PartitionId);

    EXPECT_EQ(performanceMetrics.Min.BytesPerSecond, 100);
    EXPECT_EQ(performanceMetrics.Min.BytesPerSecondExamplePartition, partitions[0].PartitionId);
}

TEST_W(TDescribeTest, FillComputationPartitionErrorMetrics)
{
    auto now = TInstant::Now();
    TExtendedComputationDescription description;
    auto& partitions = description.Partitions;
    for (int i = 1; i <= 3; ++i) {
        auto& partition = partitions.emplace_back();
        partition.PartitionId = TPartitionId(TGuid::Create());
        partition.LastRetryableErrorInstant = now - (TDuration::Minutes(1) - TDuration::Seconds(1)) * i;
        partition.PreviousJobFailInstant = now - (TDuration::Minutes(5) - TDuration::Seconds(1)) * i;
        partition.PreviousRebalancingInstant = now - (TDuration::Minutes(30) - TDuration::Seconds(1)) * i;
    }
    FillComputationPartitionErrorMetrics(description, now);
    const auto& byTime1m = description.PartitionWithErrorByTimeAndType.at("1m");

    EXPECT_EQ(byTime1m.HasRetryableError.Count, 1);
    EXPECT_EQ(byTime1m.HasRetryableError.ExamplePartition, partitions[0].PartitionId);

    EXPECT_EQ(byTime1m.RestartBecauseFail.Count, 0);
    EXPECT_FALSE(byTime1m.RestartBecauseFail.ExamplePartition);

    EXPECT_EQ(byTime1m.RestartBecauseRebalancing.Count, 0);
    EXPECT_FALSE(byTime1m.RestartBecauseRebalancing.ExamplePartition);

    EXPECT_EQ(byTime1m.TotalWithProblems.Count, 1);
    EXPECT_EQ(byTime1m.TotalWithProblems.ExamplePartition, partitions[0].PartitionId);

    const auto& byTime5m = description.PartitionWithErrorByTimeAndType.at("5m");

    EXPECT_EQ(byTime5m.HasRetryableError.Count, 3);

    EXPECT_EQ(byTime5m.RestartBecauseFail.Count, 1);
    EXPECT_EQ(byTime5m.RestartBecauseFail.ExamplePartition, partitions[0].PartitionId);

    EXPECT_EQ(byTime5m.RestartBecauseRebalancing.Count, 0);

    EXPECT_EQ(byTime5m.TotalWithProblems.Count, 3);

    const auto& byTime30m = description.PartitionWithErrorByTimeAndType.at("30m");

    EXPECT_EQ(byTime30m.HasRetryableError.Count, 3);

    EXPECT_EQ(byTime30m.RestartBecauseFail.Count, 3);

    EXPECT_EQ(byTime30m.RestartBecauseRebalancing.Count, 1);
    EXPECT_EQ(byTime30m.RestartBecauseRebalancing.ExamplePartition, partitions[0].PartitionId);

    EXPECT_EQ(byTime30m.TotalWithProblems.Count, 3);
}

TEST_W(TDescribeTest, DescribePipeline)
{
    Prepare();
    auto statusDescription = DescribePipeline({.FlowView = FlowView, .Logger = TLogger("test"), .StatusOnly = true});
    ui64 messagesSize = statusDescription.Messages.size();
    EXPECT_GE(messagesSize, 1u);

    // Helper function to check if any message contains the expected text.
    auto containsText = [] (const std::vector<TMessage>& messages, const TString& text) {
        // Serialize the entire messages vector to YSON and search in it.
        auto messagesStr = ConvertToYsonString(messages).ToString();
        return messagesStr.contains(text);
    };

    // Add authenticator.
    auto mockAuthenticator = New<StrictMock<TMockPipelineAuthenticator>>();
    EXPECT_CALL(*mockAuthenticator, GetAuthDescription())
        .WillRepeatedly(Return("FakeAuthDescription description 1"));
    statusDescription = DescribePipeline({.FlowView = FlowView, .Logger = TLogger("test"), .Authenticator = mockAuthenticator, .StatusOnly = true});
    EXPECT_TRUE(containsText(statusDescription.Messages, "FakeAuthDescription"))
        << "Messages:" << ConvertToYsonString(statusDescription.Messages).ToString();
    messagesSize = statusDescription.Messages.size();

    // Add unknown field to parameters.
    DynamicSpec->Computations["Computation_1"]->Parameters->AddChild("not_existing_field_42", GetEphemeralNodeFactory()->CreateMap());
    statusDescription = DescribePipeline({.FlowView = FlowView, .Logger = TLogger("test"), .Authenticator = mockAuthenticator, .StatusOnly = true});
    EXPECT_TRUE(containsText(statusDescription.Messages, "not_existing_field_42"))
        << "Messages:" << ConvertToYsonString(statusDescription.Messages).ToString();

    auto description = DescribePipeline({.FlowView = FlowView, .Logger = TLogger("test"), .Authenticator = mockAuthenticator});
    EXPECT_EQ(description.Status, statusDescription.Status);
    EXPECT_EQ(description.Messages.size(), statusDescription.Messages.size());
    EXPECT_EQ(description.Computations.size(), 3u);
}

TEST_W(TDescribeTest, DescribePipelineExposesControllerBuildType)
{
    Prepare();

    auto findCommonInfo = [] (const std::vector<TMessage>& messages) {
        return std::find_if(messages.begin(), messages.end(), [] (const TMessage& message) {
            return message.Text == "Pipeline common information and useful commands";
        });
    };
    auto findSlowBuildWarning = [] (const std::vector<TMessage>& messages) {
        return std::find_if(messages.begin(), messages.end(), [] (const TMessage& message) {
            return message.Level == ELogLevel::Warning &&
                message.Text.find("slow build") != std::string::npos;
        });
    };

    // Build type is carried in via TDescribePipelineArguments (sourced from the
    // leader-controller's node_info). A slow value drives the warning; a non-slow value
    // shows just the info line; an empty value omits both (old binaries / old views).
    {
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
            .ControllerBuildType = "ASAN",
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end())
            << "Common-info message missing; Messages:" << ConvertToYsonString(description.Messages).ToString();
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, HasSubstr("Controller build type: `ASAN`"));

        auto slowBuildIt = findSlowBuildWarning(description.Messages);
        ASSERT_NE(slowBuildIt, description.Messages.end())
            << "Expected slow-build warning, none found; Messages:" << ConvertToYsonString(description.Messages).ToString();
        ASSERT_TRUE(slowBuildIt->MarkdownText.has_value());
        EXPECT_THAT(*slowBuildIt->MarkdownText, HasSubstr("ASAN"));
        EXPECT_THAT(*slowBuildIt->MarkdownText, HasSubstr("ya make -r"));
    }

    {
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
            .ControllerBuildType = "release",
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end());
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, HasSubstr("Controller build type: `release`"));

        EXPECT_EQ(findSlowBuildWarning(description.Messages), description.Messages.end())
            << "Unexpected slow-build warning for a release build; Messages:" << ConvertToYsonString(description.Messages).ToString();
    }

    {
        // Empty BuildType (old binary / pre-field view): no line, no warning.
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end());
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, Not(HasSubstr("Controller build type:")));

        EXPECT_EQ(findSlowBuildWarning(description.Messages), description.Messages.end())
            << "Unexpected slow-build warning with empty BuildType; Messages:" << ConvertToYsonString(description.Messages).ToString();
    }
}

TEST_W(TDescribeTest, DescribePipelineExposesFlowTablesBundle)
{
    Prepare();

    // The bundle link uses the cluster from the ephemeral pipeline path.
    FlowView->EphemeralState->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");

    auto findCommonInfo = [] (const std::vector<TMessage>& messages) {
        return std::find_if(messages.begin(), messages.end(), [] (const TMessage& message) {
            return message.Text == "Pipeline common information and useful commands";
        });
    };

    // The bundle name is carried in via TDescribePipelineArguments (resolved via the YT
    // connector) and rendered as a clickable markdown link. A set clock_cluster_tag is shown
    // next to the link; an empty bundle omits the line (old binaries / unresolved bundle).
    {
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
            .FlowTablesBundle = {.Bundle = "my_project"},
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end());
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, HasSubstr(
            "Tablet cell bundle: [my_project]"
            "(/pipeline_cluster/tablet_cell_bundles/tablet_cells?activeBundle=my_project)"));
        EXPECT_THAT(*commonInfoIt->MarkdownText, Not(HasSubstr("clock_cluster_tag")));
    }

    {
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
            .FlowTablesBundle = {
                .Bundle = "my_project",
                .ClockClusterTag = NObjectClient::TCellTag(42),
            },
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end());
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, HasSubstr(
            "Tablet cell bundle: [my_project]"
            "(/pipeline_cluster/tablet_cell_bundles/tablet_cells?activeBundle=my_project)"
            " (clock_cluster_tag: 42)"));
    }

    {
        // Empty bundle (old binary / pre-field view): no line.
        auto description = DescribePipeline({
            .FlowView = FlowView,
            .Logger = TLogger("test"),
            .StatusOnly = true,
        });

        auto commonInfoIt = findCommonInfo(description.Messages);
        ASSERT_NE(commonInfoIt, description.Messages.end());
        ASSERT_TRUE(commonInfoIt->MarkdownText.has_value());
        EXPECT_THAT(*commonInfoIt->MarkdownText, Not(HasSubstr("Tablet cell bundle:")));
    }
}

TEST_W(TDescribeTest, DescribePipelineNoFlowView)
{
    Prepare();
    auto statusDescription = DescribePipeline({.FlowView = nullptr, .Logger = TLogger("test"), .StatusOnly = true});
    ui64 messagesSize = statusDescription.Messages.size();
    EXPECT_EQ(messagesSize, 1u)
        << "Messages:" << ConvertToYsonString(statusDescription.Messages).ToString();

    statusDescription = DescribePipeline({
        .FlowView = nullptr,
        .ControllerErrors = {
            {"a", TError("b")},
        },
        .Logger = TLogger("test"),
        .StatusOnly = true,
    });
    EXPECT_EQ(statusDescription.Messages.size(), messagesSize + 1u)
        << "Messages:" << ConvertToYsonString(statusDescription.Messages).ToString();

    EXPECT_THROW(DescribePipeline({.FlowView = nullptr, .Logger = TLogger("test")}), TErrorException);
}

TEST_W(TDescribeTest, DescribeComputations)
{
    Prepare();
    auto description = DescribeComputations(FlowView);
    EXPECT_EQ(description.Computations.size(), Spec->Computations.size());
}

TEST_W(TDescribeTest, DescribeComputation)
{
    Prepare();
    auto description = DescribeComputation(FlowView, "Computation_1");
    EXPECT_EQ(description.Name, "Computation_1");
    ASSERT_EQ(description.Partitions.size(), 3u);
    EXPECT_EQ(description.Partitions[0].ComputationId, "Computation_1");
    EXPECT_EQ(description.CpuUsage, 90);
}

TEST_W(TDescribeTest, DescribePartition)
{
    Prepare();
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        if (partition->ComputationId == "Computation_1") {
            auto description = DescribePartition(FlowView, partitionId, TError("no orchid in test"));
            EXPECT_EQ(description.ComputationId, "Computation_1");
            EXPECT_GE(description.Messages.size(), 2u);
            EXPECT_EQ(description.CpuUsage, 30);
            EXPECT_GE(description.MessagesPerSecond, 1);
            EXPECT_GE(description.BytesPerSecond, 1);
            EXPECT_TRUE(description.CurrentWorkerAddress.has_value());
            return;
        }
    }
    ASSERT_TRUE(false) << "Computation_1 not found";
}

TEST_W(TDescribeTest, DescribeWorker)
{
    Prepare();
    auto workersDescription = DescribeWorkers(FlowView);
    ASSERT_EQ(workersDescription.Workers.size(), 1u);
    auto description = DescribeWorker(FlowView, workersDescription.Workers[0].Address);
    EXPECT_EQ(description.Address, "worker-1.net:81");
    ASSERT_EQ(description.Partitions.size(), 9u);
    EXPECT_GE(description.Messages.size(), 1u);
    EXPECT_GE(description.CpuUsage, 1.0);
}

TEST_W(TDescribeTest, MakeLinks)
{
    // clang-format off
    auto testNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("empty_field").Value("")
            .Item("path_field").Value("//home/test")
            .Item("non_path_field").Value("some_value")
            .Item("cluster_path").Value("<cluster=test_cluster>//home/test")
            .Item("nested").BeginMap()
                .Item("path").Value("//tmp/data")
            .EndMap()
        .EndMap()->AsMap();
    // clang-format on

    auto traits = New<TDescribeTraitsBase>(
        TDescribeTraitsContext{.PipelinePath = NYPath::TRichYPath::Parse("<cluster=default_cluster>//pipeline")});
    traits->MakeLinks(testNode);

    EXPECT_EQ(ConvertToYsonString(testNode->FindChild("empty_field"), EYsonFormat::Text).ToString(), "\"\"");

    EXPECT_EQ(ConvertToYsonString(testNode->FindChild("non_path_field"), EYsonFormat::Text).ToString(), "\"some_value\"");

    // Path without cluster falls back to the pipeline's cluster.
    auto pathField = ConvertToYsonString(testNode->FindChild("path_field"), EYsonFormat::Text).ToString();
    EXPECT_TRUE(pathField.Contains("/default_cluster/navigation?path=//home/test")) << pathField;

    // Path with its own cluster keeps it.
    auto clusterPath = ConvertToYsonString(testNode->FindChild("cluster_path"), EYsonFormat::Text).ToString();
    EXPECT_TRUE(clusterPath.Contains("/test_cluster/navigation?path=//home/test")) << clusterPath;

    // Nested path.
    auto nestedPath = ConvertToYsonString(testNode->FindChild("nested")->AsMap()->FindChild("path"), EYsonFormat::Text).ToString();
    EXPECT_TRUE(nestedPath.Contains("/default_cluster/navigation?path=//tmp/data")) << nestedPath;
}

TEST(TMakeLinksTest, SinkAndSourceLinksApplyDefaultTraits)
{
    TDescribeTraitsContext context{.PipelinePath = NYPath::TRichYPath::Parse("<cluster=default_cluster>//pipeline")};

    // clang-format off
    auto parameters = BuildYsonNodeFluently()
        .BeginMap()
            .Item("path").Value("//home/test")
        .EndMap();
    // clang-format on

    // TTestSink and TRandomSource have no custom traits, so the helpers fall back to the default
    // TDescribeTraitsBase: the YT "path" parameter is turned into a clickable link node.
    {
        auto sink = MakeSinkLinks(parameters, TypeName<TTestSink>(), context)->AsMap();
        auto path = ConvertToYsonString(sink->FindChild("path"), EYsonFormat::Text).ToString();
        EXPECT_TRUE(path.Contains("/default_cluster/navigation?path=//home/test")) << path;
    }

    {
        auto source = MakeSourceLinks(parameters, TypeName<TRandomSource>(), context)->AsMap();
        auto path = ConvertToYsonString(source->FindChild("path"), EYsonFormat::Text).ToString();
        EXPECT_TRUE(path.Contains("/default_cluster/navigation?path=//home/test")) << path;
    }
}

TEST(TRegistryDescribeTraitsTest, EveryEntityHasDefaultTraits)
{
    TDescribeTraitsContext context;
    // Default traits come from the base interfaces, so every registered entity has them.
    EXPECT_TRUE(TRegistry::Get()->CreateSourceDescribeTraits(TypeName<TRandomSource>(), context));
    EXPECT_TRUE(TRegistry::Get()->CreateSinkDescribeTraits(TypeName<TTestSink>(), context));
    EXPECT_TRUE(TRegistry::Get()->CreateExternalStateManagerDescribeTraits(TypeName<TSimpleExternalStateManager>(), context));
    // An unknown class has no traits.
    EXPECT_FALSE(TRegistry::Get()->CreateSinkDescribeTraits("NYT::NFlow::TNoSuchSink", context));
    EXPECT_FALSE(TRegistry::Get()->CreateExternalStateManagerDescribeTraits("NYT::NFlow::TNoSuchStateManager", context));
}

TEST(TRegistryDescribeTraitsTest, StateManagerDefaultTraitsHighlightYTPaths)
{
    auto traits = TRegistry::Get()->CreateExternalStateManagerDescribeTraits(
        TypeName<TSimpleExternalStateManager>(),
        TDescribeTraitsContext{.PipelinePath = NYPath::TRichYPath::Parse("<cluster=test_cluster>//pipeline")});
    ASSERT_TRUE(traits);

    // clang-format off
    auto parameters = BuildYsonNodeFluently()
        .BeginMap()
            .Item("path").Value("//home/state")
        .EndMap()->AsMap();
    // clang-format on
    traits->MakeLinks(parameters);

    auto path = ConvertToYsonString(parameters->FindChild("path"), EYsonFormat::Text).ToString();
    EXPECT_TRUE(path.Contains("/test_cluster/navigation?path=//home/state")) << path;
}

TEST(TRegistryDescribeTraitsTest, DefaultTraitsHighlightYTPaths)
{
    auto traits = TRegistry::Get()->CreateSinkDescribeTraits(
        TypeName<TTestSink>(),
        TDescribeTraitsContext{.PipelinePath = NYPath::TRichYPath::Parse("<cluster=test_cluster>//pipeline")});
    ASSERT_TRUE(traits);

    // clang-format off
    auto parameters = BuildYsonNodeFluently()
        .BeginMap()
            .Item("path").Value("//home/test")
        .EndMap()->AsMap();
    // clang-format on
    traits->MakeLinks(parameters);

    auto path = ConvertToYsonString(parameters->FindChild("path"), EYsonFormat::Text).ToString();
    EXPECT_TRUE(path.Contains("/test_cluster/navigation?path=//home/test")) << path;
}

// Expected execution time is about 10s.
TEST_W(TDescribeTest, AdequatePerformance)
{
    ComputationPrepareSpecs = {};
    for (int i = 0; i < 10; ++i) {
        ComputationPrepareSpecs.push_back(TComputationPrepareSpec{.PartitionCount = 10000, .WithSource = (i % 2 == 0)});
    }
    WorkerCount = 1000;

    Prepare();

    // Run all major describes.

    auto run = [&] (const std::string& component, auto f) {
        auto now = TInstant::Now();
        auto value = f();
        ::testing::Test::RecordProperty(component + "_execution_time", ToString((TInstant::Now() - now).SecondsFloat()));
        return value;
    };

    run("describe_pipeline", [&] {
        return DescribePipeline({.FlowView = FlowView, .Logger = TLogger("test"), .StatusOnly = false});
    });

    auto computationsDescription = run("describe_computations", [&] {
        return DescribeComputations(FlowView);
    });
    ASSERT_GE(computationsDescription.Computations.size(), 1u);
    auto firstComputationDescription = run("describe_computation", [&] {
        return DescribeComputation(FlowView, TComputationId(computationsDescription.Computations[0].Name));
    });

    auto workersDescription = run("describe_workers", [&] {
        return DescribeWorkers(FlowView);
    });
    ASSERT_GE(workersDescription.Workers.size(), 1u);
    run("describe_worker", [&] {
        return DescribeWorker(FlowView, workersDescription.Workers[0].Address);
    });

    ASSERT_GE(firstComputationDescription.Partitions.size(), 1u);
    run("describe_partition", [&] {
        return DescribePartition(FlowView, firstComputationDescription.Partitions[0].PartitionId, TError("No orchid in test"));
    });
}

TEST_W(TDescribeTest, FillPerformanceMessage)
{
    TExtendedComputationDescription description;
    auto& partitions = description.Partitions;

    for (int i = 0; i < 3; ++i) {
        auto& partition = partitions.emplace_back();
        partition.PartitionId = TPartitionId(TGuid::FromString(Format("%v-0-0-0", i + 1)));
        partition.CpuUsage = (i + 1) * 10.0;
        partition.MemoryUsage = (i + 1) * 1000;
        partition.BytesPerSecond = (i + 1) * 100;
        partition.MessagesPerSecond = (i + 1) * 10;
    }

    // Create intermediate descriptions with epoch part times and limits.
    std::vector<TPartitionIntermediateDescription> intermediateDescriptions;
    for (int i = 0; i < 3; ++i) {
        auto& desc = intermediateDescriptions.emplace_back();
        desc.Partition = New<TPartition>();
        desc.Partition->PartitionId = partitions[i].PartitionId;
        desc.PartitionJobStatus = New<TPartitionJobStatus>();
        desc.PartitionJobStatus->CurrentJobStatus = New<TJobStatus>();

        // Add epoch part times.
        desc.PartitionJobStatus->CurrentJobStatus->EpochPartTimes = {
            {"component_a", 0.3 + i * 0.1},
            {"component_b", 0.2 + i * 0.05},
            {"component_c", 0.1 + i * 0.02},
        };

        // Add input limits.
        {
            auto& limits = desc.PartitionJobStatus->CurrentJobStatus->InputLimits["buffer"];
            NFlow::TJobEntityLimitStatus limit1;
            limit1.Limit = 100 + i * 50;
            limit1.Used = 10 + i * 5;
            limit1.Pending = 200;
            limits[TStreamId("stream1")] = limit1;

            NFlow::TJobEntityLimitStatus limit2;
            limit2.Limit = 150 + i * 75;
            limit2.Used = 15 + i * 8;
            limit2.Pending = 300;
            limits[TStreamId("stream2")] = limit2;
        }

        // Add output limits.
        {
            auto& limits = desc.PartitionJobStatus->CurrentJobStatus->OutputLimits["buffer"];
            NFlow::TJobEntityLimitStatus limit1;
            limit1.Limit = 80 + i * 40;
            limit1.Used = 8 + i * 4;
            limit1.Pending = 160;
            limits[TStreamId("stream1")] = limit1;

            NFlow::TJobEntityLimitStatus limit2;
            limit2.Limit = 120 + i * 60;
            limit2.Used = 12 + i * 6;
            limit2.Pending = 240;
            limits[TStreamId("stream2")] = limit2;
        }
    }

    FillPerformanceMessage(description, intermediateDescriptions);

    // Check that a message was added.
    ASSERT_EQ(description.Messages.size(), 1u);

    const auto& message = description.Messages.back();
    ASSERT_TRUE(message.MarkdownText.has_value());
    EXPECT_FALSE(message.MarkdownText->empty());
    // Canonize the markdown output.
#ifdef YT_INTERNAL_FLOW
    // SRC_ resolves golden files via the Arcadia source root, which is not available in opensource test runs.
    EXPECT_THAT(*message.MarkdownText, NGTest::GoldenFileEq(SRC_("canondata/fill_performance_message.md")));
#endif
}

TEST_W(TDescribeTest, FillStreamStateMessage)
{
    Prepare();

    // Add traverse data with different stream states.
    for (auto& [computationId, computationSpec] : Spec->Computations) {
        FlowView->State->TraverseData->Computations[computationId] = ConvertTo<TNodeTraverseDataPtr>(TYsonStringBuf(TStringBuf(R""""({
                "streams" = {
                    "output_stream" = {
                        "state" = "Active";
                        "system_watermark" = 100;
                        "event_watermark" = 100;
                        "inflight_metrics" = {
                            "count" = 10;
                            "byte_size" = 100;
                        }
                    };
                    "timer_stream" = {
                        "state" = "Completed";
                        "system_watermark" = 200;
                        "event_watermark" = 200;
                        "inflight_metrics" = {
                            "count" = 0;
                            "byte_size" = 0;
                        }
                    }
                }
            })"""")));
    }

    // Add traverse data for source streams.
    FlowView->State->TraverseData->Computations["Computation_3"] = ConvertTo<TNodeTraverseDataPtr>(TYsonStringBuf(TStringBuf(R""""({
            "streams" = {
                "Source" = {
                    "state" = "Drained";
                    "system_watermark" = 150;
                    "event_watermark" = 150;
                    "inflight_metrics" = {
                        "count" = 0;
                        "byte_size" = 0;
                    }
                };
                "output_stream" = {
                    "state" = "Active";
                    "system_watermark" = 100;
                    "event_watermark" = 100;
                    "inflight_metrics" = {
                        "count" = 10;
                        "byte_size" = 100;
                    }
                }
            }
        })"""")));

    // Add job status with traverse data for example partitions.
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        if (partition->ComputationId == "Computation_1") {
            FlowView->Feedback->PartitionJobStatuses[partitionId]->CurrentJobStatus->FromPartitionTraverseData =
                ConvertTo<TFromPartitionTraverseDataPtr>(TYsonStringBuf(TStringBuf(R""""({
                        "node" = {
                            "streams" = {
                                "output_stream" = {
                                    "state" = "Active";
                                    "system_watermark" = 100;
                                    "event_watermark" = 100;
                                    "inflight_metrics" = {
                                        "count" = 10;
                                        "byte_size" = 100;
                                    }
                                }
                            }
                        }
                    })"""")));
        }
    }

    std::vector<TMessage> messages;
    auto intermediateDescriptions = GetComputationPartitionIntermediateDescriptions(FlowView);
    FillStreamStateMessage(FlowView, intermediateDescriptions, messages);

    ASSERT_EQ(messages.size(), 1u);

    const auto& message = messages.back();
    ASSERT_TRUE(message.MarkdownText.has_value());
    EXPECT_FALSE(message.MarkdownText->empty());

    EXPECT_TRUE(message.Text.find("Stream state info") != std::string::npos);

    EXPECT_TRUE(message.MarkdownText->find("Active") != std::string::npos);
    EXPECT_TRUE(message.MarkdownText->find("Completed") != std::string::npos);
    EXPECT_TRUE(message.MarkdownText->find("Drained") != std::string::npos);

    auto markdownText = *message.MarkdownText;
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        SubstGlobal(markdownText, ToString(partitionId), Format("<hide %v partition>", partition->ComputationId));
    }

#ifdef YT_INTERNAL_FLOW
    // SRC_ resolves golden files via the Arcadia source root, which is not available in opensource test runs.
    EXPECT_THAT(markdownText, NGTest::GoldenFileEq(SRC_("canondata/fill_stream_state_message.md")));
#endif
}

TEST(TGraphEntityIdTest, Prefixes)
{
    EXPECT_EQ(MakeStreamGraphId(TStreamId("q")).Underlying(), "stm-q");
    EXPECT_EQ(MakeSourceGraphId(TStreamId("q")).Underlying(), "src-q");
    EXPECT_EQ(MakeSinkGraphId(TSinkId("s")).Underlying(), "snk-s");
    EXPECT_EQ(MakeComputationGraphId(TComputationId("c")).Underlying(), "cmp-c");
}

TEST(TUnrollPipelineDescriptionTest, SplitsPerStreamsDependencyPair)
{
    auto in1 = MakeStreamGraphId(TStreamId("a"));
    auto in2 = MakeStreamGraphId(TStreamId("b"));
    auto out = MakeStreamGraphId(TStreamId("c"));

    TPipelineDescription original;
    auto& computation = original.Computations[TComputationId("comp")];
    computation.Id = MakeComputationGraphId(TComputationId("comp"));
    computation.InputStreams = {in1, in2};
    computation.OutputStreams = {out};
    computation.StreamsDependency[out] = {in1, in2};

    auto unrolled = UnrollPipelineDescription(original);

    // Two sub-computations, one per (out, in) pair.
    ASSERT_EQ(unrolled.Computations.size(), 2u);
    ASSERT_TRUE(unrolled.Computations.contains(TComputationId("comp_unrolled_1")));
    ASSERT_TRUE(unrolled.Computations.contains(TComputationId("comp_unrolled_2")));

    const auto& sub1 = unrolled.Computations.at(TComputationId("comp_unrolled_1"));
    EXPECT_EQ(sub1.Id.Underlying(), "cmp-comp-unrolled-1");
    EXPECT_EQ(sub1.InputStreams.size(), 1u);
    EXPECT_EQ(sub1.OutputStreams.size(), 1u);
    EXPECT_TRUE(sub1.StreamsDependency.empty());
}

TEST_F(TDescribeTest, FillExtendedStreams)
{
    Prepare();

    auto loaded = MakeStreamGraphId(TStreamId("loaded"));
    auto calm = MakeStreamGraphId(TStreamId("calm"));

    TPipelineComputationDescription comp;
    comp.InputStreams.insert(loaded);
    comp.OutputStreams.insert(calm);
    comp.InputLimitStats[loaded]["buffer"].Max.Used = 90; // fill 0.9 -> backpressure -> warning.
    comp.InputLimitStats[loaded]["buffer"].Max.Limit = 100;
    comp.OutputLimitStats[calm]["buffer"].Max.Used = 10; // fill 0.1 -> info.
    comp.OutputLimitStats[calm]["buffer"].Max.Limit = 100;
    Pipeline.Computations[TComputationId("Computation_1")] = comp;

    FillExtendedStreams(FlowView, Pipeline);
    const auto& filled = Pipeline.Computations[TComputationId("Computation_1")];

    ASSERT_EQ(filled.ExtendedInputStreams.size(), 1u);
    const auto& loadedMsg = filled.ExtendedInputStreams[0].Messages[0];
    EXPECT_TRUE(filled.ExtendedInputStreams[0].BackpressureDetected);
    EXPECT_EQ(loadedMsg.Level, ELogLevel::Warning);
    EXPECT_THAT(loadedMsg.MarkdownText.value(), AllOf(HasSubstr("fill ratio"), HasSubstr("**"))); // exceedance bolded

    ASSERT_EQ(filled.ExtendedOutputStreams.size(), 1u);
    const auto& calmMsg = filled.ExtendedOutputStreams[0].Messages[0];
    EXPECT_FALSE(filled.ExtendedOutputStreams[0].BackpressureDetected);
    EXPECT_EQ(calmMsg.Level, ELogLevel::Info);
    EXPECT_THAT(calmMsg.MarkdownText.value(), AllOf(HasSubstr("fill ratio"), Not(HasSubstr("**")))); // Below threshold, not bold.

    // Per-edge messages are folded into one warning-level computation message.
    ASSERT_FALSE(filled.Messages.empty());
    EXPECT_EQ(filled.Messages.back().Level, ELogLevel::Warning);
}

TEST_F(TDescribeTest, FillStreamStatisticsMessages)
{
    Prepare();

    // Register streams into Pipeline so FillStreamStatisticsMessages has something to iterate over.
    for (const auto& [computationId, computationSpec] : Spec->Computations) {
        auto computationDescription = TPipelineComputationDescription();
        RegisterStreams(Spec, computationId, Pipeline, computationDescription, TDescribeTraitsContext{});
    }

    // Set speed statistics for "output_stream".
    {
        auto& stats = FlowView->State->SpeedStatistics.StreamSpeed1d[TStreamId("output_stream")];
        stats.ProcessedMessagesPerSecond = 42.5;
        stats.ProcessedBytesPerSecond = 12345.6;
    }

    // Set timestamp statistics for "output_stream".
    {
        auto& tsStats = FlowView->EphemeralState->MessageTransferingInfo->StreamTimestampStatistics[TStreamId("output_stream")];
        tsStats.AlignmentToEventTimestampBiasSum = 300;
        tsStats.MessageCount = 100;
    }

    FillStreamStatisticsMessages(FlowView, Pipeline);

    // "output_stream" should have a statistics message.
    ASSERT_TRUE(Pipeline.Streams.contains("output_stream"));
    const auto& streamMessages = Pipeline.Streams.at("output_stream").Messages;
    ASSERT_GE(streamMessages.size(), 1u);

    // Find the statistics message.
    const TMessage* statsMessage = nullptr;
    for (const auto& msg : streamMessages) {
        if (msg.Text == "Stream statistics") {
            statsMessage = &msg;
            break;
        }
    }
    ASSERT_NE(statsMessage, nullptr) << "No 'Stream statistics' message found";
    ASSERT_TRUE(statsMessage->MarkdownText.has_value());

    const auto& md = *statsMessage->MarkdownText;
    EXPECT_TRUE(md.find("messages/s") != std::string::npos) << md;
    EXPECT_TRUE(md.find("bytes/s") != std::string::npos) << md;
    EXPECT_TRUE(md.find("42.50") != std::string::npos) << md;
    EXPECT_TRUE(md.find("12345.60") != std::string::npos) << md;
    EXPECT_TRUE(md.find("Alignment bias") != std::string::npos) << md;
    EXPECT_TRUE(md.find("Message count") != std::string::npos) << md;
    EXPECT_TRUE(md.find("100") != std::string::npos) << md;

    // Streams without data should not have a statistics message.
    for (const auto& [streamIdStr, streamDescription] : Pipeline.Streams) {
        if (streamIdStr == "output_stream") {
            continue;
        }
        for (const auto& msg : streamDescription.Messages) {
            EXPECT_NE(msg.Text, "Stream statistics")
                << "Unexpected statistics message on stream " << streamIdStr.Underlying();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// Tests for BuildDeterministicTopologicalIndex.
// The function uses Kahn's algorithm with a sorted set, so independent nodes are always emitted in lexicographic order.

TEST(TBuildDeterministicTopologicalIndexTest, TopologicalOrderIsRespected)
{
    // Graph: a -> b -> d, a -> c -> d.
    // b and c are independent; b < c lexicographically, so b comes first.
    // Isolated vertex "z" is added as a key with no edges.
    // Since "b" < "c" < "z", order is: a=0, b=1, c=2, d=3, z=4.
    // (When "a" is processed, b and c become ready; z was ready from the start.
    //  ready = {b, c, z} -> pick b=1, then ready={c, z} -> pick c=2,
    //  then ready={d, z} -> pick d=3, then ready={z} -> pick z=4.)
    std::map<std::string, std::vector<std::string>> graph = {
        {"a", {"b", "c"}},
        {"b", {"d"}},
        {"c", {"d"}},
        {"d", {}},
        {"z", {}},
    };
    auto index = BuildDeterministicTopologicalIndex(graph);

    ASSERT_EQ(index.size(), 5u);

    // Topological constraints.
    EXPECT_LT(index.at("a"), index.at("b"));
    EXPECT_LT(index.at("a"), index.at("c"));
    EXPECT_LT(index.at("b"), index.at("d"));
    EXPECT_LT(index.at("c"), index.at("d"));

    // Exact deterministic positions (lexicographic tie-breaking).
    EXPECT_EQ(index.at("a"), 0u);
    EXPECT_EQ(index.at("b"), 1u);
    EXPECT_EQ(index.at("c"), 2u);
    EXPECT_EQ(index.at("d"), 3u);
    EXPECT_EQ(index.at("z"), 4u);
}

TEST(TBuildDeterministicTopologicalIndexTest, DeterministicAndStable)
{
    // Two independent chains: (p->q->r) and (x->y->z).
    // Roots p and x are both in-degree 0; p < x, so p goes first.
    // After p: q becomes ready, ready={q, x}, q < x, so q=1.
    // After q: r becomes ready, ready={r, x}, r < x, so r=2.
    // After r: ready={x}, x=3.
    // After x: y becomes ready, ready={y}, y=4.
    // After y: z becomes ready, ready={z}, z=5.
    // Isolated vertex "m" (m < p) is added as a key and goes first: m=0, then p=1, etc.
    std::map<std::string, std::vector<std::string>> graph = {
        {"m", {}},
        {"p", {"q"}},
        {"q", {"r"}},
        {"r", {}},
        {"x", {"y"}},
        {"y", {"z"}},
        {"z", {}},
    };

    auto index1 = BuildDeterministicTopologicalIndex(graph);
    auto index2 = BuildDeterministicTopologicalIndex(graph);

    // Must be identical across calls.
    EXPECT_EQ(index1, index2);

    ASSERT_EQ(index1.size(), 7u);

    // Topological constraints.
    EXPECT_LT(index1.at("p"), index1.at("q"));
    EXPECT_LT(index1.at("q"), index1.at("r"));
    EXPECT_LT(index1.at("x"), index1.at("y"));
    EXPECT_LT(index1.at("y"), index1.at("z"));

    // Exact deterministic positions.
    EXPECT_EQ(index1.at("m"), 0u); // Extra, isolated, lexicographically first.
    EXPECT_EQ(index1.at("p"), 1u);
    EXPECT_EQ(index1.at("q"), 2u);
    EXPECT_EQ(index1.at("r"), 3u);
    EXPECT_EQ(index1.at("x"), 4u);
    EXPECT_EQ(index1.at("y"), 5u);
    EXPECT_EQ(index1.at("z"), 6u);
}

////////////////////////////////////////////////////////////////////////////////

TEST_W(TDescribeTest, DescribeWorkerWithIPv4Address)
{
    // Override the default worker address to use IPv4 format produced by FormatNetworkAddress.
    WorkerCount = 0;
    Prepare();

    auto worker = New<NFlow::TWorker>();
    worker->RpcAddress = "[10.0.0.1]:81";
    worker->MonitoringAddress = "[10.0.0.1]:80";
    worker->IncarnationId = TIncarnationId(TGuid::Create());
    FlowView->State->Workers[worker->RpcAddress] = worker;

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    JobManager->DistributeJobs(FlowView);
    FlowView->State->CommitMutation();

    auto workersDescription = DescribeWorkers(FlowView);
    ASSERT_EQ(workersDescription.Workers.size(), 1u);
    EXPECT_EQ(workersDescription.Workers[0].Address, "[10.0.0.1]:81");

    auto description = DescribeWorker(FlowView, "[10.0.0.1]:81");
    EXPECT_EQ(description.Address, "[10.0.0.1]:81");
    EXPECT_EQ(description.MonitoringAddress, "[10.0.0.1]:80");

    EXPECT_EQ(description.MonitoringTag, "[10.0.0.1]");

    EXPECT_THAT(description.Address, testing::ContainsRegex(R"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"));
    EXPECT_THAT(description.Address, testing::Not(testing::HasSubstr("::")));
}

TEST_W(TDescribeTest, DescribePipelineShowsFlowCoreTargetNotSet)
{
    Prepare();
    auto description = DescribeStatus(/*controllerFlowCoreVersion*/ "");

    EXPECT_TRUE(MessagesContain(description.Messages, "Binary version (FlowCoreTarget): not set"))
        << "Messages:" << ConvertToYsonString(description.Messages).ToString();
}

TEST_W(TDescribeTest, DescribePipelineShowsFlowCoreTargetMatching)
{
    Prepare();
    FlowView->State->ExecutionSpec->FlowCoreTarget->SetValue(TFlowCoreTarget(GetBinaryChecksum()));

    auto description = DescribeStatus();

    EXPECT_TRUE(MessagesContain(description.Messages, "Flow binaries matching"))
        << "Messages:" << ConvertToYsonString(description.Messages).ToString();
}

TEST_W(TDescribeTest, DescribePipelineShowsControllerCommitInfo)
{
    Prepare();
    FlowView->State->ExecutionSpec->FlowCoreTarget->SetValue(TFlowCoreTarget(std::string("mismatched_version")));

    auto description = DescribeStatus();
    auto contains = [&] (const std::string& text) {
        return MessagesContain(description.Messages, text);
    };
    auto dump = [&] {
        return ConvertToYsonString(description.Messages).ToString();
    };

    const auto& buildInfo = GetFlowCoreBuildInfo();
    if (!buildInfo->IsEmpty()) {
        EXPECT_TRUE(contains("Controller build info:")) << "Messages:" << dump();
    }
    if (!buildInfo->CommitHash.empty()) {
#ifndef YT_INTERNAL_FLOW
        auto expectedCommit = "`" + buildInfo->CommitHash + "`";
#else
        auto expectedCommit = std::string(NInternalUrls::VcsCommitUrlPrefix) + buildInfo->CommitHash;
#endif
        EXPECT_TRUE(contains(expectedCommit)) << "Messages:" << dump();
    }
    if (buildInfo->SvnRevision > 0 && !buildInfo->CommitHash.empty()) {
#ifndef YT_INTERNAL_FLOW
        auto expectedRevision = "(`r" + std::to_string(buildInfo->SvnRevision) + "`)";
#else
        auto expectedRevision = std::string(NInternalUrls::VcsRevisionUrlPrefix) + std::to_string(buildInfo->SvnRevision);
#endif
        EXPECT_TRUE(contains(expectedRevision)) << "Messages:" << dump();
    }
    if (buildInfo->BuildTimestamp > 0) {
        EXPECT_TRUE(contains("* Built at: `")) << "Messages:" << dump();
    }
    if (!buildInfo->BuildHost.empty()) {
        EXPECT_TRUE(contains("* Built on: `" + buildInfo->BuildHost + "`")) << "Messages:" << dump();
    }
}

TEST_W(TDescribeTest, DescribePipelineShowsFlowCoreTargetMismatch)
{
    Prepare();
    FlowView->State->ExecutionSpec->FlowCoreTarget->SetValue(TFlowCoreTarget(std::string("mismatched_version")));

    auto description = DescribeStatus();
    auto dump = [&] {
        return ConvertToYsonString(description.Messages).ToString();
    };

    EXPECT_TRUE(MessagesContain(description.Messages, "Binary version (FlowCoreTarget): `mismatched_version`")) << "Messages:" << dump();
    EXPECT_TRUE(MessagesContain(description.Messages, "Controller and Runner binary mismatch")) << "Messages:" << dump();
    EXPECT_TRUE(MessagesContain(description.Messages, "Rollout in progress?")) << "Messages:" << dump();

    bool foundError = false;
    for (const auto& msg : description.Messages) {
        if (msg.Text.starts_with("Controller and Runner binary mismatch")) {
            EXPECT_EQ(msg.Level, ELogLevel::Error);
            foundError = true;
        }
    }
    EXPECT_TRUE(foundError) << "Binary mismatch error message not found";
}

TEST_W(TDescribeTest, DescribePipelineShowsFlowCoreTargetMismatchedWorkers)
{
    Prepare();
    FlowView->State->ExecutionSpec->FlowCoreTarget->SetValue(TFlowCoreTarget(GetBinaryChecksum()));
    FlowView->EphemeralState->FlowCoreTargetMismatchedWorkers["old_version_1"] = {
        .ExampleAddress = "worker-old-1.net:81",
        .Count = 1,
    };
    FlowView->EphemeralState->FlowCoreTargetMismatchedWorkers["old_version_2"] = {
        .ExampleAddress = "worker-old-2.net:81",
        .Count = 1,
    };

    auto description = DescribeStatus();
    auto dump = [&] {
        return ConvertToYsonString(description.Messages).ToString();
    };

    EXPECT_TRUE(MessagesContain(description.Messages, "**Worker build info:**")) << "Messages:" << dump();

    bool foundError = false;
    for (const auto& msg : description.Messages) {
        if (msg.Text.find("Controller and Worker binary mismatch") != std::string::npos) {
            EXPECT_EQ(msg.Level, ELogLevel::Error);
            foundError = true;
        }
    }
    EXPECT_TRUE(foundError) << "Worker binary mismatch error message not found";
}

TEST_W(TDescribeTest, DescribeWorkerShowsFlowCoreVersion)
{
    Prepare();

    auto workerIt = FlowView->State->Workers.begin();
    ASSERT_NE(workerIt, FlowView->State->Workers.end());
    workerIt->second->FlowCoreVersion = "test_flow_core_version_123";

    auto description = DescribeWorker(FlowView, workerIt->first);

    auto containsText = [] (const std::vector<TMessage>& messages, const std::string& text) {
        auto messagesStr = ConvertToYsonString(messages).ToString();
        return messagesStr.Contains(text);
    };

    EXPECT_TRUE(containsText(description.Messages, "test_flow_core_version_123"))
        << "Messages:" << ConvertToYsonString(description.Messages).ToString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDescribe
