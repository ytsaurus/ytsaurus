#include <yt/yt/flow/library/cpp/controller/lineage_aggregator.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <cmath>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

TVersionedPipelineSpecPtr MakePipelineSpec(
    const TStreamId& outputStreamId = TStreamId("output"),
    const TStreamId& inputStreamId = TStreamId("input"))
{
    auto computationSpec = New<TComputationSpec>();
    computationSpec->InputStreamIds = {inputStreamId};
    computationSpec->OutputStreamIds = {outputStreamId};
    computationSpec->StreamsDependency[outputStreamId] = {inputStreamId};

    auto pipelineSpec = New<TPipelineSpec>();
    pipelineSpec->Computations[TComputationId("computation")] = std::move(computationSpec);

    auto versionedSpec = New<TVersionedPipelineSpec>();
    versionedSpec->TrySetValue(std::move(pipelineSpec), TestVersionProvider());
    return versionedSpec;
}

TFlowViewPtr MakeFlowView()
{
    auto flowView = New<TFlowView>();
    flowView->State = New<TFlowState>();
    flowView->Feedback = New<TFlowFeedback>();
    flowView->EphemeralState = New<TFlowEphemeralState>();
    flowView->CurrentSpec = MakePipelineSpec();
    return flowView;
}

void SetActiveWorkerRatios(
    TLineageAggregator* aggregator,
    const TFlowViewPtr& flowView,
    const std::string& address,
    TIncarnationId incarnationId,
    std::optional<double> countWeight,
    std::optional<double> byteWeight)
{
    auto worker = New<TWorker>();
    worker->IncarnationId = incarnationId;
    flowView->State->Workers[address] = worker;

    TLineageRatios ratios;
    auto& value = ratios["output"]["input"];
    if (countWeight) {
        value.Count.emplace();
        value.Count->Ratio = 1;
        value.Count->Weight = *countWeight;
    }
    if (byteWeight) {
        value.ByteSize.emplace();
        value.ByteSize->Ratio = 1;
        value.ByteSize->Weight = *byteWeight;
    }
    aggregator->AddWorkerRatios(incarnationId, std::move(ratios));
}

const TLineageRatio& GetRatio(const TFlowViewPtr& flowView)
{
    return flowView->EphemeralState->LineageRatios.at("output").at("input");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLineageAggregatorTest, WeightsRatiosByObservedInput)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;
    auto first = TIncarnationId(TGuid::Create());
    auto second = TIncarnationId(TGuid::Create());
    SetActiveWorkerRatios(&aggregator, flowView, "first", first, 10, 50);
    SetActiveWorkerRatios(&aggregator, flowView, "second", second, 90, 950);
    TLineageRatios ratios;
    auto& firstValue = ratios["output"]["input"];
    firstValue.Count.emplace();
    firstValue.Count->Ratio = 10;
    firstValue.Count->Weight = 10;
    firstValue.ByteSize.emplace();
    firstValue.ByteSize->Ratio = 2;
    firstValue.ByteSize->Weight = 50;
    aggregator.AddWorkerRatios(first, std::move(ratios));
    aggregator.Update(flowView, TInstant::Seconds(100));
    const auto& value = GetRatio(flowView);
    ASSERT_TRUE(value.Count);
    ASSERT_TRUE(value.ByteSize);
    EXPECT_NEAR(value.Count->Ratio, 1.9, 1e-12);
    EXPECT_DOUBLE_EQ(value.Count->Weight, 100);
    EXPECT_NEAR(value.ByteSize->Ratio, 1.05, 1e-12);
    EXPECT_DOUBLE_EQ(value.ByteSize->Weight, 1000);
}

TEST(TLineageAggregatorTest, MissingAndZeroWeightDoNotContribute)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;
    auto first = TIncarnationId(TGuid::Create());
    SetActiveWorkerRatios(&aggregator, flowView, "first", first, 10, 100);
    auto second = TIncarnationId(TGuid::Create());
    SetActiveWorkerRatios(&aggregator, flowView, "second", second, std::nullopt, std::nullopt);
    TLineageRatios ratios;
    ratios["output"]["input"].Count.emplace();
    ratios["output"]["input"].Count->Ratio = 100;
    aggregator.AddWorkerRatios(second, std::move(ratios));
    aggregator.Update(flowView, TInstant::Seconds(100));
    const auto& value = GetRatio(flowView);
    ASSERT_TRUE(value.Count);
    ASSERT_TRUE(value.ByteSize);
    EXPECT_DOUBLE_EQ(value.Count->Ratio, 1);
    EXPECT_DOUBLE_EQ(value.Count->Weight, 10);
    EXPECT_DOUBLE_EQ(value.ByteSize->Weight, 100);
}

TEST(TLineageAggregatorTest, ZeroOutputHasPositiveWeight)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;
    auto first = TIncarnationId(TGuid::Create());
    SetActiveWorkerRatios(&aggregator, flowView, "first", first, 100, 100);
    auto second = TIncarnationId(TGuid::Create());
    SetActiveWorkerRatios(&aggregator, flowView, "second", second, 100, 100);
    TLineageRatios ratios;
    auto& zero = ratios["output"]["input"].Count.emplace();
    zero.Weight = 300;
    aggregator.AddWorkerRatios(second, std::move(ratios));
    aggregator.Update(flowView, TInstant::Seconds(100));
    ASSERT_TRUE(GetRatio(flowView).Count);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Ratio, 0.25);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 400);
}

TEST(TLineageAggregatorTest, SumsActiveWorkersWithoutAdditionalDecay)
{
    auto flowView = MakeFlowView();
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker-1", TIncarnationId(TGuid::Create()), 10, 100);
    SetActiveWorkerRatios(&aggregator, flowView, "worker-2", TIncarnationId(TGuid::Create()), 20, 200);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 30);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 300);

    aggregator.Update(flowView, startTime + TDuration::Minutes(5));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 30);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 300);
}

TEST(TLineageAggregatorTest, ReplacesSnapshotsAndThrottlesFlowViewUpdates)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 10);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 100);

    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 20, 200);
    aggregator.Update(flowView, startTime + TDuration::Seconds(30));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 10);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 100);

    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 20);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 200);
}

TEST(TLineageAggregatorTest, IgnoresSnapshotsFromUnknownWorkers)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;

    TLineageRatios ratios;
    ratios["output"]["input"].Count.emplace();
    ratios["output"]["input"].Count->Weight = 10;
    aggregator.AddWorkerRatios(TIncarnationId(TGuid::Create()), std::move(ratios));
    aggregator.Update(flowView, TInstant::Seconds(100));

    EXPECT_TRUE(flowView->EphemeralState->LineageRatios.empty());
}

TEST(TLineageAggregatorTest, StartsDecayWhenWorkerBecomesInactive)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    flowView->Feedback->WorkerStatuses.clear();

    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 100);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 1000);

    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    ASSERT_TRUE(GetRatio(flowView).Count);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Ratio, 1);
    EXPECT_NEAR(GetRatio(flowView).Count->Weight, 100 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(GetRatio(flowView).ByteSize->Weight, 1000 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(GetRatio(flowView).Count->Weight, 100 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(GetRatio(flowView).ByteSize->Weight, 1000 * std::exp(-1.0), 1e-9);

    aggregator.Update(flowView, startTime + TDuration::Minutes(1) + LineageRetentionTime);
    EXPECT_TRUE(flowView->EphemeralState->LineageRatios.empty());
}

TEST(TLineageAggregatorTest, RestoresFullWeightWhenWorkerBecomesActiveAgain)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    flowView->Feedback->WorkerStatuses.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    ASSERT_TRUE(GetRatio(flowView).Count);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Ratio, 1);
    EXPECT_NEAR(GetRatio(flowView).Count->Weight, 100 * std::exp(-1.0), 1e-9);

    auto worker = New<TWorker>();
    worker->IncarnationId = incarnationId;
    flowView->State->Workers["worker"] = std::move(worker);
    aggregator.Update(flowView, startTime + TDuration::Minutes(7));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 100);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 1000);

    flowView->State->Workers.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(8));
    aggregator.Update(flowView, startTime + TDuration::Minutes(13));
    EXPECT_NEAR(GetRatio(flowView).Count->Weight, 100 * std::exp(-1.0), 1e-9);
}

TEST(TLineageAggregatorTest, KeepsDecayOriginWhenLateInactiveSnapshotArrives)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));

    TLineageRatios lateRatios;
    auto& lateRatio = lateRatios["output"]["input"];
    lateRatio.Count.emplace();
    lateRatio.Count->Ratio = 1;
    lateRatio.Count->Weight = 200;
    lateRatio.ByteSize.emplace();
    lateRatio.ByteSize->Ratio = 1;
    lateRatio.ByteSize->Weight = 2000;
    aggregator.AddWorkerRatios(incarnationId, std::move(lateRatios));
    aggregator.Update(flowView, startTime + TDuration::Seconds(90));

    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    ASSERT_TRUE(GetRatio(flowView).Count);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Ratio, 1);
    EXPECT_NEAR(GetRatio(flowView).Count->Weight, 200 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(GetRatio(flowView).ByteSize->Weight, 2000 * std::exp(-1.0), 1e-9);
}

TEST(TLineageAggregatorTest, UsesAvailableRatiosWhileAnotherWorkerHasNoInput)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(
        &aggregator,
        flowView,
        "worker-1",
        TIncarnationId(TGuid::Create()),
        std::nullopt,
        std::nullopt);
    SetActiveWorkerRatios(&aggregator, flowView, "worker-2", TIncarnationId(TGuid::Create()), 20, 200);

    aggregator.Update(flowView, TInstant::Seconds(100));

    ASSERT_TRUE(GetRatio(flowView).Count);
    ASSERT_TRUE(GetRatio(flowView).ByteSize);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 20);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 200);
}

TEST(TLineageAggregatorTest, KeepsSnapshotsAcrossPipelineSpecChanges)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 10);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 100);

    auto newSpec = MakePipelineSpec();
    newSpec->Bump(TestVersionProvider());
    flowView->CurrentSpec = newSpec;
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 10);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 100);

    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 20, 200);
    aggregator.Update(flowView, startTime + TDuration::Minutes(2));
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 20);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).ByteSize->Weight, 200);
}

TEST(TLineageAggregatorTest, DropsEdgesRemovedByPipelineSpecChange)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(GetRatio(flowView).Count->Weight, 10);

    flowView->CurrentSpec = MakePipelineSpec(TStreamId("new_output"), TStreamId("new_input"));
    aggregator.Update(flowView, startTime + TDuration::Seconds(30));

    EXPECT_TRUE(flowView->EphemeralState->LineageRatios.empty());
}

TEST(TLineageAggregatorTest, LegacyRatePayloadDoesNotBecomeARatio)
{
    auto legacy = NYTree::ConvertTo<TFlowEphemeralStatePtr>(NYson::TYsonStringBuf(
        R"({lineage_rates={output={input={count_per_second=10.;input_count_per_second=5.}}}})"));
    EXPECT_TRUE(legacy->LineageRatios.empty());
}

TEST(TLineageAggregatorTest, SerializesRatiosAndWeightsInFlowView)
{
    auto flowView = MakeFlowView();
    TLineageAggregator aggregator;
    SetActiveWorkerRatios(&aggregator, flowView, "worker", TIncarnationId(TGuid::Create()), 10, 100);
    aggregator.Update(flowView, TInstant::Seconds(100));

    const auto serialized = NYson::ConvertToYsonString(flowView->EphemeralState);
    const auto restored = NYTree::ConvertTo<TFlowEphemeralStatePtr>(serialized);
    const auto& ratio = restored->LineageRatios.at("output").at("input");
    ASSERT_TRUE(ratio.Count);
    ASSERT_TRUE(ratio.ByteSize);
    EXPECT_DOUBLE_EQ(ratio.Count->Ratio, 1);
    EXPECT_DOUBLE_EQ(ratio.Count->Weight, 10);
    EXPECT_DOUBLE_EQ(ratio.ByteSize->Ratio, 1);
    EXPECT_DOUBLE_EQ(ratio.ByteSize->Weight, 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
