#include <yt/yt/flow/library/cpp/controller/lineage_rate_aggregator.h>

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

void SetActiveWorkerRates(
    TLineageRateAggregator* aggregator,
    const TFlowViewPtr& flowView,
    const std::string& address,
    TIncarnationId incarnationId,
    std::optional<double> countRate,
    std::optional<double> byteRate)
{
    auto worker = New<TWorker>();
    worker->IncarnationId = incarnationId;
    flowView->State->Workers[address] = worker;

    TLineageRates rates;
    auto& rate = rates["output"]["input"];
    rate.CountPerSecond = countRate;
    rate.BytesPerSecond = byteRate;
    rate.InputCountPerSecond = countRate;
    rate.InputBytesPerSecond = byteRate;
    aggregator->AddWorkerRates(incarnationId, std::move(rates));
}

const TStreamRate& GetRate(const TFlowViewPtr& flowView)
{
    return flowView->EphemeralState->LineageRates.at("output").at("input");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLineageRateAggregatorTest, SumsPairedObservationsRatherThanWorkerRatios)
{
    auto flowView = MakeFlowView();
    TLineageRateAggregator aggregator;
    const auto first = TIncarnationId(TGuid::Create());
    const auto second = TIncarnationId(TGuid::Create());
    SetActiveWorkerRates(&aggregator, flowView, "first", first, 100, 100);
    SetActiveWorkerRates(&aggregator, flowView, "second", second, 90, 900);
    TLineageRates rates;
    auto& firstRate = rates["output"]["input"];
    firstRate.CountPerSecond = 100;
    firstRate.BytesPerSecond = 100;
    firstRate.InputCountPerSecond = 10;
    firstRate.InputBytesPerSecond = 50;
    aggregator.AddWorkerRates(first, std::move(rates));
    aggregator.Update(flowView, TInstant::Seconds(100));
    const auto& rate = GetRate(flowView);
    EXPECT_DOUBLE_EQ(*rate.CountPerSecond, 190);
    EXPECT_DOUBLE_EQ(*rate.InputCountPerSecond, 100);
    EXPECT_DOUBLE_EQ(*rate.BytesPerSecond, 1000);
    EXPECT_DOUBLE_EQ(*rate.InputBytesPerSecond, 950);
}

TEST(TLineageRateAggregatorTest, ExcludesUnpairedObservationsIndependentlyByUnit)
{
    auto flowView = MakeFlowView();
    TLineageRateAggregator aggregator;
    const auto legacy = TIncarnationId(TGuid::Create());
    SetActiveWorkerRates(&aggregator, flowView, "paired", TIncarnationId(TGuid::Create()), 10, 100);
    SetActiveWorkerRates(&aggregator, flowView, "legacy", legacy, 1000, 2000);
    TLineageRates rates;
    auto& partialRate = rates["output"]["input"];
    partialRate.CountPerSecond = 1000;
    partialRate.BytesPerSecond = 2000;
    partialRate.InputBytesPerSecond = 500;
    aggregator.AddWorkerRates(legacy, std::move(rates));
    aggregator.Update(flowView, TInstant::Seconds(100));
    const auto& rate = GetRate(flowView);
    EXPECT_DOUBLE_EQ(*rate.CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*rate.InputCountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*rate.BytesPerSecond, 2100);
    EXPECT_DOUBLE_EQ(*rate.InputBytesPerSecond, 600);
}

TEST(TLineageRateAggregatorTest, SumsActiveWorkersWithoutAdditionalDecay)
{
    auto flowView = MakeFlowView();
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker-1", TIncarnationId(TGuid::Create()), 10, 100);
    SetActiveWorkerRates(&aggregator, flowView, "worker-2", TIncarnationId(TGuid::Create()), 20, 200);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 30);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 300);

    aggregator.Update(flowView, startTime + TDuration::Minutes(5));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 30);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 300);
}

TEST(TLineageRateAggregatorTest, ReplacesSnapshotsAndThrottlesFlowViewUpdates)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 100);

    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 20, 200);
    aggregator.Update(flowView, startTime + TDuration::Seconds(30));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 100);

    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 20);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 200);
}

TEST(TLineageRateAggregatorTest, IgnoresSnapshotsFromUnknownWorkers)
{
    auto flowView = MakeFlowView();
    TLineageRateAggregator aggregator;

    TLineageRates rates;
    rates["output"]["input"].CountPerSecond = 10;
    aggregator.AddWorkerRates(TIncarnationId(TGuid::Create()), std::move(rates));
    aggregator.Update(flowView, TInstant::Seconds(100));

    EXPECT_TRUE(flowView->EphemeralState->LineageRates.empty());
}

TEST(TLineageRateAggregatorTest, StartsDecayWhenWorkerBecomesInactive)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    flowView->Feedback->WorkerStatuses.clear();

    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 100);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 1000);

    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    EXPECT_NEAR(*GetRate(flowView).CountPerSecond, 100 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(*GetRate(flowView).BytesPerSecond, 1000 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(*GetRate(flowView).InputCountPerSecond, 100 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(*GetRate(flowView).InputBytesPerSecond, 1000 * std::exp(-1.0), 1e-9);

    aggregator.Update(flowView, startTime + TDuration::Minutes(1) + LineageRateRetentionTime);
    EXPECT_TRUE(flowView->EphemeralState->LineageRates.empty());
}

TEST(TLineageRateAggregatorTest, RestoresFullWeightWhenWorkerBecomesActiveAgain)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    flowView->Feedback->WorkerStatuses.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    EXPECT_NEAR(*GetRate(flowView).CountPerSecond, 100 * std::exp(-1.0), 1e-9);

    auto worker = New<TWorker>();
    worker->IncarnationId = incarnationId;
    flowView->State->Workers["worker"] = std::move(worker);
    aggregator.Update(flowView, startTime + TDuration::Minutes(7));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 100);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 1000);

    flowView->State->Workers.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(8));
    aggregator.Update(flowView, startTime + TDuration::Minutes(13));
    EXPECT_NEAR(*GetRate(flowView).CountPerSecond, 100 * std::exp(-1.0), 1e-9);
}

TEST(TLineageRateAggregatorTest, KeepsDecayOriginWhenLateInactiveSnapshotArrives)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 100, 1000);

    aggregator.Update(flowView, startTime);
    flowView->State->Workers.clear();
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));

    TLineageRates lateRates;
    lateRates["output"]["input"].CountPerSecond = 200;
    lateRates["output"]["input"].BytesPerSecond = 2000;
    lateRates["output"]["input"].InputCountPerSecond = 200;
    lateRates["output"]["input"].InputBytesPerSecond = 2000;
    aggregator.AddWorkerRates(incarnationId, std::move(lateRates));
    aggregator.Update(flowView, startTime + TDuration::Seconds(90));

    aggregator.Update(flowView, startTime + TDuration::Minutes(6));
    EXPECT_NEAR(*GetRate(flowView).CountPerSecond, 200 * std::exp(-1.0), 1e-9);
    EXPECT_NEAR(*GetRate(flowView).BytesPerSecond, 2000 * std::exp(-1.0), 1e-9);
}

TEST(TLineageRateAggregatorTest, UsesAvailableRatesWhileAnotherWorkerIsImmature)
{
    auto flowView = MakeFlowView();
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(
        &aggregator,
        flowView,
        "worker-1",
        TIncarnationId(TGuid::Create()),
        std::nullopt,
        std::nullopt);
    SetActiveWorkerRates(&aggregator, flowView, "worker-2", TIncarnationId(TGuid::Create()), 20, 200);

    aggregator.Update(flowView, TInstant::Seconds(100));

    ASSERT_TRUE(GetRate(flowView).CountPerSecond);
    ASSERT_TRUE(GetRate(flowView).BytesPerSecond);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 20);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 200);
}

TEST(TLineageRateAggregatorTest, KeepsSnapshotsAcrossPipelineSpecChanges)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 100);

    auto newSpec = MakePipelineSpec();
    newSpec->Bump(TestVersionProvider());
    flowView->CurrentSpec = newSpec;
    aggregator.Update(flowView, startTime + TDuration::Minutes(1));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 100);

    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 20, 200);
    aggregator.Update(flowView, startTime + TDuration::Minutes(2));
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 20);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).BytesPerSecond, 200);
}

TEST(TLineageRateAggregatorTest, DropsEdgesRemovedByPipelineSpecChange)
{
    auto flowView = MakeFlowView();
    const auto incarnationId = TIncarnationId(TGuid::Create());
    const auto startTime = TInstant::Seconds(100);
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", incarnationId, 10, 100);

    aggregator.Update(flowView, startTime);
    EXPECT_DOUBLE_EQ(*GetRate(flowView).CountPerSecond, 10);

    flowView->CurrentSpec = MakePipelineSpec(TStreamId("new_output"), TStreamId("new_input"));
    aggregator.Update(flowView, startTime + TDuration::Seconds(30));

    EXPECT_TRUE(flowView->EphemeralState->LineageRates.empty());
}

TEST(TLineageRateAggregatorTest, SerializesNumericRatesInFlowView)
{
    auto flowView = MakeFlowView();
    TLineageRateAggregator aggregator;
    SetActiveWorkerRates(&aggregator, flowView, "worker", TIncarnationId(TGuid::Create()), 10, 100);
    aggregator.Update(flowView, TInstant::Seconds(100));

    const auto serialized = NYson::ConvertToYsonString(flowView->EphemeralState);
    const auto restored = NYTree::ConvertTo<TFlowEphemeralStatePtr>(serialized);
    const auto& rate = restored->LineageRates.at("output").at("input");
    ASSERT_TRUE(rate.CountPerSecond);
    ASSERT_TRUE(rate.BytesPerSecond);
    EXPECT_DOUBLE_EQ(*rate.CountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*rate.BytesPerSecond, 100);
    ASSERT_TRUE(rate.InputCountPerSecond);
    ASSERT_TRUE(rate.InputBytesPerSecond);
    EXPECT_DOUBLE_EQ(*rate.InputCountPerSecond, 10);
    EXPECT_DOUBLE_EQ(*rate.InputBytesPerSecond, 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
