#include <yt/yt/flow/library/cpp/computation/controller_base.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestStorageHandler
    : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

class TProcessingRatesTestController
    : public TComputationControllerBase
{
public:
    using TComputationControllerBase::TComputationControllerBase;

    TNodeTraverseDataPtr Future = New<TNodeTraverseData>();

    TPartitioningTopology DescribePartitioningTopology() override
    {
        return {.Value = TPartitioningTopology::TSource{}};
    }

    TPartitioningDescription DescribePartitioning(const TPartitioningStatus&) override
    {
        return {.Value = TPartitioningDescription::TSource{}};
    }

    void UpdateWatermarkState(TWatermarkStatePtr) override
    { }

private:
    TNodesByAvailabilityGroupBySource GetNodesByAvailabilityGroupBySource(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>&,
        const TFlowViewPtr&) override
    {
        YT_ABORT();
    }

    std::optional<TNodeTraverseDataPtr> GetFuturePartitionsNodeTraverseData(const TFlowViewPtr&) override
    {
        return Future;
    }
};

TEST(TControllerProcessingRatesTest, SyntheticPartitionsDoNotHideRealObservations)
{
    auto context = New<TComputationControllerContext>();
    context->ComputationSpec = New<TComputationSpec>();
    context->ComputationSpec->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
    context->ComputationSpec->SourceStreams.emplace(TStreamId("source"), New<TSourceSpec>());
    context->StatusProfiler = CreateSyncStatusProfiler();
    auto dynamicContext = New<TDynamicComputationControllerContext>();
    dynamicContext->DynamicComputationSpec = New<TDynamicComputationSpec>();
    auto controller = New<TProcessingRatesTestController>(context, dynamicContext);
    auto futureStream = New<TStreamTraverseData>();
    futureStream->EventWatermark = TSystemTimestamp(100);
    controller->Future->Streams.emplace(TStreamId("source"), futureStream);
    auto view = New<TFlowView>();
    auto control = New<TPersistedStateControl<std::string>>(New<TTestStorageHandler>());
    view->State->AttachToControl(control);
    control->Recover();
    view->State->StartMutation();
    THashMap<TPartitionId, TNodeTraverseDataPtr> nodes;
    TNodeTraverseDataPtr activeNode;
    for (int index = 0; index < 3; ++index) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->SourceKey = MakeUintKey(index);
        partition->ComputationId = "source";
        partition->StateTimestamp = TInstant::Seconds(1);
        partition->State = index == 2 ? EPartitionState::Completed : EPartitionState::Executing;
        view->State->ExecutionSpec->Layout->CreatePartition(partition);
        if (partition->State == EPartitionState::Completed) {
            auto spec = New<TExtendedComputationSpec>();
            spec->AllStreamIds.insert(TStreamId("source"));
            auto completed = MakeCompletedPartitionTraverseData(0, TSystemTimestamp(300), spec);
            nodes.emplace(partition->PartitionId, completed->Node);
            continue;
        }
        auto node = New<TNodeTraverseData>();
        activeNode = node;
        auto stream = New<TStreamTraverseData>();
        stream->EventWatermark = TSystemTimestamp(200);
        node->Streams.emplace(TStreamId("source"), stream);
        node->ProcessingRates = New<TComputationProcessingRates>();
        for (auto window : {&TComputationProcessingRates::Rate1m, &TComputationProcessingRates::Rate10m}) {
            auto& rate = node->ProcessingRates.Get()->*window;
            rate.emplace();
            rate->Processed.ProcessedMessagesPerSecond = 10;
            rate->Processed.ProcessedBytesPerSecond = 100;
            rate->Capacity.emplace();
            rate->Capacity->ProcessedMessagesPerSecond = 20;
            rate->Capacity->ProcessedBytesPerSecond = 200;
        }
        nodes.emplace(partition->PartitionId, node);
    }
    view->State->CommitMutation();
    auto merged = controller->ProcessPartitionTraverseData(nodes, nullptr, view)->AcceptedTraverseData;
    EXPECT_EQ(merged->Streams.at(TStreamId("source"))->EventWatermark, TSystemTimestamp(100));
    ASSERT_TRUE(merged->ProcessingRates);
    for (auto window : {&TComputationProcessingRates::Rate1m, &TComputationProcessingRates::Rate10m}) {
        const auto& rate = merged->ProcessingRates.Get()->*window;
        ASSERT_TRUE(rate);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedMessagesPerSecond, 20);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedBytesPerSecond, 200);
        ASSERT_TRUE(rate->Capacity);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedMessagesPerSecond, 40);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedBytesPerSecond, 400);
    }

    activeNode->ProcessingRates->Rate1m->Capacity.reset();
    auto unknownCapacity = controller->ProcessPartitionTraverseData(nodes, merged, view)->AcceptedTraverseData;
    ASSERT_TRUE(unknownCapacity->ProcessingRates);
    ASSERT_TRUE(unknownCapacity->ProcessingRates->Rate1m);
    EXPECT_DOUBLE_EQ(unknownCapacity->ProcessingRates->Rate1m->Processed.ProcessedMessagesPerSecond, 20);
    EXPECT_FALSE(unknownCapacity->ProcessingRates->Rate1m->Capacity);
    ASSERT_TRUE(unknownCapacity->ProcessingRates->Rate10m);
    ASSERT_TRUE(unknownCapacity->ProcessingRates->Rate10m->Capacity);
    EXPECT_DOUBLE_EQ(unknownCapacity->ProcessingRates->Rate10m->Capacity->ProcessedMessagesPerSecond, 40);

    activeNode->ProcessingRates.Reset();
    auto incomplete = controller->ProcessPartitionTraverseData(nodes, merged, view)->AcceptedTraverseData;
    EXPECT_FALSE(incomplete->ProcessingRates);

    auto futureOnly = controller->ProcessPartitionTraverseData({}, nullptr, view)->AcceptedTraverseData;
    EXPECT_EQ(futureOnly->Streams.at(TStreamId("source"))->EventWatermark, TSystemTimestamp(100));
    ASSERT_TRUE(futureOnly->ProcessingRates);
    for (auto window : {&TComputationProcessingRates::Rate1m, &TComputationProcessingRates::Rate10m}) {
        const auto& rate = futureOnly->ProcessingRates.Get()->*window;
        ASSERT_TRUE(rate);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedMessagesPerSecond, 0);
        EXPECT_DOUBLE_EQ(rate->Processed.ProcessedBytesPerSecond, 0);
        ASSERT_TRUE(rate->Capacity);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedMessagesPerSecond, 0);
        EXPECT_DOUBLE_EQ(rate->Capacity->ProcessedBytesPerSecond, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
