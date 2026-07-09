#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/computation/universal_controller_helpers.h>

namespace NYT::NFlow {

using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Declared as friend class in TBlockedStreamComputer.
class TBlockedStreamComputerTest
    : public ::testing::Test
{
public:
    using TBlockingRange = std::tuple<TKey, TKey, std::vector<TStreamId>>;

    static inline const TComputationId ComputationId = "ComputationA";
    static inline const TStreamId StreamA = "StreamA";
    static inline const TStreamId StreamB = "StreamB";

    TPipelineSpecPtr Spec;
    TComputationSpecPtr ComputationSpec;
    TFlowViewPtr FlowView;
    TFlowLayoutPtr Layout;

    void SetUp() override
    {
        Spec = New<TPipelineSpec>();

        ComputationSpec = New<TComputationSpec>();
        ComputationSpec->OutputStreamIds = {StreamA, StreamB};
        ComputationSpec->DistributionOrdering = EDistributionOrdering::Strict;
        Spec->Computations[ComputationId] = ComputationSpec;

        FlowView = New<TFlowView>();
        FlowView->State->ExecutionSpec->PipelineSpec->SetValue(Spec);

        Layout = FlowView->State->ExecutionSpec->Layout;

        auto persistedControl = New<TPersistedStateControl<std::string>>(New<TStorageHandler>());
        FlowView->State->AttachToControl(persistedControl);
        persistedControl->Recover();
        FlowView->State->StartMutation();
    }

    TPartitionPtr CreateInterruptingPartition(const TKey& lower, const TKey& upper)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->LowerKey = lower;
        partition->UpperKey = upper;
        Layout->Partitions.insert_or_assign(partition->PartitionId, partition);
        return partition;
    }

    TPartitionPtr CreateInterruptingPartition(const TKey& sourceKey)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->SourceKey = sourceKey;
        Layout->Partitions.insert_or_assign(partition->PartitionId, partition);
        return partition;
    }

    void FillCompletedStreams(const TPartitionPtr& partition, const THashSet<TStreamId>& completedStreams)
    {
        auto status = New<TPartitionJobStatus>();
        status->LastTraverseData = New<TFromPartitionTraverseData>();
        status->LastTraverseData->Node = New<TNodeTraverseData>();
        for (const auto& streamId : completedStreams) {
            auto stream = New<TStreamTraverseData>();
            stream->State = EStreamState::Completed;
            status->LastTraverseData->Node->Streams[streamId] = std::move(stream);
        }
        FlowView->Feedback->PartitionJobStatuses[partition->PartitionId] = std::move(status);
    }

    template <class... TArgs>
    std::vector<TStreamId> GetBlockedStreams(TBlockedStreamComputer& computer, TArgs&&... args)
    {
        auto result = computer.GetBlockedStreams(std::forward<TArgs>(args)...);
        std::vector<TStreamId> resultVector(result.begin(), result.end());
        Sort(resultVector);
        return resultVector;
    }

    std::vector<TBlockingRange> GetBlockingRanges(TBlockedStreamComputer& computer)
    {
        std::vector<TBlockingRange> result;
        for (const auto& [lower, value] : computer.BlockingRanges_) {
            auto blockingStreams = std::vector<TStreamId>(value.BlockingStreams.begin(), value.BlockingStreams.end());
            Sort(blockingStreams);
            result.emplace_back(lower, value.Upper, blockingStreams);
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

TEST_F(TBlockedStreamComputerTest, NoPartitions)
{
    TBlockedStreamComputer computer(FlowView, {}, TLogger("test"));

    EXPECT_THAT(GetBlockingRanges(computer), ::testing::ElementsAre(TBlockingRange(MinKey(), MaxKey(), {})));

    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(42)), ::testing::ElementsAre());                  // Source key.
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(42), MakeUintKey(43)), ::testing::ElementsAre()); // Range.
}

TEST_F(TBlockedStreamComputerTest, OneInterruptingRangePartition)
{

    auto partition = CreateInterruptingPartition(MakeUintKey(10), MakeUintKey(20));

    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(1), MakeUintKey(10)), ::testing::ElementsAre());
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(20), MakeUintKey(30)), ::testing::ElementsAre());
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(11), MakeUintKey(15)), ::testing::ElementsAre(StreamA, StreamB));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(9), MakeUintKey(11)), ::testing::ElementsAre(StreamA, StreamB));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(9), MakeUintKey(21)), ::testing::ElementsAre(StreamA, StreamB));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(19), MakeUintKey(21)), ::testing::ElementsAre(StreamA, StreamB));
        EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MaxKey()), ::testing::ElementsAre(StreamA, StreamB));
    }

    FillCompletedStreams(partition, {StreamA});
    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(9), MakeUintKey(11)), ::testing::ElementsAre(StreamB));
    }

    FillCompletedStreams(partition, {StreamA, StreamB});
    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(9), MakeUintKey(11)), ::testing::ElementsAre());
    }
}

TEST_F(TBlockedStreamComputerTest, OneInterruptingSourceKeyPartition)
{

    auto partition = CreateInterruptingPartition(MakeUintKey(10));

    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(20)), ::testing::ElementsAre());
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(10)), ::testing::ElementsAre(StreamA, StreamB));
    }

    FillCompletedStreams(partition, {StreamA});
    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(10)), ::testing::ElementsAre(StreamB));
    }

    FillCompletedStreams(partition, {StreamA, StreamB});
    {
        TBlockedStreamComputer computer(FlowView, {partition->PartitionId}, TLogger("test"));
        EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(10)), ::testing::ElementsAre());
    }
}

TEST_F(TBlockedStreamComputerTest, TwoOverlappingInterruptingRangePartition)
{
    auto partitionA = CreateInterruptingPartition(MakeUintKey(10), MakeUintKey(30));
    FillCompletedStreams(partitionA, {StreamB}); // StreamA is blocked.
    auto partitionB = CreateInterruptingPartition(MakeUintKey(20), MakeUintKey(40));
    FillCompletedStreams(partitionB, {StreamA}); // StreamB is blocked.

    TBlockedStreamComputer computer(FlowView, {partitionA->PartitionId, partitionB->PartitionId}, TLogger("test"));

    EXPECT_THAT(GetBlockingRanges(computer),
        ::testing::ElementsAre(
            TBlockingRange(MinKey(), MakeUintKey(10), {}),
            TBlockingRange(MakeUintKey(10), MakeUintKey(20), {StreamA}),
            TBlockingRange(MakeUintKey(20), MakeUintKey(30), {StreamA, StreamB}),
            TBlockingRange(MakeUintKey(30), MakeUintKey(40), {StreamB}),
            TBlockingRange(MakeUintKey(40), MaxKey(), {})));

    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(1), MakeUintKey(10)), ::testing::ElementsAre());
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(11), MakeUintKey(15)), ::testing::ElementsAre(StreamA));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(11), MakeUintKey(25)), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(23), MakeUintKey(25)), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(1), MakeUintKey(55)), ::testing::ElementsAre(StreamA, StreamB));

    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(25), MakeUintKey(55)), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(35), MakeUintKey(36)), ::testing::ElementsAre(StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(35), MaxKey()), ::testing::ElementsAre(StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(50), MaxKey()), ::testing::ElementsAre());
}

TEST_F(TBlockedStreamComputerTest, TwoIndependentInterruptingRangePartition)
{
    auto partitionA = CreateInterruptingPartition(MakeUintKey(10), MakeUintKey(20));
    FillCompletedStreams(partitionA, {StreamB}); // StreamA is blocked.
    auto partitionB = CreateInterruptingPartition(MakeUintKey(30), MakeUintKey(40));
    FillCompletedStreams(partitionB, {StreamA}); // StreamB is blocked.

    TBlockedStreamComputer computer(FlowView, {partitionA->PartitionId, partitionB->PartitionId}, TLogger("test"));

    EXPECT_THAT(GetBlockingRanges(computer),
        ::testing::ElementsAre(
            TBlockingRange(MinKey(), MakeUintKey(10), {}),
            TBlockingRange(MakeUintKey(10), MakeUintKey(20), {StreamA}),
            TBlockingRange(MakeUintKey(20), MakeUintKey(30), {}),
            TBlockingRange(MakeUintKey(30), MakeUintKey(40), {StreamB}),
            TBlockingRange(MakeUintKey(40), MaxKey(), {})));

    EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MakeUintKey(10)), ::testing::ElementsAre());
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(10), MakeUintKey(20)), ::testing::ElementsAre(StreamA));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(20), MakeUintKey(30)), ::testing::ElementsAre());
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(30), MakeUintKey(40)), ::testing::ElementsAre(StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(39), MakeUintKey(40)), ::testing::ElementsAre(StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(40), MaxKey()), ::testing::ElementsAre());
}

TEST_F(TBlockedStreamComputerTest, MinMaxInterruptingRangePartition)
{
    auto partitionA = CreateInterruptingPartition(MinKey(), MakeUintKey(10));
    FillCompletedStreams(partitionA, {StreamB}); // StreamA is blocked.
    auto partitionB = CreateInterruptingPartition(MakeUintKey(20), MaxKey());
    FillCompletedStreams(partitionB, {StreamA}); // StreamB is blocked.

    TBlockedStreamComputer computer(FlowView, {partitionA->PartitionId, partitionB->PartitionId}, TLogger("test"));

    EXPECT_THAT(GetBlockingRanges(computer),
        ::testing::ElementsAre(
            TBlockingRange(MinKey(), MakeUintKey(10), {StreamA}),
            TBlockingRange(MakeUintKey(10), MakeUintKey(20), {}),
            TBlockingRange(MakeUintKey(20), MaxKey(), {StreamB})));

    EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MakeUintKey(5)), ::testing::ElementsAre(StreamA));
    EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MakeUintKey(15)), ::testing::ElementsAre(StreamA));
    EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MakeUintKey(25)), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MinKey(), MaxKey()), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(5), MaxKey()), ::testing::ElementsAre(StreamA, StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(15), MaxKey()), ::testing::ElementsAre(StreamB));
    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(25), MaxKey()), ::testing::ElementsAre(StreamB));

    EXPECT_THAT(GetBlockedStreams(computer, MakeUintKey(10), MakeUintKey(20)), ::testing::ElementsAre());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
