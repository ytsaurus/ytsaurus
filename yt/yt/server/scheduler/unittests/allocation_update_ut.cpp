#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/node_shard.h>

namespace NYT::NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const TControllerEpoch TestControllerEpoch = TControllerEpoch(7);

static TJobResources MakeResources(i64 memory)
{
    TJobResources resources;
    resources.SetCpu(1.0);
    resources.SetGpu(1);
    resources.SetMemory(memory);
    return resources;
}

////////////////////////////////////////////////////////////////////////////////

class TAllocationUpdateMergeTest
    : public testing::Test
{
protected:
    const TOperationId OperationId_ = TOperationId(TGuid::Create());
    const TAllocationId AllocationId_ = TAllocationId(TGuid::Create());

    //! An update for the shared allocation, carrying nothing but the identity fields.
    NStrategy::TAllocationUpdate MakeUpdate() const
    {
        return NStrategy::TAllocationUpdate{
            .OperationId = OperationId_,
            .AllocationId = AllocationId_,
            .TreeId = "gpu",
            .ControllerEpoch = TestControllerEpoch,
        };
    }

    //! An update reporting |memory| as the allocation's usage, written at |resourcesUpdateTime|.
    NStrategy::TAllocationUpdate MakeResourcesUpdate(TCpuInstant resourcesUpdateTime, i64 memory) const
    {
        auto update = MakeUpdate();
        update.AllocationResources = MakeResources(memory);
        update.ResourcesUpdateTime = resourcesUpdateTime;

        return update;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAllocationUpdateMergeTest, FinishedTargetStaysFinished)
{
    auto target = MakeUpdate();
    target.Finished = true;

    auto source = MakeResourcesUpdate(/*resourcesUpdateTime*/ 200, /*memory*/ 1000);

    MergeAllocationUpdates(&target, source);

    EXPECT_TRUE(target.Finished);
    // The policies read no other field once a finish is set, so folding the counterpart in is
    // harmless and is what keeps the merge independent of the operand order.
    EXPECT_TRUE(target.AllocationResources.has_value());
}

TEST_F(TAllocationUpdateMergeTest, FinishedSourceWinsOverNewerTarget)
{
    auto target = MakeResourcesUpdate(/*resourcesUpdateTime*/ 200, /*memory*/ 1000);

    auto source = MakeUpdate();
    source.Finished = true;

    MergeAllocationUpdates(&target, source);

    // This is the case the bug is about: the finish must survive even though it is the older update.
    EXPECT_TRUE(target.Finished);
}

TEST_F(TAllocationUpdateMergeTest, NewerSourceResourcesWin)
{
    auto target = MakeResourcesUpdate(/*resourcesUpdateTime*/ 100, /*memory*/ 1000);
    auto source = MakeResourcesUpdate(/*resourcesUpdateTime*/ 200, /*memory*/ 2000);

    MergeAllocationUpdates(&target, source);

    EXPECT_FALSE(target.Finished);
    ASSERT_TRUE(target.AllocationResources.has_value());
    EXPECT_EQ(2000, target.AllocationResources->GetMemory());
    EXPECT_EQ(200, target.ResourcesUpdateTime);
}

TEST_F(TAllocationUpdateMergeTest, NewerTargetResourcesWin)
{
    auto target = MakeResourcesUpdate(/*resourcesUpdateTime*/ 200, /*memory*/ 2000);
    auto source = MakeResourcesUpdate(/*resourcesUpdateTime*/ 100, /*memory*/ 1000);

    MergeAllocationUpdates(&target, source);

    ASSERT_TRUE(target.AllocationResources.has_value());
    EXPECT_EQ(2000, target.AllocationResources->GetMemory());
    EXPECT_EQ(200, target.ResourcesUpdateTime);
}

TEST_F(TAllocationUpdateMergeTest, SourceWithoutResourcesDoesNotDropThem)
{
    auto target = MakeResourcesUpdate(/*resourcesUpdateTime*/ 100, /*memory*/ 1000);

    // A preemptible-progress-only update carries no resources at all.
    auto source = MakeUpdate();
    source.PreemptibleProgressStartTime = TInstant::Seconds(42);

    MergeAllocationUpdates(&target, source);

    ASSERT_TRUE(target.AllocationResources.has_value());
    EXPECT_EQ(1000, target.AllocationResources->GetMemory());
    ASSERT_TRUE(target.PreemptibleProgressStartTime.has_value());
    EXPECT_EQ(TInstant::Seconds(42), *target.PreemptibleProgressStartTime);
}

TEST_F(TAllocationUpdateMergeTest, SourceFillsGaps)
{
    auto target = MakeUpdate();
    target.PreemptibleProgressStartTime = TInstant::Seconds(42);

    auto source = MakeResourcesUpdate(/*resourcesUpdateTime*/ 100, /*memory*/ 1000);

    MergeAllocationUpdates(&target, source);

    ASSERT_TRUE(target.AllocationResources.has_value());
    EXPECT_EQ(1000, target.AllocationResources->GetMemory());
    ASSERT_TRUE(target.PreemptibleProgressStartTime.has_value());
    EXPECT_EQ(TInstant::Seconds(42), *target.PreemptibleProgressStartTime);
}

TEST_F(TAllocationUpdateMergeTest, LaterPreemptibleProgressTakesTheLaterOne)
{
    auto target = MakeUpdate();
    target.PreemptibleProgressStartTime = TInstant::Seconds(42);

    auto source = MakeUpdate();
    source.PreemptibleProgressStartTime = TInstant::Seconds(10);

    MergeAllocationUpdates(&target, source);

    ASSERT_TRUE(target.PreemptibleProgressStartTime.has_value());
    EXPECT_EQ(TInstant::Seconds(42), *target.PreemptibleProgressStartTime);
}

TEST_F(TAllocationUpdateMergeTest, PreemptibleProgressDoesNotShadowNewerResources)
{
    // The entry in the submit map carries the older resources, but a preemptible-progress reset was
    // written into it afterwards. A timestamp covering the whole update would make the entry look
    // newer than the counterpart and would drop the fresher resources.
    auto target = MakeResourcesUpdate(/*resourcesUpdateTime*/ 100, /*memory*/ 1000);
    target.PreemptibleProgressStartTime = TInstant::Seconds(42);

    auto source = MakeResourcesUpdate(/*resourcesUpdateTime*/ 200, /*memory*/ 2000);

    MergeAllocationUpdates(&target, source);

    ASSERT_TRUE(target.AllocationResources.has_value());
    EXPECT_EQ(2000, target.AllocationResources->GetMemory());
    EXPECT_EQ(200, target.ResourcesUpdateTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler
