#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/strategy/policy/gpu/pool_tree_snapshot_state.h>
#include <yt/yt/server/scheduler/strategy/policy/gpu/structs.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const std::string TestModule = "ALA";
static const std::string TestAllocationGroupName = "task";

static const TJobResources UnitResources = [] {
    TJobResources resources;
    resources.SetCpu(10.0);
    resources.SetGpu(1);
    resources.SetMemory(125'000'000'000);
    return resources;
}();

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeSnapshotStateTest
    : public testing::Test
{
protected:
    TNodePtr CreateNode(NNodeTrackerClient::TNodeId nodeId, std::string module)
    {
        auto node = New<TNode>(nodeId, Format("node-%v", nodeId));
        node->SchedulingModule() = std::move(module);

        auto descriptor = New<TExecNodeDescriptor>();
        descriptor->Id = nodeId;
        descriptor->Online = true;
        descriptor->ResourceLimits = UnitResources * MaxNodeGpuCount;
        descriptor->ResourceLimits.SetUserSlots(1);
        node->SetDescriptor(std::move(descriptor));

        return node;
    }

    TOperationPtr CreateOperation()
    {
        TAllocationGroupResourcesMap grouped{
            {
                TestAllocationGroupName,
                TAllocationGroupResources{
                    .MinNeededResources = UnitResources,
                    .AllocationCount = 8,
                },
            },
        };

        auto operation = New<TOperation>(
            TOperationId(TGuid::Create()),
            EOperationType::Vanilla,
            /*gang*/ false,
            /*specifiedSchedulingModules*/ std::nullopt,
            /*schedulingTagFilter*/ TSchedulingTagFilter{});
        operation->Initialize(grouped);
        operation->ReadyToAssignGroupedNeededResources() = grouped;
        return operation;
    }

    TAssignmentPtr AddPreliminaryAssignment(
        const TOperationPtr& operation,
        const TNodePtr& node,
        bool preemptible = false)
    {
        auto assignment = New<TAssignment>(
            TestAllocationGroupName,
            TJobResourcesWithQuota(UnitResources),
            operation.Get(),
            node.Get());
        assignment->Preemptible = preemptible;
        node->AddAssignment(assignment);
        operation->AddPlannedAssignment(assignment);
        return assignment;
    }

    TAllocationStatePtr RealizeAssignment(
        const TNodePtr& node,
        const TAssignmentPtr& assignment)
    {
        auto allocationId = TAllocationId(TGuid::Create());
        auto allocation = New<TAllocationState>(
            allocationId,
            node->GetId(),
            MakeWeak(assignment),
            UnitResources);
        assignment->AddAllocation(allocation);
        return allocation;
    }

    TAllocationStatePtr AddOrphanAllocation(
        const TOperationPtr& operation,
        NNodeTrackerClient::TNodeId nodeId)
    {
        auto allocationId = TAllocationId(TGuid::Create());
        auto allocation = New<TAllocationState>(
            allocationId,
            nodeId,
            /*assignment*/ TWeakPtr<TAssignment>{},
            UnitResources);
        operation->AddOrphanAllocation(allocation);
        return allocation;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPoolTreeSnapshotStateTest, AllocationSnapshotInheritsPreemptibilityFromAssignment)
{
    auto node = CreateNode(/*nodeId*/ NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();
    auto assignment = AddPreliminaryAssignment(operation, node, /*preemptible*/ true);
    auto allocation = RealizeAssignment(node, assignment);

    auto info = allocation->BuildSnapshotInfo(operation->GetId());
    EXPECT_EQ(allocation->GetId(), info.AllocationId);
    EXPECT_EQ(operation->GetId(), info.OperationId);
    EXPECT_EQ(node->GetId(), info.NodeId);
    EXPECT_TRUE(info.Preemptible);
}

TEST_F(TPoolTreeSnapshotStateTest, OrphanAllocationSnapshotIsNotPreemptible)
{
    auto operation = CreateOperation();
    auto nodeId = NNodeTrackerClient::TNodeId(42);
    auto allocation = AddOrphanAllocation(operation, nodeId);

    auto info = allocation->BuildSnapshotInfo(operation->GetId());
    EXPECT_EQ(allocation->GetId(), info.AllocationId);
    EXPECT_EQ(operation->GetId(), info.OperationId);
    EXPECT_EQ(nodeId, info.NodeId);
    EXPECT_FALSE(info.Preemptible);
}

TEST_F(TPoolTreeSnapshotStateTest, OperationSnapshotIncludesOrphanAllocations)
{
    auto node = CreateNode(NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();

    auto realizedAssignment = AddPreliminaryAssignment(operation, node, /*preemptible*/ true);
    auto realizedAllocation = RealizeAssignment(node, realizedAssignment);

    auto orphanAllocation = AddOrphanAllocation(operation, NNodeTrackerClient::TNodeId(42));

    auto operationInfo = operation->BuildSnapshotInfo();

    EXPECT_EQ(1, operationInfo.RealizedAssignmentCount);
    EXPECT_EQ(0, operationInfo.PreliminaryAssignmentCount);

    THashSet<TAllocationId> allocationIds(
        operationInfo.AllocationIds.begin(),
        operationInfo.AllocationIds.end());
    EXPECT_THAT(allocationIds, testing::UnorderedElementsAre(
        realizedAllocation->GetId(),
        orphanAllocation->GetId()));

    auto realizedInfo = realizedAllocation->BuildSnapshotInfo(operation->GetId());
    EXPECT_TRUE(realizedInfo.Preemptible);

    auto orphanInfo = orphanAllocation->BuildSnapshotInfo(operation->GetId());
    EXPECT_FALSE(orphanInfo.Preemptible);
}

TEST_F(TPoolTreeSnapshotStateTest, OperationSnapshotCountsRealizedAndPreliminary)
{
    auto node = CreateNode(NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();
    operation->SetStarving(true);
    operation->SetEnabled(true);
    operation->SchedulingModule() = TestModule;

    auto realizedAssignment = AddPreliminaryAssignment(operation, node);
    auto realizedAllocation = RealizeAssignment(node, realizedAssignment);

    AddPreliminaryAssignment(operation, node);
    AddPreliminaryAssignment(operation, node);

    auto info = operation->BuildSnapshotInfo();
    EXPECT_EQ(1, info.RealizedAssignmentCount);
    EXPECT_EQ(2, info.PreliminaryAssignmentCount);
    EXPECT_TRUE(info.Starving);
    EXPECT_TRUE(info.Enabled);
    EXPECT_FALSE(info.Preemptible);
    ASSERT_TRUE(info.SchedulingModule.has_value());
    EXPECT_EQ(TestModule, *info.SchedulingModule);

    ASSERT_EQ(1u, info.AllocationIds.size());
    EXPECT_EQ(realizedAllocation->GetId(), info.AllocationIds[0]);
}

TEST_F(TPoolTreeSnapshotStateTest, NodeSnapshotEmitsAllocationIdsAndCounts)
{
    auto node = CreateNode(NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();

    auto firstAssignment = AddPreliminaryAssignment(operation, node);
    auto firstAllocation = RealizeAssignment(node, firstAssignment);
    auto secondAssignment = AddPreliminaryAssignment(operation, node);
    auto secondAllocation = RealizeAssignment(node, secondAssignment);

    auto info = node->BuildSnapshotInfo();
    ASSERT_TRUE(info.SchedulingModule.has_value());
    EXPECT_EQ(TestModule, *info.SchedulingModule);
    EXPECT_EQ(0, info.AllocationsToPreemptCount);
    EXPECT_EQ(0, info.PreemptedAllocationsCount);

    THashSet<TAllocationId> ids(info.AllocationIds.begin(), info.AllocationIds.end());
    EXPECT_THAT(ids, testing::UnorderedElementsAre(firstAllocation->GetId(), secondAllocation->GetId()));
}

TEST_F(TPoolTreeSnapshotStateTest, NodeSnapshotReflectsPreemptionBuckets)
{
    auto node = CreateNode(NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();

    auto firstAssignment = AddPreliminaryAssignment(operation, node);
    RealizeAssignment(node, firstAssignment);
    auto secondAssignment = AddPreliminaryAssignment(operation, node);
    RealizeAssignment(node, secondAssignment);

    node->PreemptAssignment(firstAssignment, EAllocationPreemptionReason::Preemption, "");
    node->PreemptAssignment(secondAssignment, EAllocationPreemptionReason::Preemption, "");
    node->PreemptAllocation(secondAssignment->AllocationId);

    auto info = node->BuildSnapshotInfo();
    EXPECT_EQ(1, info.AllocationsToPreemptCount);
    EXPECT_EQ(1, info.PreemptedAllocationsCount);
}

TEST_F(TPoolTreeSnapshotStateTest, PoolTreeSnapshotStateAggregatesAllMaps)
{
    auto node = CreateNode(NNodeTrackerClient::TNodeId(1), TestModule);
    auto operation = CreateOperation();
    auto assignment = AddPreliminaryAssignment(operation, node);
    auto allocation = RealizeAssignment(node, assignment);

    TOperationSnapshotStateMap operationStates;
    EmplaceOrCrash(operationStates, operation->GetId(), operation->BuildSnapshotInfo());

    TAllocationSnapshotStateMap allocationStates;
    EmplaceOrCrash(
        allocationStates,
        allocation->GetId(),
        allocation->BuildSnapshotInfo(operation->GetId()));

    TNodeSnapshotStateMap nodeStates;
    EmplaceOrCrash(nodeStates, node->GetId(), node->BuildSnapshotInfo());

    auto snapshotTime = TInstant::Now();
    auto state = New<TPoolTreeSnapshotStateImpl>(
        std::move(operationStates),
        std::move(nodeStates),
        std::move(allocationStates),
        snapshotTime);

    EXPECT_EQ(snapshotTime, state->GetSnapshotTime());
    EXPECT_EQ(1u, state->OperationStates().size());
    EXPECT_EQ(1u, state->NodeStates().size());
    EXPECT_EQ(1u, state->AllocationStates().size());
    EXPECT_TRUE(state->AllocationStates().contains(allocation->GetId()));
    EXPECT_TRUE(state->OperationStates().contains(operation->GetId()));
    EXPECT_TRUE(state->NodeStates().contains(node->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
