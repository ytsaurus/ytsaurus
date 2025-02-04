#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/fair_share_tree.h>
#include <yt/yt/server/scheduler/fair_share_tree_element.h>
#include <yt/yt/server/scheduler/fair_share_tree_allocation_scheduler.h>
#include <yt/yt/server/scheduler/gpu_allocation_scheduler.h>
#include <yt/yt/server/scheduler/operation_controller.h>
#include <yt/yt/server/scheduler/resource_tree.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <yt/yt/library/vector_hdrf/resource_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGpuAllocationSchedulerHostMock
    : public TRefCounted
{
public:
    TGpuAllocationSchedulerHostMock()
    { }

    void RegisterOperation(
        TOperationId operationId,
        TJobResourcesWithQuota allocationResource,
        i64 allocationCount,
        i64 fairAllocationCount = -1,
        bool prioritySchedulingModuleAssignmentEnabled = false)
    {
        if (fairAllocationCount == -1) {
            fairAllocationCount = allocationCount;
        }

        OperationResources_[operationId] = TAllocationResources{
            .Resources = allocationResource,
            .DemandAllocationCount = allocationCount,
            .FairAllocationCount = fairAllocationCount,
        };

        THashSet<TGpuAllocationStatePtr> allocationStates;
        for (int i = 0; i < allocationCount; ++i) {
            auto allocation = New<TGpuAllocationState>();
            allocation->OperationId = operationId;
            allocation->Resources = OperationResources_[operationId].Resources.ToJobResources();
            allocationStates.insert(allocation);
        }
        AllocationStatesPerOperation_[operationId] = std::move(allocationStates);

        OperationModulePreemptionInfo_[operationId] = prioritySchedulingModuleAssignmentEnabled;
    }

    void RegisterNode(TJobResourcesWithQuota nodeResources)
    {
        TotalResources_ += nodeResources;
    }

    TOperationRuntimeAttributes GetOperationRuntimeAttributes(
        TOperationId operationId) const
    {
        auto& allocationResources = GetOrCrash(OperationResources_, operationId);
        auto operationResourcesDemand = allocationResources.Resources.ToJobResources() * allocationResources.DemandAllocationCount;
        auto operationResourcesFair = allocationResources.Resources.ToJobResources() * allocationResources.FairAllocationCount;

        auto totalJobresources = TotalResources_.ToJobResources();
        auto fairShare = TResourceVector::FromJobResources(operationResourcesFair, totalJobresources);

        return TOperationRuntimeAttributes{
            .Demand = operationResourcesDemand,
            .UsageAtUpdate = TJobResources(),
            .FairResourceAmount = fairShare[EJobResourceType::Gpu] * NVectorHdrf::GetResource(totalJobresources, EJobResourceType::Gpu),
            .AllocationResources = GetOrCrash(AllocationStatesPerOperation_, operationId),
            .EffectivePrioritySchedulingModuleAssignmentEnabled = GetOrCrash(OperationModulePreemptionInfo_, operationId),
        };
    }

    TJobResources GetOperationMinNeededResources(
        TOperationId operationId) const
    {
        auto& allocationResources = GetOrCrash(OperationResources_, operationId);

        return allocationResources.Resources;
    }

    void SetAllocationPreemptible(const TGpuAllocationStatePtr& allocation, bool preemptible = true)
    {
        allocation->Preemptible = preemptible;
    }

private:
    struct TAllocationResources{
        TJobResourcesWithQuota Resources;
        i64 DemandAllocationCount = 0;
        i64 FairAllocationCount = 0;
    };

    THashMap<TOperationId, TAllocationResources> OperationResources_;
    THashMap<TOperationId, THashSet<TGpuAllocationStatePtr>> AllocationStatesPerOperation_;
    THashMap<TOperationId, bool> OperationModulePreemptionInfo_;
    TJobResourcesWithQuota TotalResources_;
};

using TGpuAllocationSchedulerHostMockPtr = TIntrusivePtr<TGpuAllocationSchedulerHostMock>;

////////////////////////////////////////////////////////////////////////////////

class DISABLED_TGpuAllocationSchedulerTest
    : public testing::Test
{
public:
    void SetUp() override
    {
        auto minSpareAllocationResources = New<TJobResourcesConfig>();
        minSpareAllocationResources->UserSlots = 1;
        minSpareAllocationResources->Cpu = 1.0;
        minSpareAllocationResources->Memory = 1;

        SchedulerConfig_->MinSpareAllocationResourcesOnNode = minSpareAllocationResources;

        TreeConfig_->AggressivePreemptionSatisfactionThreshold = 0.5;
        TreeConfig_->MinChildHeapSize = 3;
        TreeConfig_->EnableConditionalPreemption = true;
        TreeConfig_->UseResourceUsageWithPrecommit = false;
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren = true;

        TreeConfig_->BatchOperationScheduling = New<TBatchOperationSchedulingConfig>();
        TreeConfig_->BatchOperationScheduling->BatchSize = 3;

        Config_->ModuleType = ESchedulingModuleType::InfinibandCluster;
        Config_->InfinibandClusters = {"MODULE1", "MODULE2"};
        Config_->PreemptForLargeOperationTimeout = TDuration::Seconds(0);
        Config_->PriorityModuleAssignmentTimeout = TDuration::Seconds(0);
    }

protected:
    TSchedulerConfigPtr SchedulerConfig_ = New<TSchedulerConfig>();
    TFairShareStrategyTreeConfigPtr TreeConfig_ = New<TFairShareStrategyTreeConfig>();
    NConcurrency::TActionQueuePtr NodeShardActionQueue_ = New<NConcurrency::TActionQueue>("NodeShard");

    TGpuAllocationSchedulerConfigPtr Config_ = New<TGpuAllocationSchedulerConfig>();

    TSchedulingStageProfilingCounters RegularSchedulingProfilingCounters_{NProfiling::TProfiler("/regular_test_scheduling_stage")};
    TSchedulingStageProfilingCounters PreemptiveSchedulingProfilingCounters_{NProfiling::TProfiler("/preemptive_test_scheduling_stage")};

    int SlotIndex_ = 0;
    NNodeTrackerClient::TNodeId ExecNodeId_ = NNodeTrackerClient::TNodeId(0);

    void TearDown() override
    {
        // NB(eshcherbin): To prevent "Promise abandoned" exceptions in tree allocation scheduler's periodic activities.
        BIND([] { }).AsyncVia(NodeShardActionQueue_->GetInvoker()).Run().Get().ThrowOnError();
    }

    TGpuAllocationSchedulerHostMockPtr CreateTestGpuAllocationSchedulerHostMock()
    {
        return New<TGpuAllocationSchedulerHostMock>();
    }

    TGpuAllocationSchedulerPtr CreateTestGpuScheduler()
    {
        return New<TGpuAllocationScheduler>(
            GetCurrentInvoker(),
            Config_,
            StrategyLogger());
    }

    std::vector<TOperationId> CreateTestOperations(
        const TGpuAllocationSchedulerHostMockPtr& host,
        const TGpuAllocationSchedulerPtr& gpuScheduler,
        const TJobResourcesWithQuota& allocationResources,
        int operationCount = 1,
        int allocationCountInOperation = 1,
        int fairAllocationCountInOperation = -1,
        bool isGang = true,
        bool prioritySchedulingModuleAssignmentEnabled = false)
    {
        std::vector<TOperationId> operations;
        operations.reserve(operationCount);
        for (int i = 0; i < operationCount; ++i) {
            auto operationId = TOperationId(TGuid::Create());
            operations.push_back(operationId);

            host->RegisterOperation(
                operationId,
                allocationResources,
                allocationCountInOperation,
                fairAllocationCountInOperation,
                prioritySchedulingModuleAssignmentEnabled);

            gpuScheduler->RegisterOperation(operationId, isGang);
            gpuScheduler->UpdateOperationRuntimeAttributes(operationId, host->GetOperationRuntimeAttributes(operationId));
            gpuScheduler->UpdateOperationMinNeededResources(operationId, host->GetOperationMinNeededResources(operationId));
        }
        return operations;
    }

    TExecNodePtr CreateTestExecNode(const TJobResourcesWithQuota& nodeResources, TBooleanFormulaTags tags = {})
    {
        auto diskResources = TDiskResources{
            .DiskLocationResources = {
                TDiskResources::TDiskLocationResources{
                    .Usage = 0,
                    .Limit = GetOrDefault(
                        nodeResources.DiskQuota().DiskSpacePerMedium,
                        NChunkClient::DefaultSlotsMediumIndex),
                },
            },
        };

        auto nodeId = ExecNodeId_;
        ExecNodeId_ = NNodeTrackerClient::TNodeId(nodeId.Underlying() + 1);
        auto execNode = New<TExecNode>(nodeId, NNodeTrackerClient::TNodeDescriptor(), ENodeState::Online);
        execNode->ResourceLimits() = nodeResources.ToJobResources();
        execNode->DiskResources() = std::move(diskResources);

        execNode->SetTags(std::move(tags));

        return execNode;
    }

    std::pair<std::vector<TExecNodePtr>, TJobResources> CreateTestExecNodeList(int count, const TJobResourcesWithQuota& nodeResources)
    {
        std::vector<TExecNodePtr> execNodes;
        TJobResources totalResources;
        for (int i = 0; i < count; i++) {
            totalResources += nodeResources.ToJobResources();
            execNodes.push_back(CreateTestExecNode(nodeResources));
        }
        return {execNodes, totalResources};
    }

    void RegisterNodesInHost(
        const TGpuAllocationSchedulerHostMockPtr& host,
        const std::vector<TExecNodePtr>& execNodes)
    {
        for (const auto& node : execNodes) {
            host->RegisterNode(node->ResourceLimits());
        }
    }

    void RegisterNodesInGpuScheduler(
        const TGpuAllocationSchedulerPtr& gpuScheduler,
        const std::vector<TExecNodePtr>& execNodes,
        int nodesInFirstModule = 0,
        int unregisteredNodes = 0)
    {
        YT_ASSERT(std::ssize(execNodes) >= unregisteredNodes);

        for (int i = 0; i < std::ssize(execNodes) - unregisteredNodes; ++i) {
            auto node = execNodes[i];

            auto descriptor = New<TExecNodeDescriptor>();
            descriptor->ResourceLimits = node->ResourceLimits();
            descriptor->ResourceUsage = TJobResources{};
            descriptor->InfinibandCluster = i < nodesInFirstModule ? "MODULE1" : "MODULE2";
            gpuScheduler->RegisterNode(node->GetId(), TFairShareTreeAllocationSchedulerNodeState{
                .Descriptor = descriptor,
            });
        }
    }

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    TAllocationPtr CreateTestAllocation(
        TAllocationId allocationId,
        TOperationId operationId,
        const TExecNodePtr& execNode,
        TInstant startTime,
        TJobResourcesWithQuota allocationResources)
    {
        return New<TAllocation>(
            allocationId,
            operationId,
            /*incarnationId*/ TWrapperTraits<TIncarnationId>::RecursiveWrap(TGuid::Create()),
            /*controllerEpoch*/ TControllerEpoch(0),
            execNode,
            startTime,
            TAllocationStartDescriptor{
                .Id = allocationId,
                .ResourceLimits = TJobResourcesWithQuota{
                    allocationResources.ToJobResources(),
                    allocationResources.DiskQuota()},
            },
            /*preemptionMode*/ EPreemptionMode::Normal,
            /*treeId*/ "",
            /*schedulingIndex*/ UndefinedSchedulingIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

MATCHER_P2(ResourceVectorNear, vec, absError, "") {
    return TResourceVector::Near(arg, vec, absError);
}

#define EXPECT_RV_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVectorNear(vector1, 1e-7))

////////////////////////////////////////////////////////////////////////////////

// Schedule Gpu allocations tests.

TEST_F(DISABLED_TGpuAllocationSchedulerTest, Simple)
{
    const size_t nodeCount = 1;
    const size_t operationCount = 1;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, SimpleMultipleOperations)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 5;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);
    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 1ul);
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, SimpleMultipleSmallOperations)
{
    const size_t nodeCount = 2;
    const size_t operationCount = 9;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(1);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes, /*nodesInFirstModule*/ 0, /*unregisteredNodes*/ 1);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), 1ul);
    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), operationCount - 1);
        for (const auto& allocation : allocations) {
            scheduledOperationIds.insert(allocation->OperationId);
        }
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount - 1);
    // EXPECT_EQ(gpuScheduler->GetSmallOperationsToSchedule().size(), 1ul);

    RegisterNodesInGpuScheduler(gpuScheduler, {execNodes.back()});
    gpuScheduler->ScheduleAllocations();

    scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);
    // EXPECT_TRUE(gpuScheduler->GetSmallOperationsToSchedule().empty());
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, NotEnoughNodes)
{
    const size_t nodeCount = 7;
    const size_t operationCount = 7;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);

    // Remain two nodes unregister to satisfy demand.
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes, /*nodesInFirstModule*/ 0, /*unregisteredNodes*/ 2);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), operationCount - 2);
    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 1ul);
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount - 2);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, NewOperation)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 5;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount - 1);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), 4ul);
    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 1ul);
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), 4ul);

    // Scheduled 4 operations, register a new one.

    CreateTestOperations(host, gpuScheduler, allocationResources, 1)[0];

    gpuScheduler->ScheduleAllocations();

    scheduledOperations = gpuScheduler->GetScheduledAllocations();

    EXPECT_EQ(scheduledOperations.size(), operationCount);
    scheduledOperationIds = THashSet<TOperationId>();
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 1ul);
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, MultihostOperation)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 2;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes, /*nodesInFirstModule*/ 3);

    auto operation1 = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 2)[0];
    CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 3)[0];

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 1ul);
        auto operation = gpuScheduler->GetOperationState((*allocations.begin())->OperationId);

        EXPECT_TRUE(operation->SchedulingModule().has_value());

        EXPECT_EQ(operation->SchedulingModule().value(),
            gpuScheduler->GetNodeState(node)->Descriptor->InfinibandCluster);

        EXPECT_LT(operation->GetNeededResources(), NVectorHdrf::RatioComparisonPrecision);
        if (operation->OperationId() == operation1) {
            EXPECT_EQ(operation->GetResourceUsage(), 2 * 8);
        } else {
            EXPECT_EQ(operation->GetResourceUsage(), 3 * 8);
        }

        scheduledOperationIds.insert(operation->OperationId());
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, NotEnoughNodesInModule)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 1;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);

    // Remain two nodes unregister to satisfy demand.
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes, /*nodesInFirstModule*/ 3);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount, /*allocationCountInOperation*/ 4);

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_TRUE(scheduledOperations.empty());

    // EXPECT_EQ(gpuScheduler->GetLargeOperationsToAssign().size(), operationCount);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, UnregisterOperation)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 5;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    gpuScheduler->UnregisterOperation(operations[0]);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : gpuScheduler->GetScheduledAllocations()) {
        if (allocations.size() > 0) {
            scheduledOperationIds.insert((*allocations.begin())->OperationId);
        }
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount - 1ul);
    // EXPECT_TRUE(gpuScheduler->GetSmallOperationsToSchedule().empty());
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, UnregisterNode)
{
    const size_t nodeCount = 5;
    const size_t operationCount = 5;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(host, gpuScheduler, allocationResources, operationCount);

    gpuScheduler->ScheduleAllocations();

    gpuScheduler->UnregisterNode(execNodes[0]->GetId());
    gpuScheduler->UnregisterNode(execNodes[1]->GetId());

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 2ul);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), operationCount - 2ul);
    // EXPECT_EQ(gpuScheduler->GetSmallOperationsToSchedule().size(), 2ul);
    // for (auto operation : gpuScheduler->GetSmallOperationsToSchedule()) {
    //     EXPECT_EQ(operation->GetNeededResources(), 8.0);
    //     EXPECT_EQ(operation->ResourceUsage(), 0.0);
    // }
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, UnregisterNodeUnderMultihostOperation)
{
    const size_t nodeCount = 4;
    const size_t operationCount = 2;
    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        operationCount,
        /*allocationCountInOperation*/ 2);

    gpuScheduler->ScheduleAllocations();

    gpuScheduler->UnregisterNode(execNodes[0]->GetId());

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 1ul);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        scheduledOperationIds.insert((*allocations.begin())->OperationId);
    }

    // Operation is in both 'Scheduled' and 'ToSchedule' states.
    EXPECT_EQ(scheduledOperationIds.size(), operationCount);
    // EXPECT_EQ(gpuScheduler->GetLargeOperationsToSchedule().size(), 1ul);

    // auto preemptedOperation = *gpuScheduler->GetLargeOperationsToSchedule().begin();
    // EXPECT_EQ(preemptedOperation->GetNeededResources(), 8.0);
    // EXPECT_EQ(preemptedOperation->ResourceUsage(), 8.0);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, DemandIsNotFullySatisfied)
{
    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(1, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operations = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 2,
        /*fairAllocationCountInOperation*/ 1);

    gpuScheduler->ScheduleAllocations();

    // auto waitingOperations = gpuScheduler->GetWaitingOperations();
    // EXPECT_EQ(waitingOperations.size(), 1ul);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, PreemptSmallOperationsForLarge)
{
    const size_t nodeCount = 4;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes, /*nodesInFirstModule*/ 4);

    TJobResourcesWithQuota allocationResources1;
    allocationResources1.SetGpu(2);
    CreateTestOperations(host, gpuScheduler, allocationResources1, /*operationCount*/ 1)[0];

    TJobResourcesWithQuota allocationResources2;
    allocationResources2.SetGpu(2);
    CreateTestOperations(host, gpuScheduler, allocationResources2, /*operationCount*/ 1)[0];

    TJobResourcesWithQuota allocationResources3;
    allocationResources3.SetGpu(7);
    CreateTestOperations(host, gpuScheduler, allocationResources3, /*operationCount*/ 1)[0];

    TJobResourcesWithQuota largeAllocationResources;
    largeAllocationResources.SetGpu(8);
    auto largeOperation1 = CreateTestOperations(
        host,
        gpuScheduler,
        largeAllocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 2)[0];

    gpuScheduler->ScheduleAllocations();

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        for (const auto& allocation : allocations) {
            scheduledOperationIds.insert(allocation->OperationId);
        }
    }
    EXPECT_EQ(scheduledOperationIds.size(), 4ul);

    auto largeOperation2 = CreateTestOperations(
        host,
        gpuScheduler,
        largeAllocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 2)[0];
    auto largeOperation2State = gpuScheduler->GetOperationState(largeOperation2);

    gpuScheduler->ScheduleAllocations();
    EXPECT_EQ(largeOperation2State->GetResourceUsage(), 8.0 * 0);
    gpuScheduler->ScheduleAllocations();
    EXPECT_EQ(largeOperation2State->GetResourceUsage(), 8.0 * 2);

    scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount);

    scheduledOperationIds = THashSet<TOperationId>();
    for (const auto& [node, operations] : scheduledOperations) {
        EXPECT_EQ(operations.size(), 1ul);
        scheduledOperationIds.insert((*operations.begin())->OperationId);
    }
    EXPECT_EQ(scheduledOperationIds.size(), 2ul);

    EXPECT_TRUE(scheduledOperationIds.contains(largeOperation1));
    EXPECT_TRUE(scheduledOperationIds.contains(largeOperation2));

    // EXPECT_TRUE(gpuScheduler->GetSmallOperationsToSchedule().contains(operation1));
    // EXPECT_TRUE(gpuScheduler->GetSmallOperationsToSchedule().contains(operation2));
    // EXPECT_TRUE(gpuScheduler->GetSmallOperationsToSchedule().contains(operation3));
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, SimpleNonGangOperation)
{
    const size_t nodeCount = 4;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(4);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 8,
        /*fairAllocationCountInOperation*/ 4,
        /*isGang*/ false)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    // auto waitingOperations = gpuScheduler->GetWaitingOperations();
    // EXPECT_TRUE(waitingOperations.empty());

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 2ul);

    THashSet<TOperationId> scheduledOperationIds;
    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 2ul);
        for (const auto& allocation : allocations) {
            scheduledOperationIds.insert(allocation->OperationId);
        }
    }
    EXPECT_EQ(scheduledOperationIds.size(), 1ul);

    EXPECT_EQ(operation->GetNeededResources(), 0.0);
    EXPECT_EQ(operation->GetResourceUsage(), 4.0 * 4);
    EXPECT_EQ(operation->GetTotalResourceUsage(), 4.0 * 4);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, NonGangOperationInProgress)
{
    const size_t nodeCount = 4;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(4);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 8,
        /*fairAllocationCountInOperation*/ 4,
        /*isGang*/ false)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    // auto waitingOperations = gpuScheduler->GetWaitingOperations();
    // EXPECT_TRUE(waitingOperations.empty());

    auto scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 2ul);

    for (const auto& [node, allocations] : scheduledOperations) {
        EXPECT_EQ(allocations.size(), 2ul);
    }
    EXPECT_EQ(operation->GetNeededResources(), 0.0);
    EXPECT_EQ(operation->GetResourceUsage(), 4.0 * 4);
    EXPECT_EQ(operation->GetTotalResourceUsage(), 4.0 * 4);

    gpuScheduler->OnAllocationFinished(
        operationId,
        /*allocationId*/ *scheduledOperations.begin()->second.begin(),
        /*nodeId*/ scheduledOperations.begin()->first);

    scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 2ul);

    for (const auto& [node, operations] : scheduledOperations) {
        if (node == scheduledOperations.begin()->first) {
            EXPECT_EQ(operations.size(), 1ul);
            // EXPECT_EQ(operations.count(*operations.begin()), 1ul);
        } else {
            EXPECT_EQ(operations.size(), 2ul);
            // EXPECT_EQ(operations.count(*operations.begin()), 2ul);
        }
    }
    EXPECT_EQ(operation->GetNeededResources(), 4.0 * 1);
    EXPECT_EQ(operation->GetResourceUsage(), 4.0 * 3);
    EXPECT_EQ(operation->GetTotalResourceUsage(), 4.0 * 4);

    gpuScheduler->ScheduleAllocations();

    scheduledOperations = gpuScheduler->GetScheduledAllocations();
    EXPECT_EQ(scheduledOperations.size(), nodeCount - 2ul);

    for (const auto& [node, operations] : scheduledOperations) {
        EXPECT_EQ(operations.size(), 2ul);
        // EXPECT_EQ(operations.count(*operations.begin()), 2ul);
    }
    EXPECT_EQ(operation->GetNeededResources(), 0.0);
    EXPECT_EQ(operation->GetResourceUsage(), 4.0 * 4);
    EXPECT_EQ(operation->GetTotalResourceUsage(), 4.0 * 5);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, UnregisterNonGangOperation)
{
    const size_t nodeCount = 3;
    const size_t operationCount = 2;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(2);

    auto operations = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ operationCount,
        /*allocationCountInOperation*/ 6,
        /*fairAllocationCountInOperation*/ 6,
        /*isGang*/ false);

    gpuScheduler->ScheduleAllocations();

    gpuScheduler->UnregisterOperation(operations[0]);

    int allocationCount = 0;
    for (const auto& [node, allocations] : gpuScheduler->GetScheduledAllocations()) {
        for (const auto& allocation : allocations) {
            ++allocationCount;
            EXPECT_EQ(allocation->OperationId, operations[1]);
        }
    }
    EXPECT_EQ(allocationCount, 6);
    EXPECT_EQ(gpuScheduler->GetOperationState(operations[1])->GetNeededResources(), 0.0);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, DoNotShceduleGangOperationsPartially)
{
    const size_t nodeCount = 2;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ nodeCount + 1)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 3);

    for (const auto& [node, allocations] : gpuScheduler->GetScheduledAllocations()) {
        EXPECT_TRUE(allocations.empty());
    }
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, ShceduleNonGangOperationsPartially)
{
    const size_t nodeCount = 2;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ nodeCount + 1,
        /*fairAllocationCountInOperation*/ nodeCount + 1,
        /*isGang*/ false)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 1);
    for (const auto& [node, allocations] : gpuScheduler->GetScheduledAllocations()) {
        EXPECT_EQ(allocations.size(), 1ul);
        EXPECT_EQ((*allocations.begin())->OperationId, operation->OperationId());
    }
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, ModuleAssignmentPreemption)
{
    const size_t nodeCount = 2;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(8);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ nodeCount)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    EXPECT_TRUE(operation->SchedulingModule().has_value());

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 0);

    auto priorityOperationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ nodeCount,
        /*fairAllocationCountInOperation*/ nodeCount,
        /*isGang*/ true,
        /*prioritySchedulingModuleAssignmentEnabled*/ true)[0];
    auto priorityOperation = gpuScheduler->GetOperationState(priorityOperationId);

    gpuScheduler->ScheduleAllocations();
    EXPECT_TRUE(!priorityOperation->SchedulingModule().has_value());
    gpuScheduler->ScheduleAllocations();
    gpuScheduler->ScheduleAllocations();

    EXPECT_TRUE(!operation->SchedulingModule().has_value());
    EXPECT_TRUE(priorityOperation->SchedulingModule().has_value());

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 2);
    EXPECT_EQ(priorityOperation->GetNeededResources(), 8.0 * 0);

    for (const auto& [node, allocations] : gpuScheduler->GetScheduledAllocations()) {
        EXPECT_EQ(allocations.size(), 1ul);
        EXPECT_EQ((*allocations.begin())->OperationId, priorityOperation->OperationId());
    }
}


TEST_F(DISABLED_TGpuAllocationSchedulerTest, ScheduledAllocationsPreemption)
{
    const size_t nodeCount = 3;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(2);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ (nodeCount - 1) * 4,
        /*fairAllocationCountInOperation*/ (nodeCount - 1) * 4,
        /*isGang*/ false)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    auto scheduledAllocations = gpuScheduler->GetScheduledAllocations();
    std::vector<TGpuAllocationStatePtr> allocationsInFirstNode;
    for (const auto& allocation : scheduledAllocations.begin()->second) {
        allocationsInFirstNode.push_back(allocation);
    }

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 0);

    TJobResourcesWithQuota priorityAllocationResources;
    priorityAllocationResources.SetGpu(7);

    auto priorityOperationId = CreateTestOperations(
        host,
        gpuScheduler,
        priorityAllocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ nodeCount - 1,
        /*fairAllocationCountInOperation*/ nodeCount - 1,
        /*isGang*/ false)[0];
    auto priorityOperation = gpuScheduler->GetOperationState(priorityOperationId);

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 0);
    EXPECT_EQ(priorityOperation->GetNeededResources(), 7.0 * 1);

    host->SetAllocationPreemptible(allocationsInFirstNode[0]);
    host->SetAllocationPreemptible(allocationsInFirstNode[1]);
    gpuScheduler->UpdateOperationRuntimeAttributes(operation->OperationId(), host->GetOperationRuntimeAttributes(operation->OperationId()));

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 0);
    EXPECT_EQ(priorityOperation->GetNeededResources(), 7.0 * 1);

    host->SetAllocationPreemptible(allocationsInFirstNode[2]);
    host->SetAllocationPreemptible(allocationsInFirstNode[3]);
    gpuScheduler->UpdateOperationRuntimeAttributes(operation->OperationId(), host->GetOperationRuntimeAttributes(operation->OperationId()));

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation->GetNeededResources(), 8.0 * 1);
    EXPECT_EQ(priorityOperation->GetNeededResources(), 7.0 * 0);
}

TEST_F(DISABLED_TGpuAllocationSchedulerTest, NotEnoughResources)
{
    const size_t nodeCount = 1;

    auto host = CreateTestGpuAllocationSchedulerHostMock();
    auto gpuScheduler = CreateTestGpuScheduler();

    TJobResourcesWithQuota nodeResources;
    nodeResources.SetGpu(8);

    auto [execNodes, totalResources] = CreateTestExecNodeList(nodeCount, nodeResources);

    RegisterNodesInHost(host, execNodes);
    RegisterNodesInGpuScheduler(gpuScheduler, execNodes);

    TJobResourcesWithQuota allocationResources;
    allocationResources.SetGpu(1);

    auto operationId = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 1,
        /*fairAllocationCountInOperation*/1,
        /*isGang*/ false)[0];
    auto operation = gpuScheduler->GetOperationState(operationId);

    gpuScheduler->ScheduleAllocations();

    auto operationId1 = CreateTestOperations(
        host,
        gpuScheduler,
        allocationResources,
        /*operationCount*/ 1,
        /*allocationCountInOperation*/ 8,
        /*fairAllocationCountInOperation*/ 8,
        /*isGang*/ false)[0];
    auto operation1 = gpuScheduler->GetOperationState(operationId1);

    gpuScheduler->ScheduleAllocations();

    EXPECT_EQ(operation1->GetNeededResources(), 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
