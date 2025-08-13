#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/gpu_allocation_assignment_plan_update.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/iterator/enumerate.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

static inline constexpr i64 GB = 1000 * 1000 * 1000;

static const std::vector<std::string> TestModules{"ALA", "BEG", "EVN"};

static const std::string TestAllocationGroupName{"task"};

static const TJobResources UnitResources = [] {
    TJobResources resources;
    resources.SetCpu(10.0);
    resources.SetGpu(1);
    resources.SetMemory(125 * GB);

    return resources;
}();

static const TJobResources TestNodeResources = UnitResources * 8;

////////////////////////////////////////////////////////////////////////////////

class TGpuAllocationAssignmentPlanUpdateTest
    : public testing::Test
{
public:
    void SetUp()
    {
        NextAvailableNodeId_ = 0;
    }

protected:
    const NLogging::TLogger Logger{"Test"};

    static TGpuAllocationSchedulerConfigPtr GetTestConfig()
    {
        auto config = New<TGpuAllocationSchedulerConfig>();
        config->Modules = TestModules;

        return config;
    }

    TGpuSchedulerNodePtr CreateTestNode(
        std::string module,
        const TJobResources& nodeResources)
    {
        auto node = New<TGpuSchedulerNode>();
        node->SetSchedulingModule(std::move(module));

        auto descriptor = New<TExecNodeDescriptor>();
        descriptor->Id = static_cast<NNodeTrackerClient::TNodeId>(NextAvailableNodeId_);
        descriptor->Addresses.emplace(NNodeTrackerClient::DefaultNetworkName, Format("node-%v", NextAvailableNodeId_));
        descriptor->Online = true;
        descriptor->ResourceLimits = nodeResources;
        node->UpdateDescriptor(std::move(descriptor));

        ++NextAvailableNodeId_;

        return node;
    }

    std::vector<TGpuSchedulerNodePtr> CreateSingleModuleTestNodes(int nodeCount = 1)
    {
        std::vector<TGpuSchedulerNodePtr> nodes;
        nodes.reserve(nodeCount);

        const auto& module = *TestModules.begin();
        for (int i = 0; i < nodeCount; ++i) {
            nodes.push_back(CreateTestNode(module, TestNodeResources));
        }

        return nodes;
    }

    std::vector<TGpuSchedulerNodePtr> CreateMultiModuleTestNodes(
        const THashMap<std::string, int>& nodeCountPerModule)
    {
        std::vector<TGpuSchedulerNodePtr> nodes;
        for (const auto& [module, nodeCount] : nodeCountPerModule) {
            for (int i = 0; i < nodeCount; ++i) {
                nodes.push_back(CreateTestNode(module, TestNodeResources));
            }
        }

        return nodes;
    }

    TAllocationGroupResourcesMap GetSingleGroupOperationNeededResources(
        TJobResourcesWithQuota jobResourcesWithQuota,
        int allocationCount = 1)
    {
        return TAllocationGroupResourcesMap{
            {
                TestAllocationGroupName,
                TAllocationGroupResources{
                    .MinNeededResources = std::move(jobResourcesWithQuota),
                    .AllocationCount = allocationCount
                },
            },
        };
    }

    std::vector<TGpuSchedulerNodePtr> CreateStandardMultiModuleTestNodes()
    {
        constexpr int StandardNodeCount = 10;
        static const THashMap<std::string, int> StandardNodeCountPerModule = [&] {
            THashMap<std::string, int> result;
            for (const auto& module : TestModules) {
                result.emplace(module, StandardNodeCount);
            }
            return result;
        }();

        return CreateMultiModuleTestNodes(StandardNodeCountPerModule);
    }

    TGpuSchedulerOperationPtr CreateTestOperation(
        TAllocationGroupResourcesMap groupedNeededResources,
        EOperationType type = EOperationType::Vanilla,
        std::optional<THashSet<std::string>> specifiedSchedulingModules = {},
        bool gang = false)
    {
        return New<TGpuSchedulerOperation>(
            TOperationId(TGuid::Create()),
            type,
            groupedNeededResources,
            gang,
            std::move(specifiedSchedulingModules));
    }

    TGpuSchedulerOperationPtr CreateSingleGroupTestOperation(
        TJobResourcesWithQuota allocationResources,
        int allocationCount,
        EOperationType type = EOperationType::Vanilla,
        std::optional<THashSet<std::string>> specifiedSchedulingModules = {},
        std::optional<bool> gang = {})
    {
        bool defaultGang = type == EOperationType::Vanilla &&
            allocationResources.GetGpu() == MaxNodeGpuCount &&
            allocationCount > 1;
        return CreateTestOperation(
            GetSingleGroupOperationNeededResources(std::move(allocationResources), allocationCount),
            type,
            std::move(specifiedSchedulingModules),
            gang.value_or(defaultGang));
    }

    TGpuSchedulerOperationPtr CreateFullHostTestOperation(
        int allocationCount = 1,
        EOperationType type = EOperationType::Vanilla,
        std::optional<bool> gang = {},
        std::optional<THashSet<std::string>> specifiedSchedulingModules = {})
    {
        return CreateSingleGroupTestOperation(
            TestNodeResources,
            allocationCount,
            type,
            std::move(specifiedSchedulingModules),
            gang);
    }

    TGpuSchedulerOperationPtr CreateSimpleTestOperation(
        int gpuCount = 1,
        int allocationCount = 1,
        EOperationType type = EOperationType::Vanilla,
        std::optional<THashSet<std::string>> specifiedSchedulingModules = {})
    {
        YT_VERIFY(1 <= gpuCount && gpuCount <= 8);

        return CreateSingleGroupTestOperation(
            UnitResources * gpuCount,
            allocationCount,
            type,
            std::move(specifiedSchedulingModules));
    }

    void DoAllocationAssignmentPlanUpdate(
        const TGpuSchedulerOperationMap& operations,
        const TGpuSchedulerNodeMap& nodes,
        TGpuAllocationSchedulerConfigPtr config = GetTestConfig(),
        TInstant now = {})
    {
        TGpuAllocationAssignmentPlanUpdateExecutor updateExecutor(
            operations,
            nodes,
            now,
            std::move(config),
            Logger);
        updateExecutor.Run();
    }

    void DoAllocationAssignmentPlanUpdate(
        const std::vector<TGpuSchedulerOperationPtr>& operations,
        const std::vector<TGpuSchedulerNodePtr>& nodes,
        TGpuAllocationSchedulerConfigPtr config = GetTestConfig(),
        TInstant now = {})
    {
        TGpuSchedulerOperationMap operationMap;
        for (const auto& operation : operations) {
            EmplaceOrCrash(operationMap, operation->GetId(), operation);
        }

        TGpuSchedulerNodeMap nodeMap;
        for (const auto& node : nodes) {
            EmplaceOrCrash(nodeMap, node->Descriptor()->Id, node);
        }

        DoAllocationAssignmentPlanUpdate(operationMap, nodeMap, std::move(config), now);
    }

    void AddReadyToAssignAllocations(
        const TGpuSchedulerOperationPtr& operation,
        int allocationCount,
        const std::string& allocationGroupName = TestAllocationGroupName)
    {
        auto& allocationGroupResources = GetOrCrash(operation->ReadyToAssignGroupedNeededResources(), allocationGroupName);
        allocationGroupResources.AllocationCount += allocationCount;
    }

    void RemoveAssignment(TGpuSchedulerAssignmentPtr assignment)
    {
        assignment->Operation->RemoveAssignment(assignment);
        assignment->Node->RemoveAssignment(assignment);
    }

    void RemoveOperationAssignments(const TGpuSchedulerOperationPtr& operation)
    {
        for (auto&& assignment : GetItems(operation->Assignments())) {
            RemoveAssignment(std::move(assignment));
        }
    }

    void CheckOperationAssignmentsAreInCorrectModule(const TGpuSchedulerOperationPtr& operation)
    {
        if (const auto& module = operation->SchedulingModule()) {
            for (const auto& assignment : operation->Assignments()) {
                EXPECT_EQ(module, assignment->Node->SchedulingModule());
            }
        }
    }

private:
    ui32 NextAvailableNodeId_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Single module tests.

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSimple)
{
    auto nodes = CreateSingleModuleTestNodes();
    auto operation = CreateSimpleTestOperation();

    EXPECT_FALSE(operation->IsFullHost());
    EXPECT_EQ(1, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(1, operation->GetReadyToAssignNeededAllocationCount());
    EXPECT_FALSE(operation->SpecifiedSchedulingModules());
    EXPECT_FALSE(operation->IsPriorityModuleBindingEnabled());
    EXPECT_FALSE(operation->SchedulingModule());

    DoAllocationAssignmentPlanUpdate({operation}, nodes);

    const auto& node = *nodes.begin();
    ASSERT_EQ(1, std::ssize(node->Assignments()));
    ASSERT_EQ(1, std::ssize(operation->Assignments()));
    EXPECT_EQ(*begin(node->Assignments()), *begin(operation->Assignments()));

    const auto& assignment = *begin(node->Assignments());
    EXPECT_EQ(node.Get(), assignment->Node);
    EXPECT_EQ(operation.Get(), assignment->Operation);
    EXPECT_EQ(TestAllocationGroupName, assignment->AllocationGroupName);
    EXPECT_EQ(UnitResources, assignment->ResourceUsage);

    EXPECT_EQ(UnitResources, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources, operation->AssignedResourceUsage());
    EXPECT_EQ(1, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(0, operation->GetReadyToAssignNeededAllocationCount());
    EXPECT_FALSE(operation->SchedulingModule());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSimpleFullHost)
{
    auto nodes = CreateSingleModuleTestNodes();
    auto operation = CreateFullHostTestOperation();

    EXPECT_TRUE(operation->IsFullHost());
    EXPECT_EQ(1, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(1, operation->GetReadyToAssignNeededAllocationCount());
    EXPECT_FALSE(operation->SpecifiedSchedulingModules());
    EXPECT_FALSE(operation->IsPriorityModuleBindingEnabled());
    EXPECT_FALSE(operation->SchedulingModule());

    DoAllocationAssignmentPlanUpdate({operation}, nodes);

    const auto& node = *nodes.begin();
    ASSERT_EQ(1, std::ssize(node->Assignments()));
    ASSERT_EQ(1, std::ssize(operation->Assignments()));
    EXPECT_EQ(*begin(node->Assignments()), *begin(operation->Assignments()));

    const auto& assignment = *begin(node->Assignments());
    EXPECT_EQ(node.Get(), assignment->Node);
    EXPECT_EQ(operation.Get(), assignment->Operation);
    EXPECT_EQ(TestAllocationGroupName, assignment->AllocationGroupName);
    EXPECT_EQ(TestNodeResources, assignment->ResourceUsage);

    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TestNodeResources, operation->AssignedResourceUsage());
    EXPECT_EQ(1, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(0, operation->GetReadyToAssignNeededAllocationCount());
    EXPECT_EQ(node->SchedulingModule(), operation->SchedulingModule());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSimpleMultipleOperations)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 4),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    ASSERT_EQ(3, std::ssize(node->Assignments()));
    EXPECT_EQ(UnitResources * 7, node->AssignedResourceUsage());

    for (const auto& [index, operation] : Enumerate(operations)) {
        ASSERT_EQ(1, std::ssize(operation->Assignments()));

        const auto& assignment = *begin(operation->Assignments());
        EXPECT_EQ(UnitResources * (1 << index), assignment->ResourceUsage);
        EXPECT_EQ(UnitResources * (1 << index), operation->AssignedResourceUsage());
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSimpleMap)
{
    auto nodes = CreateSingleModuleTestNodes();
    auto operation = CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 4, EOperationType::Map);

    EXPECT_FALSE(operation->IsFullHost());
    EXPECT_EQ(4, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(4, operation->GetReadyToAssignNeededAllocationCount());
    EXPECT_FALSE(operation->SpecifiedSchedulingModules());
    EXPECT_FALSE(operation->IsPriorityModuleBindingEnabled());
    EXPECT_FALSE(operation->SchedulingModule());

    DoAllocationAssignmentPlanUpdate({operation}, nodes);

    const auto& node = *nodes.begin();
    ASSERT_EQ(4, std::ssize(node->Assignments()));
    ASSERT_EQ(4, std::ssize(operation->Assignments()));

    for (const auto& assignment : node->Assignments()) {
        EXPECT_EQ(node.Get(), assignment->Node);
        EXPECT_EQ(operation.Get(), assignment->Operation);
        EXPECT_EQ(TestAllocationGroupName, assignment->AllocationGroupName);
        EXPECT_EQ(UnitResources * 2, assignment->ResourceUsage);
    }

    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TestNodeResources, operation->AssignedResourceUsage());
    EXPECT_EQ(4, operation->GetInitialNeededAllocationCount());
    EXPECT_EQ(0, operation->GetReadyToAssignNeededAllocationCount());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestOperationOrderMix)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 4, EOperationType::Map),
        CreateFullHostTestOperation(),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestBiggerGpuDemandGoesFirst)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 4),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 6),
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 6, operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[4]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestBiggerMapGoesFirst)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 2, EOperationType::Map),
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 3, EOperationType::Map),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 6, operations[1]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestMapWithBiggerDemandGoesFirst)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 2, EOperationType::Map),
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 2, EOperationType::Map),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 6, operations[1]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestGangCannotScheduleOnSingleNode)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 2),
        CreateFullHostTestOperation(/*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[1]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSkewedCpuDemandFirst)
{
    auto skewedAllocationResources = UnitResources;
    skewedAllocationResources.SetCpu(12.0);

    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSingleGroupTestOperation(skewedAllocationResources * 3, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 3),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ((UnitResources + skewedAllocationResources) * 3, node->AssignedResourceUsage());

    EXPECT_EQ(skewedAllocationResources * 3, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 3, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSkewedCpuDemandLast)
{
    auto skewedAllocationResources = UnitResources;
    skewedAllocationResources.SetCpu(12.0);

    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSingleGroupTestOperation(skewedAllocationResources, /*allocationCount*/ 3),
        CreateSimpleTestOperation(/*gpuCount*/ 3),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(UnitResources * 5 + skewedAllocationResources * 2, node->AssignedResourceUsage());

    EXPECT_EQ(skewedAllocationResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 3, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[2]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestOperationAddedBetweenUpdates)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 4),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(UnitResources * 6, node->AssignedResourceUsage());

    auto assignmentsAfterFirstUpdate = node->Assignments();

    EXPECT_EQ(UnitResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());

    operations.push_back(CreateSimpleTestOperation(/*gpuCount*/ 2));

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[2]->AssignedResourceUsage());

    for (const auto& assignment : assignmentsAfterFirstUpdate) {
        EXPECT_TRUE(node->Assignments().contains(assignment));
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestRemoveAssignmentAfterPlanning)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(UnitResources * 4, node->AssignedResourceUsage());

    const auto& operation = operations[0];
    EXPECT_EQ(UnitResources * 4, operation->AssignedResourceUsage());

    RemoveAssignment(*node->Assignments().begin());

    AddReadyToAssignAllocations(operation, /*allocationCount*/ 1);

    EXPECT_EQ(UnitResources * 2, node->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operation->AssignedResourceUsage());
    EXPECT_EQ(1, operation->GetReadyToAssignNeededAllocationCount());

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 4, node->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 4, operation->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestVanillaGoesBeforeMap)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 2, EOperationType::Map),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 6),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 6, operations[2]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestMapPartialAssignment)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 3, EOperationType::Map),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();
    EXPECT_EQ(UnitResources * 6, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(1, operations[0]->GetReadyToAssignNeededAllocationCount());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSmallOperationsPack)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 2);

    std::vector<TGpuSchedulerOperationPtr> operations;
    for (int i = 0; i < 8; ++i) {
        operations.push_back(CreateSimpleTestOperation());
    }

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    for (const auto& operation : operations) {
        ASSERT_EQ(UnitResources, operation->AssignedResourceUsage());
    }

    auto nodeWithAssignment = (*operations[0]->Assignments().begin())->Node;
    for (const auto& operation : operations) {
        EXPECT_EQ(nodeWithAssignment, (*operation->Assignments().begin())->Node);
    }

    auto otherNode = nodes[0] == nodeWithAssignment ? nodes[1] : nodes[0];
    EXPECT_EQ(TestNodeResources, nodeWithAssignment->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), otherNode->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSmallOperationsPackBestEffort)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 2);

    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 3),
        CreateSimpleTestOperation(/*gpuCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 3, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[3]->AssignedResourceUsage());

    auto fullNode = (*operations[0]->Assignments().begin())->Node;
    EXPECT_EQ(fullNode, (*operations[2]->Assignments().begin())->Node);
    EXPECT_EQ(fullNode, (*operations[3]->Assignments().begin())->Node);

    auto otherNode = nodes[0] == fullNode ? nodes[1] : nodes[0];
    EXPECT_EQ(otherNode, (*operations[1]->Assignments().begin())->Node);
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestBiggerGangGoesFirst)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 8);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 1),
        CreateFullHostTestOperation(/*allocationCount*/ 4),
        CreateFullHostTestOperation(/*allocationCount*/ 2),
        CreateFullHostTestOperation(/*allocationCount*/ 6),
        CreateFullHostTestOperation(/*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    for (const auto& node : nodes) {
        EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());
    }

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 2, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 6, operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[4]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[5]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPartiallyScheduledGangGoesFirst)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 4);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 3),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 3, operations[0]->AssignedResourceUsage());

    RemoveAssignment(*operations[0]->Assignments().begin());

    AddReadyToAssignAllocations(operations[0], /*allocationCount*/ 1);

    EXPECT_EQ(TestNodeResources * 2, operations[0]->AssignedResourceUsage());

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 2));

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 3, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestTwoAllocationGroups)
{
    auto cpuIntensiveSkewedAllocationResources = UnitResources * 4;
    cpuIntensiveSkewedAllocationResources.SetCpu(42.0);

    auto memoryIntensiveSkewedAllocationResources = UnitResources * 4;
    memoryIntensiveSkewedAllocationResources.SetCpu(38.0);

    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSingleGroupTestOperation(cpuIntensiveSkewedAllocationResources, /*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    operations.push_back(CreateTestOperation(
        TAllocationGroupResourcesMap{
            {
                Format("%v_1", TestAllocationGroupName),
                TAllocationGroupResources{
                    .MinNeededResources = UnitResources * 4,
                    .AllocationCount = 1,
                },
            },
            {
                Format("%v_2", TestAllocationGroupName),
                TAllocationGroupResources{
                    .MinNeededResources = memoryIntensiveSkewedAllocationResources,
                    .AllocationCount = 1,
                },
            },
        },
        EOperationType::Vanilla,
        /*specifiedSchedulingModules*/ {},
        /*gang*/ false));

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(cpuIntensiveSkewedAllocationResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(memoryIntensiveSkewedAllocationResources, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(1, operations[1]->GetReadyToAssignNeededAllocationCount());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestHeterogeneousNodes)
{
    auto otherNodeResources = TestNodeResources;
    otherNodeResources.SetCpu(120.0);

    const auto& module = *TestModules.begin();
    std::vector<TGpuSchedulerNodePtr> nodes{
        CreateTestNode(module, TestNodeResources),
        CreateTestNode(module, otherNodeResources),
    };

    auto skewedAllocationResources = UnitResources;
    skewedAllocationResources.SetCpu(15.0);

    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSingleGroupTestOperation(skewedAllocationResources, /*allocationCount*/ 16, EOperationType::Map),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(skewedAllocationResources * 13, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(3, operations[0]->GetReadyToAssignNeededAllocationCount());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSimplePreemption)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 6),
        CreateSimpleTestOperation(/*gpuCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    const auto& node = *nodes.begin();

    auto checkBeforePreemption = [&] {
        EXPECT_EQ(TestNodeResources, node->AssignedResourceUsage());

        EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
        EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());
        EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

        EXPECT_EQ(0, operations[0]->GetReadyToAssignNeededAllocationCount());
        EXPECT_EQ(0, operations[1]->GetReadyToAssignNeededAllocationCount());
        EXPECT_EQ(1, operations[2]->GetReadyToAssignNeededAllocationCount());

        EXPECT_TRUE(node->PreemptedAssignments().empty());
        for (const auto& assignment : node->Assignments()) {
            EXPECT_FALSE(assignment->Preempted);
        }
    };
    checkBeforePreemption();

    DoAllocationAssignmentPlanUpdate(operations, nodes);
    checkBeforePreemption();

    operations[2]->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);
    checkBeforePreemption();

    auto preemptibleAssignment = *operations[1]->Assignments().begin();
    preemptibleAssignment->Preemptible = true;

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 7, node->AssignedResourceUsage());

    EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[2]->AssignedResourceUsage());

    EXPECT_EQ(0, operations[0]->GetReadyToAssignNeededAllocationCount());
    EXPECT_EQ(0, operations[1]->GetReadyToAssignNeededAllocationCount());
    EXPECT_EQ(0, operations[2]->GetReadyToAssignNeededAllocationCount());

    EXPECT_EQ(1, std::ssize(node->PreemptedAssignments()));
    EXPECT_EQ(preemptibleAssignment, *node->PreemptedAssignments().begin());
    EXPECT_TRUE(preemptibleAssignment->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::Preemption, preemptibleAssignment->PreemptionReason);
    EXPECT_TRUE(preemptibleAssignment->PreemptionDescription);

    EXPECT_TRUE(operations[1]->Assignments().empty());

    EXPECT_EQ(2, std::ssize(node->Assignments()));
    for (const auto& assignment : node->Assignments()) {
        EXPECT_FALSE(assignment->Preempted);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestNotEnoughPreemptibleAssignments)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 8),
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());

    operations[1]->SetStarving(true);

    auto preemptibleAssignment = *operations[0]->Assignments().begin();
    preemptibleAssignment->Preemptible = true;

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 7, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[1]->AssignedResourceUsage());

    EXPECT_EQ(1, operations[1]->GetReadyToAssignNeededAllocationCount());

    const auto& node = *nodes.begin();
    EXPECT_EQ(1, std::ssize(node->PreemptedAssignments()));

    EXPECT_TRUE(preemptibleAssignment->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::Preemption, preemptibleAssignment->PreemptionReason);
    EXPECT_TRUE(preemptibleAssignment->PreemptionDescription);

    for (const auto& assignment : node->Assignments()) {
        EXPECT_FALSE(assignment->Preempted);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPreemptionOfSeveralAssignments)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 8),
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());

    operations[1]->SetStarving(true);

    auto preemptibleAssignments = GetItems(operations[0]->Assignments(), /*limit*/ 2);
    for (const auto& assignment : preemptibleAssignments) {
        assignment->Preemptible = true;
    }

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());

    const auto& node = *nodes.begin();
    EXPECT_EQ(2, std::ssize(node->PreemptedAssignments()));

    for (const auto& assignment : preemptibleAssignments) {
        EXPECT_TRUE(assignment->Preempted);
        EXPECT_EQ(EAllocationPreemptionReason::Preemption, assignment->PreemptionReason);
        EXPECT_TRUE(assignment->PreemptionDescription);
    }

    for (const auto& assignment : node->Assignments()) {
        EXPECT_FALSE(assignment->Preempted);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPreemptionFromSeveralNodes)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 2);
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 8, operations[1]->AssignedResourceUsage());

    for (const auto& operation : operations) {
        // Check that all operation's assignments are assigned to the same node.
        for (const auto& assignment : operation->Assignments()) {
            EXPECT_EQ((*operation->Assignments().begin())->Node, assignment->Node);
        }
    }

    std::vector<TGpuSchedulerAssignmentPtr> preemptibleAssignments;
    for (const auto& operation : operations) {
        auto preemptibleAssignment = *operation->Assignments().begin();
        preemptibleAssignment->Preemptible = true;
        preemptibleAssignments.push_back(preemptibleAssignment);
    }

    auto starvingOperation = CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 3);
    starvingOperation->SetStarving(true);
    operations.push_back(starvingOperation);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 4, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 6, operations[2]->AssignedResourceUsage());

    for (const auto& node : nodes) {
        EXPECT_EQ(1, std::ssize(node->PreemptedAssignments()));
    }

    for (const auto& assignment : preemptibleAssignments) {
        EXPECT_TRUE(assignment->Preempted);
        EXPECT_EQ(EAllocationPreemptionReason::Preemption, assignment->PreemptionReason);
        EXPECT_TRUE(assignment->PreemptionDescription);
    }

    for (const auto& node : nodes) {
        for (const auto& assignment : node->Assignments()) {
            EXPECT_FALSE(assignment->Preempted);
        }
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestSeveralStarvingOperations)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 4),
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 3),
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

    auto preemptibleAssignments = GetItems(operations[0]->Assignments(), /*limit*/ 2);
    for (const auto& assignment : preemptibleAssignments) {
        assignment->Preemptible = true;
    }

    operations[1]->SetStarving(true);
    operations[2]->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 3, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[2]->AssignedResourceUsage());

    const auto& node = *nodes.begin();
    EXPECT_EQ(2, std::ssize(node->PreemptedAssignments()));

    for (const auto& assignment : preemptibleAssignments) {
        EXPECT_TRUE(assignment->Preempted);
        EXPECT_EQ(EAllocationPreemptionReason::Preemption, assignment->PreemptionReason);
        EXPECT_TRUE(assignment->PreemptionDescription);
    }

    for (const auto& assignment : node->Assignments()) {
        EXPECT_FALSE(assignment->Preempted);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestDoNotPreemptMoreThanNeeded)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 4),
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());

    auto preemptibleAssignments = GetItems(operations[0]->Assignments(), /*limit*/ 2);
    for (const auto& assignment : preemptibleAssignments) {
        assignment->Preemptible = true;
    }

    operations[1]->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 6, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[1]->AssignedResourceUsage());

    for (const auto& node : nodes) {
        EXPECT_EQ(1, std::ssize(node->PreemptedAssignments()));
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestOrderOfAssignmentsDuringPreemption)
{
    auto nodes = CreateSingleModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 1, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 3, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 4, operations[3]->AssignedResourceUsage());

    (*operations[0]->Assignments().begin())->Preemptible = true;
    (*operations[2]->Assignments().begin())->Preemptible = true;
    (*operations[3]->Assignments().begin())->Preemptible = true;

    operations[1]->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 4, operations[3]->AssignedResourceUsage());

    const auto& node = *nodes.begin();
    EXPECT_EQ(2, std::ssize(node->PreemptedAssignments()));
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestOrderOfNodesDuringPreemption)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 3);
    std::vector<TGpuSchedulerOperationPtr> operations{
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 2),
        CreateSimpleTestOperation(/*gpuCount*/ 2, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 5, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 1),
        CreateSimpleTestOperation(/*gpuCount*/ 5, /*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    ASSERT_EQ(UnitResources * 8, operations[0]->AssignedResourceUsage());
    ASSERT_EQ(UnitResources * 2, operations[1]->AssignedResourceUsage());
    ASSERT_EQ(UnitResources * 5, operations[2]->AssignedResourceUsage());
    ASSERT_EQ(UnitResources * 3, operations[3]->AssignedResourceUsage());
    ASSERT_EQ(UnitResources * 5, operations[4]->AssignedResourceUsage());

    (*operations[0]->Assignments().begin())->Preemptible = true;
    (*operations[1]->Assignments().begin())->Preemptible = true;
    (*operations[2]->Assignments().begin())->Preemptible = true;
    (*operations[3]->Assignments().begin())->Preemptible = true;

    operations.push_back(CreateSimpleTestOperation(/*gpuCount*/ 3, /*allocationCount*/ 3));
    operations.back()->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 5, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 5, operations[4]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 9, operations[5]->AssignedResourceUsage());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostRegularPreemption)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 10);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 9));

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 9, operations[1]->AssignedResourceUsage());

    operations.push_back(CreateFullHostTestOperation());

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 9, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

    EXPECT_EQ(operations[1]->SchedulingModule(), operations[2]->SchedulingModule());
    EXPECT_TRUE(operations[2]->WaitingForAssignmentsSince());

    operations[2]->SetStarving(true);

    auto smallAssignment = *operations[0]->Assignments().begin();
    smallAssignment->Preemptible = true;

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 9, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 1, operations[2]->AssignedResourceUsage());
    EXPECT_FALSE(operations[2]->WaitingForAssignmentsSince());

    EXPECT_TRUE(smallAssignment->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::Preemption, smallAssignment->PreemptionReason);
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostAggressivePreemption)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 10);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateSimpleTestOperation(/*gpuCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 10));

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 9, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(now, operations[1]->WaitingForAssignmentsSince());

    now += TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(UnitResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 9, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(now - TDuration::Seconds(1), operations[1]->WaitingForAssignmentsSince());

    auto smallAssignment = *operations[0]->Assignments().begin();

    now += config->FullHostAggressivePreemptionTimeout;
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 10, operations[1]->AssignedResourceUsage());
    EXPECT_FALSE(operations[1]->WaitingForAssignmentsSince());

    EXPECT_TRUE(smallAssignment->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::FullHostAggressivePreemption, smallAssignment->PreemptionReason);
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostAggressivePreemptionDoesNotPreemptOtherFullHost)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 10);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 5),
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 2),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 8, operations[1]->AssignedResourceUsage());

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 5));

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 4, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(now, operations[2]->WaitingForAssignmentsSince());

    now += TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 4, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(now - TDuration::Seconds(1), operations[2]->WaitingForAssignmentsSince());

    now += config->FullHostAggressivePreemptionTimeout;
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 5, operations[2]->AssignedResourceUsage());
    EXPECT_FALSE(operations[2]->WaitingForAssignmentsSince());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPreemptibleFullHostOperation)
{
    auto nodes = CreateSingleModuleTestNodes(/*nodeCount*/ 10);
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 5),
        CreateFullHostTestOperation(/*allocationCount*/ 5),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 5, operations[1]->AssignedResourceUsage());

    operations.push_back(CreateSimpleTestOperation());

    operations[0]->SetPreemptible(true);

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 5, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ((*nodes.begin())->SchedulingModule(), operations[0]->GetUsedSchedulingModule());

    operations[2]->SetStarving(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 4, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 5, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources, operations[2]->AssignedResourceUsage());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ((*nodes.begin())->SchedulingModule(), operations[0]->GetUsedSchedulingModule());

    auto smallAssignment = *operations[2]->Assignments().begin();
    const auto& expectedNode = smallAssignment->Node;
    ASSERT_EQ(1, std::ssize(expectedNode->PreemptedAssignments()));
    const auto& preemptedAssignment = *expectedNode->PreemptedAssignments().begin();
    EXPECT_EQ(operations[0].Get(), preemptedAssignment->Operation);
    EXPECT_EQ(EAllocationPreemptionReason::Preemption, preemptedAssignment->PreemptionReason);

    operations[2]->SetStarving(false);
    smallAssignment->Preemptible = true;

    operations[0]->SetPreemptible(false);
    AddReadyToAssignAllocations(operations[0], /*allocationCount*/ 1);

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(expectedNode->SchedulingModule(), operations[0]->SchedulingModule());
    EXPECT_EQ(now, operations[0]->WaitingForAssignmentsSince());

    now += config->FullHostAggressivePreemptionTimeout + TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ(TestNodeResources * 5, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 5, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

    EXPECT_TRUE(smallAssignment->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::FullHostAggressivePreemption, smallAssignment->PreemptionReason);
}

////////////////////////////////////////////////////////////////////////////////

// Multiple modules tests.

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostSingleAllocationOperationsPack)
{
    auto nodes = CreateStandardMultiModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations;
    for (int i = 0; i < 10; ++i) {
        operations.push_back(CreateFullHostTestOperation());
    }

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    for (const auto& operation : operations) {
        EXPECT_EQ(TestNodeResources, operation->AssignedResourceUsage());
    }

    const auto& module = operations[0]->SchedulingModule();
    ASSERT_TRUE(module);

    for (const auto& operation : operations) {
        EXPECT_EQ(module, operation->SchedulingModule());
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostSingleAllocationOperationsPackBestEffort)
{
    auto nodes = CreateStandardMultiModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 1),
        CreateFullHostTestOperation(/*allocationCount*/ 4),
        CreateFullHostTestOperation(/*allocationCount*/ 2),
        CreateFullHostTestOperation(/*allocationCount*/ 8),
        CreateFullHostTestOperation(/*allocationCount*/ 1),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 4, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 2, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[4]->AssignedResourceUsage());

    const auto& firstModule = operations[3]->SchedulingModule();
    ASSERT_TRUE(firstModule);
    EXPECT_EQ(firstModule, operations[2]->SchedulingModule());

    const auto& secondModule = operations[1]->SchedulingModule();
    ASSERT_TRUE(secondModule);
    EXPECT_EQ(secondModule, operations[0]->SchedulingModule());
    EXPECT_EQ(secondModule, operations[4]->SchedulingModule());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostSpecifiedModules)
{
    auto nodes = CreateStandardMultiModuleTestNodes();

    std::vector<TGpuSchedulerOperationPtr> operations;
    {
        std::vector<std::pair<int, std::optional<THashSet<std::string>>>> operationDescriptions{
            {1, {}},
            {4, {{"EVN"}}},
            {2, {}},
            {8, {{"ALA"}}},
            {1, {{"BEG"}}},
        };
        for (auto&& [allocationCount, specifiedSchedulingModules] : std::move(operationDescriptions)) {
            operations.push_back(CreateFullHostTestOperation(
                /*allocationCount*/ allocationCount,
                EOperationType::Vanilla,
                /*gang*/ {},
                std::move(specifiedSchedulingModules)));
        };
    }

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 4, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 2, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[4]->AssignedResourceUsage());

    EXPECT_EQ("EVN", operations[0]->SchedulingModule());
    EXPECT_EQ("EVN", operations[1]->SchedulingModule());
    EXPECT_EQ("ALA", operations[2]->SchedulingModule());
    EXPECT_EQ("ALA", operations[3]->SchedulingModule());
    EXPECT_EQ("BEG", operations[4]->SchedulingModule());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostLessSpecifiedModulesGoesFirst)
{
    auto nodes = CreateStandardMultiModuleTestNodes();

    std::vector<TGpuSchedulerOperationPtr> operations;
    {
        std::vector<std::pair<int, std::optional<THashSet<std::string>>>> operationDescriptions{
            {10, {{"ALA", "BEG"}}},
            {1, {{"ALA"}}},
            {1, {{"BEG"}}},
        };
        for (auto&& [allocationCount, specifiedSchedulingModules] : std::move(operationDescriptions)) {
            operations.push_back(CreateFullHostTestOperation(
                /*allocationCount*/ allocationCount,
                EOperationType::Vanilla,
                /*gang*/ {},
                std::move(specifiedSchedulingModules)));
        };
    }

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources, operations[2]->AssignedResourceUsage());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ("ALA", operations[1]->SchedulingModule());
    EXPECT_EQ("BEG", operations[2]->SchedulingModule());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPriorityModuleBinding)
{
    auto nodes = CreateStandardMultiModuleTestNodes();

    std::vector<TGpuSchedulerOperationPtr> operations;
    {
        std::vector<std::pair<int, std::optional<THashSet<std::string>>>> operationDescriptions{
            {1, {{"EVN"}}},
            {2, {{"ALA"}}},
            {3, {{"BEG"}}},
        };
        for (auto&& [allocationCount, specifiedSchedulingModules] : std::move(operationDescriptions)) {
            operations.push_back(CreateFullHostTestOperation(
                /*allocationCount*/ allocationCount,
                EOperationType::Vanilla,
                /*gang*/ {},
                std::move(specifiedSchedulingModules)));
        };
    }

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 10));

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(now, operations[3]->WaitingForModuleBindingSince());

    operations[3]->SetPriorityModuleBindingEnabled(true);

    now += TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TInstant::FromValue(117), operations[3]->WaitingForModuleBindingSince());

    auto assignmentToBePreempted = *operations[0]->Assignments().begin();

    now += config->PriorityModuleBindingTimeout;
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ("EVN", operations[3]->SchedulingModule());
    EXPECT_EQ(TestNodeResources * 10, operations[3]->AssignedResourceUsage());
    EXPECT_FALSE(operations[3]->WaitingForModuleBindingSince());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[0]->AssignedResourceUsage());

    EXPECT_TRUE(assignmentToBePreempted->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::EvictionFromSchedulingModule, assignmentToBePreempted->PreemptionReason);

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPriorityModuleBindingWithSpecifiedModule)
{
    auto nodes = CreateStandardMultiModuleTestNodes();

    std::vector<TGpuSchedulerOperationPtr> operations;
    {
        std::vector<std::pair<int, std::optional<THashSet<std::string>>>> operationDescriptions{
            {1, {{"EVN"}}},
            {2, {{"ALA"}}},
            {3, {{"BEG"}}},
        };
        for (auto&& [allocationCount, specifiedSchedulingModules] : std::move(operationDescriptions)) {
            operations.push_back(CreateFullHostTestOperation(
                /*allocationCount*/ allocationCount,
                EOperationType::Vanilla,
                /*gang*/ {},
                std::move(specifiedSchedulingModules)));
        };
    }

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    operations.push_back(CreateFullHostTestOperation(
        /*allocationCount*/ 10,
        EOperationType::Vanilla,
        /*gang*/ true,
        /*specifiedSchedulingModules*/ {{"BEG"}}));

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(now, operations[3]->WaitingForModuleBindingSince());

    operations[3]->SetPriorityModuleBindingEnabled(true);

    now += TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TInstant::FromValue(117), operations[3]->WaitingForModuleBindingSince());

    now += config->PriorityModuleBindingTimeout;
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ("BEG", operations[3]->SchedulingModule());
    EXPECT_EQ(TestNodeResources * 10, operations[3]->AssignedResourceUsage());
    EXPECT_FALSE(operations[3]->WaitingForModuleBindingSince());

    EXPECT_FALSE(operations[2]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[2]->AssignedResourceUsage());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestPriorityModuleBindingOtherPriorityOperationsAreUnavailable)
{
    auto nodes = CreateStandardMultiModuleTestNodes();

    std::vector<TGpuSchedulerOperationPtr> operations;
    {
        std::vector<std::pair<int, std::optional<THashSet<std::string>>>> operationDescriptions{
            {1, {{"EVN"}}},
            {2, {{"ALA"}}},
            {3, {{"BEG"}}},
        };
        for (auto&& [allocationCount, specifiedSchedulingModules] : std::move(operationDescriptions)) {
            operations.push_back(CreateFullHostTestOperation(
                /*allocationCount*/ allocationCount,
                EOperationType::Vanilla,
                /*gang*/ {},
                std::move(specifiedSchedulingModules)));
        };
    }

    operations[0]->SetPriorityModuleBindingEnabled(true);

    auto config = GetTestConfig();
    auto now = TInstant::FromValue(117);

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    operations.push_back(CreateFullHostTestOperation(/*allocationCount*/ 10));

    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(now, operations[3]->WaitingForModuleBindingSince());

    operations[3]->SetPriorityModuleBindingEnabled(true);

    now += TDuration::Seconds(1);
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_FALSE(operations[3]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[3]->AssignedResourceUsage());
    EXPECT_EQ(TInstant::FromValue(117), operations[3]->WaitingForModuleBindingSince());

    now += config->PriorityModuleBindingTimeout;
    DoAllocationAssignmentPlanUpdate(operations, nodes, config, now);

    EXPECT_EQ("ALA", operations[3]->SchedulingModule());
    EXPECT_EQ(TestNodeResources * 10, operations[3]->AssignedResourceUsage());
    EXPECT_FALSE(operations[3]->WaitingForModuleBindingSince());

    EXPECT_FALSE(operations[1]->SchedulingModule());
    EXPECT_EQ(TJobResources(), operations[1]->AssignedResourceUsage());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostMap)
{
    auto nodes = CreateStandardMultiModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 15, EOperationType::Map),
        CreateFullHostTestOperation(/*allocationCount*/ 7),
        CreateFullHostTestOperation(/*allocationCount*/ 8),
        CreateSimpleTestOperation(),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 14, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 7, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources, operations[3]->AssignedResourceUsage());

    EXPECT_EQ(1, operations[0]->GetReadyToAssignNeededAllocationCount());

    for (const auto& operation : operations) {
        CheckOperationAssignmentsAreInCorrectModule(operation);
    }
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostOperationPreemptedAndLaterReturnsToSameModule)
{
    auto nodes = CreateStandardMultiModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 2),
        CreateFullHostTestOperation(/*allocationCount*/ 8),
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 40),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 160, operations[2]->AssignedResourceUsage());

    auto expectedModule = operations[0]->SchedulingModule();

    operations.push_back(CreateSimpleTestOperation());
    operations[3]->SetStarving(true);

    operations[0]->SetPreemptible(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 1, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 160, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[3]->AssignedResourceUsage());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ(expectedModule, operations[0]->GetUsedSchedulingModule());

    RemoveOperationAssignments(operations[3]);
    operations.pop_back();

    RemoveOperationAssignments(operations[2]);
    operations.pop_back();

    operations[0]->SetPreemptible(false);
    AddReadyToAssignAllocations(operations[0], /*allocationCount*/ 1);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());

    EXPECT_EQ(expectedModule, operations[0]->SchedulingModule());
}

TEST_F(TGpuAllocationAssignmentPlanUpdateTest, TestFullHostOperationPreemptedAndLaterBoundToOtherModule)
{
    auto nodes = CreateStandardMultiModuleTestNodes();
    std::vector<TGpuSchedulerOperationPtr> operations = {
        CreateFullHostTestOperation(/*allocationCount*/ 2),
        CreateFullHostTestOperation(/*allocationCount*/ 8),
        CreateSimpleTestOperation(/*gpuCount*/ 4, /*allocationCount*/ 40),
    };

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 160, operations[2]->AssignedResourceUsage());

    auto oldModule = operations[0]->SchedulingModule();

    operations.push_back(CreateSimpleTestOperation());
    operations[3]->SetStarving(true);

    operations[0]->SetPreemptible(true);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 1, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 160, operations[2]->AssignedResourceUsage());
    EXPECT_EQ(UnitResources * 1, operations[3]->AssignedResourceUsage());

    EXPECT_FALSE(operations[0]->SchedulingModule());
    EXPECT_EQ(oldModule, operations[0]->GetUsedSchedulingModule());

    auto assignmentInOldModule = *operations[0]->Assignments().begin();

    RemoveOperationAssignments(operations[3]);
    operations.pop_back();

    RemoveOperationAssignments(operations[2]);
    operations.pop_back();

    // Add operation that will block us from binding to the old module.
    operations.push_back(CreateFullHostTestOperation());

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 1, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 1, operations[2]->AssignedResourceUsage());

    EXPECT_EQ(oldModule, operations[2]->SchedulingModule());

    operations[0]->SetPreemptible(false);
    AddReadyToAssignAllocations(operations[0], /*allocationCount*/ 1);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    // NB: Only one assignment was planned, because old assignment was preempted.
    EXPECT_EQ(TestNodeResources * 1, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 1, operations[2]->AssignedResourceUsage());

    EXPECT_NE(oldModule, operations[0]->SchedulingModule());
    EXPECT_TRUE(assignmentInOldModule->Preempted);
    EXPECT_EQ(EAllocationPreemptionReason::OperationBoundToOtherModule, assignmentInOldModule->PreemptionReason);

    AddReadyToAssignAllocations(operations[0], /*allocationCount*/ 1);

    DoAllocationAssignmentPlanUpdate(operations, nodes);

    EXPECT_EQ(TestNodeResources * 2, operations[0]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 8, operations[1]->AssignedResourceUsage());
    EXPECT_EQ(TestNodeResources * 1, operations[2]->AssignedResourceUsage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
