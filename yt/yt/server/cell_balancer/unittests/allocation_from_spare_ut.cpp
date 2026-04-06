#include "helpers.h"

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/helpers.h>
#include <yt/yt/server/cell_balancer/input_state.h>
#include <yt/yt/server/cell_balancer/mutations.h>
#include <yt/yt/server/cell_balancer/orchid_bindings.h>
#include <yt/yt/server/cell_balancer/pod_id_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NCellBalancer{
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("BundleController");

////////////////////////////////////////////////////////////////////////////////

class TNoAllocatorTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
      /*initiallyDecommissioned*/ bool,
      /*decommissionReleasedNodes*/ bool>>
{
protected:
    static constexpr int CellsPerNode = 3;

    static void SetUpTestSuite()
    {
        NLogging::TLogManager::Get()->ConfigureFromEnv();
    }

    int Foo()
    {
        return 1;
    }

    TSchedulerInputState GenerateInputContext(int nodeCount, int cellsPerNode = 0, int proxyCount = 0)
    {
        auto input = GenerateSimpleInputContext(nodeCount, cellsPerNode, proxyCount);
        input.Config->EnableSpareNodeAssignment = false;
        input.Config->HasInstanceAllocatorService = false;
        input.Config->DecommissionReleasedNodes = std::get<1>(GetParam());
        // All nodes and bundles must have the same resource guarantee in this mode.
        input.Zones.begin()->second->SpareTargetConfig =
            CloneYsonStruct(input.Bundles["bigd"]->TargetConfig);
        input.Zones.begin()->second->SpareTargetConfig->CpuLimits->WriteThreadPoolSize = 1;
        input.BundleStates[SpareBundleName] = New<TBundleControllerState>();
        input.BundleStates["bigd"] = New<TBundleControllerState>();
        input.Bundles["bigd"]->EnableNodeTagFilterManagement = true;
        return input;
    }

    THashSet<std::string> GenerateNodesForBundle(
        TSchedulerInputState& inputState,
        const std::string& bundleName,
        int nodeCount,
        TGenerateNodeOptions options = {})
    {
        options.DC = std::nullopt;
        auto nodes = NCellBalancer::GenerateNodesForBundle(inputState, bundleName, nodeCount, options);
        for (const auto& address : nodes) {
            const auto& node = inputState.TabletNodes[address];

            node->BundleControllerAnnotations->DeallocationStrategy =
                DeallocationStrategyReturnToSpareBundle;

            if (bundleName == SpareBundleName) {
                if (IsInitiallyDecommissioned()) {
                    node->Decommissioned = true;
                }

                for (const auto& slot : node->TabletSlots) {
                    slot->State = TabletSlotStateEmpty;
                }
            }
        }
        return nodes;
    }

    bool IsInitiallyDecommissioned() const
    {
        return std::get<0>(GetParam());
    }
};

////////////////////////////////////////////////////////////////////////////////

void ApplyMutations(TSchedulerInputState* input, const TSchedulerMutations& mutations)
{
    int appliedCount = 0;

    ApplyChangedStates(input, mutations);
    appliedCount += ssize(mutations.ChangedStates);

    for (const auto& [k, v] : mutations.NewAllocations) {
        input->AllocationRequests[k] = v;
        ++appliedCount;
    }
    for (const auto& [k, v] : mutations.ChangedAllocations) {
        input->AllocationRequests[k] = v;
        ++appliedCount;
    }
    for (const auto& [node, annotations] : mutations.ChangedNodeAnnotations) {
        GetOrCrash(input->TabletNodes, node)->BundleControllerAnnotations = annotations;
        ++appliedCount;
    }
    for (const auto& [node, flag] : mutations.ChangedDecommissionedFlag) {
        GetOrCrash(input->TabletNodes, node)->Decommissioned = flag;
        ++appliedCount;
    }
    for (const auto& id : mutations.CompletedAllocations) {
        EraseOrCrash(input->AllocationRequests, id);
        ++appliedCount;
    }
    for (const auto& [node, tags] : mutations.ChangedNodeUserTags) {
        GetOrCrash(input->TabletNodes, node)->UserTags = tags;
        ++appliedCount;
    }
    for (const auto& [bundleName, count] : mutations.CellsToCreate) {
        GenerateTabletCellsForBundle(*input, bundleName, count);
        ++appliedCount;
    }
    if (mutations.BundlesDynamicConfig) {
        input->BundlesDynamicConfig = *mutations.BundlesDynamicConfig;
        ++appliedCount;
    }
    appliedCount += ssize(mutations.CellsToRemove);

    ASSERT_EQ(appliedCount, mutations.GetMutationCount());
}

void ValidateDecommissions(const TSchedulerMutations& mutations, int count, bool expectedValue)
{
    EXPECT_EQ(count, ssize(mutations.ChangedDecommissionedFlag));
    for (const auto& [_, flag] : mutations.ChangedDecommissionedFlag) {
        EXPECT_EQ(flag, expectedValue);
    }
}

void FinishTabletCellRemoval(TSchedulerInputState* input)
{
    const auto& bundle = input->Bundles["bigd"];
    auto& bundleTabletCells = bundle->TabletCellIds;
    const auto& state = input->BundleStates["bigd"];

    for (const auto& [cellId, _] : state->RemovingCells) {
        YT_LOG_DEBUG("Removing tablet cell (CellId: %v)", cellId);

        EXPECT_TRUE(Contains(bundleTabletCells, cellId));
        bundleTabletCells.erase(std::ranges::find(bundleTabletCells, cellId));

        EXPECT_TRUE(input->TabletCells.contains(cellId));
        input->TabletCells.erase(cellId);
    }
}

void FakeApplyDynamicConfigAtNodes(TSchedulerInputState* input)
{
    auto getTargetSlotCount = [&] (const TTabletNodeInfoPtr& nodeInfo)
        -> std::optional<int>
    {
        const auto& tags = nodeInfo->UserTags;
        if (tags.empty()) {
            return std::nullopt;
        }

        const auto& tag = *tags.begin();

        // Not all tests set dynamic config. Use its value for those that do.
        if (auto config = GetOrDefault(input->BundlesDynamicConfig, tag)) {
            return config->CpuLimits->WriteThreadPoolSize;
        }

        for (const auto& [bundleName, bundleInfo] : input->Bundles) {
            if (tags.contains(bundleInfo->NodeTagFilter)) {
                return *bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize;
            }
        }

        return std::nullopt;
    };

    for (const auto& [address, nodeInfo] : input->TabletNodes) {
        bool empty = false;
        int targetSlotCount;

        if (auto slotCount = getTargetSlotCount(nodeInfo)) {
            targetSlotCount = *slotCount;
        } else {
            empty = true;
            targetSlotCount = *input->Zones.begin()->second->SpareTargetConfig->CpuLimits->WriteThreadPoolSize;
        }

        if (ssize(nodeInfo->TabletSlots) == targetSlotCount) {
            continue;
        }

        nodeInfo->TabletSlots.clear();
        for (int i = 0; i < targetSlotCount; ++i) {
            auto slot = New<TTabletSlot>();
            if (empty) {
                slot->State = TabletSlotStateEmpty;
            }
            nodeInfo->TabletSlots.push_back(slot);
        }

        YT_LOG_DEBUG("Applied dynamic config at node (NodeAddress: %v, SlotCount: %v)",
            address,
            targetSlotCount);
    }
}

void RemoveCellsFromDecommissionedNodes(TSchedulerInputState* input)
{
    for (const auto& [address, nodeInfo] : input->TabletNodes) {
        if (nodeInfo->Decommissioned && GetUsedSlotCount(nodeInfo) > 0) {
            YT_LOG_DEBUG("Removed cells from decommissioned node (NodeAddress: %v)", address);
            SetTabletSlotsState(*input, address, TabletSlotStateEmpty);
        }
    }
}

void RemoveCellsFromOfflineNodes(TSchedulerInputState* input)
{
    for (const auto& [address, nodeInfo] : input->TabletNodes) {
        if ((!nodeInfo->IsOnline() || nodeInfo->UserTags.empty()) && GetUsedSlotCount(nodeInfo) > 0) {
            YT_LOG_DEBUG("Removed cells from offline node or node without tags "
                "(NodeAddress: %v, Online: %v, UserTags: %v)",
                address,
                nodeInfo->IsOnline(),
                nodeInfo->UserTags);
            SetTabletSlotsState(*input, address, TabletSlotStateEmpty);
        }
    }
}

void ValidateConsistency(const TSchedulerInputState& input)
{
    const auto& state = GetOrCrash(input.BundleStates, "bigd");
    EXPECT_TRUE(state->BundleNodeAssignments.empty());
    EXPECT_TRUE(state->BundleNodeReleasements.empty());
    EXPECT_TRUE(state->SpareNodeAssignments.empty());
    EXPECT_TRUE(state->SpareNodeReleasements.empty());
    EXPECT_TRUE(state->NodeAllocations.empty());
    EXPECT_TRUE(state->NodeDeallocations.empty());

    const auto& bundle = GetOrCrash(input.Bundles, "bigd");

    const auto& tag = bundle->NodeTagFilter;

    int bundleNodeCount = 0;
    for (const auto& [address, node] : input.TabletNodes) {
        if (node->BundleControllerAnnotations->AllocatedForBundle == "bigd") {
            EXPECT_TRUE(node->IsOnline()) << address;
            EXPECT_TRUE(!node->Decommissioned) << address;
            EXPECT_EQ(1, ssize(node->UserTags));
            if (!node->UserTags.empty()) {
                EXPECT_EQ(*node->UserTags.begin(), tag) << address;
            }

            ++bundleNodeCount;
        } else {
            EXPECT_EQ(node->BundleControllerAnnotations->AllocatedForBundle, SpareBundleName) << address;
            EXPECT_EQ(0, GetUsedSlotCount(node)) << address;
            EXPECT_EQ(0, ssize(node->UserTags)) << address;
        }
    }

    EXPECT_EQ(bundleNodeCount, bundle->TargetConfig->TabletNodeCount);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TNoAllocatorTest, AllocationFromScratch)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    std::string selectedNode;

    // Create new allocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, ssize(mutations.AlertsToFire));

        VerifyNodeAllocationRequests(mutations, 1);
        EXPECT_EQ(1, ssize(mutations.ChangedStates));

        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Assign node to allocation.
    // Set node tags.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedAllocations));
        EXPECT_EQ(1, ssize(mutations.ChangedNodeAnnotations));

        selectedNode = mutations.ChangedAllocations.begin()->second->Status->NodeId;
        EXPECT_TRUE(mutations.ChangedNodeAnnotations.contains(selectedNode));
        const auto& annotation = mutations.ChangedNodeAnnotations[selectedNode];
        EXPECT_EQ(annotation->AllocatedForBundle, "bigd");

        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Undecommission picked node.
    if (IsInitiallyDecommissioned()) {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedDecommissionedFlag));
        EXPECT_EQ(mutations.ChangedDecommissionedFlag[selectedNode], false);
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Complete allocation.
    // Remove allocation from bundle state.
    // Set node tags.
    // Decommission assigning node.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.CompletedAllocations));
        EXPECT_EQ(1, ssize(mutations.ChangedStates));

        EXPECT_EQ(1, ssize(mutations.ChangedNodeUserTags));

        if (input.Config->DecommissionReleasedNodes) {
            EXPECT_EQ(1, ssize(mutations.ChangedDecommissionedFlag));
            EXPECT_EQ(4, mutations.GetMutationCount());
        } else {
            EXPECT_EQ(3, mutations.GetMutationCount());
        }

        ApplyMutations(&input, mutations);
    }

    // Node has not applied dynamic config.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Apply dynamic config.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        const auto& nodeInfo = input.TabletNodes[selectedNode];
        nodeInfo->TabletSlots.clear();
        for (int i = 0; i < CellsPerNode; ++i) {
            nodeInfo->TabletSlots.push_back(New<TTabletSlot>());
        }
    }

    // Undecommission assigning node.
    if (input.Config->DecommissionReleasedNodes) {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedDecommissionedFlag));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Complete assignment.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, OfflineNodeReplacement)
{
    auto input = GenerateInputContext(/*nodeCount*/ 2, CellsPerNode);
    GenerateNodesForBundle(input, "bigd", 2, {.SlotCount = CellsPerNode});
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode * 2);

    for (const auto& [_, node] : input.TabletNodes) {
        if (node->BundleControllerAnnotations->AllocatedForBundle == "bigd") {
            node->State = "offline";
            node->LastSeenTime = TInstant::Now() - TDuration::Hours(1);
            for (const auto& slot : node->TabletSlots) {
                slot->State = TabletSlotStateEmpty;
            }
        }
    }

    // Create new allocations.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        VerifyNodeAllocationRequests(mutations, 2);
        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(3, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Assign node to allocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(2, ssize(mutations.ChangedAllocations));
        EXPECT_EQ(2, ssize(mutations.ChangedNodeAnnotations));

        EXPECT_EQ(4, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Undecommission picked nodes.
    if (IsInitiallyDecommissioned()) {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ValidateDecommissions(mutations, 2, false);
        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Complete allocations.
    // Remove allocations from bundle state.
    // Set node tags.
    // Decommission assigning nodes.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(2, ssize(mutations.CompletedAllocations));
        EXPECT_EQ(1, ssize(mutations.ChangedStates));

        EXPECT_EQ(2, ssize(mutations.ChangedNodeUserTags));

        if (input.Config->DecommissionReleasedNodes) {
            ValidateDecommissions(mutations, 2, true);
            EXPECT_EQ(7, mutations.GetMutationCount());
        } else {
            EXPECT_EQ(5, mutations.GetMutationCount());
        }

        ApplyMutations(&input, mutations);
    }

    // Nodes have not applied dynamic config.

    // Decommission released nodes.
    if (input.Config->DecommissionReleasedNodes) {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ValidateDecommissions(mutations, 2, true);
        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Change deallocated nodes |allocated_for_bundle| attribute to "".
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(2, ssize(mutations.ChangedNodeAnnotations));
        for (const auto& [node, annotation] : mutations.ChangedNodeAnnotations) {
            EXPECT_EQ("", annotation->AllocatedForBundle);
        }

        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Apply dynamic config.
    for (const auto& [address, _] : input.BundleStates["bigd"]->BundleNodeAssignments) {
        const auto& nodeInfo = input.TabletNodes[address];
        nodeInfo->TabletSlots.clear();
        for (int i = 0; i < CellsPerNode; ++i) {
            nodeInfo->TabletSlots.push_back(New<TTabletSlot>());
        }
    }

    // Undecommission assigning nodes.
    // Change deallocated nodes |allocated_for_bundle| attribute to "spare"
    // and remove tags.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(2, ssize(mutations.ChangedNodeAnnotations));
        for (const auto& [node, annotation] : mutations.ChangedNodeAnnotations) {
            EXPECT_EQ("spare", annotation->AllocatedForBundle);
        }
        EXPECT_EQ(2, ssize(mutations.ChangedNodeUserTags));

        int expectedCount = 4;

        if (input.Config->DecommissionReleasedNodes) {
            ValidateDecommissions(mutations, 2, false);
            expectedCount += 2;
        } else {
            // Assignment is completed.
            EXPECT_EQ(1, ssize(mutations.ChangedStates));
            ++expectedCount;
        }

        EXPECT_EQ(expectedCount, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Complete deallocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, CmsMaintenance)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = true, .SlotCount = CellsPerNode});
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    for (const auto& [_, node] : input.TabletNodes) {
        if (node->BundleControllerAnnotations->AllocatedForBundle == "bigd") {
            node->CmsMaintenanceRequests["foo"] = New<TCmsMaintenanceRequest>();
            break;
        }
    }

    for (int i = 0; i < 100; ++i) {
        YT_LOG_DEBUG("Step %v %i", __LINE__, i);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ApplyMutations(&input, mutations);

        if (RandomNumber<ui32>() % 5 == 0) {
            FakeApplyDynamicConfigAtNodes(&input);
        }
        if (RandomNumber<ui32>() % 5 == 0) {
            RemoveCellsFromDecommissionedNodes(&input);
        }

        RemoveCellsFromOfflineNodes(&input);
    }

    ValidateConsistency(input);
}

TEST_P(TNoAllocatorTest, Stress)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, "bigd", 3, {.SetFilterTag = true, .SlotCount = CellsPerNode});
    GenerateNodesForBundle(input, SpareBundleName, 10, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    auto runIteration = [&] {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);
        ApplyMutations(&input, mutations);
    };

    auto rand = [] (int n) {
        return RandomNumber<ui32>() % n == 0;
    };

    auto getRandomNode = [&] {
        auto its = GetSortedIterators(input.TabletNodes);
        return *its[RandomNumber<ui32>() % ssize(its)];
    };

    auto getGoodNodeCount = [&] {
        int count = 0;
        for (const auto& [node, info] : input.TabletNodes) {
            count += info->IsOnline() && info->CmsMaintenanceRequests.empty();
        }
        return count;
    };

    for (int i = 0; i < 500; ++i) {
        YT_LOG_DEBUG("Step %v %i", __LINE__, i);

        if (rand(30)) {
            auto [node, info] = getRandomNode();
            YT_LOG_DEBUG("Disturbance: mark node offline (Address: %v)", node);
            info->State = "offline";
        }

        if (rand(25)) {
            auto [node, info] = getRandomNode();
            YT_LOG_DEBUG("Disturbance: mark node online (Address: %v)", node);
            info->State = "online";
        }

        if (rand(30)) {
            auto [node, info] = getRandomNode();
            YT_LOG_DEBUG("Disturbance: CMS (Address: %v)", node);
            info->CmsMaintenanceRequests["foo"] = New<TCmsMaintenanceRequest>();
        }

        if (rand(25)) {
            auto [node, info] = getRandomNode();
            YT_LOG_DEBUG("Disturbance: remove CMS (Address: %v)", node);
            info->CmsMaintenanceRequests.clear();
        }

        if (RandomNumber<ui32>() % 10 == 0) {
            FakeApplyDynamicConfigAtNodes(&input);
        }
        if (RandomNumber<ui32>() % 10 == 0) {
            RemoveCellsFromDecommissionedNodes(&input);
        }

        RemoveCellsFromOfflineNodes(&input);

        runIteration();
    }

    while (getGoodNodeCount() < 3) {
        auto [node, info] = getRandomNode();
        YT_LOG_DEBUG("Disturbance: remove CMS and mark node online (Address: %v)", node);
        info->CmsMaintenanceRequests.clear();
        info->State = "online";
    }

    for (int i = 0; i < 30; ++i) {
        YT_LOG_DEBUG("Step %v %i", __LINE__, i);

        FakeApplyDynamicConfigAtNodes(&input);
        RemoveCellsFromDecommissionedNodes(&input);
        RemoveCellsFromOfflineNodes(&input);
        runIteration();
    }

    ValidateConsistency(input);
}

TEST_P(TNoAllocatorTest, PendingAssignmentsWithoutDcForbidDeallocation)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    // Allocate two nodes for bigd. They should be not ready yet.

    for (auto [count, it] = std::pair{0, input.TabletNodes.begin()};
        count < 2;
        ++count, ++it)
    {
        it->second->BundleControllerAnnotations->AllocatedForBundle = "bigd";
        it->second->Decommissioned = false;
    }

    input.Bundles["bigd"]->EnableInstanceAllocation = false;

    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(2, ssize(mutations.ChangedNodeUserTags));

        if (input.Config->DecommissionReleasedNodes) {
            EXPECT_EQ(2, ssize(mutations.ChangedDecommissionedFlag));
            EXPECT_EQ(5, mutations.GetMutationCount());
        } else {
            EXPECT_EQ(3, mutations.GetMutationCount());
        }

        ApplyMutations(&input, mutations);
    }

    input.Bundles["bigd"]->EnableInstanceAllocation = true;

    // No deallocation must happen.
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, NodeGoneOfflineDuringAllocation)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateNodesForBundle(input, "bigd", 1, {.SetFilterTag = true, .SlotCount = CellsPerNode});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    for (const auto& [address, node] : input.TabletNodes) {
        if (node->BundleControllerAnnotations->AllocatedForBundle == "bigd") {
            YT_LOG_DEBUG("Node marked as offline (NodeAddress: %v)", address);
            node->State = "offline";
            node->LastSeenTime = TInstant::Now() - TDuration::Hours(1);
            for (const auto& slot : node->TabletSlots) {
                slot->State = TabletSlotStateEmpty;
            }
        }
    }

    // Create new allocations.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        VerifyNodeAllocationRequests(mutations, 1);
        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Assign node to allocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedAllocations));
        EXPECT_EQ(1, ssize(mutations.ChangedNodeAnnotations));

        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    auto allocationId = input.BundleStates["bigd"]->NodeAllocations.begin()->first;
    auto firstNode = input.AllocationRequests[allocationId]->Status->NodeId;
    YT_LOG_DEBUG("Node marked as offline during allocation (NodeAddress: %v)", firstNode);
    ASSERT_TRUE(!firstNode.empty());
    input.TabletNodes[firstNode]->State = "offline";

    // Assign another node to allocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedAllocations));
        EXPECT_EQ(1, ssize(mutations.ChangedNodeAnnotations));
        ASSERT_TRUE(mutations.ChangedAllocations.contains(allocationId));
        EXPECT_NE(GetOrCrash(mutations.ChangedAllocations, allocationId)->Status->NodeId, firstNode);

        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    for (int i = 0; i < 10; ++i) {
        YT_LOG_DEBUG("Step %v %v", __LINE__, i);

        FakeApplyDynamicConfigAtNodes(&input);
        RemoveCellsFromOfflineNodes(&input);

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ApplyMutations(&input, mutations);
    }

    ValidateConsistency(input);
}

TEST_P(TNoAllocatorTest, NoAssignmentOnDeallocatedNode)
{
    auto input = GenerateInputContext(/*nodeCount*/ 1, CellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    auto bundleNodes = GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = true, .SlotCount = CellsPerNode});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);

    auto doomedNode = *bundleNodes.begin();

    {
        const auto& nodeInfo = input.TabletNodes[doomedNode];
        nodeInfo->UserTags.clear();
        nodeInfo->TabletSlots.clear();
        nodeInfo->State = "offline";
    }

    // Create new deallocation.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    input.TabletNodes[doomedNode]->State = "online";

    if (input.Config->DecommissionReleasedNodes) {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ValidateDecommissions(mutations, 1, true);
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedNodeAnnotations));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedNodeAnnotations));
        EXPECT_EQ(1, ssize(mutations.ChangedNodeUserTags));
        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, IncreaseCellsPerNode)
{
    const int OldCellsPerNode = 3;
    const int NewCellsPerNode = 4;

    auto input = GenerateInputContext(/*nodeCount*/ 2, OldCellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = true, .SlotCount = OldCellsPerNode});
    GenerateTabletCellsForBundle(input, "bigd", OldCellsPerNode * 2);
    input.Bundles["bigd"]->EnableTabletNodeDynamicConfig = true;

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_TRUE(mutations.BundlesDynamicConfig.has_value());
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    input.Bundles["bigd"]->TargetConfig->CpuLimits->WriteThreadPoolSize = NewCellsPerNode;
    FakeApplyDynamicConfigAtNodes(&input);

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.CellsToCreate));
        EXPECT_TRUE(mutations.BundlesDynamicConfig.has_value());
        EXPECT_EQ(2, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    FakeApplyDynamicConfigAtNodes(&input);

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, DecreaseCellsPerNode)
{
    const int OldCellsPerNode = 4;
    const int NewCellsPerNode = 3;

    auto input = GenerateInputContext(/*nodeCount*/ 2, OldCellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateNodesForBundle(input, "bigd", 2, {.SetFilterTag = true, .SlotCount = OldCellsPerNode});
    GenerateTabletCellsForBundle(input, "bigd", OldCellsPerNode * 2);
    input.Bundles["bigd"]->EnableTabletNodeDynamicConfig = true;

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_TRUE(mutations.BundlesDynamicConfig.has_value());
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    input.Bundles["bigd"]->TargetConfig->CpuLimits->WriteThreadPoolSize = NewCellsPerNode;
    FakeApplyDynamicConfigAtNodes(&input);

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(2, ssize(mutations.CellsToRemove));
        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_FALSE(mutations.BundlesDynamicConfig.has_value());
        EXPECT_EQ(3, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }

    FinishTabletCellRemoval(&input);

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(1, ssize(mutations.ChangedStates));
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    // Now cells are removed and config can be applied.
    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_TRUE(mutations.BundlesDynamicConfig.has_value());
        EXPECT_EQ(1, mutations.GetMutationCount());

        ApplyMutations(&input, mutations);
    }

    FakeApplyDynamicConfigAtNodes(&input);

    {
        YT_LOG_DEBUG("Step %v", __LINE__);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, mutations.GetMutationCount());
    }
}

TEST_P(TNoAllocatorTest, MultipeerAllocation)
{
    auto input = GenerateInputContext(/*nodeCount*/ 2, CellsPerNode);
    GenerateNodesForBundle(input, SpareBundleName, 3, {.SlotCount = 1});
    GenerateTabletCellsForBundle(input, "bigd", CellsPerNode);
    input.Bundles["bigd"]->Options->PeerCount = 1;

    for (int i = 0; i < 10; ++i) {
        YT_LOG_DEBUG("Step %v %i", __LINE__, i);
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        ApplyMutations(&input, mutations);

        FakeApplyDynamicConfigAtNodes(&input);
    }

    ValidateConsistency(input);
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    TNoAllocatorTest,
    TNoAllocatorTest,
    ::testing::Values(
        std::tuple(false, false),
        std::tuple(true, false),
        std::tuple(false, true),
        std::tuple(true, true)
    )
);

} // namespace
} // namespace NYT::NCellBalancer
