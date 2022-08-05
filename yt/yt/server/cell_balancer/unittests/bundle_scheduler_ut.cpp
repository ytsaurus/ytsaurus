#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NCellBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetPodIdForNode(const TString& name)
{
    auto endPos = name.find(".");
    YT_VERIFY(endPos != TString::npos);

    return name.substr(0, endPos);
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerInputState GenerateSimpleInputContext(int requestedTabletNodeCount, int writeThreadCount = 0)
{
    TSchedulerInputState input;
    input.Config = New<TBundleControllerConfig>();
    input.Config->Cluster = "default-cluster";

    {
        auto zoneInfo = New<TZoneInfo>();
        input.Zones["default-zone"] = zoneInfo;
        zoneInfo->YPCluster = "pre-pre";
        zoneInfo->NannyService = "nanny-bunny";
    }

    {
        auto bundleInfo = New<TBundleInfo>();
        input.Bundles["default-bundle"] = bundleInfo;
        bundleInfo->Health = NTabletClient::ETabletCellHealth::Good;
        bundleInfo->Zone = "default-zone";
        bundleInfo->NodeTagFilter = "default-zone/default-bundle";
        bundleInfo->EnableBundleController = true;
        bundleInfo->EnableTabletCellManagement = true;

        auto config = New<TBundleConfig>();
        bundleInfo->TargetConfig = config;
        config->TabletNodeCount = requestedTabletNodeCount;
        config->TabletNodeResourceGuarantee = New<TInstanceResources>();
        config->TabletNodeResourceGuarantee->VCpu = 9999;
        config->TabletNodeResourceGuarantee->Memory = 88_GB;
        config->CpuLimits->WriteThreadPoolSize = writeThreadCount;
    }

    return input;
}

void VerifyAllocationRequests(const TSchedulerMutations& mutations, int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewAllocations));

    for (const auto& [id, request] : mutations.NewAllocations) {
        EXPECT_FALSE(id.empty());
        
        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_EQ(spec->NannyService, "nanny-bunny");
        EXPECT_FALSE(spec->PodIdTemplate.empty());
        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);
    }
}

void VerifyDeallocationRequests(
    const TSchedulerMutations& mutations,
    TBundleControllerStatePtr& bundleState,
    int expectedCount)
{
    EXPECT_EQ(expectedCount, std::ssize(mutations.NewDeallocations));

    for (const auto& [id, request] : mutations.NewDeallocations) {
        EXPECT_FALSE(id.empty());

        auto spec = request->Spec;
        EXPECT_TRUE(static_cast<bool>(spec));
        EXPECT_EQ(spec->YPCluster, "pre-pre");
        EXPECT_FALSE(spec->PodId.empty());

        EXPECT_TRUE(spec->InstanceRole == YTRoleTypeTabNode);

        EXPECT_FALSE(bundleState->Deallocations[id]->NodeName.empty());
    }
}

THashSet<TString> GenerateNodesForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int nodeCount,
    bool setFilterTag = false,
    int slotCount = 5)
{
    THashSet<TString> result;

    for (int index = 0; index < nodeCount; ++index) {
        int nodeIndex = std::ssize(inputState.TabletNodes);
        auto nodeId = Format("seneca-ayt-%v-%v-aa-tab-node-%v.search.yandex.net",
            nodeIndex,
            bundleName,
            inputState.Config->Cluster);
        auto nodeInfo = New<TTabletNodeInfo>();
        nodeInfo->Banned = false;
        nodeInfo->Decommissioned = false;
        nodeInfo->Host = Format("seneca-ayt-%v.search.yandex.net", nodeIndex);
        nodeInfo->State = "online";
        nodeInfo->Annotations->Allocated = true;
        nodeInfo->Annotations->NannyService = "nanny-bunny";
        nodeInfo->Annotations->YPCluster = "pre-pre";
        nodeInfo->Annotations->AllocatedForBundle = bundleName;

        for (int index = 0; index < slotCount; ++index) {
            nodeInfo->TabletSlots.push_back(New<TTabletSlot>());
        }

        if (setFilterTag) {
            nodeInfo->UserTags.insert(GetOrCrash(inputState.Bundles, bundleName)->NodeTagFilter);
        }

        inputState.TabletNodes[nodeId] = nodeInfo;
        result.insert(nodeId);
    }

    return result;
}

void SetTabletSlotsState(TSchedulerInputState& inputState, const TString& nodeName, const TString& state)
{
    const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);

    for (const auto& slot : nodeInfo->TabletSlots) {
        slot->State = state;
    }
}

void GenerateAllocationsForBundle(TSchedulerInputState& inputState, const TString& bundleName, int count)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (int index = 0; index < count; ++index) {
        auto requestId = Format("alloc-%v", state->Allocations.size());
        state->Allocations[requestId] = New<TAllocationRequestState>();
        state->Allocations[requestId]->CreationTime = TInstant::Now();
        inputState.AllocationRequests[requestId] = New<TAllocationRequest>();
        auto& spec = inputState.AllocationRequests[requestId]->Spec;
        spec->NannyService = "nanny-bunny";
        spec->YPCluster = "pre-pre";
    }
}

void GenerateTabletCellsForBundle(
    TSchedulerInputState& inputState,
    const TString& bundleName,
    int cellCount,
    int peerCount = 1)
{
    auto bundleInfo = GetOrCrash(inputState.Bundles, bundleName);

    for (int index = 0; index < cellCount; ++index) {
        auto cellId = Format("tablet-cell-%v-%v", bundleName, bundleInfo->TabletCellIds.size());
        auto cellInfo = New<TTabletCellInfo>();
        cellInfo->TabletCount = 2;
        cellInfo->TabletCellBundle = bundleName;
        cellInfo->Peers.resize(peerCount, New<TTabletCellPeer>());
        bundleInfo->TabletCellIds.push_back(cellId);
        inputState.TabletCells[cellId] = cellInfo;
    }
}

void GenerateDeallocationsForBundle(
    TSchedulerInputState& inputState, 
    const TString& bundleName,
    const std::vector<TString>& nodeNames)
{
    auto& state = inputState.BundleStates[bundleName];
    if (!state) {
        state = New<TBundleControllerState>();
    }

    for (const auto& nodeName : nodeNames) {
        const auto& nodeInfo = GetOrCrash(inputState.TabletNodes, nodeName);
        nodeInfo->Decommissioned = true;
        SetTabletSlotsState(inputState, nodeName, TabletSlotStateEmpty);

        auto requestId = Format("dealloc-%v", state->Allocations.size());

        auto deallocationState = New<TDeallocationRequestState>();
        state->Deallocations[requestId] = deallocationState;
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->NodeName = nodeName;
        deallocationState->HulkRequestCreated = true;


        inputState.DeallocationRequests[requestId] = New<TDeallocationRequest>();
        auto& spec = inputState.DeallocationRequests[requestId]->Spec;
        spec->YPCluster = "pre-pre";
        spec->PodId = "random_pod_id";
    }
}

void SetNodeAnnotations(const TString& nodeId, const TString& bundleName, const TSchedulerInputState& input)
{
    auto& annotation = GetOrCrash(input.TabletNodes, nodeId)->Annotations;
    annotation->YPCluster = "pre-pre";
    annotation->AllocatedForBundle = bundleName;
    annotation->Allocated = true;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBundleSchedulerTest, AllocationCreated)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 1);
    GenerateAllocationsForBundle(input, "default-bundle", 1);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyAllocationRequests(mutations, 3);

    EXPECT_EQ(4, std::ssize(mutations.ChangedStates["default-bundle"]->Allocations));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(2);

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateAllocationsForBundle(input, "default-bundle", 1);

    const TString nodeId = input.TabletNodes.begin()->first;
    GetOrCrash(input.TabletNodes, nodeId)->Annotations = New<TTabletNodeAnnotationsInfo>();

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = GetOrCrash(input.TabletNodes, nodeId)->Host;
        status->PodId = GetPodIdForNode(nodeId);
        status->State = "COMPLETED";
    }

    // Check Setting node attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        VerifyAllocationRequests(mutations, 0);
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->Allocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangeNodeAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
        EXPECT_EQ(annotations->YPCluster, "pre-pre");
        EXPECT_EQ(annotations->AllocatedForBundle, "default-bundle");
        EXPECT_EQ(annotations->NannyService, "nanny-bunny");
        EXPECT_TRUE(annotations->Allocated);

        input.TabletNodes[nodeId]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->Allocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));
    VerifyAllocationRequests(mutations, 0);
}

TEST(TBundleSchedulerTest, AllocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyAllocationRequests(mutations, 0);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->Allocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    // TODO(capone212): use constants instead of inline strings
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "table_node_allocation_failed");
}

TEST(TBundleSchedulerTest, AllocationProgressTrackCompletedButNoNode)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyAllocationRequests(mutations, 0);
    EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->Allocations));
}

TEST(TBundleSchedulerTest, AllocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(2);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateAllocationsForBundle(input, "default-bundle", 1);

    {
        auto& allocState = input.BundleStates["default-bundle"]->Allocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    {
        auto& request = input.AllocationRequests.begin()->second;
        auto status = New<TAllocationRequestStatus>();
        request->Status = status;
        status->NodeId = "non-existing-node";
        status->State = "COMPLETED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyAllocationRequests(mutations, 0);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->Allocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_tablet_node_allocation");
}

TEST(TBundleSchedulerTest, DoNotCreateNewDeallocationsWhileInProgress)
{
    auto input = GenerateSimpleInputContext(2);
    auto nodes = GenerateNodesForBundle(input, "default-bundle", 5);
    GenerateDeallocationsForBundle(input, "default-bundle", { *nodes.begin()});

    for (const auto& [nodeId, _] : input.TabletNodes) {
        SetNodeAnnotations(nodeId, "default-bundle", input);
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    auto& bundleState = mutations.ChangedStates["default-bundle"];
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates["default-bundle"]->Deallocations));
    VerifyDeallocationRequests(mutations, bundleState, 0);
}

TEST(TBundleSchedulerTest, CreateNewDeallocations)
{
    auto input = GenerateSimpleInputContext(2);
    GenerateNodesForBundle(input, "default-bundle", 5);

    for (auto& [nodeId, _] : input.TabletNodes) {
        SetNodeAnnotations(nodeId, "default-bundle", input);
    }

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedStates["default-bundle"]->Deallocations));

    input.BundleStates = mutations.ChangedStates;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(3, std::ssize(mutations.ChangedDecommissionedFlag));

    std::vector<TString> nodesToRemove;
    for (auto& [nodeName, decommissioned] : mutations.ChangedDecommissionedFlag) {
        GetOrCrash(input.TabletNodes, nodeName)->Decommissioned = decommissioned;
        EXPECT_TRUE(decommissioned);
        nodesToRemove.push_back(nodeName);

        SetTabletSlotsState(input, nodeName, PeerStateLeading);
    }

    input.BundleStates = mutations.ChangedStates;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Node are decommissioned but tablet slots have to be empty.
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));

    for (const auto& nodeName : nodesToRemove) {
        SetTabletSlotsState(input, nodeName, TabletSlotStateEmpty);
    }

    input.BundleStates = mutations.ChangedStates;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Hulk deallocation requests are finally created.
    auto& bundleState = mutations.ChangedStates["default-bundle"];
    VerifyDeallocationRequests(mutations, bundleState, 3);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackFailed)
{
    auto input = GenerateSimpleInputContext(1);
    TSchedulerMutations mutations;

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateDeallocationsForBundle(input, "default-bundle", { *bundleNodes.begin()});

    {
        auto& request = input.DeallocationRequests.begin()->second;
        request->Status->State = "FAILED";
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    VerifyAllocationRequests(mutations, 0);
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->Deallocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    // TODO(capone212): use constants instead of inline strings
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "table_node_deallocation_failed");
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackCompleted)
{
    auto input = GenerateSimpleInputContext(1);

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    const TString nodeId = *bundleNodes.begin();

    GenerateDeallocationsForBundle(input, "default-bundle", {nodeId});

    {
        auto& request = input.DeallocationRequests.begin()->second;
        auto& status = request->Status;
        status->State = "COMPLETED";
    }

    // Check Setting node attributes
    {
        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
        EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
        EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
        EXPECT_EQ(1, std::ssize(input.BundleStates["default-bundle"]->Deallocations));

        EXPECT_EQ(1, std::ssize(mutations.ChangeNodeAnnotations));
        const auto& annotations = GetOrCrash(mutations.ChangeNodeAnnotations, nodeId);
        EXPECT_TRUE(annotations->YPCluster.empty());
        EXPECT_TRUE(annotations->AllocatedForBundle.empty());
        EXPECT_TRUE(annotations->NannyService.empty());
        EXPECT_FALSE(annotations->Allocated);

        input.TabletNodes[nodeId]->Annotations = annotations;
    }

    // Schedule one more time with annotation tags set
    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates["default-bundle"]->Allocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangeNodeAnnotations));
    VerifyAllocationRequests(mutations, 0);
}

TEST(TBundleSchedulerTest, DeallocationProgressTrackStaledAllocation)
{
    auto input = GenerateSimpleInputContext(1);
    TSchedulerMutations mutations;

    auto bundleNodes = GenerateNodesForBundle(input, "default-bundle", 2);
    const TString nodeId = *bundleNodes.begin();

    GenerateDeallocationsForBundle(input, "default-bundle", {nodeId});

    {
        auto& allocState = input.BundleStates["default-bundle"]->Deallocations.begin()->second;
        allocState->CreationTime = TInstant::Now() - TDuration::Days(1);
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(mutations.AlertsToFire.front().Id, "stuck_tablet_node_deallocation");
}

TEST(TBundleSchedulerTest, CreateNewCellsCreation)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 3);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(1, std::ssize(mutations.CellsToCreate));

    EXPECT_EQ(7, mutations.CellsToCreate.at("default-bundle"));
}

TEST(TBundleSchedulerTest, CreateNewCellsNoRemoveNoCreate)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
}

TEST(TBundleSchedulerTest, CreateNewCellsRemove)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 13);

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(3, std::ssize(mutations.CellsToRemove));
}

TEST(TBundleSchedulerTest, PeekRightCellToRemove)
{
    auto input = GenerateSimpleInputContext(2, 5);
    TSchedulerMutations mutations;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 11);

    auto cellId = input.Bundles["default-bundle"]->TabletCellIds[RandomNumber<ui32>(11)];
    input.TabletCells[cellId]->TabletCount = 0;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(1, std::ssize(mutations.CellsToRemove));

    EXPECT_EQ(cellId, mutations.CellsToRemove.front());
}

TEST(TBundleSchedulerTest, TestSpareNodesAllocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(3, std::ssize(mutations.NewAllocations));
}

TEST(TBundleSchedulerTest, TestSpareNodesDeallocate)
{
    auto input = GenerateSimpleInputContext(0);
    auto zoneInfo = input.Zones["default-zone"];

    zoneInfo->SpareTargetConfig->TabletNodeCount = 2;
    GenerateNodesForBundle(input, "spare", 3);

    TSchedulerMutations mutations;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.CellsToCreate));
    EXPECT_EQ(0, std::ssize(mutations.CellsToRemove));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(1, std::ssize(mutations.ChangedStates["spare"]->Deallocations));
}

////////////////////////////////////////////////////////////////////////////////

void CheckEmptyAlerts(const TSchedulerMutations& mutations)
{
    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));

    for (const auto& alert : mutations.AlertsToFire) {
        EXPECT_EQ("", alert.Id);
        EXPECT_EQ("", alert.Description);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TNodeTagsFilterManager, TestBundleWithNoTagFilter)
{
    auto input = GenerateSimpleInputContext(2, 5);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;
    input.Bundles["default-bundle"]->NodeTagFilter = {};

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ("bundle_with_no_tag_filter", mutations.AlertsToFire.front().Id);

    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
}

TEST(TNodeTagsFilterManager, TestBundleNodeTagsAssigned)
{
    auto input = GenerateSimpleInputContext(2, 5);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;

    GenerateNodesForBundle(input, "default-bundle", 2);
    GenerateTabletCellsForBundle(input, "default-bundle", 10);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2, std::ssize(mutations.ChangedNodeUserTags));

    for (const auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);

    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
}

TEST(TNodeTagsFilterManager, TestBundleNodesWithSpare)
{
    const bool SetNodeFilterTag = true; 
    const int SlotCount = 5;

    auto input = GenerateSimpleInputContext(2, SlotCount);
    input.Bundles["default-bundle"]->EnableNodeTagFilterManagement = true;

    GenerateNodesForBundle(input, "default-bundle", 1, SetNodeFilterTag, SlotCount);
    GenerateTabletCellsForBundle(input, "default-bundle", 15);

    // Generate Spare nodes
    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    auto spareNodes = GenerateNodesForBundle(input, "spare", 3, false, SlotCount);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(2, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(2, std::ssize(mutations.ChangedNodeUserTags));

    const TString BundleNodeTagFilter = input.Bundles["default-bundle"]->NodeTagFilter;

    THashSet<TString> usedSpare;

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_FALSE(mutations.ChangedDecommissionedFlag.at(nodeName));
        EXPECT_TRUE(tags.find(BundleNodeTagFilter) != tags.end());
        EXPECT_TRUE(spareNodes.find(nodeName) != spareNodes.end());

        usedSpare.insert(nodeName);
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    EXPECT_EQ(2, std::ssize(usedSpare));

    // Populate slots with cell peers.
    for (const auto& spareNode : usedSpare) {
        SetTabletSlotsState(input, spareNode, PeerStateLeading);
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Add new node to bundle
    auto newNodes = GenerateNodesForBundle(input, "default-bundle", 1, SetNodeFilterTag, SlotCount);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(1, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    TString spareNodeToRelease;

    for (const auto& [nodeName, decommission] : mutations.ChangedDecommissionedFlag) {
        EXPECT_TRUE(usedSpare.count(nodeName) != 0);
        EXPECT_TRUE(decommission);
        input.TabletNodes[nodeName]->Decommissioned = decommission;
        spareNodeToRelease = nodeName;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));

    // Populate slots with cell peers.
    SetTabletSlotsState(input, spareNodeToRelease, TabletSlotStateEmpty);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(1, std::ssize(mutations.ChangedNodeUserTags));

    for (auto& [nodeName, tags] : mutations.ChangedNodeUserTags) {
        EXPECT_EQ(spareNodeToRelease, nodeName);
        EXPECT_TRUE(tags.count(BundleNodeTagFilter) == 0);
        input.TabletNodes[nodeName]->UserTags = tags;
    }

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    EXPECT_EQ(0, std::ssize(mutations.ChangedDecommissionedFlag));
    EXPECT_EQ(0, std::ssize(mutations.ChangedNodeUserTags));
}

TEST(TBundleSchedulerTest, CheckDisruptedState)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, "spare", 3);
    GenerateNodesForBundle(input, "default-bundle", 4);

    for (auto& [_, nodeInfo] : input.TabletNodes) {
        nodeInfo->State = InstanceStateOffline;
    }

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.AlertsToFire));
    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));
    EXPECT_EQ(0, std::ssize(mutations.ChangedStates));
}

TEST(TBundleSchedulerTest, CheckAllocationLimit)
{
    auto input = GenerateSimpleInputContext(5);
    TSchedulerMutations mutations;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, "spare", 3);
    GenerateNodesForBundle(input, "default-bundle", 4);

    zoneInfo->MaxTabletNodeCount = 5;

    ScheduleBundles(input, &mutations);

    EXPECT_EQ(0, std::ssize(mutations.NewDeallocations));
    EXPECT_EQ(0, std::ssize(mutations.NewAllocations));

    EXPECT_EQ(1, std::ssize(mutations.AlertsToFire));
}

TEST(TBundleSchedulerTest, CheckDynamicConfig)
{
    auto input = GenerateSimpleInputContext(5, 5);
    input.Bundles["default-bundle"]->EnableTabletNodeDynamicConfig = true;

    auto zoneInfo = input.Zones["default-zone"];
    zoneInfo->SpareTargetConfig->TabletNodeCount = 3;
    GenerateNodesForBundle(input, "spare", 3);
    GenerateNodesForBundle(input, "default-bundle", 5);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);

    // Check that new dynamic config is set for bundles.
    EXPECT_TRUE(mutations.DynamicConfig);

    input.DynamicConfig = *mutations.DynamicConfig;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config did not change.
    EXPECT_FALSE(mutations.DynamicConfig);

    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    input.Bundles["default-bundle"]->TargetConfig->CpuLimits->WriteThreadPoolSize = 212;
    mutations = TSchedulerMutations{};
    ScheduleBundles(input, &mutations);

    // Dynamic config is changed.
    EXPECT_TRUE(mutations.DynamicConfig);
}

////////////////////////////////////////////////////////////////////////////////

struct TFooBarStruct
    : public TYsonStructAttributes<TFooBarStruct>
{
    TString Foo;
    int Bar;

    REGISTER_YSON_STRUCT(TFooBarStruct);

    static void Register(TRegistrar registrar)
    {
        RegisterAttribute(registrar, "foo", &TThis::Foo)
            .Default();
        RegisterAttribute(registrar, "bar", &TThis::Bar)
            .Default(0);
    }
};

TEST(TBundleSchedulerTest, CheckCypressBindings)
{
    EXPECT_EQ(TFooBarStruct::GetAttributes().size(), 2u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // NYT::NCellBalancer
