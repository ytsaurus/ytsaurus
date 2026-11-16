#include "helpers.h"

#include <yt/yt/server/master/cell_master/master_cell_group.h>
#include <yt/yt/server/master/cell_master/master_cell_group_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/stream/str.h>

namespace NYT::NSecurityServer {
namespace {

using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBootstrapTestBase, RecomputeMissingGroupStatistics)
{
    TAccount account(MakeId(EObjectType::Account, TCellTag(10), 1, 0));
    TMasterCellGroup group(MakeId(EObjectType::MasterCellGroup, TCellTag(10), 2, 0));
    group.CellTags() = {TCellTag(10), TCellTag(11)};

    // This is the source state available after loading a snapshot without group statistics.
    for (auto [cellTag, nodeCount, committedNodeCount] : {
        std::tuple(10, 7, 5),
        std::tuple(11, 3, 2),
        std::tuple(12, 100, 90),
    })
    {
        auto& statistics = account.CellToStatistics()[TCellTag(cellTag)];
        statistics.ResourceUsage.SetNodeCount(nodeCount);
        statistics.CommittedResourceUsage.SetNodeCount(committedNodeCount);
    }

    EXPECT_FALSE(account.CellGroupToStatistics().contains(group.GetId()));
    account.RecomputeGroupStatistics(&group);
    EXPECT_EQ(10, account.GetStatistics(group.GetId()).ResourceUsage.GetNodeCount());
    EXPECT_EQ(7, account.GetStatistics(group.GetId()).CommittedResourceUsage.GetNodeCount());

    group.CellTags() = {TCellTag(11)};
    account.RecomputeGroupStatistics(&group);
    EXPECT_EQ(3, account.GetStatistics(group.GetId()).ResourceUsage.GetNodeCount());
    EXPECT_EQ(2, account.GetStatistics(group.GetId()).CommittedResourceUsage.GetNodeCount());

    TMasterCellGroup universum(GetUniversumGroupId(TCellTag(10)));
    universum.CellTags() = {TCellTag(10), TCellTag(11), TCellTag(12)};
    account.RecomputeGroupStatistics(&universum);
    EXPECT_EQ(110, account.GetStatistics(universum.GetId()).ResourceUsage.GetNodeCount());
    EXPECT_EQ(97, account.GetStatistics(universum.GetId()).CommittedResourceUsage.GetNodeCount());
}

TEST(TAccountResourceLimitsTest, MissingGroupIsUnlimited)
{
    auto groupId = MakeId(EObjectType::MasterCellGroup, TCellTag(10), 1, 0);
    TCellGroupToResourceLimits limits;
    EXPECT_TRUE(GetAccountResourceLimits(limits, groupId).GetNodeCount().IsInfinite());
    EXPECT_TRUE(GetAccountResourceLimits(limits, groupId).DiskSpace().GetOrDefault(0).IsInfinite());

    limits.emplace(groupId, TClusterResourceLimits::Infinity().SetNodeCount(TLimit64(0)));
    EXPECT_EQ(TLimit64(0), GetAccountResourceLimits(limits, groupId).GetNodeCount());
    EXPECT_TRUE(GetAccountResourceLimits(limits, groupId).GetChunkCount().IsInfinite());

    limits.erase(groupId);
    EXPECT_TRUE(GetAccountResourceLimits(limits, groupId).GetNodeCount().IsInfinite());
}

TEST_F(TBootstrapTestBase, LoadLegacyAccountLimits)
{
    auto accountId = MakeId(EObjectType::Account, TCellTag(10), 1, 0);
    TAccount original(accountId);
    auto universumGroupId = original.GetUniversumGroupId();
    auto& originalLimits = GetOrCrash(original.CellGroupToResourceLimits(), universumGroupId);
    // Built-in accounts used an explicit empty map, which selected a zero default.
    originalLimits = TClusterResourceLimits::Zero()
        .SetNodeCount(TLimit64(42))
        .SetMasterMemory({TLimit64(100), TLimit64(100), {}});
    originalLimits.MasterMemory().PerCell[TCellTag(10)] = TLimit64(0);
    originalLimits.MasterMemory().PerCell[TCellTag(11)] = TLimit64(50);
    original.SetMergeJobRateLimit(7);

    TStringStream stream;
    auto output = NHydra::CreateCheckpointableOutputStream(&stream);
    TSaveContext saveContext(output.get(), {}, nullptr);
    // Write the account layout from before AccountMasterCellGroupLimits.
    original.NObjectServer::TNonversionedMapObjectBase<TAccount>::Save(saveContext);
    Save(saveContext, original.GetStatistics(universumGroupId));
    Save(saveContext, original.CellToStatistics());
    Save(saveContext, originalLimits);
    Save(saveContext, original.GetAllowChildrenLimitOvercommit());
    Save(saveContext, original.GetMergeJobRateLimit());
    Save(saveContext, false); // Null AbcConfig, serialized by TNullableIntrusivePtrSerializer.
    Save(saveContext, original.GetFolderId());
    Save(saveContext, original.GetChunkMergerNodeTraversalConcurrency());
    Save(saveContext, original.GetAllowUsingChunkMerger());
    Save(saveContext, original.ChunkMergerCriteria());
    Save(saveContext, original.GetEnableChunkReincarnation());
    Save(saveContext, original.GetBackupConfig());
    saveContext.Flush();

    TStringInput stringInput(stream.Str());
    auto input = NHydra::CreateCheckpointableInputStream(&stringInput);
    TLoadContext loadContext(nullptr, input.get(), nullptr);
    loadContext.SetVersion(ToUnderlying(EMasterReign::MasterCellGroups));
    TAccount restored(accountId);
    restored.Load(loadContext);

    const auto& restoredLimits = restored.GetResourceLimits(universumGroupId);
    EXPECT_EQ(1u, restored.CellGroupToResourceLimits().size());
    EXPECT_EQ(originalLimits, restoredLimits);
    EXPECT_EQ(7, restored.GetMergeJobRateLimit());
    const auto& memoryLimits = restoredLimits.MasterMemory();
    EXPECT_EQ(TLimit64(0), memoryLimits.PerCell.DefaultValue());
    EXPECT_TRUE(memoryLimits.PerCell.contains(TCellTag(10)));
    EXPECT_EQ(TLimit64(0), memoryLimits.PerCell.GetOrDefault(TCellTag(10)));
    EXPECT_EQ(TLimit64(50), memoryLimits.PerCell.GetOrDefault(TCellTag(11)));
    EXPECT_EQ(TLimit64(0), memoryLimits.PerCell.GetOrDefault(TCellTag(12)));

    // Comparisons must keep using the default saved in the snapshot.
    auto parentLimits = TMasterMemoryLimits::Infinity();
    parentLimits.PerCell[TCellTag(12)] = TLimit64(1);
    EXPECT_FALSE(parentLimits.IsViolatedBy(memoryLimits));
}

TEST_F(TBootstrapTestBase, ComputeTotalChildrenLimits)
{
    TAccount parent(MakeId(EObjectType::Account, TCellTag(10), 1, 0));
    TAccount first(MakeId(EObjectType::Account, TCellTag(10), 2, 0));
    TAccount second(MakeId(EObjectType::Account, TCellTag(10), 3, 0));
    auto groupId = MakeId(EObjectType::MasterCellGroup, TCellTag(10), 1, 0);
    auto universumGroupId = parent.GetUniversumGroupId();

    for (auto id : {universumGroupId, groupId}) {
        auto emptySum = parent.ComputeTotalChildrenLimits(id);
        EXPECT_EQ(TClusterResourceLimits::Zero(), emptySum);
        EXPECT_EQ(TLimit64(0), emptySum.DiskSpace().DefaultValue());
        EXPECT_EQ(TLimit64(0), emptySum.MasterMemory().PerCell.DefaultValue());
    }

    auto& firstUniversumGroupLimits = GetOrCrash(first.CellGroupToResourceLimits(), universumGroupId);
    firstUniversumGroupLimits.SetNodeCount(TLimit64(10));
    firstUniversumGroupLimits.SetMasterMemory({TLimit64(100), TLimit64(100), {}});
    firstUniversumGroupLimits.MasterMemory().PerCell[TCellTag(10)] = TLimit64(20);
    auto firstLimits = TClusterResourceLimits::Infinity().SetNodeCount(TLimit64(5));
    firstLimits.SetMediumDiskSpace(0, TLimit64(0));
    firstLimits.SetMediumDiskSpace(1, TLimit64(10));
    firstLimits.MasterMemory().PerCell[TCellTag(10)] = TLimit64(20);
    firstLimits.MasterMemory().PerCell[TCellTag(11)] = TLimit64(0);
    first.CellGroupToResourceLimits().emplace(groupId, firstLimits);
    parent.KeyToChild().emplace("first", &first);

    EXPECT_EQ(first.GetResourceLimits(universumGroupId), parent.ComputeTotalChildrenLimits(universumGroupId));
    auto singleChildSum = parent.ComputeTotalChildrenLimits(groupId);
    EXPECT_EQ(TLimit64(5), singleChildSum.GetNodeCount());
    EXPECT_EQ(firstLimits.DiskSpace(), singleChildSum.DiskSpace());
    EXPECT_TRUE(singleChildSum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_TRUE(singleChildSum.MasterMemory().Total.IsInfinite());
    EXPECT_TRUE(singleChildSum.MasterMemory().ChunkHost.IsInfinite());
    EXPECT_EQ(TLimit64(0), singleChildSum.MasterMemory().PerCell.DefaultValue());
    EXPECT_EQ(TLimit64(20), singleChildSum.MasterMemory().PerCell.GetOrDefault(TCellTag(10)));
    EXPECT_FALSE(singleChildSum.MasterMemory().PerCell.contains(TCellTag(11)));

    GetOrCrash(second.CellGroupToResourceLimits(), universumGroupId).SetNodeCount(TLimit64(20));
    auto secondLimits = TClusterResourceLimits::Infinity().SetNodeCount(TLimit64(7));
    secondLimits.SetMediumDiskSpace(0, TLimit64(0));
    secondLimits.SetMediumDiskSpace(1, TLimit64(20));
    second.CellGroupToResourceLimits().emplace(groupId, secondLimits);
    parent.KeyToChild().emplace("second", &second);

    auto universumSum = parent.ComputeTotalChildrenLimits(universumGroupId);
    EXPECT_EQ(TLimit64(30), universumSum.GetNodeCount());
    EXPECT_EQ(TLimit64(0), universumSum.MasterMemory().PerCell.DefaultValue());
    EXPECT_EQ(TLimit64(20), universumSum.MasterMemory().PerCell.GetOrDefault(TCellTag(10)));
    auto sum = parent.ComputeTotalChildrenLimits(groupId);
    EXPECT_EQ(TLimit64(12), sum.GetNodeCount());
    EXPECT_EQ(TLimit64(0), sum.DiskSpace().GetOrDefault(0));
    EXPECT_EQ(TLimit64(30), sum.DiskSpace().GetOrDefault(1));
    EXPECT_TRUE(sum.DiskSpace().GetOrDefault(2).IsInfinite());
    EXPECT_EQ(TLimit64(0), sum.MasterMemory().PerCell.DefaultValue());
    EXPECT_EQ(TLimit64(20), sum.MasterMemory().PerCell.GetOrDefault(TCellTag(10)));
    EXPECT_FALSE(sum.MasterMemory().PerCell.contains(TCellTag(11)));

    second.CellGroupToResourceLimits().erase(groupId);
    auto missingGroupSum = parent.ComputeTotalChildrenLimits(groupId);
    EXPECT_EQ(singleChildSum, missingGroupSum);
    EXPECT_EQ(TLimit64(5), missingGroupSum.GetNodeCount());
    EXPECT_TRUE(missingGroupSum.GetChunkCount().IsInfinite());
    EXPECT_TRUE(missingGroupSum.GetTabletCount().IsInfinite());
    EXPECT_TRUE(missingGroupSum.GetTabletStaticMemory().IsInfinite());
    EXPECT_TRUE(missingGroupSum.DiskSpace().DefaultValue().IsInfinite());
    EXPECT_EQ(firstLimits.DiskSpace(), missingGroupSum.DiskSpace());
    EXPECT_TRUE(missingGroupSum.MasterMemory().Total.IsInfinite());
    EXPECT_TRUE(missingGroupSum.MasterMemory().ChunkHost.IsInfinite());
    EXPECT_EQ(TLimit64(0), missingGroupSum.MasterMemory().PerCell.DefaultValue());
    EXPECT_EQ(TLimit64(20), missingGroupSum.MasterMemory().PerCell.GetOrDefault(TCellTag(10)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSecurityServer
