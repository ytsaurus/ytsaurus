#include "chaos_scheduler.h"

#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellBalancer {

using namespace NYson;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BundleControllerLogger;

static const TString BetaAreaName = "beta";
static const TString DefaultAreaName = "default";

////////////////////////////////////////////////////////////////////////////////

TObjectId MakeChaosCellId(TCellTag cellTag)
{
    return MakeId(
        EObjectType::ChaosCell,
        cellTag,
        (0xbcll << 32) + RandomNumber<ui32>(),
        RandomNumber<ui32>());
}

////////////////////////////////////////////////////////////////////////////////

TString GetSystemAccountName(const TString& bundleName)
{
    return Format("%v_bundle_system_quotas", bundleName);
}

void CreateTabletCellBundle(
    const TString& bundleName,
    const TString& clusterName,
    const TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    const auto& chaosConfig = input.Config->ChaosConfig;
    auto bundleInfo = GetOrCrash(input.TabletCellBundles, bundleName);
    auto systemAccountName = GetSystemAccountName(bundleName);

    mutations->ForeignSystemAccountsToCreate[clusterName].insert(systemAccountName);

    auto attributes = NYTree::BuildAttributeDictionaryFluently()
        .Item("name").Value(bundleName)
        .Item("enable_bundle_controller").Value(true)
        .Item("zone").Value("zone_default")
        .Item("abc_managed").Value(true)
        .Item("folder_id").Value(bundleInfo->FolderId)
        .Item("abc").Value(bundleInfo->Abc)
        .Item("options")
            .BeginMap()
                .Item("changelog_account").Value(systemAccountName)
                .Item("snapshot_account").Value(systemAccountName)
                .Item("clock_cluster_tag").Value(chaosConfig->ClockClusterTag)
            .EndMap()
        .Finish();

    mutations->ForeignTabletCellBundlesToCreate[clusterName][bundleName] = attributes;

    YT_LOG_INFO("Creating new foreign tablet cell bundle (Cluster: %v, TabletCellBundle: %v, BundleInfo: %v)",
        clusterName,
        bundleName,
        ConvertToYsonString(attributes, EYsonFormat::Text));
}

void SetClockClusterTag(
    const TString& bundleName,
    const TString& clusterName,
    const TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    const auto& chaosConfig = input.Config->ChaosConfig;
    const auto& clusterBundles = GetOrCrash(input.ForeignTabletCellBundles, clusterName);
    const auto& bundleInfo = GetOrCrash(clusterBundles, bundleName);

    if (bundleInfo->Options->ClockClusterTag == chaosConfig->ClockClusterTag) {
        return;
    }

    if (!bundleInfo->TabletCellIds.empty()) {
        YT_LOG_WARNING("Cannot set clock cluster tag for a bundle with created cells (Cluster: %v, TabletCellBundle: %v)",
            clusterName,
            bundleName);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "cannot_set_clock_cluster_tag",
            .BundleName = bundleName,
            .Description = Format("Cannot set clock cluster tag for %v on cluster %v with created cells",
                bundleName,
                clusterName),
        });
        return;
    }

    YT_LOG_INFO("Setting clock cluster tag for a bundle (Cluster: %v, TabletCellBundle: %v, ClockClusterTag: %v)",
        clusterName,
        bundleName,
        chaosConfig->ClockClusterTag);

    mutations->ForeignBundleCellTagsToSet[clusterName][bundleName] = chaosConfig->ClockClusterTag;
}

void ProcessTabletCellBundles(const TString& bundleName, const TChaosSchedulerInputState& input, TChaosSchedulerMutations* mutations)
{
    const auto& chaosConfig = input.Config->ChaosConfig;
    for (const auto& clusterName : chaosConfig->TabletCellClusters) {
        auto clusterIt = input.ForeignTabletCellBundles.find(clusterName);
        if (clusterIt == input.ForeignTabletCellBundles.end()) {
            YT_LOG_WARNING("Cannot find tablet cell bundles for cluster (Cluster: %v)",
                clusterName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "cannot_find_tablet_cell_bundles",
                .BundleName = bundleName,
                .Description = Format("Cannot find tablet cell bundles for cluster %v", clusterName),
            });
            continue;
        }

        const auto& clusterBundles = clusterIt->second;

        if (clusterBundles.contains(bundleName)) {
            SetClockClusterTag(bundleName, clusterName, input, mutations);
        } else {
            CreateTabletCellBundle(bundleName, clusterName, input, mutations);
        }
    }
}

void CreateChaosCellBundle(
    const TString& bundleName,
    const TString& clusterName,
    TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    static const TString SysAccountName = "sys";
    const auto& chaosConfig = input.Config->ChaosConfig;

    auto attributes = NYTree::BuildAttributeDictionaryFluently()
        .Item("name").Value(bundleName)
        .Item("options")
            .BeginMap()
                .Item("changelog_account").Value(SysAccountName)
                .Item("snapshot_account").Value(SysAccountName)
                .Item("clock_cluster_tag").Value(chaosConfig->ClockClusterTag)
                .Item("independent_peers").Value(false)
                .Item("peer_count").Value(1)
            .EndMap()
        .Item("chaos_options")
            .BeginMap()
                .Item("peers")
                    .BeginList()
                        .Item()
                            .BeginMap()
                                .DoIf(clusterName != chaosConfig->AlphaChaosCluster, [&] (auto fluent) {
                                    fluent
                                        .Item("alien_cluster").Value(chaosConfig->AlphaChaosCluster);
                                })
                            .EndMap()
                    .EndList()
            .EndMap()
        .Finish();

    mutations->ForeignChaosCellBundlesToCreate[clusterName][bundleName] = attributes;

    YT_LOG_INFO("Creating new foreign chaos cell bundle (Cluster: %v, ChaosCellBundle: %v, BundleInfo: %v)",
        clusterName,
        bundleName,
        ConvertToYsonString(attributes, EYsonFormat::Text));
}

void CreateBetaArea(
    const TString& bundleName,
    const TString& clusterName,
    TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    const auto& clusterBundles = GetOrCrash(input.ForeignChaosCellBundles, clusterName);
    const auto& chaosBundleInfo = GetOrCrash(clusterBundles, bundleName);

    if (chaosBundleInfo->Areas.contains(BetaAreaName)) {
        return;
    }

    const auto& chaosConfig = input.Config->ChaosConfig;

    auto attributes = NYTree::BuildAttributeDictionaryFluently()
        .Item("name").Value(BetaAreaName)
        .Item("cell_bundle_id").Value(chaosBundleInfo->Id)
        .Item("chaos_options")
            .BeginMap()
                .Item("peers")
                    .BeginList()
                        .Item()
                            .BeginMap()
                                .DoIf(clusterName != chaosConfig->BetaChaosCluster, [&] (auto fluent) {
                                    fluent
                                        .Item("alien_cluster").Value(chaosConfig->BetaChaosCluster);
                                })
                            .EndMap()
                    .EndList()
            .EndMap()
        .Finish();

    mutations->ForeignChaosAreasToCreate[clusterName][bundleName] = attributes;

    YT_LOG_INFO("Creating new foreign chaos area (Cluster: %v, ChaosCellBundle: %v, AreaInfo: %v)",
        clusterName,
        bundleName,
        ConvertToYsonString(attributes, EYsonFormat::Text));
}

void RegisterChaosCells(
    const TString& bundleName,
    TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    if (input.CellTagsByBundle.contains(bundleName)) {
        return;
    }

    const auto& registry = input.GlobalRegistry;
    auto lastCellTag = mutations->ChangedChaosCellTagLast.value_or(registry->CellTagLast);

    if (lastCellTag + 1 < registry->CellTagRangeBegin) {
        YT_LOG_WARNING("Cannot register new chaos cells for bundle: inconsistent global registry "
            "(ChaosCellBundle: %v, CellTagRangeBegin: %v, CellTagRangeEnd: %v, LastCellTag: %v)",
            bundleName,
            registry->CellTagRangeBegin,
            registry->CellTagRangeEnd,
            lastCellTag);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "global_cell_registry_is_inconsistent",
            .BundleName = bundleName,
            .Description = Format("Cannot register new chaos cells for bundle: inconsistent global registry"),
        });
        return;
    }

    if (lastCellTag + 2 > registry->CellTagRangeEnd) {
        YT_LOG_WARNING("Cannot register new chaos cells for bundle: "
            "chaos cell tag range is exhausted (ChaosCellBundle: %v)",
            bundleName);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "chaos_cell_tag_range_is_exhausted",
            .BundleName = bundleName,
            .Description = "Cannot register new chaos cells for bundle: chaos cell tag range is exhausted",
        });
        return;
    }

    for (const auto& areaName : {DefaultAreaName, BetaAreaName}) {
        auto nextCellTag = TCellTag(++lastCellTag);
        auto cellTagInfo = New<TCellTagInfo>();
        cellTagInfo->Area = areaName;
        cellTagInfo->CellBundle = bundleName;
        cellTagInfo->CellId = MakeChaosCellId(nextCellTag);

        YT_LOG_INFO("Registering new chaos cell to global registry "
            "(ChaosCellBundle: %v, Area: %v, CellTag: %v, CellId: %v)",
            bundleName,
            cellTagInfo->Area,
            nextCellTag,
            cellTagInfo->CellId);

        mutations->CellTagsToRegister[nextCellTag] = std::move(cellTagInfo);
    }

    YT_LOG_INFO("Updating last cell tag (CurrentLastCellTag: %v, NewLastCellTag: %v)",
        mutations->ChangedChaosCellTagLast,
        lastCellTag);

    mutations->ChangedChaosCellTagLast = lastCellTag;
}

void RegisterAdditionalChaos(
    const TString& bundleName,
    TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    const auto& registry = input.GlobalRegistry;
    auto lastCellTag = mutations->ChangedChaosCellTagLast.value_or(registry->CellTagLast);
    auto bundleInfo = GetOrCrash(input.TabletCellBundles, bundleName);

    auto registeredChaosCells = [&] {
        auto it = input.AdditionalCellTagsByBundle.find(bundleName);
        if (it == input.AdditionalCellTagsByBundle.end()) {
            return 0;
        }

        return static_cast<int>(it->second.size());
    }();

    auto cellsToRegister = std::max(bundleInfo->TargetConfig->AdditionalChaosCellCount - registeredChaosCells / 2, 0);

    if (cellsToRegister == 0) {
        return;
    }

    if (lastCellTag + 1 < registry->CellTagRangeBegin) {
        YT_LOG_WARNING("Cannot register additional chaos cells for bundle: inconsistent global registry "
            "(ChaosCellBundle: %v, CellTagRangeBegin: %v, CellTagRangeEnd: %v, LastCellTag: %v)",
            bundleName,
            registry->CellTagRangeBegin,
            registry->CellTagRangeEnd,
            lastCellTag);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "global_cell_registry_is_inconsistent",
            .BundleName = bundleName,
            .Description = Format("Cannot register new chaos cells for bundle: inconsistent global registry"),
        });
        return;
    }

    if (lastCellTag + cellsToRegister > registry->CellTagRangeEnd) {
        YT_LOG_WARNING("Cannot register new chaos cells for bundle: "
            "chaos cell tag range is exhausted (ChaosCellBundle: %v)",
            bundleName);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "chaos_cell_tag_range_is_exhausted",
            .BundleName = bundleName,
            .Description = "Cannot register new chaos cells for bundle: chaos cell tag range is exhausted",
        });
        return;
    }

    for (const auto& areaName : {DefaultAreaName, BetaAreaName}) {
        for (int index = 0; index < cellsToRegister; ++index) {
            auto nextCellTag = TCellTag(++lastCellTag);
            auto cellTagInfo = New<TCellTagInfo>();
            cellTagInfo->Area = areaName;
            cellTagInfo->CellBundle = bundleName;
            cellTagInfo->CellId = MakeChaosCellId(nextCellTag);

            YT_LOG_INFO("Registering additional chaos cell to global registry "
                "(ChaosCellBundle: %v, Area: %v, CellTag: %v, CellId: %v)",
                bundleName,
                cellTagInfo->Area,
                nextCellTag,
                cellTagInfo->CellId);

            mutations->AdditionalCellTagsToRegister[nextCellTag] = std::move(cellTagInfo);
        }
    }

    YT_LOG_INFO("Updating last cell tag (CurrentLastCellTag: %v, NewLastCellTag: %v)",
        mutations->ChangedChaosCellTagLast,
        lastCellTag);

    mutations->ChangedChaosCellTagLast = lastCellTag;
}

void CreateChaosCells(
    const TString& bundleName,
    const TString& clusterName,
    TChaosSchedulerInputState& input,
    const std::vector<TCellTagInfoPtr>& cellsByBundle,
    TChaosSchedulerMutations* mutations)
{
    const auto& clusterBundles = GetOrCrash(input.ForeignChaosCellBundles, clusterName);
    const auto& chaosBundleInfo = GetOrCrash(clusterBundles, bundleName);

    if (!chaosBundleInfo->Areas.contains(BetaAreaName)) {
        return;
    }

    for (const auto& cellInfo : cellsByBundle) {
        if (chaosBundleInfo->ChaosCellIds.contains(cellInfo->CellId)) {
            continue;
        }
        auto attributes = NYTree::BuildAttributeDictionaryFluently()
            .Item("id").Value(cellInfo->CellId)
            .Item("area").Value(cellInfo->Area)
            .Item("cell_bundle").Value(cellInfo->CellBundle)
            .Finish();

        YT_LOG_INFO("Creating chaos cell for bundle (Cluster: %v, Attributes: %v)",
            clusterName,
            ConvertToYsonString(attributes, EYsonFormat::Text));

        mutations->ForeignChaosCellsToCreate[clusterName][ToString(cellInfo->CellId)] = std::move(attributes);
    }
}

void SetMetadataCellIds(
    const TString& bundleName,
    const TString& clusterName,
    TChaosSchedulerInputState& input,
    TChaosSchedulerMutations* mutations)
{
    const auto& clusterBundles = GetOrCrash(input.ForeignChaosCellBundles, clusterName);
    const auto& chaosBundleInfo = GetOrCrash(clusterBundles, bundleName);

    const auto& bundleCells = input.CellTagsByBundle[bundleName];

    THashSet<TChaosCellId> metadataCellIds;
    for (const auto& cellInfo : bundleCells) {
        metadataCellIds.insert(cellInfo->CellId);
    }

    if (chaosBundleInfo->MetadataCellIds == metadataCellIds) {
        return;
    }

    YT_LOG_INFO("Setting metadata cell ids for chaos bundle "
        "(Cluster: %v, ChaosCellBundle: %v, MetadataCellIds: %v, CurrentMetadataCellIds: %v)",
        clusterName,
        bundleName,
        metadataCellIds,
        chaosBundleInfo->MetadataCellIds);

    mutations->ForeignMetadataCellIdsToSet[clusterName][bundleName] = std::move(metadataCellIds);
}

void ProcessChaosCellBundles(const TString& bundleName, TChaosSchedulerInputState& input, TChaosSchedulerMutations* mutations)
{
    const auto& chaosConfig = input.Config->ChaosConfig;
    for (const auto& clusterName : chaosConfig->ChaosCellClusters) {
        auto clusterIt = input.ForeignChaosCellBundles.find(clusterName);
        if (clusterIt == input.ForeignChaosCellBundles.end()) {
            YT_LOG_WARNING("Cannot find chaos cell bundles for cluster (Cluster: %v)",
                clusterName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "cannot_find_chaos_cell_bundles",
                .BundleName = bundleName,
                .Description = Format("Cannot find chaos cell bundles for cluster %v", clusterName),
            });
            continue;
        }

        const auto& clusterBundles = clusterIt->second;

        if (!clusterBundles.contains(bundleName)) {
            CreateChaosCellBundle(bundleName, clusterName, input, mutations);
        } else {
            CreateBetaArea(bundleName, clusterName, input, mutations);
            CreateChaosCells(bundleName, clusterName, input, input.CellTagsByBundle[bundleName], mutations);
            CreateChaosCells(bundleName, clusterName, input, input.AdditionalCellTagsByBundle[bundleName], mutations);
            SetMetadataCellIds(bundleName, clusterName, input, mutations);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

TCellTagsByBundle MakeIndexByBundle(const auto& cellTags)
{
    TCellTagsByBundle result;

    for (const auto& [_, cellInfo] : cellTags) {
        result[cellInfo->CellBundle].push_back(cellInfo);
    }

    return result;
}

///////////////////////////////////////////////////////////////////////////////

void ScheduleChaosBundles(TChaosSchedulerInputState& input, TChaosSchedulerMutations* mutations)
{
    if (!input.Config->EnableChaosBundleManagement) {
        return;
    }

    input.CellTagsByBundle = MakeIndexByBundle(input.GlobalRegistry->CellTags);
    input.AdditionalCellTagsByBundle = MakeIndexByBundle(input.GlobalRegistry->AdditionalCellTags);

    for (const auto& [bundleName, bundleInfo] : input.TabletCellBundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
            continue;
        }

        if (!bundleInfo->TargetConfig->InitChaosBundles) {
            continue;
        }

        RegisterChaosCells(bundleName, input, mutations);
        RegisterAdditionalChaos(bundleName, input, mutations);
        ProcessTabletCellBundles(bundleName, input, mutations);
        ProcessChaosCellBundles(bundleName, input, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
