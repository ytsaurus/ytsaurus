#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

using TBundlesInfo = TIndexedEntries<TBundleInfo>;
using TChaosBundlesInfo = TIndexedEntries<TChaosBundleInfo>;
using TCellTagsByBundle = THashMap<std::string, std::vector<TCellTagInfoPtr>>;

struct TChaosSchedulerInputState
{
    TBundleControllerConfigPtr Config;
    TIndexedEntries<TBundleInfo> TabletCellBundles;
    THashMap<std::string, TBundlesInfo> ForeignTabletCellBundles;
    THashMap<std::string, TChaosBundlesInfo> ForeignChaosCellBundles;

    TGlobalCellRegistryPtr GlobalRegistry;
    TCellTagsByBundle CellTagsByBundle;
    TCellTagsByBundle AdditionalCellTagsByBundle;

    TSysConfigPtr SysConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TChaosSchedulerMutations
{
    using TBundlesCellTag = THashMap<std::string, NObjectClient::TCellTag>;
    using TBundlesMetadataCells = THashMap<std::string, THashSet<TChaosCellId>>;
    using TObjectWithCreationInfo = THashMap<std::string, NYTree::IAttributeDictionaryPtr>;

    std::vector<TAlert> AlertsToFire;

    THashMap<std::string, THashSet<std::string>> ForeignSystemAccountsToCreate;
    THashMap<std::string, TObjectWithCreationInfo> ForeignTabletCellBundlesToCreate;
    THashMap<std::string, TBundlesCellTag> ForeignBundleCellTagsToSet;
    THashMap<std::string, TObjectWithCreationInfo> ForeignChaosCellBundlesToCreate;
    THashMap<std::string, TObjectWithCreationInfo> ForeignChaosAreasToCreate;
    THashMap<std::string, TObjectWithCreationInfo> ForeignChaosCellsToCreate;
    THashMap<std::string, TBundlesMetadataCells> ForeignMetadataCellIdsToSet;

    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> CellTagsToRegister;
    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> AdditionalCellTagsToRegister;
    std::optional<ui16> ChangedChaosCellTagLast;
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleChaosBundles(TChaosSchedulerInputState& input, TChaosSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
