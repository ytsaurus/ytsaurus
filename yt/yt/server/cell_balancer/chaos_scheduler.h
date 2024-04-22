#pragma once

#include "cypress_bindings.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

using TBundlesInfo = TIndexedEntries<TBundleInfo>;
using TChaosBundlesInfo = TIndexedEntries<TChaosBundleInfo>;
using TCellTagsByBundle = THashMap<TString, std::vector<TCellTagInfoPtr>>;

struct TChaosSchedulerInputState
{
    TBundleControllerConfigPtr Config;
    TIndexedEntries<TBundleInfo> TabletCellBundles;
    THashMap<TString, TBundlesInfo> ForeignTabletCellBundles;
    THashMap<TString, TChaosBundlesInfo> ForeignChaosCellBundles;

    TGlobalCellRegistryPtr GlobalRegistry;
    TCellTagsByBundle CellTagsByBundle;
    TCellTagsByBundle AdditionalCellTagsByBundle;

    TSysConfigPtr SysConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct TChaosSchedulerMutations
{
    using TBundlesCellTag = THashMap<TString, NObjectClient::TCellTag>;
    using TBundlesMetadataCells = THashMap<TString, THashSet<TChaosCellId>>;
    using TObjectWithCreationInfo = THashMap<TString, NYTree::IAttributeDictionaryPtr>;

    std::vector<TAlert> AlertsToFire;

    THashMap<TString, THashSet<TString>> ForeignSystemAccountsToCreate;
    THashMap<TString, TObjectWithCreationInfo> ForeignTabletCellBundlesToCreate;
    THashMap<TString, TBundlesCellTag> ForeignBundleCellTagsToSet;
    THashMap<TString, TObjectWithCreationInfo> ForeignChaosCellBundlesToCreate;
    THashMap<TString, TObjectWithCreationInfo> ForeignChaosAreasToCreate;
    THashMap<TString, TObjectWithCreationInfo> ForeignChaosCellsToCreate;
    THashMap<TString, TBundlesMetadataCells> ForeignMetadataCellIdsToSet;

    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> CellTagsToRegister;
    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> AdditionalCellTagsToRegister;
    std::optional<ui16> ChangedChaosCellTagLast;
};

////////////////////////////////////////////////////////////////////////////////

void ScheduleChaosBundles(TChaosSchedulerInputState& input, TChaosSchedulerMutations* mutations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
