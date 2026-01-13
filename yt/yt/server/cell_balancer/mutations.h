#pragma once

#include "public.h"
#include "bundle_mutation.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    std::string Id;
    std::optional<std::string> BundleName;
    std::optional<std::string> DataCenter;
    std::string Description;

    using TKey = std::tuple<
        /*Id*/ std::string,
        /*DataCenter*/ std::optional<std::string>
    >;

    TKey GetKey() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerMutations
{
private:
    class TBundleNameGuard;

public:
    template <class T>
    using TMutationMap = THashMap<std::string, TBundleMutation<TIntrusivePtr<T>>>;

    TIndexedEntries<TAllocationRequest> NewAllocations;
    TIndexedEntries<TAllocationRequest> ChangedAllocations;
    TIndexedEntries<TDeallocationRequest> NewDeallocations;
    TIndexedEntries<TBundleControllerState> ChangedStates;
    TMutationMap<TBundleControllerInstanceAnnotations> ChangedNodeAnnotations;
    TMutationMap<TBundleControllerInstanceAnnotations> ChangedProxyAnnotations;

    THashSet<TBundleMutation<std::string>> CompletedAllocations;

    using TUserTags = THashSet<std::string>;
    THashMap<std::string, TBundleMutation<TUserTags>> ChangedNodeUserTags;

    THashMap<std::string, TBundleMutation<bool>> ChangedDecommissionedFlag;
    THashMap<std::string, TBundleMutation<bool>> ChangedBannedFlag;
    THashMap<std::string, TBundleMutation<bool>> ChangedEnableBundleBalancerFlag;
    THashMap<std::string, TBundleMutation<bool>> ChangedMuteTabletCellsCheck;
    THashMap<std::string, TBundleMutation<bool>> ChangedMuteTabletCellSnapshotsCheck;

    THashMap<std::string, TBundleMutation<std::string>> ChangedProxyRole;
    THashSet<TBundleMutation<std::string>> RemovedProxyRole;

    std::vector<TBundleMutation<std::string>> CellsToRemove;

    // Maps bundle name to new tablet cells count to create.
    THashMap<std::string, int> CellsToCreate;

    std::vector<TAlert> AlertsToFire;

    THashMap<std::string, TBundleMutation<TAccountResourcesPtr>> LiftedSystemAccountLimit;
    THashMap<std::string, TBundleMutation<TAccountResourcesPtr>> LoweredSystemAccountLimit;

    // We store only the last bundle at a time because this mutation could be
    // annotated with one bundle only in order not to violate liveness.
    std::string LastBundleWithChangedRootSystemAccountLimit;
    TAccountResourcesPtr ChangedRootSystemAccountLimit;

    std::optional<TBundlesDynamicConfig> DynamicConfig;

    THashSet<std::string> NodesToCleanup;
    THashSet<std::string> ProxiesToCleanup;

    THashMap<std::string, i64> ChangedTabletStaticMemory;
    THashMap<std::string, std::string> ChangedBundleShortName;

    THashMap<std::string, TBundleMutation<std::string>> ChangedNodeTagFilters;
    THashMap<std::string, TBundleConfigPtr> InitializedBundleTargetConfig;

    TBundleNameGuard MakeBundleNameGuard(std::string bundleName);

    template <class T, class... Args>
        requires std::derived_from<T, TBundleNameMixin>
    TIntrusivePtr<T> NewMutation(Args&&... args);

    template <class T>
    TBundleMutation<T> WrapMutation(T mutation);

    void Log(const NLogging::TLogger& Logger) const;

    TOnAlertCallback MakeOnAlertCallback();

private:
    class TBundleNameGuard
        : TNonCopyable
    {
    public:
        TBundleNameGuard(std::string bundleName, TSchedulerMutations* mutations);
        ~TBundleNameGuard();

    private:
        std::string PrevBundleName_;
        TSchedulerMutations* Owner_;
    };

    std::string BundleNameContext_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

#define MUTATIONS_INL_H_
#include "mutations-inl.h"
#undef MUTATIONS_INL_H_
