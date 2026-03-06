#include "mutations.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/misc/mpl.h>

namespace NYT::NCellBalancer {

using namespace NYson;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

auto TAlert::GetKey() const -> TKey
{
    return {Id, DataCenter};
}

////////////////////////////////////////////////////////////////////////////////

void TSchedulerMutations::Log(const TLogger& Logger) const
{
    auto doFormatValue = [] <class T> (const T& value, auto&& self) -> std::string {
        std::string result;
        if constexpr (NMpl::IsSpecialization<T, TIntrusivePtr>) {
            result = ConvertToYsonString(value, EYsonFormat::Text).ToString();
            if constexpr (CMutationWithBundleName<typename T::TUnderlying>) {
                result += ", BundleName: " + value->BundleName;
            }
        } else if constexpr (NMpl::IsSpecialization<T, TBundleMutation>) {
            result = self(value.Mutation, self);
        } else {
            result = Format("%v", value);
        }

        if constexpr (CMutationWithBundleName<T>) {
            result += ", BundleName: " + value.BundleName;
        }

        return result;
    };

    auto formatValue = [&] (const auto& value) {
        return doFormatValue(value, doFormatValue);
    };

    auto onIndexedEntries = [&] (TStringBuf prefix, const auto& entries) {
        for (const auto& [key, value] : entries) {
            YT_LOG_DEBUG("Mutation: %v (Key: %v, Value: %v)",
                prefix,
                key,
                formatValue(value));
        }
    };

    auto onSet = [&] (TStringBuf prefix, const auto& entries) {
        for (const auto& entry : entries) {
            YT_LOG_DEBUG("Mutation: %v (Value: %v)",
                prefix,
                formatValue(entry));
        }
    };

    onIndexedEntries("new allocation", NewAllocations);
    onIndexedEntries("changed allocation", ChangedAllocations);
    onIndexedEntries("new deallocation", NewDeallocations);
    onIndexedEntries("change bundle state", ChangedStates);
    onIndexedEntries("change node annotations", ChangedNodeAnnotations);
    onIndexedEntries("change proxy annotations", ChangedProxyAnnotations);
    onSet("complete allocation", CompletedAllocations);
    onIndexedEntries("change node user tags", ChangedNodeUserTags);
    onIndexedEntries("change decommissioned flag", ChangedDecommissionedFlag);
    onIndexedEntries("change banned flag", ChangedBannedFlag);
    onIndexedEntries("change enable_bundle_balancer flag", ChangedEnableBundleBalancerFlag);
    onIndexedEntries("change mute_tablet_cells_check flag", ChangedMuteTabletCellsCheck);
    onIndexedEntries("change mute_tablet_cell_snapshots_check flag", ChangedMuteTabletCellSnapshotsCheck);
    onIndexedEntries("change proxy role", ChangedProxyRole);
    onSet("remove proxy role", RemovedProxyRole);
    for (const auto& mutation : CellsToRemove) {
        YT_LOG_DEBUG("Mutation: remove cell (Value: %v)",
            mutation.Mutation);
    }
    onIndexedEntries("create cells", CellsToCreate);
    onIndexedEntries("lift system account limit", LiftedSystemAccountLimit);
    onIndexedEntries("lower system account limit", LoweredSystemAccountLimit);
    if (ChangedRootSystemAccountLimit) {
        YT_LOG_DEBUG("Mutation: change root system account limit (Value: %v)",
            ConvertToYsonString(ChangedRootSystemAccountLimit, EYsonFormat::Text));
    }
    onSet("node to cleanup", NodesToCleanup);
    onSet("proxy to cleanup", ProxiesToCleanup);
    onIndexedEntries("change tablet static memory", ChangedTabletStaticMemory);
    onIndexedEntries("change bundle short name", ChangedBundleShortName);
    onIndexedEntries("change bundle node tag filter", ChangedNodeTagFilters);
    onIndexedEntries("initialize bundle target config", InitializedBundleTargetConfig);
}

auto TSchedulerMutations::MakeOnAlertCallback() -> TOnAlertCallback
{
    return BIND([this] (TAlert alert) {
        AlertsToFire.push_back(std::move(alert));
    });
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerMutations::TBundleNameGuard::TBundleNameGuard(std::string bundleName, TSchedulerMutations* mutations)
    : Owner_(mutations)
{
    YT_VERIFY(mutations);
    PrevBundleName_ = std::exchange(Owner_->BundleNameContext_, std::move(bundleName));
}

TSchedulerMutations::TBundleNameGuard::~TBundleNameGuard()
{
    std::exchange(Owner_->BundleNameContext_, std::move(PrevBundleName_));
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerMutations::TBundleNameGuard TSchedulerMutations::MakeBundleNameGuard(std::string bundleName)
{
    return TBundleNameGuard(std::move(bundleName), this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
