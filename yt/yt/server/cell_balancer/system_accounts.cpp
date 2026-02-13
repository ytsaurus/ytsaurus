#include "system_accounts.h"

#include "bundle_mutation.h"
#include "config.h"
#include "cypress_bindings.h"
#include "helpers.h"
#include "input_state.h"
#include "mutations.h"
#include "private.h"

namespace NYT::NCellBalancer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TQuotaDiff
{
    i64 ChunkCount = 0;
    THashMap<std::string, i64> DiskSpacePerMedium;
    i64 NodeCount = 0;

    bool Empty() const
    {
        return ChunkCount == 0 &&
            NodeCount == 0 &&
            std::all_of(DiskSpacePerMedium.begin(), DiskSpacePerMedium.end(), [] (const auto& pair) {
                return pair.second == 0;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

using TQuotaChanges = THashMap<std::string, TBundleMutation<TQuotaDiff>>;

void AddQuotaChanges(
    const std::string& bundleName,
    const TBundleInfoPtr& bundleInfo,
    const TSchedulerInputState& input,
    int cellCount,
    TQuotaChanges& changes)
{
    const auto& bundleOptions = bundleInfo->Options;

    if (bundleOptions->SnapshotAccount != bundleOptions->ChangelogAccount) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with different "
            "snapshot and changelog accounts (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto accountName = bundleOptions->SnapshotAccount;
    auto accountIt = input.SystemAccounts.find(accountName);
    if (accountIt == input.SystemAccounts.end()) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with custom account"
            " (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto& currentLimit = accountIt->second->ResourceLimits;
    const auto& config = input.Config;

    cellCount = std::max(cellCount, 1);
    auto multiplier = bundleInfo->SystemAccountQuotaMultiplier * cellCount;

    TQuotaDiff quotaDiff;

    quotaDiff.ChunkCount = std::max<i64>(config->ChunkCountPerCell * multiplier, config->MinChunkCount) - currentLimit->ChunkCount;
    quotaDiff.NodeCount = std::max<i64>(config->NodeCountPerCell * multiplier, config->MinNodeCount) - currentLimit->NodeCount;

    auto getSpace = [&] (const std::string& medium) -> i64 {
        auto it = currentLimit->DiskSpacePerMedium.find(medium);
        if (it == currentLimit->DiskSpacePerMedium.end()) {
            return 0;
        }
        return it->second;
    };

    i64 snapshotSpace = config->SnapshotDiskSpacePerCell * multiplier;
    i64 changelogSpace = config->JournalDiskSpacePerCell * multiplier;

    if (bundleOptions->ChangelogPrimaryMedium == bundleOptions->SnapshotPrimaryMedium) {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] =
            snapshotSpace + changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
    } else {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] =
            changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
        quotaDiff.DiskSpacePerMedium[bundleOptions->SnapshotPrimaryMedium] =
            snapshotSpace - getSpace(bundleOptions->SnapshotPrimaryMedium);
    }

    if (!quotaDiff.Empty()) {
        changes[accountName] = TBundleMutation(bundleName, quotaDiff);
    }
}

void ApplyQuotaChange(const TQuotaDiff& change, const TAccountResourcesPtr& limits)
{
    limits->ChunkCount += change.ChunkCount;
    limits->NodeCount += change.NodeCount;

    for (const auto& [medium, spaceDiff] : change.DiskSpacePerMedium) {
        limits->DiskSpacePerMedium[medium] += spaceDiff;
    }
}

void SplitQuotaChange(const TQuotaDiff& change, TQuotaDiff* lifted, TQuotaDiff* lowered)
{
    auto writeSplit = [] (const TQuotaDiff& change, TQuotaDiff* lowered, TQuotaDiff* lifted, auto TQuotaDiff::* proj)
    {
        const auto& diff = change.*proj;
        if (diff < 0) {
            lowered->*proj = diff;
        } else {
            lifted->*proj = diff;
        }
    };

    for (const auto& [medium, diff] : change.DiskSpacePerMedium) {
        if (diff < 0) {
            lowered->DiskSpacePerMedium[medium] = diff;
        } else if (diff > 0) {
            lifted->DiskSpacePerMedium[medium] = diff;
        }
    }

    writeSplit(change, lowered, lifted, &TQuotaDiff::ChunkCount);
    writeSplit(change, lowered, lifted, &TQuotaDiff::NodeCount);
}

////////////////////////////////////////////////////////////////////////////////

void ManageSystemAccountLimit(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TQuotaChanges quotaChanges;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        auto guard = mutations->MakeBundleNameGuard(bundleName);

        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableTabletCellManagement ||
            !bundleInfo->EnableSystemAccountManagement)
        {
            continue;
        }

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }
        const auto& zoneInfo = zoneIt->second;

        int cellCount = std::max<int>(GetTargetCellCount(bundleInfo, zoneInfo), std::ssize(bundleInfo->TabletCellIds));
        int cellPeerCount = cellCount * bundleInfo->Options->PeerCount;
        AddQuotaChanges(bundleName, bundleInfo, input, cellPeerCount, quotaChanges);
    }

    if (quotaChanges.empty()) {
        return;
    }

    auto rootQuota = CloneYsonStruct(input.RootSystemAccount->ResourceLimits);

    for (const auto& [accountName, quotaChange] : quotaChanges) {
        auto guard = mutations->MakeBundleNameGuard(quotaChange.BundleName);

        const auto& accountInfo = GetOrCrash(input.SystemAccounts, accountName);
        auto newLiftedQuota = CloneYsonStruct(accountInfo->ResourceLimits);
        auto newLoweredQuota = CloneYsonStruct(accountInfo->ResourceLimits);

        TQuotaDiff liftedDiff;
        TQuotaDiff loweredDiff;
        SplitQuotaChange(quotaChange, &liftedDiff, &loweredDiff);

        ApplyQuotaChange(quotaChange, rootQuota);
        ApplyQuotaChange(loweredDiff, newLoweredQuota);
        // Apply full quota change because lifting should be performed after
        // limiting without loosing previous changes.
        ApplyQuotaChange(quotaChange, newLiftedQuota);

        mutations->LastBundleWithChangedRootSystemAccountLimit = quotaChange.BundleName;

        if (!loweredDiff.Empty()) {
            mutations->LoweredSystemAccountLimit[accountName] = mutations->WrapMutation(newLoweredQuota);
            YT_LOG_INFO("Lowering system account resource limits (Account: %v, NewResourceLimit: %v, OldResourceLimit: %v)",
                accountName,
                ConvertToYsonString(newLoweredQuota, EYsonFormat::Text),
                ConvertToYsonString(accountInfo->ResourceLimits, EYsonFormat::Text));
        }
        if (!liftedDiff.Empty()) {
            mutations->LiftedSystemAccountLimit[accountName] = mutations->WrapMutation(newLiftedQuota);
            YT_LOG_INFO("Lifting system account resource limits (Account: %v, NewResourceLimit: %v, OldResourceLimit: %v)",
                accountName,
                ConvertToYsonString(newLiftedQuota, EYsonFormat::Text),
                ConvertToYsonString(accountInfo->ResourceLimits, EYsonFormat::Text));
        }
    }

    mutations->ChangedRootSystemAccountLimit = rootQuota;
    YT_LOG_INFO("Adjusting root system account resource limits (NewResourceLimit: %v, OldResourceLimit: %v)",
        ConvertToYsonString(rootQuota, EYsonFormat::Text),
        ConvertToYsonString(input.RootSystemAccount->ResourceLimits, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
