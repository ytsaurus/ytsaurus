#include "store_rotator.h"

#include "tablet.h"
#include "store.h"
#include "partition.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NLsm {

using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NTabletNode;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

struct TMemoryDigest
{
    i64 TotalUsage = 0;
    i64 PassiveUsage = 0;
    i64 BackingUsage = 0;
    i64 Limit = 0;

    TMemoryDigest& operator += (const TMemoryDigest& other)
    {
        TotalUsage += other.TotalUsage;
        PassiveUsage += other.PassiveUsage;
        BackingUsage += other.BackingUsage;

        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStoreRotator
    : public ILsmBackend
{
public:
    virtual void StartNewRound(const TLsmBackendState& state) override
    {
        BackendState_ = state;

        const auto& dynamicConfig = BackendState_.TabletNodeDynamicConfig->StoreFlusher;
        const auto& config = BackendState_.TabletNodeConfig;
        MinForcedFlushDataSize_ = dynamicConfig->MinForcedFlushDataSize.value_or(
            config->StoreFlusher->MinForcedFlushDataSize);

        BundleMemoryDigests_.clear();
        ForcedRotationCandidates_.clear();
        SavedTablets_.clear();
        MemoryDigest_ = {};

        EnableForcedRotationBackingMemoryAccounting_ =
            dynamicConfig->EnableForcedRotationBackingMemoryAccounting.value_or(
                config->EnableForcedRotationBackingMemoryAccounting);
        ForcedRotationMemoryRatio_ =
            dynamicConfig->ForcedRotationMemoryRatio.value_or(
                config->ForcedRotationMemoryRatio);
    }

    virtual TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const TString& bundleName) override
    {
        if (!BackendState_.Bundles.contains(bundleName)) {
            YT_LOG_WARNING("Backend state does not contain bundle, will not "
                "process tablets (BundleName: %v)",
                bundleName);
            return {};
        }

        YT_LOG_DEBUG("Started building store rotator action batch");

        TLsmActionBatch batch;
        TMemoryDigest digest;
        for (const auto& tablet : tablets) {
            batch.MergeWith(ScanTabletForImmediateRotation(tablet.Get()));
            digest += ScanTabletForMemoryDigest(tablet.Get());
        }

        {
            auto guard = Guard(SpinLock_);
            BundleMemoryDigests_[bundleName] += digest;
            MemoryDigest_ += digest;
        }

        for (const auto& tablet : tablets) {
            if (tablet->GetIsForcedRotationPossible()) {
                const auto& store = tablet->FindActiveStore();
                YT_VERIFY(store);
                if (store->GetCompressedDataSize() >= MinForcedFlushDataSize_) {
                    auto guard = Guard(SpinLock_);
                    ForcedRotationCandidates_[bundleName].push_back(store);
                    SavedTablets_.push_back(MakeStrong(store->GetTablet()));
                }
            }
        }

        YT_LOG_DEBUG("Finished building store rotator action batch");

        return batch;
    }

    virtual TLsmActionBatch BuildOverallLsmActions() override
    {
        TLsmActionBatch batch;

        MemoryDigest_.Limit = BackendState_.DynamicMemoryLimit;
        MemoryDigest_.TotalUsage = BackendState_.DynamicMemoryUsage;

        for (const auto& [bundleName, state] : BackendState_.Bundles) {
            BundleMemoryDigests_[bundleName].Limit = state.DynamicMemoryLimit;
            BundleMemoryDigests_[bundleName].TotalUsage = state.DynamicMemoryUsage;
        }

        for (const auto& [bundleName, candidates] : ForcedRotationCandidates_) {
            if (candidates.empty()) {
                continue;
            }

            batch.MergeWith(PickForcedRotationFinalists(
                candidates,
                bundleName,
                BackendState_.Bundles[bundleName],
                &BundleMemoryDigests_[bundleName]));
        }

        return batch;
    }

private:
    TLsmBackendState BackendState_;
    TMemoryDigest MemoryDigest_;
    THashMap<TString, TMemoryDigest> BundleMemoryDigests_;
    THashMap<TString, std::vector<TStore*>> ForcedRotationCandidates_;
    std::vector<TTabletPtr> SavedTablets_;
    bool EnableForcedRotationBackingMemoryAccounting_;
    double ForcedRotationMemoryRatio_;
    i64 MinForcedFlushDataSize_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

    TLsmActionBatch ScanTabletForImmediateRotation(TTablet* tablet)
    {
        TLsmActionBatch batch;

        if (auto rotationReason = GetImmediateRotationReason(tablet);
            rotationReason != EStoreRotationReason::None)
        {
            YT_LOG_DEBUG("Scheduling store rotation (Reason: %v, %v)",
                rotationReason,
                tablet->GetLoggingTag());
            YT_VERIFY(static_cast<bool>(tablet->FindActiveStore()));
            batch.Rotations.push_back(TRotateStoreRequest{
                .Tablet = MakeStrong(tablet),
                .Reason = rotationReason,
            });
        }

        return batch;
    }

    TMemoryDigest ScanTabletForMemoryDigest(TTablet* tablet) const
    {
        TMemoryDigest digest;

        auto onStore = [&] (TStore* store) {
            switch (store->GetStoreState()) {
                case EStoreState::PassiveDynamic:
                    digest.PassiveUsage += store->GetDynamicMemoryUsage();
                    break;

                case EStoreState::Persistent:
                    digest.BackingUsage += store->GetBackingStoreMemoryUsage();
                    break;

                default:
                    break;
            }
        };

        if (tablet->IsPhysicallySorted()) {
            for (const auto& store : tablet->Eden()->Stores()) {
                onStore(store.get());
            }
            for (const auto& partition : tablet->Partitions()) {
                for (const auto& store : partition->Stores()) {
                    onStore(store.get());
                }
            }
        } else {
            for (const auto& store : tablet->Stores()) {
                onStore(store.get());
            }
        }

        return digest;
    }

    TLsmActionBatch PickForcedRotationFinalists(
        std::vector<TStore*> candidates,
        const TString& bundleName,
        const TTabletCellBundleState& bundleState,
        TMemoryDigest* bundleMemoryDigest)
    {
        auto isRotationForced = [&] {
            // Per-bundle memory pressure.
            if (bundleState.EnablePerBundleMemoryLimit) {
                if (IsRotationForced(
                    *bundleMemoryDigest,
                    bundleState.EnableForcedRotationBackingMemoryAccounting,
                    bundleState.ForcedRotationMemoryRatio))
                {
                    return true;
                }
            }

            // Global memory pressure.
            return IsRotationForced(
                MemoryDigest_,
                EnableForcedRotationBackingMemoryAccounting_,
                ForcedRotationMemoryRatio_);
        };

        // Order candidates by increasing memory usage.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [&] (const auto* lhs, const auto* rhs) {
                return lhs->GetDynamicMemoryUsage() < rhs->GetDynamicMemoryUsage();
            });

        TLsmActionBatch batch;

        // Pick the heaviest candidates until no more rotations are needed.
        while (isRotationForced() && !candidates.empty()) {
            auto candidate = candidates.back();
            candidates.pop_back();

            YT_LOG_INFO("Scheduling store rotation due to memory pressure condition (%v, "
                "GlobalMemory: {TotalUsage: %v, PassiveUsage: %v, BackingUsage: %v, Limit: %v}, "
                "Bundle: %v, "
                "BundleMemory: {TotalUsage: %v, PassiveUsage: %v, BackingUsage: %v, Limit: %v}, "
                "TabletMemoryUsage: %v, ForcedRotationMemoryRatio: %v)",
                candidate->GetTablet()->GetLoggingTag(),
                MemoryDigest_.TotalUsage,
                MemoryDigest_.PassiveUsage,
                MemoryDigest_.BackingUsage,
                MemoryDigest_.Limit,
                bundleName,
                bundleMemoryDigest->TotalUsage,
                bundleMemoryDigest->PassiveUsage,
                bundleMemoryDigest->BackingUsage,
                bundleMemoryDigest->Limit,
                candidate->GetDynamicMemoryUsage(),
                bundleState.ForcedRotationMemoryRatio);

            batch.Rotations.push_back(TRotateStoreRequest{
                .Tablet = MakeStrong(candidate->GetTablet()),
                .Reason = EStoreRotationReason::Forced,
            });

            MemoryDigest_.PassiveUsage += candidate->GetDynamicMemoryUsage();
            bundleMemoryDigest->PassiveUsage += candidate->GetDynamicMemoryUsage();
        }

        return batch;
    }

    static bool IsRotationForced(
        const TMemoryDigest& memoryDigest,
        bool enableForcedRotationBackingMemoryAccounting,
        double forcedRotationMemoryRatio)
    {
        i64 adjustedUsage = memoryDigest.TotalUsage;
        adjustedUsage -= memoryDigest.PassiveUsage;
        if (!enableForcedRotationBackingMemoryAccounting) {
            adjustedUsage -= memoryDigest.BackingUsage;
        }
        return adjustedUsage > memoryDigest.Limit * forcedRotationMemoryRatio;
    }

    static EStoreRotationReason GetImmediateRotationReason(TTablet* tablet)
    {
        if (tablet->GetIsOverflowRotationNeeded()) {
            return EStoreRotationReason::Overflow;
        } else if (tablet->GetIsPeriodicRotationNeeded()) {
            return EStoreRotationReason::Periodic;
        } else {
            return EStoreRotationReason::None;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateStoreRotator()
{
    return New<TStoreRotator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
