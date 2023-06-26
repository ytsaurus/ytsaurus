#include "store_rotator.h"

#include "tablet.h"
#include "store.h"
#include "partition.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/utilex/random.h>

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

TString ToString(const TMemoryDigest& memoryDigest)
{
    return Format("{TotalUsage: %v, PassiveUsage: %v, BackingUsage: %v, Limit: %v}",
        memoryDigest.TotalUsage,
        memoryDigest.PassiveUsage,
        memoryDigest.BackingUsage,
        memoryDigest.Limit);
}

////////////////////////////////////////////////////////////////////////////////

class TStoreRotator
    : public ILsmBackend
{
public:
    void StartNewRound(const TLsmBackendState& state) override
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

        ForcedRotationMemoryRatio_ =
            dynamicConfig->ForcedRotationMemoryRatio.value_or(
                config->ForcedRotationMemoryRatio);
    }

    TLsmActionBatch BuildLsmActions(
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
                    ForcedRotationCandidates_.push_back(store);
                    SavedTablets_.push_back(MakeStrong(store->GetTablet()));
                }
            }
        }

        YT_LOG_DEBUG("Finished building store rotator action batch");

        return batch;
    }

    TLsmActionBatch BuildOverallLsmActions() override
    {
        MemoryDigest_.Limit = BackendState_.DynamicMemoryLimit;
        MemoryDigest_.TotalUsage = BackendState_.DynamicMemoryUsage;

        for (const auto& [bundleName, state] : BackendState_.Bundles) {
            BundleMemoryDigests_[bundleName].Limit = state.DynamicMemoryLimit;
            BundleMemoryDigests_[bundleName].TotalUsage = state.DynamicMemoryUsage;
        }

        return PickForcedRotationFinalists();
    }

private:
    TLsmBackendState BackendState_;
    TMemoryDigest MemoryDigest_;
    THashMap<TString, TMemoryDigest> BundleMemoryDigests_;
    std::vector<TStore*> ForcedRotationCandidates_;
    std::vector<TTabletPtr> SavedTablets_;
    double ForcedRotationMemoryRatio_;
    i64 MinForcedFlushDataSize_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

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

            TRotateStoreRequest request{
                .Tablet = MakeStrong(tablet),
                .Reason = rotationReason,
            };

            if (rotationReason == EStoreRotationReason::Periodic) {
                PatchLastPeriodicRotationTime(&request);
            }

            batch.Rotations.push_back(request);
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

    TLsmActionBatch PickForcedRotationFinalists()
    {
        auto isRotationForcedPerBundle = [&] (
            const auto& bundleState,
            const auto& bundleMemoryDigest)
        {
            if (!bundleState.EnablePerBundleMemoryLimit) {
                return false;
            }

            return IsRotationForced(
                bundleMemoryDigest,
                bundleState.ForcedRotationMemoryRatio);
        };

        // Order candidates by decreasing memory usage.
        std::sort(
            ForcedRotationCandidates_.begin(),
            ForcedRotationCandidates_.end(),
            [&] (const auto* lhs, const auto* rhs) {
                return lhs->GetDynamicMemoryUsage() > rhs->GetDynamicMemoryUsage();
            });

        TLsmActionBatch batch;
        for (auto* store : ForcedRotationCandidates_) {
            const auto& bundleName = store->GetTablet()->TabletCellBundle();
            const auto& bundleState = BackendState_.Bundles[bundleName];
            auto& bundleMemoryDigest = BundleMemoryDigests_[bundleName];

            TStringBuf reason;

            if (IsRotationForced(
                MemoryDigest_,
                ForcedRotationMemoryRatio_))
            {
                reason = "global memory pressure condition";
            } else if (isRotationForcedPerBundle(bundleState, bundleMemoryDigest)) {
                reason = "per-bundle memory pressure condition";
            } else {
                continue;
            }

            YT_LOG_INFO("Scheduling store rotation due to %v (%v, "
                "GlobalMemory: %v, Bundle: %v, BundleMemory: %v, "
                "TabletMemoryUsage: %v, ForcedRotationMemoryRatio: %v)",
                reason,
                store->GetTablet()->GetLoggingTag(),
                MemoryDigest_,
                bundleName,
                bundleMemoryDigest,
                store->GetDynamicMemoryUsage(),
                bundleState.ForcedRotationMemoryRatio);

            batch.Rotations.push_back(TRotateStoreRequest{
                .Tablet = MakeStrong(store->GetTablet()),
                .Reason = EStoreRotationReason::Forced,

            });

            MemoryDigest_.PassiveUsage += store->GetDynamicMemoryUsage();
            bundleMemoryDigest.PassiveUsage += store->GetDynamicMemoryUsage();
        }

        return batch;
    }

    static bool IsRotationForced(
        const TMemoryDigest& memoryDigest,
        double forcedRotationMemoryRatio)
    {
        i64 adjustedUsage = memoryDigest.TotalUsage;
        adjustedUsage -= memoryDigest.PassiveUsage;
        adjustedUsage -= memoryDigest.BackingUsage;
        return adjustedUsage > memoryDigest.Limit * forcedRotationMemoryRatio;
    }

    EStoreRotationReason GetImmediateRotationReason(TTablet* tablet) const
    {
        if (tablet->GetIsOverflowRotationNeeded()) {
            return EStoreRotationReason::Overflow;
        } else if (IsPeriodicRotationNeeded(tablet)) {
            return EStoreRotationReason::Periodic;
        } else if (tablet->GetIsOutOfBandRotationRequested()) {
            return EStoreRotationReason::OutOfBand;
        } else {
            return EStoreRotationReason::None;
        }
    }

    bool IsPeriodicRotationNeeded(TTablet* tablet) const
    {
        // NB: We don't check for IsRotationPossible here and issue periodic
        // rotation requests even if rotation is not possible just to update
        // last periodic rotation time.
        if (!tablet->FindActiveStore()) {
            return false;
        }

        if (!tablet->GetLastPeriodicRotationTime()) {
            return false;
        }

        auto timeElapsed = BackendState_.CurrentTime - *tablet->GetLastPeriodicRotationTime();
        const auto& mountConfig = tablet->GetMountConfig();
        return
            mountConfig->DynamicStoreAutoFlushPeriod &&
            timeElapsed > *mountConfig->DynamicStoreAutoFlushPeriod;
    }

    void PatchLastPeriodicRotationTime(TRotateStoreRequest* request) const
    {
        const auto& mountConfig = request->Tablet->GetMountConfig();

        YT_VERIFY(request->Tablet->GetLastPeriodicRotationTime().has_value());

        auto period = *mountConfig->DynamicStoreAutoFlushPeriod;
        auto lastRotated = *request->Tablet->GetLastPeriodicRotationTime();
        auto timeElapsed = BackendState_.CurrentTime - lastRotated;
        i64 passedPeriodCount = (timeElapsed.GetValue() - 1) / period.GetValue();

        auto newLastRotated =
            lastRotated +
            (period * passedPeriodCount) +
            RandomDuration(mountConfig->DynamicStoreFlushPeriodSplay);

        if (passedPeriodCount > 1) {
            YT_LOG_DEBUG("More than one periodic rotation period passed between subsequent attempts "
                "(%v, LastRotated: %v, RotationPeriod: %v, SkippedPeriodCount: %v)",
                request->Tablet->GetLoggingTag(),
                lastRotated,
                period,
                passedPeriodCount - 1);
        }

        request->ExpectedLastPeriodicRotationTime = lastRotated;
        request->NewLastPeriodicRotationTime = newLastRotated;
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateStoreRotator()
{
    return New<TStoreRotator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
