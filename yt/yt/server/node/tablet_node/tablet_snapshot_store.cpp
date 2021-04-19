#include "private.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletSnapshotStore
    : public ITabletSnapshotStore
{
public:
    TTabletSnapshotStore(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    virtual std::vector<TTabletSnapshotPtr> GetTabletSnapshots() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        std::vector<TTabletSnapshotPtr> snapshots;
        snapshots.reserve(TabletIdToSnapshot_.size());
        for (const auto& [tabletId, snapshot] : TabletIdToSnapshot_) {
            snapshots.push_back(snapshot);
        }
        return snapshots;
    }

    virtual TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoFindTabletSnapshot(tabletId, std::nullopt);
    }

    virtual TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindLatestTabletSnapshot(tabletId);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        return snapshot;
    }

    virtual TTabletSnapshotPtr FindTabletSnapshot(TTabletId tabletId, TRevision mountRevision) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        return snapshot && snapshot->MountRevision == mountRevision
            ? snapshot
            : nullptr;
    }

    virtual TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId,
        TRevision mountRevision) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        snapshot->ValidateMountRevision(mountRevision);
        return snapshot;
    }

    virtual void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTimestamp timestamp) override
    {
        if (timestamp != AsyncLastCommittedTimestamp) {
            const auto& hydraManager = tabletSnapshot->HydraManager;
            if (!hydraManager->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }
        }
    }

    virtual void RegisterTabletSnapshot(
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto newSnapshot = tablet->BuildSnapshot(slot, epoch);

        {
            auto guard = WriterGuard(TabletSnapshotsSpinLock_);
            auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
            for (auto it = range.first; it != range.second; ++it) {
                auto& snapshot = it->second;
                if (snapshot->CellId == slot->GetCellId()) {
                    auto deadSnapshot = std::move(snapshot);
                    snapshot = std::move(newSnapshot);
                    guard.Release();
                    // This is where deadSnapshot dies. It's also nice to have logging moved outside
                    // of a critical section.
                    YT_LOG_DEBUG("Tablet snapshot updated (TabletId: %v, CellId: %v)",
                        tablet->GetId(),
                        slot->GetCellId());
                    return;
                }
            }
            TabletIdToSnapshot_.emplace(tablet->GetId(), newSnapshot);
        }

        YT_LOG_DEBUG("Tablet snapshot registered (TabletId: %v, CellId: %v)",
            tablet->GetId(),
            slot->GetCellId());
    }

    virtual void UnregisterTabletSnapshot(const ITabletSlotPtr& slot, TTablet* tablet) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
        for (auto it = range.first; it != range.second; ++it) {
            auto snapshot = it->second;
            if (snapshot->CellId == slot->GetCellId()) {
                guard.Release();

                YT_LOG_DEBUG("Tablet snapshot unregistered; eviction scheduled (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    slot->GetCellId());

                TDelayedExecutor::Submit(
                    BIND(&TTabletSnapshotStore::EvictTabletSnapshot, MakeStrong(this), tablet->GetId(), snapshot)
                        .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()),
                    Config_->TabletSnapshotEvictionTimeout);

                break;
            }
        }
        // NB: It's fine not to find anything.
    }

    virtual void UnregisterTabletSnapshots(const ITabletSlotPtr& slot) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TTabletSnapshotPtr> deadSnapshots;

        {
            auto guard = WriterGuard(TabletSnapshotsSpinLock_);
            for (auto it = TabletIdToSnapshot_.begin(); it != TabletIdToSnapshot_.end();) {
                auto jt = it++;
                auto& snapshot = jt->second;
                if (snapshot->CellId == slot->GetCellId()) {
                    deadSnapshots.emplace_back(std::move(snapshot));
                    TabletIdToSnapshot_.erase(jt);
                }
            }
            // NB: It's fine not to find anything.
        }

        // This is where deadSnapshots die. It's also nice to have logging moved outside
        // of a critical section.
        for (const auto& snapshot : deadSnapshots) {
            YT_LOG_DEBUG("Tablet snapshot unregistered (TabletId: %v, CellId: %v)",
                snapshot->TabletId,
                snapshot->CellId);
        }
    }

private:
    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, TabletSnapshotsSpinLock_);
    THashMultiMap<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void EvictTabletSnapshot(TTabletId tabletId, const TTabletSnapshotPtr& snapshot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(TabletSnapshotsSpinLock_);

        auto range = TabletIdToSnapshot_.equal_range(tabletId);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == snapshot) {
                TabletIdToSnapshot_.erase(it);
                guard.Release();

                // This is where snapshot dies. It's also nice to have logging moved outside
                // of a critical section.
                YT_LOG_DEBUG("Tablet snapshot evicted (TabletId: %v, CellId: %v)",
                    tabletId,
                    snapshot->CellId);
                break;
            }
        }
    }

    TTabletSnapshotPtr DoFindTabletSnapshot(TTabletId tabletId, std::optional<TRevision> mountRevision)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TTabletSnapshotPtr snapshot;

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tabletId);

        // NB: It is uncommon but possible to have multiple cells pretending to serve the same tablet.
        // Tie breaking order is:
        //  - if there is an instance with matching mount revision, pick it;
        //  - if there are active instances, pick one of them with maximum mount revision;
        //  - otherwise pick the instance with maximum mount revision.
        // Outdated snapshots are useful for AsyncLastCommitted reads and ReadDynamicStore requests.

        auto getComparisonSurrogate = [] (const TTabletSnapshotPtr& snapshot) {
            return std::make_pair(snapshot->HydraManager->IsActive(), snapshot->MountRevision);
        };

        for (auto it = range.first; it != range.second; ++it) {
            if (mountRevision && it->second->MountRevision == mountRevision) {
                return it->second;
            }

            if (!snapshot || getComparisonSurrogate(snapshot) < getComparisonSurrogate(it->second)) {
                snapshot = it->second;
            }
        }

        return snapshot;
    }

    void ThrowOnMissingTabletSnapshot(
        TTabletId tabletId,
        TCellId cellId,
        const TTabletSnapshotPtr& snapshot)
    {
        if (snapshot) {
            return;
        }

        if (!cellId) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        auto slot = slotManager->FindSlot(cellId);
        if (!slot){
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchCell,
                "Cell %v is not known",
                cellId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        }

        auto hydraManager = slot->GetHydraManager();
        if (hydraManager && !hydraManager->IsActive()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchCell,
                "Cell %v is not active",
                cellId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        } else {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId)
                << TErrorAttribute("cell_id", cellId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletSnapshotStorePtr CreateTabletSnapshotStore(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    return New<TTabletSnapshotStore>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
