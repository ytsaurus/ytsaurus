#include "tablet_snapshot_store.h"

#include "private.h"
#include "bootstrap.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "security_manager.h"
#include "slot_manager.h"

#include <yt/yt/server/lib/cellar_agent/cellar.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/yson/consumer.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/generic/hash_multi_map.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletSnapshotStore
    : public ITabletSnapshotStore
{
public:
    TTabletSnapshotStore(
        TTabletNodeConfigPtr config,
        IBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , OrchidService_(TOrchidService::Create(MakeWeak(this)))
    { }

    std::vector<TTabletSnapshotPtr> GetTabletSnapshots() const override
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

    TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoFindTabletSnapshot(tabletId, std::nullopt);
    }

    TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindLatestTabletSnapshot(tabletId);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        return snapshot;
    }

    TTabletSnapshotPtr FindTabletSnapshot(TTabletId tabletId, TRevision mountRevision) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        return snapshot && snapshot->MountRevision == mountRevision
            ? snapshot
            : nullptr;
    }

    TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId tabletId,
        TCellId cellId,
        TRevision mountRevision) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        ThrowOnMissingTabletSnapshot(tabletId, cellId, snapshot);
        snapshot->ValidateMountRevision(mountRevision);
        return snapshot;
    }

    void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTimestamp timestamp) const override
    {
        if (timestamp != AsyncLastCommittedTimestamp) {
            const auto& hydraManager = tabletSnapshot->HydraManager;
            if (!hydraManager->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }

            if (tabletSnapshot->Unregistered.load()) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::NoSuchTablet,
                    "Tablet %v has been unregistered",
                    tabletSnapshot->TabletId)
                    << TErrorAttribute("tablet_id", tabletSnapshot->TabletId);
            }
        }
    }

    void ValidateBundleNotBanned(
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITabletSlotPtr& slot) const override
    {
        TDynamicTabletCellOptionsPtr dynamicOptions;
        TString bundleName;
        if (slot) {
            dynamicOptions = slot->GetDynamicOptions();
            bundleName = slot->GetTabletCellBundleName();
        } else {
            const auto& occupant = Bootstrap_
                ->GetCellarNodeBootstrap()
                ->GetCellarManager()
                ->GetCellar(NCellarClient::ECellarType::Tablet)
                ->FindOccupant(tabletSnapshot->CellId);
            if (!occupant) {
                return;
            }

            dynamicOptions = occupant->GetDynamicOptions();
            bundleName = occupant->GetCellBundleName();
        }

        if (dynamicOptions->BanMessage) {
            THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::BundleIsBanned,
                "Bundle %Qv is banned", bundleName)
                << TError(dynamicOptions->BanMessage.value())
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("table_path", tabletSnapshot->TablePath);
        }
    }

    void RegisterTabletSnapshot(
        const ITabletSlotPtr& slot,
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto newSnapshot = tablet->BuildSnapshot(slot, epoch);
        tablet->RecomputeNonActiveStoresUnmergedRowCount();

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

    void UnregisterTabletSnapshot(const ITabletSlotPtr& slot, TTablet* tablet) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
        for (auto it = range.first; it != range.second; ++it) {
            auto snapshot = it->second;
            if (snapshot->CellId == slot->GetCellId()) {
                guard.Release();

                snapshot->Unregistered.store(true);

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

    void UnregisterTabletSnapshots(const ITabletSlotPtr& slot) override
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

    NYTree::IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TTabletSnapshotStore> store)
        {
            return New<TOrchidService>(std::move(store));
        }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            if (auto owner = Owner_.Lock()) {
                auto keys = owner->GetTabletIds();
                keys.resize(std::min(limit, std::ssize(keys)));
                return keys;
            }
            return {};
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->GetTabletIds().size();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                auto snapshots = owner->GetTabletSnapshots(TTabletId::FromString(key));
                if (!snapshots.empty()) {
                    auto producer = BIND(&TTabletSnapshotStore::BuildSnapshotsListOrchidYson, owner, snapshots);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TTabletSnapshotStore> Owner_;

        explicit TOrchidService(TWeakPtr<TTabletSnapshotStore> store)
            : Owner_(std::move(store))
        { }

        DECLARE_NEW_FRIEND()
    };

private:
    const TTabletNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, TabletSnapshotsSpinLock_);
    THashMultiMap<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    const IYPathServicePtr OrchidService_;

private:
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

    TTabletSnapshotPtr DoFindTabletSnapshot(TTabletId tabletId, std::optional<TRevision> mountRevision) const
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
            return std::pair(snapshot->HydraManager->IsActive(), snapshot->MountRevision);
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
        const TTabletSnapshotPtr& snapshot) const
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

        const auto& slotManager = Bootstrap_->GetSlotManager();
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

    std::vector<TString> GetTabletIds() const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);

        std::vector<TString> result;
        TTabletId last;
        result.reserve(TabletIdToSnapshot_.size());
        for (const auto& [key, _] : TabletIdToSnapshot_) {
            if (last != key) {
                result.push_back(ToString(key));
                last = key;
            }
        }
        return result;
    }

    std::vector<TTabletSnapshotPtr> GetTabletSnapshots(TTabletId tabletId) const
    {
        VERIFY_THREAD_AFFINITY_ANY();
        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tabletId);
        std::vector<TTabletSnapshotPtr> result;
        for (auto it = range.first; it != range.second; ++it) {
            result.push_back(it->second);
        }
        return result;
    }

    void BuildSnapshotsListOrchidYson(const std::vector<TTabletSnapshotPtr>& snapshots, IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .DoListFor(snapshots, [&] (TFluentList fluent, const TTabletSnapshotPtr& snapshot) {
                fluent
                    .Item().BeginMap()
                        .Do(BIND(&TTabletSnapshotStore::BuildSnapshotOrchidYson, Unretained(this), snapshot))
                    .EndMap();
            });
    }

    bool IsPhysicallySorted(const TTabletSnapshotPtr& snapshot) const
    {
        return snapshot->PhysicalSchema->GetKeyColumnCount() > 0;
    }

    bool IsPhysicallyOrdered(const TTabletSnapshotPtr& snapshot) const
    {
        return snapshot->PhysicalSchema->GetKeyColumnCount() == 0;
    }

    void BuildSnapshotOrchidYson(const TTabletSnapshotPtr& snapshot, TFluentMap fluent) const
    {
        fluent
            .Item("table_id").Value(snapshot->TableId)
            .Item("mount_revision").Value(snapshot->MountRevision)
            .Item("tablet_id").Value(snapshot->TabletId)
            .Item("cell_id").Value(snapshot->CellId)
            .Item("table_path").Value(snapshot->TablePath)
            .Do([snapshot] (auto fluent) {
                BuildTableSettingsOrchidYson(snapshot->Settings, fluent);
            })
            .DoIf(IsPhysicallySorted(snapshot), [&] (auto fluent) {
                fluent
                    .Item("pivot_key").Value(snapshot->PivotKey)
                    .Item("next_pivot_key").Value(snapshot->NextPivotKey)
                    .Item("eden").DoMap(BIND(&TTabletSnapshotStore::BuildPartitionOrchidYson, Unretained(this), snapshot->Eden))
                    .Item("partitions").DoListFor(
                        snapshot->PartitionList, [&] (auto fluent, const TPartitionSnapshotPtr& partition) {
                            fluent
                                .Item()
                                .DoMap(BIND(&TTabletSnapshotStore::BuildPartitionOrchidYson, Unretained(this), partition));
                        });
            })
            .DoIf(IsPhysicallyOrdered(snapshot), [&] (auto fluent) {
                fluent
                    .Item("stores").DoMapFor(snapshot->OrderedStores, [&] (auto fluent, const IStorePtr& store) {
                        fluent
                            .Item(ToString(store->GetId()))
                            .Do(BIND(&TTabletSnapshotStore::BuildStoreOrchidYson, Unretained(this), store));
                    });
            })
            .DoIf(!snapshot->Replicas.empty(), [&] (auto fluent) {
                fluent
                    .Item("replicas").DoMapFor(
                        snapshot->Replicas,
                        [&] (auto fluent, const auto& pair) {
                            const auto& [replicaId, replica] = pair;
                            fluent
                                .Item(ToString(replicaId))
                                .Do(BIND(&TTabletSnapshotStore::BuildReplicaOrchidYson, Unretained(this), replica));
                        });
            })
            .Item("atomicity").Value(snapshot->Atomicity)
            .Item("upstream_replica_id").Value(snapshot->UpstreamReplicaId)
            .Item("hash_table_size").Value(snapshot->HashTableSize)
            .Item("overlapping_store_count").Value(snapshot->OverlappingStoreCount)
            .Item("eden_overlapping_store_count").Value(snapshot->EdenOverlappingStoreCount)
            .Item("critical_partition_count").Value(snapshot->CriticalPartitionCount)
            .Item("retained_timestamp").Value(snapshot->RetainedTimestamp)
            .Item("store_count").Value(snapshot->StoreCount)
            .Item("preload_pending_store_count").Value(snapshot->PreloadPendingStoreCount)
            .Item("preload_completed_store_count").Value(snapshot->PreloadCompletedStoreCount)
            .Item("preload_failed_store_count").Value(snapshot->PreloadFailedStoreCount);
    }

    void BuildPartitionOrchidYson(const TPartitionSnapshotPtr& partition, TFluentMap fluent) const
    {
        fluent
            .Item("id").Value(partition->Id)
            .Item("pivot_key").Value(partition->PivotKey)
            .Item("next_pivot_key").Value(partition->NextPivotKey)
            .Item("sample_key_count").Value(partition->SampleKeys ? partition->SampleKeys->Keys.Size() : 0)
            .Item("stores").DoMapFor(partition->Stores, [&] (auto fluent, const IStorePtr& store) {
                fluent
                    .Item(ToString(store->GetId()))
                    .Do(BIND(&TTabletSnapshotStore::BuildStoreOrchidYson, Unretained(this), store));
            });
    }

    void BuildStoreOrchidYson(const IStorePtr& store, TFluentAny fluent) const
    {
        fluent
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Do(BIND(&IStore::BuildOrchidYson, store))
            .EndMap();
    }

    void BuildReplicaOrchidYson(const TTableReplicaSnapshotPtr& replica, TFluentAny fluent) const
    {
        if (!replica || !replica->RuntimeData) {
            return;
        }
        fluent
            .BeginMap()
                .Item("mode").Value(replica->RuntimeData->Mode.load(std::memory_order::relaxed))
                .Item("atomicity").Value(replica->RuntimeData->Atomicity.load(std::memory_order::relaxed))
                .Item("preserve_timestamps").Value(replica->RuntimeData->PreserveTimestamps)
                .Item("start_replication_timestamp").Value(replica->StartReplicationTimestamp)
                .Item("last_replication_timestamp").Value(replica->RuntimeData->LastReplicationTimestamp)
                .Item("current_replication_row_index").Value(replica->RuntimeData->CurrentReplicationRowIndex)
                .Item("committed_replication_row_index").Value(replica->RuntimeData->CommittedReplicationRowIndex)
                .Item("current_replication_timestamp").Value(replica->RuntimeData->CurrentReplicationTimestamp)
                .Item("prepared_replication_row_index").Value(replica->RuntimeData->PreparedReplicationRowIndex)
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletSnapshotStorePtr CreateTabletSnapshotStore(
    TTabletNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TTabletSnapshotStore>(std::move(config), bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TDummyTabletSnapshotStore
    : public ITabletSnapshotStore
{
public:
    TDummyTabletSnapshotStore(TTabletSnapshotPtr tabletSnapshot)
        : TabletSnapshot_(std::move(tabletSnapshot))
    { }

    std::vector<TTabletSnapshotPtr> GetTabletSnapshots() const override
    {
        return {TabletSnapshot_};
    }

    TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId /*tabletId*/) const override
    {
        return TabletSnapshot_;
    }

    TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(
        TTabletId /*tabletId*/,
        TCellId /*cellId*/) const override
    {
        return TabletSnapshot_;
    }

    TTabletSnapshotPtr FindTabletSnapshot(
        TTabletId /*tabletId*/,
        NHydra::TRevision /*mountRevision*/) const override
    {
        return TabletSnapshot_;
    }

    TTabletSnapshotPtr GetTabletSnapshotOrThrow(
        TTabletId /*tabletId*/,
        TCellId /*cellId*/,
        NHydra::TRevision /*mountRevision*/) const override
    {
        return TabletSnapshot_;
    }

    void ValidateTabletAccess(
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        NTransactionClient::TTimestamp /*timestamp*/) const override
    {
        return;
    }

    void ValidateBundleNotBanned(
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        const ITabletSlotPtr& /*slot*/) const override
    {
        return;
    }

    void RegisterTabletSnapshot(
        const ITabletSlotPtr& /*slot*/,
        TTablet* /*tablet*/,
        std::optional<TLockManagerEpoch> /*epoch*/) override
    {
        YT_ABORT();
    }

    void UnregisterTabletSnapshot(
        const ITabletSlotPtr& /*slot*/,
        TTablet* /*tablet*/) override
    {
        YT_ABORT();
    }

    void UnregisterTabletSnapshots(const ITabletSlotPtr& /*slot*/) override
    {
        YT_ABORT();
    }

    NYTree::IYPathServicePtr GetOrchidService() const override
    {
        YT_ABORT();
    }

private:
    const TTabletSnapshotPtr TabletSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

ITabletSnapshotStorePtr CreateDummyTabletSnapshotStore(TTabletSnapshotPtr tabletSnapshot)
{
    return New<TDummyTabletSnapshotStore>(std::move(tabletSnapshot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
