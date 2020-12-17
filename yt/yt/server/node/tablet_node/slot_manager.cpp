#include "automaton.h"
#include "slot_manager.h"
#include "private.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "security_manager.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>
#include <yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/server/node/data_node/config.h>
#include <yt/server/node/data_node/master_connector.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/virtual.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NDataNode;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TSlotManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnScanSlots, Unretained(this)),
            Config_->SlotScanPeriod))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Bootstrap_->GetControlInvoker()))
    { }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SetTotalTabletSlotCount(Config_->ResourceLimits->Slots);
        SlotScanExecutor_->Start();

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigUpdated(BIND(&TImpl::OnDynamicConfigUpdated, MakeWeak(this)));
    }

    void SetTotalTabletSlotCount(int slotCount)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Slots_.size() == slotCount) {
            return;
        }

        YT_LOG_INFO("Updating tablet slot count (OldTabletSlotCount: %v, NewTabletSlotCount: %v)",
            Slots_.size(),
            slotCount);

        if (slotCount < Slots_.size()) {
            std::vector<TFuture<void>> asyncFinalizations;
            asyncFinalizations.reserve(Slots_.size() - slotCount);
            for (int slotIndex = slotCount; slotIndex < Slots_.size(); ++slotIndex) {
                const auto& slot = Slots_[slotIndex];
                if (slot) {
                    asyncFinalizations.push_back(slot->Finalize());
                }
            }

            auto error = WaitFor(AllSet(asyncFinalizations));
            YT_LOG_WARNING_UNLESS(error.IsOK(), error, "Failed to finalize tablet slot during slot manager reconfiguration");
        }

        Slots_.resize(slotCount);

        if (slotCount > 0) {
            // Requesting latest timestamp enables periodic background time synchronization.
            // For tablet nodes, it is crucial because of non-atomic transactions that require
            // in-sync time for clients.
            Bootstrap_
                ->GetMasterConnection()
                ->GetTimestampProvider()
                ->GetLatestTimestamp();
        }
    }

    int GetTotalTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Slots_.size();
    }

    int GetAvailableTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int availableSlotCount = 0;
        for (const auto& slot : Slots_) {
            if (!slot) {
                ++availableSlotCount;
            }
        }

        return availableSlotCount;
    }

    int GetUsedTabletSlotConut() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Slots_.size() - GetAvailableTabletSlotCount();
    }

    bool HasFreeTabletSlots() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return GetAvailableTabletSlotCount() > 0;
    }

    bool IsOutOfMemory(const std::optional<TString>& poolTag) const
    {
        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        return tracker->IsExceeded(EMemoryCategory::TabletDynamic, poolTag);
    }

    double GetUsedCpu(double cpuPerTabletSlot) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        double result = 0;
        for (const auto& slot : Slots_) {
            if (slot) {
                result += slot->GetUsedCpu(cpuPerTabletSlot);
            }
        }
        return result;
    }

    const std::vector<TTabletSlotPtr>& Slots() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Slots_;
    }

    TTabletSlotPtr FindSlot(TCellId id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& slot : Slots_) {
            if (slot && slot->GetCellId() == id) {
                return slot;
            }
        }

        return nullptr;
    }



    void CreateSlot(const TCreateTabletSlotInfo& createInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int index = GetFreeSlotIndex();
        Slots_[index] = New<TTabletSlot>(index, Config_, createInfo, Bootstrap_);
        Slots_[index]->Initialize();

        UpdateTabletCellBundleMemoryPoolWeight(createInfo.tablet_cell_bundle());
    }

    void ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        slot->Configure(configureInfo);
    }

    void RemoveSlot(TTabletSlotPtr slot)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        slot->Finalize().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError&) {
            VERIFY_THREAD_AFFINITY(ControlThread);

            if (Slots_[slot->GetIndex()] == slot) {
                Slots_[slot->GetIndex()].Reset();
            }

            UpdateTabletCellBundleMemoryPoolWeight(slot->GetTabletCellBundleName());
        }).Via(Bootstrap_->GetControlInvoker()));
    }


    std::vector<TTabletSnapshotPtr> GetTabletSnapshots()
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

    TTabletSnapshotPtr FindLatestTabletSnapshot(TTabletId tabletId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DoFindTabletSnapshot(tabletId, std::nullopt);
    }

    TTabletSnapshotPtr GetLatestTabletSnapshotOrThrow(TTabletId tabletId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindLatestTabletSnapshot(tabletId);
        if (!snapshot) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }
        return snapshot;
    }

    TTabletSnapshotPtr FindTabletSnapshot(TTabletId tabletId, TRevision mountRevision)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        return snapshot && snapshot->MountRevision == mountRevision
            ? snapshot
            : nullptr;
    }

    TTabletSnapshotPtr GetTabletSnapshotOrThrow(TTabletId tabletId, TRevision mountRevision)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = DoFindTabletSnapshot(tabletId, mountRevision);
        if (!snapshot) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }
        snapshot->ValidateMountRevision(mountRevision);
        return snapshot;
    }

    void ValidateTabletAccess(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp)
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

    void RegisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet, std::optional<TLockManagerEpoch> epoch)
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

    void UnregisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
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
                    BIND(&TImpl::EvictTabletSnapshot, MakeStrong(this), tablet->GetId(), snapshot)
                        .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()),
                    Config_->TabletSnapshotEvictionTimeout);

                break;
            }
        }
        // NB: It's fine not to find anything.
    }

    void UnregisterTabletSnapshots(TTabletSlotPtr slot)
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

    void UpdateTabletCellBundleMemoryPoolWeight(const TString& bundleName)
    {
        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();

        i64 totalWeight = 0;
        for (int index = 0; index < Slots_.size(); ++index) {
            if (Slots_[index] && Slots_[index]->GetTabletCellBundleName() == bundleName) {
                totalWeight += Slots_[index]->GetDynamicOptions()->DynamicMemoryPoolWeight;
            }
        }

        YT_LOG_DEBUG("Tablet cell bundle memory pool weight updated (Bundle: %v, Weight: %v)",
            bundleName,
            totalWeight);
        memoryTracker->SetPoolWeight(bundleName, totalWeight);
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(TabletSnapshotsSpinLock_);

        // TODO(sandello): Make this one symmetrical to checks in tablet_manager.cpp
        /*
        for (const auto& it : TabletIdToSnapshot_) {
            if (it.second->OverlappingStoreCount >= it.second->Config->MaxOverlappingStoreCount) {
                auto alert = TError("Too many overlapping stores in tablet %v, rotation disabled", it.first)
                    << TErrorAttribute("overlapping_store_count", it.second->OverlappingStoreCount)
                    << TErrorAttribute("overlapping_store_limit", it.second->Config->MaxOverlappingStoreCount);
                alerts->push_back(std::move(alert));
            }
        }
        */
    }


    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    void ValidateCellSnapshot(IAsyncZeroCopyInputStreamPtr reader)
    {
        if (Slots().empty()) {
            THROW_ERROR_EXCEPTION("No tablet slots in node config");
        }

        // We create fake tablet slot here populating descriptors with the least amount
        // of data such that configuration succeeds.
        {
            auto options = New<TTabletCellOptions>();
            options->SnapshotAccount = "a";
            options->ChangelogAccount = "a";

            NTabletClient::NProto::TCreateTabletSlotInfo protoInfo;
            ToProto(protoInfo.mutable_cell_id(), TGuid{});
            protoInfo.set_peer_id(0);
            protoInfo.set_options(ConvertToYsonString(*options).GetData());
            protoInfo.set_tablet_cell_bundle("b");

            CreateSlot(protoInfo);
        }

        {
            TCellDescriptor cellDescriptor;
            cellDescriptor.CellId = TGuid{};
            cellDescriptor.ConfigVersion = 0;
            TCellPeerDescriptor peerDescriptor;
            peerDescriptor.SetVoting(true);
            cellDescriptor.Peers = {peerDescriptor};
            // Object type should match EObjectType::Transaction, which is 1.
            TGuid prerequisiteTransactionId(1, 1, 1, 1);

            NTabletClient::NProto::TConfigureTabletSlotInfo protoInfo;
            ToProto(protoInfo.mutable_cell_descriptor(), cellDescriptor);
            ToProto(protoInfo.mutable_prerequisite_transaction_id(), prerequisiteTransactionId);

            const auto& slot = Slots()[0];
            YT_VERIFY(slot);
            ConfigureSlot(slot, protoInfo);
        }

        const auto& slot = Slots()[0];
        BIND([&] {
            const auto& hydraManager = slot->GetHydraManager();
            hydraManager->ValidateSnapshot(reader);
        })
            .AsyncVia(slot->GetAutomatonInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }


    DEFINE_SIGNAL(void(), BeginSlotScan);
    DEFINE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DEFINE_SIGNAL(void(), EndSlotScan);

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TImpl> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        virtual std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& slot : owner->Slots()) {
                    if (keys.size() >= limit) {
                        break;
                    }
                    if (slot) {
                        keys.push_back(ToString(slot->GetCellId()));
                    }
                }
            }
            return keys;
        }

        virtual i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->GetUsedTabletSlotConut();
            }
            return 0;
        }

        virtual IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto slot = owner->FindSlot(TCellId::FromString(key))) {
                    return slot->GetOrchidService();
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TImpl> Owner_;

        explicit TOrchidService(TWeakPtr<TImpl> owner)
            : Owner_(std::move(owner))
        { }

        DECLARE_NEW_FRIEND();
    };

    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    std::vector<TTabletSlotPtr> Slots_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, TabletSnapshotsSpinLock_);
    THashMultiMap<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;

    IYPathServicePtr OrchidService_;


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


    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Slot scan started");

        BeginSlotScan_.Fire();

        std::vector<TFuture<void>> asyncResults;
        for (auto slot : Slots_) {
            if (!slot)
                continue;

            asyncResults.push_back(
                BIND([=, this_ = MakeStrong(this)] () {
                    ScanSlot_.Fire(slot);
                })
                .AsyncVia(slot->GetGuardedAutomatonInvoker())
                .Run()
                // Silent any error to avoid premature return from WaitFor.
                .Apply(BIND([] (const TError&) { })));
        }
        auto result = WaitFor(AllSucceeded(asyncResults));
        YT_VERIFY(result.IsOK());

        EndSlotScan_.Fire();

        YT_LOG_DEBUG("Slot scan completed");
    }


    int GetFreeSlotIndex()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (int index = 0; index < Slots_.size(); ++index) {
            if (!Slots_[index]) {
                return index;
            }
        }
        YT_ABORT();
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

    void OnDynamicConfigUpdated(const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Update tablet slot count.
        auto tabletSlotCount = newConfig->TabletNode->Slots.value_or(Config_->ResourceLimits->Slots);
        SetTotalTabletSlotCount(tabletSlotCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TSlotManager::~TSlotManager()
{ }

void TSlotManager::Initialize()
{
    Impl_->Initialize();
}

bool TSlotManager::IsOutOfMemory(const std::optional<TString>& poolTag) const
{
    return Impl_->IsOutOfMemory(poolTag);
}

int TSlotManager::GetTotalTabletSlotCount() const
{
    return Impl_->GetTotalTabletSlotCount();
}

int TSlotManager::GetAvailableTabletSlotCount() const
{
    return Impl_->GetAvailableTabletSlotCount();
}

int TSlotManager::GetUsedTabletSlotCount() const
{
    return Impl_->GetUsedTabletSlotConut();
}

bool TSlotManager::HasFreeTabletSlots() const
{
    return Impl_->HasFreeTabletSlots();
}

double TSlotManager::GetUsedCpu(double cpuPerTabletSlot) const
{
    return Impl_->GetUsedCpu(cpuPerTabletSlot);
}

const std::vector<TTabletSlotPtr>& TSlotManager::Slots() const
{
    return Impl_->Slots();
}

TTabletSlotPtr TSlotManager::FindSlot(TCellId id)
{
    return Impl_->FindSlot(id);
}

void TSlotManager::CreateSlot(const TCreateTabletSlotInfo& createInfo)
{
    Impl_->CreateSlot(createInfo);
}

void TSlotManager::ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
{
    Impl_->ConfigureSlot(slot, configureInfo);
}

void TSlotManager::RemoveSlot(TTabletSlotPtr slot)
{
    Impl_->RemoveSlot(slot);
}

std::vector<TTabletSnapshotPtr> TSlotManager::GetTabletSnapshots()
{
    return Impl_->GetTabletSnapshots();
}

TTabletSnapshotPtr TSlotManager::FindLatestTabletSnapshot(TTabletId tabletId)
{
    return Impl_->FindLatestTabletSnapshot(tabletId);
}

TTabletSnapshotPtr TSlotManager::GetLatestTabletSnapshotOrThrow(TTabletId tabletId)
{
    return Impl_->GetLatestTabletSnapshotOrThrow(tabletId);
}

TTabletSnapshotPtr TSlotManager::FindTabletSnapshot(TTabletId tabletId, TRevision mountRevision)
{
    return Impl_->FindTabletSnapshot(tabletId, mountRevision);
}

TTabletSnapshotPtr TSlotManager::GetTabletSnapshotOrThrow(TTabletId tabletId, TRevision mountRevision)
{
    return Impl_->GetTabletSnapshotOrThrow(tabletId, mountRevision);
}

void TSlotManager::ValidateTabletAccess(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp)
{
    Impl_->ValidateTabletAccess(tabletSnapshot, timestamp);
}

void TSlotManager::RegisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet, std::optional<TLockManagerEpoch> epoch)
{
    Impl_->RegisterTabletSnapshot(std::move(slot), tablet, epoch);
}

void TSlotManager::UnregisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
{
    Impl_->UnregisterTabletSnapshot(std::move(slot), tablet);
}

void TSlotManager::UnregisterTabletSnapshots(TTabletSlotPtr slot)
{
    Impl_->UnregisterTabletSnapshots(std::move(slot));
}

void TSlotManager::UpdateTabletCellBundleMemoryPoolWeight(const TString& bundleName)
{
    Impl_->UpdateTabletCellBundleMemoryPoolWeight(bundleName);
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
    Impl_->PopulateAlerts(alerts);
}

IYPathServicePtr TSlotManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

void TSlotManager::ValidateCellSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader)
{
    Impl_->ValidateCellSnapshot(reader);
}

DELEGATE_SIGNAL(TSlotManager, void(), BeginSlotScan, *Impl_);
DELEGATE_SIGNAL(TSlotManager, void(TTabletSlotPtr), ScanSlot, *Impl_);
DELEGATE_SIGNAL(TSlotManager, void(), EndSlotScan, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
