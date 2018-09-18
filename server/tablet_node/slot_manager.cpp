#include "slot_manager.h"
#include "private.h"
#include "config.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "security_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/config.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;
using namespace NTabletClient::NProto;
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
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnScanSlots, Unretained(this)),
            Config_->SlotScanPeriod,
            EPeriodicExecutorMode::Automatic))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Bootstrap_->GetControlInvoker()))
    { }

    void Initialize()
    {
        Slots_.resize(Config_->ResourceLimits->Slots);
        SlotScanExecutor_->Start();
    }


    bool IsOutOfMemory() const
    {
        const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        return tracker->IsExceeded(EMemoryCategory::TabletDynamic);
    }

    bool IsRotationForced(i64 passiveUsage) const
    {
        const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        return
            tracker->GetUsed(EMemoryCategory::TabletDynamic) - passiveUsage >
            tracker->GetLimit(EMemoryCategory::TabletDynamic) * Config_->ForcedRotationsMemoryRatio;
    }


    int GetAvailableTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_->ResourceLimits->Slots - UsedSlotCount_;
    }

    int GetUsedTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return UsedSlotCount_;
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

    TTabletSlotPtr FindSlot(const TCellId& id)
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
        ++UsedSlotCount_;
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
                --UsedSlotCount_;
            }
        }).Via(Bootstrap_->GetControlInvoker()));
    }


    std::vector<TTabletSnapshotPtr> GetTabletSnapshots()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(TabletSnapshotsSpinLock_);
        std::vector<TTabletSnapshotPtr> snapshots;
        snapshots.reserve(TabletIdToSnapshot_.size());
        for (const auto& pair : TabletIdToSnapshot_) {
            snapshots.push_back(pair.second);
        }
        return snapshots;
    }

    TTabletSnapshotPtr FindTabletSnapshot(const TTabletId& tabletId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tabletId);
        // NB: It is uncommon but possible to have multiple cells pretending to serve the same tablet.
        for (auto it = range.first; it != range.second; ++it) {
            const auto& snapshot = it->second;
            if (snapshot->HydraManager->IsActive()) {
                return snapshot;
            }
        }
        return nullptr;
    }

    TTabletSnapshotPtr GetTabletSnapshotOrThrow(const TTabletId& tabletId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindTabletSnapshot(tabletId);
        if (!snapshot) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "Tablet %v is not known",
                tabletId)
                << TErrorAttribute("tablet_id", tabletId);
        }
        return snapshot;
    }

    void ValidateTabletAccess(
        const TTabletSnapshotPtr& tabletSnapshot,
        EPermission permission,
        TTimestamp timestamp)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(tabletSnapshot, permission);

        if (timestamp != AsyncLastCommittedTimestamp) {
            const auto& hydraManager = tabletSnapshot->HydraManager;
            if (!hydraManager->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }
        }
    }

    void RegisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto newSnapshot = tablet->BuildSnapshot(slot);

        {
            TWriterGuard guard(TabletSnapshotsSpinLock_);
            auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
            for (auto it = range.first; it != range.second; ++it) {
                auto& snapshot = it->second;
                if (snapshot->CellId == slot->GetCellId()) {
                    auto deadSnapshot = std::move(snapshot);
                    snapshot = std::move(newSnapshot);
                    guard.Release();
                    // This is were deadSnapshot dies. It's also nice to have logging moved outside
                    // of a critical section.
                    LOG_DEBUG("Tablet snapshot updated (TabletId: %v, CellId: %v)",
                        tablet->GetId(),
                        slot->GetCellId());
                    return;
                }
            }
            TabletIdToSnapshot_.emplace(tablet->GetId(), newSnapshot);
        }

        LOG_DEBUG("Tablet snapshot registered (TabletId: %v, CellId: %v)",
            tablet->GetId(),
            slot->GetCellId());
    }

    void UnregisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWriterGuard guard(TabletSnapshotsSpinLock_);
        auto range = TabletIdToSnapshot_.equal_range(tablet->GetId());
        for (auto it = range.first; it != range.second; ++it) {
            auto& snapshot = it->second;
            if (snapshot->CellId == slot->GetCellId()) {
                auto deadSnapshot = std::move(snapshot);
                TabletIdToSnapshot_.erase(it);
                guard.Release();
                // This is were deadSnapshot dies. It's also nice to have logging moved outside
                // of a critical section.
                LOG_DEBUG("Tablet snapshot unregistered (TabletId: %v, CellId: %v)",
                    tablet->GetId(),
                    slot->GetCellId());
                return;
            }
        }
        // NB: It's fine not to find anything.
    }

    void UnregisterTabletSnapshots(TTabletSlotPtr slot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TTabletSnapshotPtr> deadSnapshots;

        {
            TWriterGuard guard(TabletSnapshotsSpinLock_);
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

        // This is were deadSnapshots die. It's also nice to have logging moved outside
        // of a critical section.
        for (const auto& snapshot : deadSnapshots) {
            LOG_DEBUG("Tablet snapshot unregistered (TabletId: %v, CellId: %v)",
                snapshot->TabletId,
                snapshot->CellId);
        }
    }


    void PopulateAlerts(std::vector<TError>* alerts)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(TabletSnapshotsSpinLock_);

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
                return owner->GetUsedTabletSlotCount();
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
    NCellNode::TBootstrap* const Bootstrap_;

    int UsedSlotCount_ = 0;
    std::vector<TTabletSlotPtr> Slots_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    TReaderWriterSpinLock TabletSnapshotsSpinLock_;
    THashMultiMap<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;

    IYPathServicePtr OrchidService_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Slot scan started");

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
        auto result = WaitFor(Combine(asyncResults));
        YCHECK(result.IsOK());

        EndSlotScan_.Fire();

        LOG_DEBUG("Slot scan completed");
    }


    int GetFreeSlotIndex()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (int index = 0; index < Slots_.size(); ++index) {
            if (!Slots_[index]) {
                return index;
            }
        }
        Y_UNREACHABLE();
    }
};

////////////////////////////////////////////////////////////////////////////////

TSlotManager::TSlotManager(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
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

bool TSlotManager::IsOutOfMemory() const
{
    return Impl_->IsOutOfMemory();
}

bool TSlotManager::IsRotationForced(i64 passiveUsage) const
{
    return Impl_->IsRotationForced(passiveUsage);
}

int TSlotManager::GetAvailableTabletSlotCount() const
{
    return Impl_->GetAvailableTabletSlotCount();
}

int TSlotManager::GetUsedTabletSlotCount() const
{
    return Impl_->GetUsedTabletSlotCount();
}

double TSlotManager::GetUsedCpu(double cpuPerTabletSlot) const
{
    return Impl_->GetUsedCpu(cpuPerTabletSlot);
}

const std::vector<TTabletSlotPtr>& TSlotManager::Slots() const
{
    return Impl_->Slots();
}

TTabletSlotPtr TSlotManager::FindSlot(const TCellId& id)
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

TTabletSnapshotPtr TSlotManager::FindTabletSnapshot(const TTabletId& tabletId)
{
    return Impl_->FindTabletSnapshot(tabletId);
}

TTabletSnapshotPtr TSlotManager::GetTabletSnapshotOrThrow(const TTabletId& tabletId)
{
    return Impl_->GetTabletSnapshotOrThrow(tabletId);
}

void TSlotManager::ValidateTabletAccess(
    const TTabletSnapshotPtr& tabletSnapshot,
    EPermission permission,
    TTimestamp timestamp)
{
    Impl_->ValidateTabletAccess(tabletSnapshot, permission, timestamp);
}

void TSlotManager::RegisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
{
    Impl_->RegisterTabletSnapshot(std::move(slot), tablet);
}

void TSlotManager::UnregisterTabletSnapshot(TTabletSlotPtr slot, TTablet* tablet)
{
    Impl_->UnregisterTabletSnapshot(std::move(slot), tablet);
}

void TSlotManager::UnregisterTabletSnapshots(TTabletSlotPtr slot)
{
    Impl_->UnregisterTabletSnapshots(std::move(slot));
}

void TSlotManager::PopulateAlerts(std::vector<TError>* alerts)
{
    Impl_->PopulateAlerts(alerts);
}

IYPathServicePtr TSlotManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TSlotManager, void(), BeginSlotScan, *Impl_);
DELEGATE_SIGNAL(TSlotManager, void(TTabletSlotPtr), ScanSlot, *Impl_);
DELEGATE_SIGNAL(TSlotManager, void(), EndSlotScan, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
