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

#include <yt/server/misc/memory_usage_tracker.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
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
    { }

    void Initialize()
    {
        LOG_INFO("Initializing tablet node");

        Slots_.resize(Config_->ResourceLimits->Slots);

        SlotScanExecutor_->Start();

        LOG_INFO("Tablet node initialized");
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

    const std::vector<TTabletSlotPtr>& Slots() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Slots_;
    }

    TTabletSlotPtr FindSlot(const TCellId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto slot : Slots_) {
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
        Slots_[index] = New<TTabletSlot>(index, Config_, Bootstrap_);
        Slots_[index]->Initialize(createInfo);
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

            Slots_[slot->GetIndex()].Reset();
            --UsedSlotCount_;
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
        auto it = TabletIdToSnapshot_.find(tabletId);
        return it == TabletIdToSnapshot_.end() ? nullptr : it->second;
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
        auto securityManager = Bootstrap_->GetSecurityManager();
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

    void RegisterTabletSnapshot(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = tablet->RebuildSnapshot();

        {
            TWriterGuard guard(TabletSnapshotsSpinLock_);
            YCHECK(TabletIdToSnapshot_.insert(std::make_pair(tablet->GetId(), snapshot)).second);
        }

        LOG_INFO("Tablet snapshot registered (TabletId: %v)",
            tablet->GetId());
    }

    void UnregisterTabletSnapshot(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        tablet->ResetSnapshot();

        {
            TWriterGuard guard(TabletSnapshotsSpinLock_);
            // NB: Don't check the result.
            TabletIdToSnapshot_.erase(tablet->GetId());
        }

        LOG_INFO("Tablet snapshot unregistered (TabletId: %v)",
            tablet->GetId());
    }

    void UpdateTabletSnapshot(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = tablet->RebuildSnapshot();

        {
            TWriterGuard guard(TabletSnapshotsSpinLock_);
            auto it = TabletIdToSnapshot_.find(tablet->GetId());
            if (it == TabletIdToSnapshot_.end()) {
                // NB: Snapshots could be forcefully dropped by UnregisterTabletSnapshots.
                return;
            }
            it->second = snapshot;
        }

        LOG_DEBUG("Tablet snapshot updated (TabletId: %v, CellId: %v)",
            tablet->GetId(),
            tablet->GetSlot()->GetCellId());
    }

    void UnregisterTabletSnapshots(TTabletSlotPtr slot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWriterGuard guard(TabletSnapshotsSpinLock_);
        auto it = TabletIdToSnapshot_.begin();
        while (it != TabletIdToSnapshot_.end()) {
            auto jt = it++;
            if (jt->second->CellId == slot->GetCellId()) {
                LOG_INFO("Tablet snapshot removed (TabletId: %v, CellId: %v)",
                    jt->first,
                    slot->GetCellId());
                TabletIdToSnapshot_.erase(jt);
            }
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

        auto producer = BIND(&TImpl::BuildOrchidYson, MakeStrong(this));
        return IYPathService::FromProducer(producer)
            ->Via(Bootstrap_->GetControlInvoker());
    }


    DEFINE_SIGNAL(void(), BeginSlotScan);
    DEFINE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DEFINE_SIGNAL(void(), EndSlotScan);

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    int UsedSlotCount_ = 0;
    std::vector<TTabletSlotPtr> Slots_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    TReaderWriterSpinLock TabletSnapshotsSpinLock_;
    yhash_map<TTabletId, TTabletSnapshotPtr> TabletIdToSnapshot_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .DoMapFor(Slots_, [&] (TFluentMap fluent, TTabletSlotPtr slot) {
                if (slot) {
                    fluent
                        .Item(ToString(slot->GetCellId()))
                        .Do(BIND(&TTabletSlot::BuildOrchidYson, slot));
                }
            });
    }


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
                    if (slot->GetHydraManager()->IsActiveLeader()) {
                        ScanSlot_.Fire(slot);
                    }
                })
                .AsyncVia(slot->GetGuardedAutomatonInvoker())
                .Run()
                // Silent any error to avoid premature return from WaitFor.
                .Apply(BIND([] (const TError&) { })));
        }
        WaitFor(Combine(asyncResults));

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
        YUNREACHABLE();
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

int TSlotManager::GetUsedTableSlotCount() const
{
    return Impl_->GetUsedTabletSlotCount();
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

void TSlotManager::RegisterTabletSnapshot(TTablet* tablet)
{
    Impl_->RegisterTabletSnapshot(tablet);
}

void TSlotManager::UnregisterTabletSnapshot(TTablet* tablet)
{
    Impl_->UnregisterTabletSnapshot(tablet);
}

void TSlotManager::UpdateTabletSnapshot(TTablet* tablet)
{
    Impl_->UpdateTabletSnapshot(tablet);
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
