#include "tablet_slot_manager.h"
#include "config.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/ypath_service.h>
#include <core/ytree/fluent.h>

#include <server/data_node/master_connector.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/data_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NFS;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NNodeTrackerClient::NProto;
using namespace NDataNode;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static auto SlotScanPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TTabletSlotManager::TImpl
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
            SlotScanPeriod,
            EPeriodicExecutorMode::Manual))
    { }

    void Initialize()
    {
        LOG_INFO("Initializing tablet node");

        ForcePath(Config_->Snapshots->TempPath);
        CleanTempFiles(Config_->Snapshots->TempPath);

        Slots_.resize(Config_->Slots);

        SlotScanExecutor_->Start();

        LOG_INFO("Tablet node initialized");
    }


    bool IsOutOfMemory() const
    {
        const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        return
            tracker->GetUsed(NCellNode::EMemoryConsumer::Tablet) >
            Config_->MemoryLimit;
    }

    bool IsRotationForced(i64 passiveUsage) const
    {
        const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        return
            tracker->GetUsed(NCellNode::EMemoryConsumer::Tablet) - passiveUsage >
            Config_->MemoryLimit * Config_->ForcedRotationsMemoryRatio;
    }

    
    int GetAvailableTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_->Slots - UsedSlotCount_;
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

    TTabletSlotPtr FindSlot(const TCellGuid& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto slot : Slots_) {
            if (slot && slot->GetCellGuid() == id) {
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

        slot->Finalize();
        Slots_[slot->GetIndex()].Reset();
        --UsedSlotCount_;
    }


    TTabletDescriptorPtr FindTabletDescriptor(const TTabletId& tabletId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(TabletDescriptorsSpinLock_);
        auto it = TabletIdToDescriptor_.find(tabletId);
        return it == TabletIdToDescriptor_.end() ? nullptr : it->second;
    }

    void RegisterTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(TabletDescriptorsSpinLock_);
            auto descriptor = BuildTabletDescriptor(tablet);
            YCHECK(TabletIdToDescriptor_.insert(std::make_pair(tablet->GetId(), descriptor)).second);
        }

        LOG_INFO("Tablet descriptor added (TabletId: %s, CellGuid: %s)",
            ~ToString(tablet->GetId()),
            ~ToString(tablet->GetSlot()->GetCellGuid()));
    }

    void UnregisterTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(TabletDescriptorsSpinLock_);
            // NB: Don't check the result.
            TabletIdToDescriptor_.erase(tablet->GetId());
        }

        LOG_INFO("Tablet descriptor removed (TabletId: %s, CellGuid: %s)",
            ~ToString(tablet->GetId()),
            ~ToString(tablet->GetSlot()->GetCellGuid()));
    }

    void UpdateTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto descriptor = BuildTabletDescriptor(tablet);

        {
            TGuard<TSpinLock> guard(TabletDescriptorsSpinLock_);
            auto it = TabletIdToDescriptor_.find(tablet->GetId());
            YCHECK(it != TabletIdToDescriptor_.end());
            it->second = descriptor;
        }

        LOG_INFO("Tablet descriptor updated (TabletId: %s, CellGuid: %s)",
            ~ToString(tablet->GetId()),
            ~ToString(tablet->GetSlot()->GetCellGuid()));
    }

    void UnregisterTablets(TTabletSlotPtr slot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(TabletDescriptorsSpinLock_);
        auto it = TabletIdToDescriptor_.begin();
        while (it != TabletIdToDescriptor_.end()) {
            auto jt = it++;
            if (jt->second->Slot == slot) {
                LOG_INFO("Tablet descriptor removed (TabletId: %s, CellGuid: %s)",
                    ~ToString(jt->first),
                    ~ToString(slot->GetCellGuid()));
                TabletIdToDescriptor_.erase(jt);
            }
        }
    }


    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto producer = BIND(&TImpl::BuildOrchidYson, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    
    DEFINE_SIGNAL(void(), BeginSlotScan);
    DEFINE_SIGNAL(void(TTabletSlotPtr), ScanSlot);
    DEFINE_SIGNAL(void(), EndSlotScan);

private:
    TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    int UsedSlotCount_ = 0;
    std::vector<TTabletSlotPtr> Slots_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    TSpinLock TabletDescriptorsSpinLock_;
    yhash_map<TTabletId, TTabletDescriptorPtr> TabletIdToDescriptor_;


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .DoListFor(Slots_, [&] (TFluentList fluent, TTabletSlotPtr slot) {
                if (slot) {
                    fluent
                        .Item()
                        .Do(BIND(&TTabletSlot::BuildOrchidYson, slot));
                }
            });
    }


    class TSlotScanner
        : public TRefCounted
    {
    public:
        explicit TSlotScanner(TIntrusivePtr<TImpl> owner)
            : Owner_(owner)
        { }

        TFuture<void> Run()
        {
            auto awaiter = New<TParallelAwaiter>(Owner_->Bootstrap_->GetControlInvoker());
            auto this_ = MakeStrong(this);

            Owner_->BeginSlotScan_.Fire();

            for (auto slot : Owner_->Slots_) {
                if (!slot)
                    continue;

                auto invoker = slot->GetGuardedAutomatonInvoker();
                awaiter->Await(BIND([this, this_, slot] () {
                        Owner_->ScanSlot_.Fire(slot);
                    })
                    .AsyncVia(invoker)
                    .Run()
                    .Finally());
            }

            return awaiter
                ->Complete()
                .Apply(BIND(&TSlotScanner::OnComplete, this_));
        }

    private:
        TIntrusivePtr<TImpl> Owner_;


        void OnComplete()
        {
            Owner_->EndSlotScan_.Fire();
        }

    };

    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Slot scan started");

        auto scanner = New<TSlotScanner>(this);

        auto this_ = MakeStrong(this);
        scanner->Run().Subscribe(BIND([this, this_] () {
            VERIFY_THREAD_AFFINITY(ControlThread);
            LOG_DEBUG("Slot scan completed");
            SlotScanExecutor_->ScheduleNext();
        }));
    }


    static TTabletDescriptorPtr BuildTabletDescriptor(TTablet* tablet)
    {
        auto descriptor = New<TTabletDescriptor>();
        descriptor->Slot = tablet->GetSlot();
        for (const auto& partition : tablet->Partitions()) {
            descriptor->SplitKeys.insert(
                descriptor->SplitKeys.end(),
                partition->SampleKeys().begin(),
                partition->SampleKeys().end());
        }
        return descriptor;
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

TTabletSlotManager::TTabletSlotManager(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TTabletSlotManager::~TTabletSlotManager()
{ }

void TTabletSlotManager::Initialize()
{
    Impl_->Initialize();
}

bool TTabletSlotManager::IsOutOfMemory() const
{
    return Impl_->IsOutOfMemory();
}

bool TTabletSlotManager::IsRotationForced(i64 passiveUsage) const
{
    return Impl_->IsRotationForced(passiveUsage);
}

int TTabletSlotManager::GetAvailableTabletSlotCount() const
{
    return Impl_->GetAvailableTabletSlotCount();
}

int TTabletSlotManager::GetUsedTableSlotCount() const
{
    return Impl_->GetUsedTabletSlotCount();
}

const std::vector<TTabletSlotPtr>& TTabletSlotManager::Slots() const
{
    return Impl_->Slots();
}

TTabletSlotPtr TTabletSlotManager::FindSlot(const TCellGuid& id)
{
    return Impl_->FindSlot(id);
}

void TTabletSlotManager::CreateSlot(const TCreateTabletSlotInfo& createInfo)
{
    Impl_->CreateSlot(createInfo);
}

void TTabletSlotManager::ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
{
    Impl_->ConfigureSlot(slot, configureInfo);
}

void TTabletSlotManager::RemoveSlot(TTabletSlotPtr slot)
{
    Impl_->RemoveSlot(slot);
}

TTabletDescriptorPtr TTabletSlotManager::FindTabletDescriptor(const TTabletId& tabletId)
{
    return Impl_->FindTabletDescriptor(tabletId);
}

void TTabletSlotManager::RegisterTablet(TTablet* tablet)
{
    Impl_->RegisterTablet(tablet);
}

void TTabletSlotManager::UnregisterTablet(TTablet* tablet)
{
    Impl_->UnregisterTablet(tablet);
}

void TTabletSlotManager::UpdateTablet(TTablet* tablet)
{
    Impl_->UpdateTablet(tablet);
}

void TTabletSlotManager::UnregisterTablets(TTabletSlotPtr slot)
{
    Impl_->UnregisterTablets(std::move(slot));
}

IYPathServicePtr TTabletSlotManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TTabletSlotManager, void(), BeginSlotScan, *Impl_);
DELEGATE_SIGNAL(TTabletSlotManager, void(TTabletSlotPtr), ScanSlot, *Impl_);
DELEGATE_SIGNAL(TTabletSlotManager, void(), EndSlotScan, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
