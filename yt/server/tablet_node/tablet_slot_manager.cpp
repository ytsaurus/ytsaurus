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

#include <server/hydra/changelog.h>
#include <server/hydra/changelog_catalog.h>
#include <server/hydra/file_changelog_catalog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/remote_snapshot_store.h>

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

static auto& Logger = TabletNodeLogger;
static auto SlotScanPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TTabletSlotManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NCellNode::TCellNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , UsedSlotCount_(0)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnScanSlots, Unretained(this)),
            SlotScanPeriod,
            EPeriodicExecutorMode::Manual))
    { }

    void Initialize()
    {
        LOG_INFO("Initializing tablet node");

        ChangelogCatalog_ = CreateFileChangelogCatalog(Config_->TabletNode->Changelogs);

        // Clean snapshot temporary directory.
        ForcePath(Config_->TabletNode->Snapshots->TempPath);
        CleanTempFiles(Config_->TabletNode->Snapshots->TempPath);

        // Look for existing changelog stores; readjust config.
        yhash_set<TCellGuid> cellGuids;
        for (auto store : ChangelogCatalog_->GetStores()) {
            auto cellGuid = store->GetCellGuid();
            YCHECK(cellGuids.insert(cellGuid).second);
            LOG_INFO("Found slot %s", ~ToString(cellGuid));
        }

        if (Config_->TabletNode->Slots < cellGuids.size()) {
            LOG_WARNING("Found %d active slots while at most %d is suggested by configuration; allowing more slots",
                static_cast<int>(cellGuids.size()),
                Config_->TabletNode->Slots);
            Config_->TabletNode->Slots = static_cast<int>(cellGuids.size());
        }

        // Create slots.
        for (int index = 0; index < Config_->TabletNode->Slots; ++index) {
            auto slot = CreateSlot(index);
            Slots_.push_back(slot);
        }

        // Load active slots.
        YCHECK(UsedSlotCount_ == 0);
        for (const auto& cellGuid : cellGuids) {
            Slots_[UsedSlotCount_++]->Load(cellGuid);
        }

        SlotScanExecutor_->Start();

        LOG_INFO("Tablet node initialized");
    }


    int GetAvailableTabletSlotCount() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Config_->TabletNode->Slots - UsedSlotCount_;
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
            if (slot->GetCellGuid() == id) {
                return slot;
            }
        }
        return nullptr;
    }

    TTabletSlotPtr FindFreeSlot()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return FindSlot(NullCellGuid);
    }

    TTabletSlotPtr GetFreeSlot()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto slot = FindFreeSlot();
        YCHECK(slot);
        return slot;
    }


    void CreateSlot(const TCreateTabletSlotInfo& createInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto slot = GetFreeSlot();
        slot->Create(createInfo);
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

        // Notify the existing instance.
        slot->Remove();

        // Recreate the slot.
        int index = slot->GetIndex();
        Slots_[index] = CreateSlot(index);

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


    IChangelogCatalogPtr GetChangelogCatalog()
    {
        return ChangelogCatalog_;
    }

    ISnapshotStorePtr GetSnapshotStore(const TCellGuid& cellGuid)
    {
        return CreateRemoteSnapshotStore(
            Config_->TabletNode->Snapshots,
            cellGuid,
            Sprintf("//sys/tablet_cells/%s/snapshots", ~ToString(cellGuid)),
            Bootstrap_->GetMasterClient());
    }


    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto producer = BIND(&TImpl::BuildOrchidYson, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    
    DEFINE_SIGNAL(void(TTabletSlotPtr), SlotScan);

private:
    NCellNode::TCellNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    IChangelogCatalogPtr ChangelogCatalog_;

    int UsedSlotCount_;
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
                slot->BuildOrchidYson(fluent);
            });
    }


    class TTabletScanner
        : public TRefCounted
    {
    public:
        explicit TTabletScanner(TIntrusivePtr<TImpl> owner)
            : Owner_(owner)
        { }

        TFuture<void> Run()
        {
            auto awaiter = New<TParallelAwaiter>(Owner_->Bootstrap_->GetControlInvoker());
            auto this_ = MakeStrong(this);

            for (auto slot : Owner_->Slots_) {
                auto invoker = slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Read);
                if (!invoker)
                    continue;

                awaiter->Await(BIND([this, this_, slot] () {
                        Owner_->SlotScan_.Fire(slot);
                    })
                    .AsyncVia(invoker)
                    .Run());
            }

            return awaiter->Complete();
        }

    private:
        TIntrusivePtr<TImpl> Owner_;

    };

    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Slot scan started");

        auto scanner = New<TTabletScanner>(this);

        auto this_ = MakeStrong(this);
        scanner->Run().Subscribe(BIND([this, this_] () {
            VERIFY_THREAD_AFFINITY(ControlThread);
            LOG_DEBUG("Slot scan completed");
            SlotScanExecutor_->ScheduleNext();
        }));
    }


    TTabletSlotPtr CreateSlot(int index)
    {
        return New<TTabletSlot>(
            index,
            Config_,
            Bootstrap_);
    }

    static TTabletDescriptorPtr BuildTabletDescriptor(TTablet* tablet)
    {
        auto descriptor = New<TTabletDescriptor>();
        descriptor->Slot = tablet->GetSlot();
        descriptor->PartitionKeys.reserve(tablet->Partitions().size());
        for (const auto& partition : tablet->Partitions()) {
            descriptor->PartitionKeys.push_back(partition->GetPivotKey());
        }
        return descriptor;
    }


};

////////////////////////////////////////////////////////////////////////////////

TTabletSlotManager::TTabletSlotManager(
    NCellNode::TCellNodeConfigPtr config,
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

IChangelogCatalogPtr TTabletSlotManager::GetChangelogCatalog()
{
    return Impl_->GetChangelogCatalog();
}

ISnapshotStorePtr TTabletSlotManager::GetSnapshotStore(const TCellGuid& cellGuid)
{
    return Impl_->GetSnapshotStore(cellGuid);
}

IYPathServicePtr TTabletSlotManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TTabletSlotManager, void(TTabletSlotPtr), SlotScan, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
