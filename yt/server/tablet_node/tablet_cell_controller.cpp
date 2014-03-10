#include "tablet_cell_controller.h"
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
static auto TabletScanPeriod = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TTabletCellController::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NCellNode::TCellNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , UsedSlotCount_(0)
        , TabletScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnScanTablets, Unretained(this)),
            TabletScanPeriod,
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
        for (int slotIndex = 0; slotIndex < Config_->TabletNode->Slots; ++slotIndex) {
            auto slot = New<TTabletSlot>(
                slotIndex,
                Config_,
                Bootstrap_);
            Slots_.push_back(slot);
        }

        // Load active slots.
        YCHECK(UsedSlotCount_ == 0);
        for (const auto& cellGuid : cellGuids) {
            Slots_[UsedSlotCount_++]->Load(cellGuid);
        }

        TabletScanExecutor_->Start();

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

    TTabletSlotPtr FindSlot(const TCellGuid& guid)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto slot : Slots_) {
            if (slot->GetCellGuid() == guid) {
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

        slot->Remove();
        --UsedSlotCount_;
    }


    TTabletCellId FindCellByTablet(const TTabletId& tabletId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = TabletIdToCellIds_.find(tabletId);
        return it == TabletIdToCellIds_.end() ? NullTabletCellId : it->second;
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
            Bootstrap_->GetMasterChannel(),
            Bootstrap_->GetTransactionManager());
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

    TPeriodicExecutorPtr TabletScanExecutor_;
    yhash_map<TTabletId, TTabletCellId> TabletIdToCellIds_;


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
                auto result = NewPromise();
                auto callback = BIND(&TTabletScanner::ScanSlot, this_, slot, result);
                if (!invoker->Invoke(callback))
                    continue;
                awaiter->Await(result);
            }

            return awaiter->Complete();
        }

        const yhash_map<TTabletId, TTabletCellId>& GetResult() const
        {
            return TabletIdToCellIds_;
        }


    private:
        TIntrusivePtr<TImpl> Owner_;

        TSpinLock SpinLock_;
        yhash_map<TTabletId, TTabletCellId> TabletIdToCellIds_;

        void ScanSlot(TTabletSlotPtr slot, TPromise<void> result)
        {
            auto tabletManager = slot->GetTabletManager();

            {
                TGuard<TSpinLock> guard(SpinLock_);
                for (auto* tablet : tabletManager->Tablets().GetValues()) {
                    if (tablet->GetState() == ETabletState::Mounted) {
                        // NB: Allow collisions.
                        TabletIdToCellIds_[tablet->GetId()] = slot->GetCellGuid();
                    }
                }
            }

            Owner_->SlotScan_.Fire(slot);

            result.Set();
        }

    };

    void OnScanTablets()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_DEBUG("Tablet scan started");

        auto scanner = New<TTabletScanner>(this);

        auto this_ = MakeStrong(this);
        scanner->Run().Subscribe(BIND([this, this_, scanner] () {
            VERIFY_THREAD_AFFINITY(ControlThread);

            const auto& result = scanner->GetResult();
            LOG_DEBUG("Tablet scan completed, %d tablet(s) found",
                static_cast<int>(result.size()));

            TabletIdToCellIds_ = result;
            TabletScanExecutor_->ScheduleNext();
        }));
    }

};

////////////////////////////////////////////////////////////////////////////////

TTabletCellController::TTabletCellController(
    NCellNode::TCellNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        bootstrap))
{ }

TTabletCellController::~TTabletCellController()
{ }

void TTabletCellController::Initialize()
{
    Impl_->Initialize();
}

int TTabletCellController::GetAvailableTabletSlotCount() const
{
    return Impl_->GetAvailableTabletSlotCount();
}

int TTabletCellController::GetUsedTableSlotCount() const
{
    return Impl_->GetUsedTabletSlotCount();
}

const std::vector<TTabletSlotPtr>& TTabletCellController::Slots() const
{
    return Impl_->Slots();
}

TTabletSlotPtr TTabletCellController::FindSlot(const TCellGuid& guid)
{
    return Impl_->FindSlot(guid);
}

void TTabletCellController::CreateSlot(const TCreateTabletSlotInfo& createInfo)
{
    Impl_->CreateSlot(createInfo);
}

void TTabletCellController::ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
{
    Impl_->ConfigureSlot(slot, configureInfo);
}

void TTabletCellController::RemoveSlot(TTabletSlotPtr slot)
{
    Impl_->RemoveSlot(slot);
}

TTabletCellId TTabletCellController::FindCellByTablet(const TTabletId& tabletId)
{
    return Impl_->FindCellByTablet(tabletId);
}

IChangelogCatalogPtr TTabletCellController::GetChangelogCatalog()
{
    return Impl_->GetChangelogCatalog();
}

ISnapshotStorePtr TTabletCellController::GetSnapshotStore(const TCellGuid& cellGuid)
{
    return Impl_->GetSnapshotStore(cellGuid);
}

IYPathServicePtr TTabletCellController::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_SIGNAL(TTabletCellController, void(TTabletSlotPtr), SlotScan, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
