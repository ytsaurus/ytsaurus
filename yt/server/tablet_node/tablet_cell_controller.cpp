#include "tablet_cell_controller.h"
#include "config.h"
#include "tablet_slot.h"
#include "private.h"

#include <core/concurrency/action_queue.h>

#include <server/hydra/changelog.h>
#include <server/hydra/changelog_catalog.h>
#include <server/hydra/file_changelog_catalog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/snapshot_catalog.h>
#include <server/hydra/file_snapshot_catalog.h>

#include <server/data_node/master_connector.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/data_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NNodeTrackerClient::NProto;
using namespace NDataNode;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

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
        , CompactionQueue_(New<TActionQueue>("Compaction"))
    { }

    void Initialize()
    {
        LOG_INFO("Initializing tablet node");

        ChangelogCatalog_ = CreateFileChangelogCatalog(Config_->TabletNode->Changelogs);
        SnapshotCatalog_ = CreateFileSnapshotCatalog(Config_->TabletNode->Snapshots);

        // Analyze catalogs, remove orphaned stores.
        {
            auto changelogStores = ChangelogCatalog_->GetStores();
            yhash_set<TCellGuid> changelogStoreGuids;
            for (auto store : changelogStores) {
                YCHECK(changelogStoreGuids.insert(store->GetCellGuid()).second);
            }

            auto snapshotStores = SnapshotCatalog_->GetStores();
            yhash_set<TCellGuid> snapshotStoreGuids;
            for (auto store : snapshotStores) {
                YCHECK(snapshotStoreGuids.insert(store->GetCellGuid()).second);
            }

            for (const auto& cellGuid : changelogStoreGuids) {
                if (snapshotStoreGuids.find(cellGuid) == snapshotStoreGuids.end()) {
                    LOG_INFO("Changelog store %s is orphaned, removing", ~ToString(cellGuid));
                    ChangelogCatalog_->RemoveStore(cellGuid);
                }
            }

            for (const auto& cellGuid : snapshotStoreGuids) {
                if (changelogStoreGuids.find(cellGuid) == changelogStoreGuids.end()) {
                    LOG_INFO("Snapshot store %s is orphaned, removing", ~ToString(cellGuid));
                    SnapshotCatalog_->RemoveStore(cellGuid);
                }
            }
        }

        // Examine remaining stores; readjust config.
        yhash_set<TCellGuid> cellGuids;
        {
            for (auto store : SnapshotCatalog_->GetStores()) {
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
        {
            int slotIndex = 0;
            for (const auto& cellGuid : cellGuids) {
                Slots_[slotIndex]->Load(cellGuid);
                ++slotIndex;
                ++UsedSlotCount_;
            }
        }

        LOG_INFO("Tablet node initialized");
    }


    int GetAvailableTabletSlotCount() const
    {
        return Config_->TabletNode->Slots - UsedSlotCount_;
    }

    int GetUsedTabletSlotCount() const
    {
        return UsedSlotCount_;
    }

    const std::vector<TTabletSlotPtr>& GetSlots() const
    {
        return Slots_;
    }

    TTabletSlotPtr FindSlot(const TCellGuid& guid)
    {
        for (auto slot : Slots_) {
            if (slot->GetCellGuid() == guid) {
                return slot;
            }
        }
        return nullptr;
    }

    TTabletSlotPtr FindFreeSlot()
    {
        return FindSlot(NullCellGuid);
    }

    TTabletSlotPtr GetFreeSlot()
    {
        auto slot = FindFreeSlot();
        YCHECK(slot);
        return slot;
    }


    void CreateSlot(const TCreateTabletSlotInfo& createInfo)
    {
        auto slot = GetFreeSlot();
        slot->Create(createInfo);
        ++UsedSlotCount_;
    }

    void ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
    {
        slot->Configure(configureInfo);
    }

    void RemoveSlot(TTabletSlotPtr slot)
    {
        slot->Remove();
        --UsedSlotCount_;
    }


    IChangelogCatalogPtr GetChangelogCatalog()
    {
        return ChangelogCatalog_;
    }

    ISnapshotCatalogPtr GetSnapshotCatalog()
    {
        return SnapshotCatalog_;
    }


    IInvokerPtr GetCompactionInvoker()
    {
        return CompactionQueue_->GetInvoker();
    }

private:
    NCellNode::TCellNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    IChangelogCatalogPtr ChangelogCatalog_;
    ISnapshotCatalogPtr SnapshotCatalog_;

    int UsedSlotCount_;
    std::vector<TTabletSlotPtr> Slots_;

    TActionQueuePtr CompactionQueue_;

};

////////////////////////////////////////////////////////////////////////////////

TTabletCellController::TTabletCellController(
    NCellNode::TCellNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        config,
        bootstrap))
{ }

TTabletCellController::~TTabletCellController()
{ }

void TTabletCellController::Initialize()
{
    Impl->Initialize();
}

int TTabletCellController::GetAvailableTabletSlotCount() const
{
    return Impl->GetAvailableTabletSlotCount();
}

int TTabletCellController::GetUsedTableSlotCount() const
{
    return Impl->GetUsedTabletSlotCount();
}

const std::vector<TTabletSlotPtr>& TTabletCellController::GetSlots() const
{
    return Impl->GetSlots();
}

TTabletSlotPtr TTabletCellController::FindSlot(const TCellGuid& guid)
{
    return Impl->FindSlot(guid);
}

void TTabletCellController::CreateSlot(const TCreateTabletSlotInfo& createInfo)
{
    Impl->CreateSlot(createInfo);
}

void TTabletCellController::ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
{
    Impl->ConfigureSlot(slot, configureInfo);
}

void TTabletCellController::RemoveSlot(TTabletSlotPtr slot)
{
    Impl->RemoveSlot(slot);
}

IChangelogCatalogPtr TTabletCellController::GetChangelogCatalog()
{
    return Impl->GetChangelogCatalog();
}

ISnapshotCatalogPtr TTabletCellController::GetSnapshotCatalog()
{
    return Impl->GetSnapshotCatalog();
}

NYT::IInvokerPtr TTabletCellController::GetCompactionInvoker()
{
    return Impl->GetCompactionInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
