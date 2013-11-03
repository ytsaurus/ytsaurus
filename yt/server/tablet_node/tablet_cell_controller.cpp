#include "tablet_cell_controller.h"
#include "config.h"
#include "tablet_slot.h"
#include "private.h"

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
        : Config(config)
        , Bootstrap(bootstrap)
        , UsedSlotCount(0)
    { }

    void Initialize()
    {
        LOG_INFO("Initializing tablet node");

        ChangelogCatalog = CreateFileChangelogCatalog(Config->TabletNode->Changelogs);
        SnapshotCatalog = CreateFileSnapshotCatalog(Config->TabletNode->Snapshots);

        // Analyze catalogs, remove orphaned stores.
        {
            auto changelogStores = ChangelogCatalog->GetStores();
            yhash_set<TCellGuid> changelogStoreGuids;
            for (auto store : changelogStores) {
                YCHECK(changelogStoreGuids.insert(store->GetCellGuid()).second);
            }

            auto snapshotStores = SnapshotCatalog->GetStores();
            yhash_set<TCellGuid> snapshotStoreGuids;
            for (auto store : snapshotStores) {
                YCHECK(snapshotStoreGuids.insert(store->GetCellGuid()).second);
            }

            for (const auto& cellGuid : changelogStoreGuids) {
                if (snapshotStoreGuids.find(cellGuid) == snapshotStoreGuids.end()) {
                    LOG_INFO("Changelog store %s is orphaned, removing", ~ToString(cellGuid));
                    ChangelogCatalog->RemoveStore(cellGuid);
                }
            }

            for (const auto& cellGuid : snapshotStoreGuids) {
                if (changelogStoreGuids.find(cellGuid) == changelogStoreGuids.end()) {
                    LOG_INFO("Snapshot store %s is orphaned, removing", ~ToString(cellGuid));
                    SnapshotCatalog->RemoveStore(cellGuid);
                }
            }
        }

        // Examine remaining stores; readjust config.
        yhash_set<TCellGuid> cellGuids;
        {
            for (auto store : SnapshotCatalog->GetStores()) {
                auto cellGuid = store->GetCellGuid();
                YCHECK(cellGuids.insert(cellGuid).second);
                LOG_INFO("Found slot %s", ~ToString(cellGuid));
            }

            if (Config->TabletNode->Slots < cellGuids.size()) {
                LOG_WARNING("Found %d active slots while at most %d is suggested by configuration; allowing more slots",
                    static_cast<int>(cellGuids.size()),
                    Config->TabletNode->Slots);
                Config->TabletNode->Slots = static_cast<int>(cellGuids.size());
            }
        }

        // Create slots.
        for (int slotIndex = 0; slotIndex < Config->TabletNode->Slots; ++slotIndex) {
            auto slot = New<TTabletSlot>(
                slotIndex,
                Config,
                Bootstrap);
            Slots.push_back(slot);
        }

        // Load active slots.
        {
            int slotIndex = 0;
            for (const auto& cellGuid : cellGuids) {
                Slots[slotIndex]->Load(cellGuid);
                ++slotIndex;
                ++UsedSlotCount;
            }
        }

        LOG_INFO("Tablet node initialized");
    }


    int GetAvailableTabletSlotCount() const
    {
        return Config->TabletNode->Slots;
    }

    int GetUsedTabletSlotCount() const
    {
        return UsedSlotCount;
    }

    const std::vector<TTabletSlotPtr>& GetSlots() const
    {
        return Slots;
    }

    TTabletSlotPtr FindSlot(const TCellGuid& guid)
    {
        for (auto slot : Slots) {
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
        ++UsedSlotCount;
    }

    void ConfigureSlot(TTabletSlotPtr slot, const TConfigureTabletSlotInfo& configureInfo)
    {
        slot->Configure(configureInfo);
    }

    void RemoveSlot(TTabletSlotPtr slot)
    {
        slot->Remove();
        --UsedSlotCount;
    }


    IChangelogCatalogPtr GetChangelogCatalog()
    {
        return ChangelogCatalog;
    }

    ISnapshotCatalogPtr GetSnapshotCatalog()
    {
        return SnapshotCatalog;
    }

private:
    NCellNode::TCellNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    IChangelogCatalogPtr ChangelogCatalog;
    ISnapshotCatalogPtr SnapshotCatalog;

    int UsedSlotCount;
    std::vector<TTabletSlotPtr> Slots;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
