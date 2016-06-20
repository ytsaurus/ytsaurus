#include "store_trimmer.h"
#include "store.h"
#include "ordered_chunk_store.h"
#include "config.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoreTrimmer
    : public TRefCounted
{
public:
    TStoreTrimmer(
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TStoreTrimmer::OnScanSlot, MakeStrong(this)));
    }

private:
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;


    void OnScanSlot(TTabletSlotPtr slot)
    {
        auto tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (tablet->IsSorted()) {
            return;
        }

        auto stores = PickStoresForTrimming(tablet);
        if (stores.empty()) {
            return;
        }

        LOG_INFO("Trimming tablet stores (TabletId: %v, StoreIds: %v)",
            tablet->GetId(),
            MakeFormattableRange(stores, TStoreIdFormatter()));

        const auto& storeManager = tablet->GetStoreManager();

        TReqCommitTabletStoresUpdate hydraRequest;
        ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
        hydraRequest.set_mount_revision(tablet->GetMountRevision());
        for (const auto& store : stores) {
            auto* descriptor = hydraRequest.add_stores_to_remove();
            ToProto(descriptor->mutable_store_id(), store->GetId());
            storeManager->BeginStoreCompaction(store);
        }

        CreateMutation(slot->GetHydraManager(), hydraRequest)
            ->CommitAndLog(Logger);
    }

    std::vector<TOrderedChunkStorePtr> PickStoresForTrimming(TTablet* tablet)
    {
        std::vector<TOrderedChunkStorePtr> result;
        for (const auto& pair : tablet->StoreRowIndexMap()) {
            const auto& store = pair.second;
            if (!store->IsChunk()) {
                break;
            }
            auto chunkStore = store->AsOrderedChunk();
            if (chunkStore->GetCompactionState() != EStoreCompactionState::None) {
                break;
            }
            if (chunkStore->GetStartingRowIndex() + chunkStore->GetRowCount() > tablet->GetTrimmedRowCount()) {
                break;
            }
            result.push_back(chunkStore);
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

void StartStoreTrimmer(
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
{
    if (config->EnableStoreTrimmer) {
        New<TStoreTrimmer>(config, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
