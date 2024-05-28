#include "chunk_replica_cache_pinger.h"
#include "bootstrap.h"
#include "hunk_chunk.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicaCachePinger
    : public IChunkReplicaCachePinger
{
public:
    TChunkReplicaCachePinger(
        IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , ChunkReplicaCache_(Bootstrap_->GetClient()->GetNativeConnection()->GetChunkReplicaCache())
    { }

    void Start() override
    {
        const auto& slotManager = Bootstrap_->GetSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TChunkReplicaCachePinger::OnScanSlot, MakeWeak(this)));
    }

private:
    const IBootstrap* const Bootstrap_;
    const IChunkReplicaCachePtr ChunkReplicaCache_;

    void OnScanSlot(const ITabletSlotPtr& slot)
    {
        if (slot->GetAutomatonState() != NHydra::EPeerState::Leading) {
            return;
        }

        YT_LOG_DEBUG("Chunk replica cache pinger scans slot (CellId: %v)", slot->GetCellId());

        const auto& tabletManager = slot->GetTabletManager();

        {
            TForbidContextSwitchGuard guard;

            for (auto [tabletId, tablet] : tabletManager->Tablets()) {
                ScanTablet(tablet);
            }
        }

        YT_LOG_DEBUG("Chunk replica cache pinger slot scanning finished (CellId: %v)",
            slot->GetCellId());
    }

    void ScanTablet(TTablet* tablet)
    {
        for (const auto& [id, store] : tablet->StoreIdMap()) {
            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                ChunkReplicaCache_->PingChunk(chunkStore->GetChunkId());
            }
        }

        for (const auto& [id, hunkChunk] : tablet->HunkChunkMap()) {
            ChunkReplicaCache_->PingChunk(hunkChunk->GetId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaCachePingerPtr CreateChunkReplicaCachePinger(IBootstrap* bootstrap)
{
    return New<TChunkReplicaCachePinger>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
