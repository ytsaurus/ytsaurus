#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "chunk.h"
#include "chunk_store.h"
#include "master_connector.h"

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <server/cell_node/bootstrap.h>

#include <util/random/random.h>

#include <utility>
#include <limits>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NCellNode;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{ }

void TChunkStore::Start()
{
    LOG_INFO("Chunk store scan started");

    try {
        for (int i = 0; i < Config->StoreLocations.size(); ++i) {
            auto locationConfig = Config->StoreLocations[i];

            auto location = New<TLocation>(
                ELocationType::Store,
                "store" + ToString(i),
                locationConfig,
                Bootstrap);

            location->SubscribeDisabled(BIND(&TChunkStore::OnLocationDisabled, Unretained(this), location));

            Locations_.push_back(location);

            auto descriptors = location->Initialize();
            FOREACH (const auto& descriptor, descriptors) {
                auto chunk = New<TStoredChunk>(
                    location,
                    descriptor,
                    Bootstrap->GetMemoryUsageTracker());
                RegisterChunk(chunk);
            }
        }

        FOREACH (const auto& location, Locations_) {
            const auto& locationCellGuid = location->GetCellGuid();
            if (locationCellGuid.IsEmpty())
                continue;

            if (CellGuid.IsEmpty()) {
                CellGuid = locationCellGuid;
            } else if (CellGuid != locationCellGuid) {
                LOG_FATAL("Inconsistent cell guid across chunk store locations: %s vs %s",
                    ~ToString(CellGuid),
                    ~ToString(locationCellGuid));
            }
        }

        if (!CellGuid.IsEmpty()) {
            DoSetCellGuid();
        }

    } catch (const std::exception& ex) {
        LOG_FATAL(ex, "Failed to initialize storage locations");
    }

    LOG_INFO("Chunk store scan complete, %d chunks found",
        GetChunkCount());
}

void TChunkStore::RegisterChunk(TStoredChunkPtr chunk)
{
    auto result = ChunkMap.insert(std::make_pair(chunk->GetId(), chunk));
    if (!result.second) {
        auto oldChunk = result.first->second;
        LOG_FATAL("Duplicate chunk (Current: %s, Previous: %s)",
            ~chunk->GetLocation()->GetChunkFileName(chunk->GetId()),
            ~oldChunk->GetLocation()->GetChunkFileName(oldChunk->GetId()));
    }

    auto location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+chunk->GetInfo().size());

    LOG_DEBUG("Chunk registered (ChunkId: %s, Size: %" PRId64 ")",
        ~ToString(chunk->GetId()),
        chunk->GetInfo().size());

    ChunkAdded_.Fire(chunk);
}

TStoredChunkPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->second;
}

TFuture<void> TChunkStore::RemoveChunk(TStoredChunkPtr chunk)
{
    auto promise = NewPromise<void>();
    chunk->ScheduleRemoval().Subscribe(
        BIND([=] () mutable {
            // NB: No result check here, the location might got disabled.
            ChunkMap.erase(chunk->GetId());

            auto location = chunk->GetLocation();
            location->UpdateChunkCount(-1);
            location->UpdateUsedSpace(-chunk->GetInfo().size());

            ChunkRemoved_.Fire(chunk);
            promise.Set();
        })
        .Via(Bootstrap->GetControlInvoker()));
    return promise;
}

TLocationPtr TChunkStore::GetNewChunkLocation()
{
    YASSERT(!Locations_.empty());

    std::vector<TLocationPtr> candidates;
    candidates.reserve(Locations_.size());

    int minCount = std::numeric_limits<int>::max();
    FOREACH (const auto& location, Locations_) {
        if (location->IsFull() || !location->IsEnabled()) {
            continue;
        }
        int count = location->GetSessionCount();
        if (count < minCount) {
            candidates.clear();
            minCount = count;
        }
        if (count == minCount) {
            candidates.push_back(location);
        }
    }

    if (candidates.empty()) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::OutOfSpace,
            "All locations are either disabled or full");
    }

    return candidates[RandomNumber(candidates.size())];
}

TChunkStore::TChunks TChunkStore::GetChunks() const
{
    TChunks result;
    result.reserve(ChunkMap.size());
    FOREACH (const auto& pair, ChunkMap) {
        result.push_back(pair.second);
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    return static_cast<int>(ChunkMap.size());
}

void TChunkStore::SetCellGuid(const TGuid& cellGuid)
{
    CellGuid = cellGuid;
    DoSetCellGuid();
}

void TChunkStore::DoSetCellGuid()
{
    FOREACH (const auto& location, Locations_) {
        location->SetCellGuid(CellGuid);
    }
}

const TGuid& TChunkStore::GetCellGuid() const
{
    return CellGuid;
}

void TChunkStore::OnLocationDisabled(TLocationPtr location)
{
    // Scan through all chunks and remove those residing on this dead location.
    {
        LOG_INFO("Started cleaning up chunk map");
        int count = 0;
        auto it = ChunkMap.begin();
        while (it != ChunkMap.end()) {
            auto jt = it++;
            auto chunk = jt->second;
            if (chunk->GetLocation() == location) {
                ChunkMap.erase(jt);
                ++count;
            }
        }
        LOG_INFO("Chunk map cleaned, %d chunks removed", count);
    }

    // Schedule an out-of-order heartbeat to notify the master about the disaster.
    {
        auto masterConnector = Bootstrap->GetMasterConnector();
        masterConnector->ForceRegister();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
