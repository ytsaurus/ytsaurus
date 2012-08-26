#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "chunk.h"
#include "reader_cache.h"
#include "chunk_store.h"
#include "reader_cache.h"
#include "bootstrap.h"

#include <ytlib/misc/foreach.h>

#include <ytlib/chunk_client/chunk_holder_service_proxy.h>


#include <utility>
#include <limits>
#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;

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
            auto& locationConfig = Config->StoreLocations[i];

            auto location = New<TLocation>(
                ELocationType::Store,
                "store" + ToString(i),
                locationConfig,
                Bootstrap);
            Locations_.push_back(location);

            FOREACH (const auto& descriptor, location->Scan()) {
                auto chunk = New<TStoredChunk>(location, descriptor);
                RegisterChunk(chunk);
            }
        }

        FOREACH(const auto& location, Locations_) {
            auto cellGuid = location->GetCellGuid();
            if (cellGuid.IsEmpty())
                continue;

            if (CellGuid.IsEmpty()) {
                CellGuid = cellGuid;
            } else if (CellGuid != cellGuid) {
                LOG_FATAL(
                    "Inconsistent cell guid across chunk store locations (%s and %s)", 
                    ~CellGuid.ToString(),
                    ~cellGuid.ToString());
            }
        }

        if (!CellGuid.IsEmpty()) {
            DoUpdateCellGuid();
        }

    } catch (const std::exception& ex) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ex.what());
    }

    LOG_INFO("Chunk store scan completed, %d chunks found", ChunkMap.ysize());
}

void TChunkStore::RegisterChunk(TStoredChunkPtr chunk)
{
    YCHECK(ChunkMap.insert(MakePair(chunk->GetId(), chunk)).second);
    chunk->GetLocation()->UpdateUsedSpace(chunk->GetInfo().size());

    LOG_DEBUG("Chunk registered (ChunkId: %s, Size: %" PRId64 ")",
        ~chunk->GetId().ToString(),
        chunk->GetInfo().size());

    ChunkAdded_.Fire(chunk);
}

TStoredChunkPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->second;
}

void TChunkStore::RemoveChunk(TStoredChunkPtr chunk)
{
    auto chunkId = chunk->GetId();

    YCHECK(ChunkMap.erase(chunkId) == 1);
    
    auto location = chunk->GetLocation();
    location->UpdateUsedSpace(-chunk->GetInfo().size());

    chunk->ScheduleRemoval();

    ChunkRemoved_.Fire(chunk);
}

TLocationPtr TChunkStore::GetNewChunkLocation()
{
    YASSERT(!Locations_.empty());

    std::vector<TLocationPtr> candidates;
    candidates.reserve(Locations_.size());

    int minCount = Max<int>();
    FOREACH (const auto& location, Locations_) {
        if (location->IsFull()) {
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
        ythrow TServiceException(TChunkHolderServiceProxy::EErrorCode::OutOfSpace) <<
            "All locations are full";
    }
    return candidates[RandomNumber(candidates.size())];
}

TChunkStore::TChunks TChunkStore::GetChunks() const
{
    TChunks result;
    result.reserve(ChunkMap.ysize());
    FOREACH (const auto& pair, ChunkMap) {
        result.push_back(pair.second);
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    return ChunkMap.ysize();
}

void TChunkStore::UpdateCellGuid(const TGuid& cellGuid)
{
    CellGuid = cellGuid;
    DoUpdateCellGuid();
}

void TChunkStore::DoUpdateCellGuid()
{
    FOREACH(auto& location, Locations_) {
        location->UpdateCellGuid(CellGuid);
    }
}

const TGuid& TChunkStore::GetCellGuid() const
{
    return CellGuid;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
