#include "stdafx.h"
#include "common.h"
#include "config.h"
#include "location.h"
#include "chunk.h"
#include "reader_cache.h"
#include "chunk_store.h"
#include "reader_cache.h"
#include "chunk_holder_service_proxy.h"
#include "bootstrap.h"

#include <ytlib/misc/foreach.h>

#include <utility>
#include <limits>
#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(TChunkHolderConfigPtr config, TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{ }

void TChunkStore::Start()
{
    LOG_INFO("Chunk store scan started");

    try {
        for (int i = 0; i < Config->StoreLocations.ysize(); ++i) {
            auto& locationConfig = Config->StoreLocations[i];

            auto location = New<TLocation>(
                ELocationType::Store,
                ~locationConfig,
                ~Bootstrap->GetReaderCache(),
                Sprintf("ChunkStore-%d", i));
            Locations_.push_back(location);

            FOREACH (const auto& descriptor, location->Scan()) {
                auto chunk = New<TStoredChunk>(~location, descriptor);
                RegisterChunk(~chunk);
            }
        }
    } catch (const std::exception& ex) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ex.what());
    }

    LOG_INFO("Chunk store scan completed, %d chunks found", ChunkMap.ysize());
}

void TChunkStore::RegisterChunk(TStoredChunkPtr chunk)
{
    YVERIFY(ChunkMap.insert(MakePair(chunk->GetId(), chunk)).second);
    chunk->GetLocation()->UpdateUsedSpace(chunk->GetSize());

    LOG_DEBUG("Chunk registered (ChunkId: %s, Size: %" PRId64 ")",
        ~chunk->GetId().ToString(),
        chunk->GetSize());

    ChunkAdded_.Fire(chunk);
}

TStoredChunkPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->second;
}

void TChunkStore::RemoveChunk(TStoredChunkPtr chunk)
{
    // Hold the chunk during removal.
    TStoredChunkPtr chunk_ = chunk;
    auto chunkId = chunk->GetId();

    YVERIFY(ChunkMap.erase(chunkId) == 1);
    
    auto location = chunk->GetLocation();
    location->UpdateUsedSpace(-chunk->GetSize());
    location->RemoveChunk(chunk);

    LOG_INFO("Chunk removed (ChunkId: %s)", ~chunkId.ToString());

    ChunkRemoved_.Fire(chunk);
}

TLocationPtr TChunkStore::GetNewChunkLocation()
{
    YASSERT(!Locations_.empty());

    yvector<TLocationPtr> candidates;
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
            candidates.push_back(~location);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
