#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "blob_chunk.h"
#include "journal_chunk.h"
#include "chunk_store.h"
#include "master_connector.h"

#include <core/misc/fs.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/object_client/helpers.h>

#include <server/cell_node/bootstrap.h>

#include <util/random/random.h>

#include <utility>
#include <limits>

namespace NYT {
namespace NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NCellNode;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{ }

void TChunkStore::Initialize()
{
    LOG_INFO("Chunk store scan started");

    for (int i = 0; i < Config_->StoreLocations.size(); ++i) {
        auto locationConfig = Config_->StoreLocations[i];

        auto location = New<TLocation>(
            ELocationType::Store,
            "store" + ToString(i),
            locationConfig,
            Bootstrap_);

        location->SubscribeDisabled(
            BIND(&TChunkStore::OnLocationDisabled, Unretained(this), location));
            
        auto descriptors = location->Initialize();
        for (const auto& descriptor : descriptors) {
            auto chunk = CreateFromDescriptor(location, descriptor);
            RegisterExistingChunk(chunk);
        }

        Locations_.push_back(location);
    }

    LOG_INFO("Chunk store scan complete, %d chunks found", GetChunkCount());
}

void TChunkStore::RegisterNewChunk(IChunkPtr chunk)
{
    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    auto entry = BuildEntry(chunk);
    auto result = ChunkMap_.insert(std::make_pair(chunk->GetId(), entry));
    if (!result.second) {
        auto oldChunk = result.first->second.Chunk;
        LOG_FATAL("Duplicate chunk (Current: %s, Previous: %s)",
            ~chunk->GetLocation()->GetChunkFileName(chunk->GetId()),
            ~oldChunk->GetLocation()->GetChunkFileName(oldChunk->GetId()));
    }

    DoRegisterChunk(entry);
}

void TChunkStore::RegisterExistingChunk(IChunkPtr chunk)
{
    YCHECK(chunk->GetLocation()->IsEnabled());

    auto entry = BuildEntry(chunk);
    auto result = ChunkMap_.insert(std::make_pair(chunk->GetId(), entry));
    if (!result.second) {
        auto oldChunk = result.first->second.Chunk;
        auto oldPath = oldChunk->GetLocation()->GetChunkFileName(oldChunk->GetId());
        auto currentPath = chunk->GetLocation()->GetChunkFileName(chunk->GetId());

        // Compare if replicas are equal.
        LOG_FATAL_IF(
            oldChunk->GetInfo().disk_space() != chunk->GetInfo().disk_space(),
            "Duplicate chunks with different size (Current: %s, Previous: %s)",
            ~currentPath,
            ~oldPath);

        // Check that replicas point to the different inodes.
        LOG_FATAL_IF(
            NFS::AreInodesIdentical(oldPath, currentPath),
            "Duplicate chunks point to the same inode (Current: %s, Previous: %s)",
            ~currentPath,
            ~oldPath);

        // Remove duplicate replica.
        LOG_WARNING("Removing duplicate chunk (Current: %s, Previous: %s)",
            ~currentPath,
            ~oldPath);

        chunk->ScheduleRemoval().Get();
        return;
    }

    DoRegisterChunk(entry);
}

void TChunkStore::DoRegisterChunk(const TChunkEntry& entry)
{
    auto chunk = entry.Chunk;
    auto location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+entry.DiskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            LOG_DEBUG("Blob chunk registered (ChunkId: %s, DiskSpace: %" PRId64 ")",
                ~ToString(chunk->GetId()),
                entry.DiskSpace);
            break;

        case EObjectType::JournalChunk:
            LOG_DEBUG("Journal chunk registered (ChunkId: %s, Version: %d, Sealed: %s, Active: %s)",
                ~ToString(chunk->GetId()),
                chunk->GetVersion(),
                ~FormatBool(chunk->GetInfo().sealed()),
                ~FormatBool(chunk->IsActive()));
            break;

        default:
            YUNREACHABLE();
    }

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UpdateExistingChunk(IChunkPtr chunk)
{
    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    chunk->IncrementVersion();

    auto it = ChunkMap_.find(chunk->GetId());
    YCHECK(it != ChunkMap_.end());
    auto& entry = it->second;

    location->UpdateUsedSpace(-entry.DiskSpace);

    entry = BuildEntry(chunk);

    location->UpdateUsedSpace(+entry.DiskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::JournalChunk:
            LOG_DEBUG("Journal chunk updated (ChunkId: %s, Version: %d, Sealed: %s, Active: %s)",
                ~ToString(chunk->GetId()),
                chunk->GetVersion(),
                ~FormatBool(chunk->GetInfo().sealed()),
                ~FormatBool(chunk->IsActive()));
            break;

        default:
            YUNREACHABLE();
    }

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UnregisterChunk(IChunkPtr chunk)
{
    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    auto it = ChunkMap_.find(chunk->GetId());
    YCHECK(it != ChunkMap_.end());
    const auto& entry = it->second;

    location->UpdateChunkCount(-1);
    location->UpdateUsedSpace(-entry.DiskSpace);

    YCHECK(ChunkMap_.erase(chunk->GetId()) == 1);

    LOG_DEBUG("Chunk unregistered (ChunkId: %s)",
        ~ToString(chunk->GetId()));

    ChunkRemoved_.Fire(chunk);
}

TChunkStore::TChunkEntry TChunkStore::BuildEntry(IChunkPtr chunk)
{
    TChunkEntry result;
    result.Chunk = chunk;
    result.DiskSpace = chunk->GetInfo().disk_space();
    return result;
}

IChunkPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap_.find(chunkId);
    return it == ChunkMap_.end() ? nullptr : it->second.Chunk;
}

TFuture<void> TChunkStore::RemoveChunk(IChunkPtr chunk)
{
    return chunk->ScheduleRemoval().Apply(
        BIND(&TChunkStore::UnregisterChunk, MakeStrong(this), chunk)
            .Via(Bootstrap_->GetControlInvoker()));
}

TLocationPtr TChunkStore::GetNewChunkLocation()
{
    YASSERT(!Locations_.empty());

    std::vector<TLocationPtr> candidates;
    candidates.reserve(Locations_.size());

    int minCount = std::numeric_limits<int>::max();
    for (const auto& location : Locations_) {
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
    result.reserve(ChunkMap_.size());
    for (const auto& pair : ChunkMap_) {
        result.push_back(pair.second.Chunk);
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    return static_cast<int>(ChunkMap_.size());
}

void TChunkStore::OnLocationDisabled(TLocationPtr location)
{
    // Scan through all chunks and remove those residing on this dead location.
    LOG_INFO("Started cleaning up chunk map");
    int count = 0;
    auto it = ChunkMap_.begin();
    while (it != ChunkMap_.end()) {
        auto jt = it++;
        auto chunk = jt->second.Chunk;
        if (chunk->GetLocation() == location) {
            ChunkMap_.erase(jt);
            ++count;
        }
    }
    LOG_INFO("Chunk map cleaned, %d chunks discarded", count);

    // Register an alert and
    // schedule an out-of-order heartbeat to notify the master about the disaster.
    auto masterConnector = Bootstrap_->GetMasterConnector();
    masterConnector->RegisterAlert(Sprintf("Chunk store %s is disabled",
        ~location->GetId().Quote()));
    masterConnector->ForceRegister();
}

IChunkPtr TChunkStore::CreateFromDescriptor(
    TLocationPtr location,
    const TChunkDescriptor& descriptor)
{
    auto chunkType = TypeFromId(DecodeChunkId(descriptor.Id).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            if (location->GetType() == ELocationType::Store) {
                return New<TStoredBlobChunk>(
                    Bootstrap_,
                    location,
                    descriptor.Id,
                    descriptor.Info);
            } else {
                return New<TCachedBlobChunk>(
                    Bootstrap_,
                    location,
                    descriptor.Id,
                    descriptor.Info);
            }

        case EObjectType::JournalChunk:
            return New<TJournalChunk>(
                Bootstrap_,
                location,
                descriptor.Id,
                descriptor.Info);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
