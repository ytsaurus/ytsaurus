#include "chunk_store.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_manager.h"
#include "location.h"
#include "master_connector.h"
#include "session.h"
#include "session_manager.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <util/random/random.h>

#include <limits>
#include <utility>

namespace NYT {
namespace NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NCellNode;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);
}

void TChunkStore::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Initializing chunk store");

    for (int i = 0; i < Config_->StoreLocations.size(); ++i) {
        auto locationConfig = Config_->StoreLocations[i];

        auto location = New<TStoreLocation>(
            "store" + ToString(i),
            locationConfig,
            Bootstrap_);

        auto descriptors = location->Scan();
        for (const auto& descriptor : descriptors) {
            auto chunk = CreateFromDescriptor(location, descriptor);
            RegisterExistingChunk(chunk);
        }

        Locations_.push_back(location);
    }

    for (auto location : Locations_) {
        location->Start();
    }

    LOG_INFO("Chunk store initialized, %v chunks total",
        GetChunkCount());
}

void TChunkStore::SetMediumIndexes(const yhash_map<Stroka, int>& mediumNameToIndex)
{
    for (const auto& location : Locations_) {
        auto mediumName = location->GetMediumName();
        auto it = mediumNameToIndex.find(mediumName);
        if (it == mediumNameToIndex.end()) {
            THROW_ERROR_EXCEPTION(
                "Location %v is configured with medium %Qv, but no such medium is known at master",
                location->GetId(),
                mediumName);
        }

        location->SetMediumIndex(it->second);
    }
}

void TChunkStore::RegisterNewChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // NB: The location was surely enabled the moment the chunk was created
    // but it may have got disabled later.
    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    auto entry = BuildEntry(chunk);

    {
        TWriterGuard guard(ChunkMapLock_);
        auto result = ChunkMap_.insert(std::make_pair(chunk->GetId(), entry));
        if (!result.second) {
            auto oldChunk = result.first->second.Chunk;
            LOG_FATAL("Duplicate chunk: %v vs %v",
                chunk->GetLocation()->GetChunkPath(chunk->GetId()),
                oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId()));
        }
    }

    DoRegisterChunk(entry);
}

void TChunkStore::RegisterExistingChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(chunk->GetLocation()->IsEnabled());

    bool doRegister = true;
    auto it = ChunkMap_.find(chunk->GetId());
    if (it != ChunkMap_.end()) {
        auto oldChunk = it->second.Chunk;
        auto oldPath = oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId());
        auto currentPath = chunk->GetLocation()->GetChunkPath(chunk->GetId());

        // Check that replicas point to the different inodes.
        LOG_FATAL_IF(
            NFS::AreInodesIdentical(oldPath, currentPath),
            "Duplicate chunks point to the same inode: %v vs %v",
            currentPath,
            oldPath);

        switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                // NB: Unfortunaly we cannot ensure size equality of duplicate chunks
                // since different replicas may have different chunk meta formats.

                // Remove duplicate replica.
                LOG_WARNING("Removing duplicate blob chunk: %v vs %v",
                    currentPath,
                    oldPath);
                chunk->SyncRemove(true);
                doRegister = false;
                break;
            }

            case EObjectType::JournalChunk: {
                auto longerRowCount = chunk->AsJournalChunk()->GetRowCount();
                auto shorterRowCount = oldChunk->AsJournalChunk()->GetRowCount();

                auto longerChunk = chunk;
                auto shorterChunk = oldChunk;

                if (longerRowCount < shorterRowCount) {
                    std::swap(longerRowCount, shorterRowCount);
                    std::swap(longerChunk, shorterChunk);
                }

                // Remove shorter replica.
                LOG_WARNING("Removing shorter journal chunk: %v (%v rows) vs %v (%v rows)",
                    shorterChunk->GetFileName(),
                    shorterRowCount,
                    longerChunk->GetFileName(),
                    longerRowCount);
                shorterChunk->SyncRemove(true);
                if (shorterChunk == oldChunk) {
                    UnregisterChunk(oldChunk);
                } else {
                    doRegister = false;
                }
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    if (doRegister) {
        auto entry = BuildEntry(chunk);
        {
            TWriterGuard guard(ChunkMapLock_);
            YCHECK(ChunkMap_.insert(std::make_pair(chunk->GetId(), entry)).second);
        }
        DoRegisterChunk(entry);
    }
}

void TChunkStore::DoRegisterChunk(const TChunkEntry& entry)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto chunk = entry.Chunk;
    auto location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+entry.DiskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            LOG_DEBUG("Blob chunk registered (ChunkId: %v, LocationId: %v, DiskSpace: %v)",
                chunk->GetId(),
                location->GetId(),
                entry.DiskSpace);
            break;

        case EObjectType::JournalChunk:
            LOG_DEBUG("Journal chunk registered (ChunkId: %v, LocationId: %v, Version: %v, Sealed: %v, Active: %v)",
                chunk->GetId(),
                location->GetId(),
                chunk->GetVersion(),
                chunk->GetInfo().sealed(),
                chunk->IsActive());
            break;

        default:
            Y_UNREACHABLE();
    }

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UpdateExistingChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

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

    switch (chunk->GetType()) {
        case EObjectType::JournalChunk: {
            auto journalChunk = chunk->AsJournalChunk();
            LOG_DEBUG("Journal chunk updated (ChunkId: %v, Version: %v, Sealed: %v, Active: %v)",
                journalChunk->GetId(),
                journalChunk->GetVersion(),
                journalChunk->IsSealed(),
                journalChunk->IsActive());
            break;
        }

        default:
            Y_UNREACHABLE();
    }

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UnregisterChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    auto it = ChunkMap_.find(chunk->GetId());
    // NB: Concurrent chunk removals are possible.
    if (it == ChunkMap_.end())
        return;

    const auto& entry = it->second;

    location->UpdateChunkCount(-1);
    location->UpdateUsedSpace(-entry.DiskSpace);

    {
        TWriterGuard guard(ChunkMapLock_);
        chunk->SetDead();
        ChunkMap_.erase(it);
    }

    LOG_DEBUG("Chunk unregistered (ChunkId: %v)",
        chunk->GetId());

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
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);
    auto it = ChunkMap_.find(chunkId);
    return it == ChunkMap_.end() ? nullptr : it->second.Chunk;
}

IChunkPtr TChunkStore::GetChunkOrThrow(const TChunkId& chunkId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunk = FindChunk(chunkId);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v",
            chunkId);
    }

    return chunk;
}

TChunkStore::TChunks TChunkStore::GetChunks() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);
    TChunks result;
    result.reserve(ChunkMap_.size());
    for (const auto& pair : ChunkMap_) {
        result.push_back(pair.second.Chunk);
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);
    return static_cast<int>(ChunkMap_.size());
}

TFuture<void> TChunkStore::RemoveChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto sessionManager = Bootstrap_->GetSessionManager();
    auto session = sessionManager->FindSession(chunk->GetId());
    if (session) {
        // NB: Cannot remove the chunk while there's a corresponding session for it.
        // Must wait for the session cancelation (which is an asynchronous process).
        session->Cancel(TError("Chunk %v is about to be removed",
            chunk->GetId()));
        return MakeFuture<void>(TError("Chunk %v is still being written",
            chunk->GetId()));
    }

    return chunk->ScheduleRemove().Apply(
        BIND(&TChunkStore::UnregisterChunk, MakeStrong(this), chunk)
            .Via(Bootstrap_->GetControlInvoker()));
}

TStoreLocationPtr TChunkStore::GetNewChunkLocation(
    const TChunkId& chunkId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ExpirePlacementInfos();

    auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);

    std::vector<int> candidates;
    int minCount = std::numeric_limits<int>::max();
    for (int index = 0; index < static_cast<int>(Locations_.size()); ++index) {
        const auto& location = Locations_[index];
        if (!CanStartNewSession(location, chunkType, options.WorkloadDescriptor)) {
            continue;
        }
        if (options.PlacementId) {
            candidates.push_back(index);
        } else {
            int count = location->GetSessionCount();
            if (count < minCount) {
                candidates.clear();
                minCount = count;
            }
            if (count == minCount) {
                candidates.push_back(index);
            }
        }
    }

    if (candidates.empty()) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoLocationAvailable,
            "No write location is available")
            << TErrorAttribute("chunk_type", chunkType);
    }

    TStoreLocationPtr result;
    if (options.PlacementId) {
        auto* placementInfo = GetOrCreatePlacementInfo(options.PlacementId);
        auto& currentIndex = placementInfo->CurrentLocationIndex;
        do {
            ++currentIndex;
            if (currentIndex >= Locations_.size()) {
                currentIndex = 0;
            }
        } while (std::find(candidates.begin(), candidates.end(), currentIndex) == candidates.end());
        result = Locations_[currentIndex];
        LOG_DEBUG("Next round-robin location is chosen for chunk (PlacementId: %v, ChunkId: %v, LocationId: %v)",
            options.PlacementId,
            chunkId,
            result->GetId());
    } else {
        result = Locations_[candidates[RandomNumber(candidates.size())]];
        LOG_DEBUG("Random location is chosen for chunk (ChunkId: %v, LocationId: %v)",
            chunkId,
            result->GetId());
    }
    return result;
}

bool TChunkStore::CanStartNewSession(
    const TStoreLocationPtr& location,
    EObjectType chunkType,
    const TWorkloadDescriptor& workloadDescriptor)
{
    if (!location->IsChunkTypeAccepted(chunkType)) {
        return false;
    }

    if (location->GetPendingIOSize(EIODirection::Write, workloadDescriptor) > Config_->DiskWriteThrottlingLimit) {
        return false;
    }

    return true;
}

IChunkPtr TChunkStore::CreateFromDescriptor(
    const TStoreLocationPtr& location,
    const TChunkDescriptor& descriptor)
{
    auto chunkType = TypeFromId(DecodeChunkId(descriptor.Id).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return New<TStoredBlobChunk>(
                Bootstrap_,
                location,
                descriptor);

        case EObjectType::JournalChunk:
            return New<TJournalChunk>(
                Bootstrap_,
                location,
                descriptor);

        default:
            Y_UNREACHABLE();
    }
}

TChunkStore::TPlacementInfo* TChunkStore::GetOrCreatePlacementInfo(const TPlacementId& placementId)
{
    auto deadline = Config_->PlacementExpirationTime.ToDeadLine();
    auto it = PlacementIdToInfo_.find(placementId);
    if (it == PlacementIdToInfo_.end()) {
        TPlacementInfo placementInfo;
        placementInfo.CurrentLocationIndex = static_cast<int>(RandomNumber(Locations_.size()));
        auto pair = PlacementIdToInfo_.emplace(placementId, placementInfo);
        YCHECK(pair.second);
        it = pair.first;
        LOG_DEBUG("Placement info registered (PlacementId: %v)",
            placementId);
    } else {
        DeadlineToPlacementId_.erase(it->second.DeadlineIterator);
    }
    auto* placementInfo = &it->second;
    placementInfo->DeadlineIterator = DeadlineToPlacementId_.emplace(deadline, placementId);
    return placementInfo;
}

void TChunkStore::ExpirePlacementInfos()
{
    auto now = TInstant::Now();
    while (!DeadlineToPlacementId_.empty()) {
        auto it = DeadlineToPlacementId_.begin();
        if (it->first > now) {
            break;
        }
        const auto& placementId = it->second;
        LOG_DEBUG("Placement info unregistered (PlacementId: %v)",
            placementId);
        YCHECK(PlacementIdToInfo_.erase(placementId) == 1);
        DeadlineToPlacementId_.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
