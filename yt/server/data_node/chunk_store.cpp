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

#include <yt/client/object_client/helpers.h>

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
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , ProfilingExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TChunkStore::OnProfiling, MakeWeak(this)),
        ProfilingPeriod))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetControlInvoker(), ControlThread);

    ProfilingExecutor_->Start();
}

void TChunkStore::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Initializing chunk store");

    std::vector<TFuture<std::vector<TChunkDescriptor>>> asyncDescriptors;

    for (int i = 0; i < Config_->StoreLocations.size(); ++i) {
        auto locationConfig = Config_->StoreLocations[i];

        auto location = New<TStoreLocation>(
            "store" + ToString(i),
            locationConfig,
            Bootstrap_);

        asyncDescriptors.push_back(
            BIND(&TStoreLocation::Scan, location)
                .AsyncVia(location->GetWritePoolInvoker()).Run());

        Locations_.push_back(location);
    }

    auto allDescriptors = WaitFor(Combine(asyncDescriptors))
        .ValueOrThrow();

    for (int index = 0; index < Config_->StoreLocations.size(); ++index) {
        const auto& location = Locations_[index];
        
        for (const auto& descriptor : allDescriptors[index]) {
            auto chunk = CreateFromDescriptor(location, descriptor);
            RegisterExistingChunk(chunk);
        }
    }

    for (auto location : Locations_) {
        location->Start();
    }

    LOG_INFO("Chunk store initialized, %v chunks total",
        GetChunkCount());
}

void TChunkStore::RegisterNewChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // NB: The location was surely enabled the moment the chunk was created
    // but it may have got disabled later.
    auto location = chunk->GetLocation();
    if (!location->IsEnabled())
        return;

    auto oldChunk = FindExistingChunk(chunk).Chunk;

    LOG_FATAL_IF(
        oldChunk,
        "Duplicate chunk: %v vs %v",
        chunk->GetLocation()->GetChunkPath(chunk->GetId()),
        oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId()));

    auto entry = BuildEntry(chunk);

    {
        TWriterGuard guard(ChunkMapLock_);
        ChunkMap_.emplace(chunk->GetId(), entry);
    }

    DoRegisterChunk(chunk);
}

TChunkStore::TChunkEntry TChunkStore::FindExistingChunk(IChunkPtr chunk) const
{
    TReaderGuard guard(ChunkMapLock_);

    auto itRange = ChunkMap_.equal_range(chunk->GetId());
    if (itRange.first == itRange.second) {
        return {};
    }

    const auto& mediumName = chunk->GetLocation()->GetMediumName();

    // Do not convert medium names to indexes here. Name-to-index mapping may
    // not be available because this method is called before the node is
    // registered at master.
    for (auto it = itRange.first; it != itRange.second; ++it) {
        if (it->second.Chunk->GetLocation()->GetMediumName() == mediumName) {
            return it->second;
        }
    }

    return {};
}

IChunkPtr TChunkStore::FindChunk(const TChunkId& chunkId, int mediumIndex) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);

    auto itRange = ChunkMap_.equal_range(chunkId);
    if (itRange.first == itRange.second) {
        return nullptr;
    }

    if (mediumIndex == AllMediaIndex) {
        auto masterConnector = Bootstrap_->GetMasterConnector();

        // Find chunk copy on a medium with the highest priority.
        auto resultIt = std::max_element(
            itRange.first,
            itRange.second,
            [&] (const TChunkIdEntryPair& lhs, const TChunkIdEntryPair& rhs) {
                return
                    lhs.second.Chunk->GetLocation()->GetMediumDescriptor().Priority <
                    rhs.second.Chunk->GetLocation()->GetMediumDescriptor().Priority;
            });

        return resultIt->second.Chunk;
    }

    for (auto it = itRange.first; it != itRange.second; ++it) {
        if (it->second.Chunk->GetLocation()->GetMediumDescriptor().Index == mediumIndex) {
            return it->second.Chunk;
        }
    }

    return nullptr;
}

TChunkStore::TChunkEntry TChunkStore::DoUpdateChunk(IChunkPtr oldChunk, IChunkPtr newChunk)
{
    Y_ASSERT(oldChunk->GetId() == newChunk->GetId());
    Y_ASSERT(oldChunk->GetLocation()->GetMediumDescriptor().Index == newChunk->GetLocation()->GetMediumDescriptor().Index);

    auto itRange = ChunkMap_.equal_range(oldChunk->GetId());
    YCHECK(itRange.first != itRange.second);

    auto it = std::find_if(
        itRange.first,
        itRange.second,
        [=] (const TChunkIdEntryPair& pair) {
            return pair.second.Chunk == oldChunk;
        });

    YCHECK(it != itRange.second);

    it->second = BuildEntry(newChunk);

    return it->second;
}

TChunkStore::TChunkEntry TChunkStore::DoEraseChunk(IChunkPtr chunk)
{
    auto itRange = ChunkMap_.equal_range(chunk->GetId());
    if (itRange.first == itRange.second) {
        return {};
    }

    auto it = std::find_if(
        itRange.first,
        itRange.second,
        [=] (const TChunkIdEntryPair& pair) {
            return pair.second.Chunk == chunk;
        });

    if (it == itRange.second) {
        return {};
    }

    auto result = it->second;
    ChunkMap_.erase(it);
    return result;
}

void TChunkStore::RegisterExistingChunk(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(chunk->GetLocation()->IsEnabled());

    bool doRegister = true;

    auto oldChunk = FindExistingChunk(chunk).Chunk;
    if (oldChunk) {
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
            ChunkMap_.emplace(chunk->GetId(), entry);
        }
        DoRegisterChunk(chunk);
    }
}

void TChunkStore::DoRegisterChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto diskSpace = chunk->GetInfo().disk_space();
    auto location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+diskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            LOG_DEBUG("Blob chunk registered (ChunkId: %v, LocationId: %v, DiskSpace: %v)",
                chunk->GetId(),
                location->GetId(),
                diskSpace);
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

    auto oldChunkEntry = FindExistingChunk(chunk);
    YCHECK(oldChunkEntry.Chunk);
    location->UpdateUsedSpace(-oldChunkEntry.DiskSpace);

    // TODO(shakurov): shouldn't we be write-locking ChunkMap_ here?
    auto newChunkEntry = DoUpdateChunk(oldChunkEntry.Chunk, chunk);

    location->UpdateUsedSpace(+newChunkEntry.DiskSpace);

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

    TChunkEntry entry;
    {
        TWriterGuard guard(ChunkMapLock_);
        entry = DoEraseChunk(chunk);
        // NB: Concurrent chunk removals are possible.
        if (!entry.Chunk) {
            return;
        }
        chunk->SetDead();
    }

    location->UpdateChunkCount(-1);
    location->UpdateUsedSpace(-entry.DiskSpace);

    LOG_DEBUG("Chunk unregistered (ChunkId: %v, LocationId: %v)",
        chunk->GetId(), location->GetId());

    ChunkRemoved_.Fire(chunk);
}

TChunkStore::TChunkEntry TChunkStore::BuildEntry(IChunkPtr chunk)
{
    TChunkEntry result;
    result.Chunk = chunk;
    result.DiskSpace = chunk->GetInfo().disk_space();
    return result;
}

IChunkPtr TChunkStore::GetChunkOrThrow(const TChunkId& chunkId, int mediumIndex) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunk = FindChunk(chunkId, mediumIndex);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v",
            chunkId);
    }

    return chunk;
}

std::vector<IChunkPtr> TChunkStore::GetChunks() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);
    std::vector<IChunkPtr> result;
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

    const auto& sessionManager = Bootstrap_->GetSessionManager();

    auto sessionId = TSessionId(chunk->GetId(), chunk->GetLocation()->GetMediumDescriptor().Index);
    auto session = sessionManager->FindSession(sessionId);
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
    const TSessionId& sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ExpirePlacementInfos();

    const auto& chunkId = sessionId.ChunkId;
    auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);

    std::vector<int> candidates;
    int minCount = std::numeric_limits<int>::max();
    for (int index = 0; index < static_cast<int>(Locations_.size()); ++index) {
        const auto& location = Locations_[index];
        if (!CanStartNewSession(location, chunkType, sessionId.MediumIndex, options.WorkloadDescriptor)) {
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
            sessionId,
            result->GetId());
    } else {
        result = Locations_[candidates[RandomNumber(candidates.size())]];
        LOG_DEBUG("Random location is chosen for chunk (ChunkId: %v, LocationId: %v)",
            sessionId,
            result->GetId());
    }
    return result;
}

bool TChunkStore::CanStartNewSession(
    const TStoreLocationPtr& location,
    EObjectType chunkType,
    int mediumIndex,
    const TWorkloadDescriptor& workloadDescriptor)
{
    if (!location->IsEnabled()) {
        return false;
    }

    if (location->IsFull()) {
        return false;
    }

    if (location->GetPendingIOSize(EIODirection::Write, workloadDescriptor) > Config_->DiskWriteThrottlingLimit) {
        return false;
    }

    if (location->GetMediumDescriptor().Index != mediumIndex) {
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

void TChunkStore::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto* profilingManager = NProfiling::TProfileManager::Get();
    for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
        NProfiling::TTagIdList tagIds = {
            profilingManager->RegisterTag("type", type)
        };
        for (const auto& location : Locations_) {
            const auto& profiler = location->GetProfiler();
            profiler.Enqueue(
                "/session_count",
                location->GetSessionCount(type),
                NProfiling::EMetricType::Gauge,
                tagIds);
        }
    }

    for (const auto& location : Locations_) {
        const auto& profiler = location->GetProfiler();
        profiler.Enqueue(
            "/available_space",
            location->GetAvailableSpace(),
            NProfiling::EMetricType::Gauge);
        profiler.Enqueue(
            "/full",
            location->IsFull() ? 1 : 0,
            NProfiling::EMetricType::Gauge);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
