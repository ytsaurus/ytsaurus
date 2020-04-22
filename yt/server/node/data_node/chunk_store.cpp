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

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/fs.h>

#include <util/random/random.h>

#include <limits>
#include <utility>

namespace NYT::NDataNode {

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
{ }

void TChunkStore::Initialize()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

    YT_LOG_INFO("Initializing chunk store");

    std::vector<TFuture<void>> futures;
    for (int index = 0; index < Config_->StoreLocations.size(); ++index) {
        auto locationConfig = Config_->StoreLocations[index];

        auto location = New<TStoreLocation>(
            "store" + ToString(index),
            locationConfig,
            Bootstrap_);

        futures.push_back(
            BIND(&TChunkStore::InitializeLocation, MakeStrong(this))
                .AsyncVia(location->GetWritePoolInvoker())
                .Run(location));

        Locations_.push_back(location);
    }

    WaitFor(Combine(futures))
        .ThrowOnError();

    YT_LOG_INFO("Chunk store initialized (ChunkCount: %v)",
        GetChunkCount());

    ProfilingExecutor_->Start();
}

void TChunkStore::InitializeLocation(const TStoreLocationPtr& location)
{
    VERIFY_INVOKER_AFFINITY(location->GetWritePoolInvoker());

    auto descriptors = location->Scan();
    for (const auto& descriptor : descriptors) {
        auto chunk = CreateFromDescriptor(location, descriptor);
        DoRegisterExistingChunk(chunk);
    }

    location->Start();
}

void TChunkStore::RegisterNewChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // NB: The location was surely enabled the moment the chunk was created
    // but it may have got disabled later.
    const auto& location = chunk->GetLocation();
    if (!location->IsEnabled()) {
        return;
    }

    auto entry = BuildChunkEntry(chunk);

    {
        TWriterGuard guard(ChunkMapLock_);

        auto oldChunk = DoFindExistingChunk(chunk).Chunk;
        YT_LOG_FATAL_IF(
            oldChunk,
            "Duplicate chunk: %v vs %v",
            chunk->GetLocation()->GetChunkPath(chunk->GetId()),
            oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId()));

        // NB: This is multimap.
        ChunkMap_.emplace(chunk->GetId(), entry);
    }

    OnChunkRegistered(chunk);
}

TChunkStore::TChunkEntry TChunkStore::DoFindExistingChunk(const IChunkPtr& chunk) const
{
    VERIFY_SPINLOCK_AFFINITY(ChunkMapLock_);

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

IChunkPtr TChunkStore::FindChunk(TChunkId chunkId, int mediumIndex) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ChunkMapLock_);

    auto itRange = ChunkMap_.equal_range(chunkId);
    if (itRange.first == itRange.second) {
        return nullptr;
    }

    if (mediumIndex == AllMediaIndex) {
        // Find chunk copy on a medium with the highest priority.
        auto resultIt = std::max_element(
            itRange.first,
            itRange.second,
            [&] (const auto& lhs, const auto& rhs) {
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

TChunkStore::TChunkEntry TChunkStore::DoUpdateChunk(const IChunkPtr& oldChunk, const IChunkPtr& newChunk)
{
    VERIFY_SPINLOCK_AFFINITY(ChunkMapLock_);
    YT_ASSERT(oldChunk->GetId() == newChunk->GetId());
    YT_ASSERT(oldChunk->GetLocation()->GetMediumDescriptor().Index == newChunk->GetLocation()->GetMediumDescriptor().Index);

    auto itRange = ChunkMap_.equal_range(oldChunk->GetId());
    YT_VERIFY(itRange.first != itRange.second);

    auto it = std::find_if(
        itRange.first,
        itRange.second,
        [=] (const auto& pair) {
            return pair.second.Chunk == oldChunk;
        });

    YT_VERIFY(it != itRange.second);

    it->second = BuildChunkEntry(newChunk);

    return it->second;
}

TChunkStore::TChunkEntry TChunkStore::DoEraseChunk(const IChunkPtr& chunk)
{
    VERIFY_SPINLOCK_AFFINITY(ChunkMapLock_);

    auto itRange = ChunkMap_.equal_range(chunk->GetId());
    if (itRange.first == itRange.second) {
        return {};
    }

    auto it = std::find_if(
        itRange.first,
        itRange.second,
        [=] (const auto& pair) {
            return pair.second.Chunk == chunk;
        });

    if (it == itRange.second) {
        return {};
    }

    auto result = it->second;
    ChunkMap_.erase(it);
    return result;
}

void TChunkStore::DoRegisterExistingChunk(const IChunkPtr& chunk)
{
    VERIFY_INVOKER_AFFINITY(chunk->GetLocation()->GetWritePoolInvoker());

    IChunkPtr oldChunk;
    {
        TReaderGuard guard(ChunkMapLock_);
        oldChunk = DoFindExistingChunk(chunk).Chunk;
    }

    bool doRegister = true;
    if (oldChunk) {
        auto oldPath = oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId());
        auto currentPath = chunk->GetLocation()->GetChunkPath(chunk->GetId());

        // Check that replicas point to the different inodes.
        YT_LOG_FATAL_IF(
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
                YT_LOG_WARNING("Removing duplicate blob chunk: %v vs %v",
                    currentPath,
                    oldPath);
                chunk->SyncRemove(true);
                doRegister = false;
                break;
            }

            case EObjectType::JournalChunk: {
                auto longerRowCount = chunk->AsJournalChunk()->GetCachedRowCount();
                auto shorterRowCount = oldChunk->AsJournalChunk()->GetCachedRowCount();

                auto longerChunk = chunk;
                auto shorterChunk = oldChunk;

                if (longerRowCount < shorterRowCount) {
                    std::swap(longerRowCount, shorterRowCount);
                    std::swap(longerChunk, shorterChunk);
                }

                // Remove shorter replica.
                YT_LOG_WARNING("Removing shorter journal chunk: %v (%v rows) vs %v (%v rows)",
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
                YT_ABORT();
        }
    }

    if (doRegister) {
        auto chunkEntry = BuildChunkEntry(chunk);
        {
            TWriterGuard guard(ChunkMapLock_);
            ChunkMap_.emplace(chunk->GetId(), chunkEntry);
        }
        OnChunkRegistered(chunk);
    }
}

void TChunkStore::OnChunkRegistered(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto diskSpace = chunk->GetInfo().disk_space();
    
    const auto& location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+diskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            YT_LOG_DEBUG("Blob chunk registered (ChunkId: %v, LocationId: %v, DiskSpace: %v)",
                chunk->GetId(),
                location->GetId(),
                diskSpace);
            break;

        case EObjectType::JournalChunk:
            YT_LOG_DEBUG("Journal chunk registered (ChunkId: %v, LocationId: %v, Version: %v, Sealed: %v, Active: %v)",
                chunk->GetId(),
                location->GetId(),
                chunk->GetVersion(),
                chunk->GetInfo().sealed(),
                chunk->IsActive());
            break;

        default:
            YT_ABORT();
    }

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UpdateExistingChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& location = chunk->GetLocation();
    if (!location->IsEnabled()) {
        return;
    }

    int version = chunk->IncrementVersion();

    YT_VERIFY(chunk->GetType() == EObjectType::JournalChunk);
    auto journalChunk = chunk->AsJournalChunk();

    TChunkEntry oldChunkEntry;
    TChunkEntry newChunkEntry;
    {
        TWriterGuard guard(ChunkMapLock_);

        oldChunkEntry = DoFindExistingChunk(chunk);
        if (!oldChunkEntry.Chunk) {
            YT_LOG_DEBUG("Journal chunk no longer exists and will not be updated (ChunkId: %v)",
                journalChunk->GetId(),
                version,
                journalChunk->IsSealed(),
                journalChunk->IsActive());
            return;
        }

        newChunkEntry = DoUpdateChunk(oldChunkEntry.Chunk, chunk);
    }
    
    location->UpdateUsedSpace(newChunkEntry.DiskSpace - oldChunkEntry.DiskSpace);

    ChunkAdded_.Fire(chunk);
}

void TChunkStore::UnregisterChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& location = chunk->GetLocation();
    if (!location->IsEnabled()) {
        return;
    }

    TChunkEntry chunkEntry;
    {
        TWriterGuard guard(ChunkMapLock_);
        chunkEntry = DoEraseChunk(chunk);
        // NB: Concurrent chunk removals are possible.
        if (!chunkEntry.Chunk) {
            return;
        }
    }

    location->UpdateChunkCount(-1);
    location->UpdateUsedSpace(-chunkEntry.DiskSpace);

    YT_LOG_DEBUG("Chunk unregistered (ChunkId: %v, LocationId: %v)",
        chunk->GetId(),
        location->GetId());

    ChunkRemoved_.Fire(chunk);
}

TChunkStore::TChunkEntry TChunkStore::BuildChunkEntry(const IChunkPtr& chunk)
{
    return TChunkEntry{
        .Chunk = chunk,
        .DiskSpace = chunk->GetInfo().disk_space()
    };
}

IChunkPtr TChunkStore::GetChunkOrThrow(TChunkId chunkId, int mediumIndex) const
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

TFuture<void> TChunkStore::RemoveChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto sessionId = TSessionId(
        chunk->GetId(),
        chunk->GetLocation()->GetMediumDescriptor().Index);
    const auto& sessionManager = Bootstrap_->GetSessionManager();
    if (auto session = sessionManager->FindSession(sessionId)) {
        session->Cancel(TError("Chunk %v is about to be removed",
            chunk->GetId()));
    }

    return chunk->ScheduleRemove().Apply(
        BIND(&TChunkStore::UnregisterChunk, MakeStrong(this), chunk));
}

TStoreLocationPtr TChunkStore::GetNewChunkLocation(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<int> candidates;
    candidates.reserve(Locations_.size());
    
    int minCount = std::numeric_limits<int>::max();
    for (int index = 0; index < static_cast<int>(Locations_.size()); ++index) {
        const auto& location = Locations_[index];
        if (!CanStartNewSession(location, sessionId.MediumIndex, options.WorkloadDescriptor)) {
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
            "No write location is available");
    }

    TStoreLocationPtr result;
    if (options.PlacementId) {
        auto guard = Guard(PlacementLock_);
        ExpirePlacementInfos();
        auto* placementInfo = GetOrCreatePlacementInfo(options.PlacementId);
        auto& currentIndex = placementInfo->CurrentLocationIndex;
        do {
            ++currentIndex;
            if (currentIndex >= Locations_.size()) {
                currentIndex = 0;
            }
        } while (std::find(candidates.begin(), candidates.end(), currentIndex) == candidates.end());
        result = Locations_[currentIndex];
        YT_LOG_DEBUG("Next round-robin location is chosen for chunk (PlacementId: %v, ChunkId: %v, LocationId: %v)",
            options.PlacementId,
            sessionId,
            result->GetId());
    } else {
        result = Locations_[candidates[RandomNumber(candidates.size())]];
        YT_LOG_DEBUG("Random location is chosen for chunk (ChunkId: %v, LocationId: %v)",
            sessionId,
            result->GetId());
    }
    return result;
}

bool TChunkStore::CanStartNewSession(
    const TStoreLocationPtr& location,
    int mediumIndex,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!location->IsEnabled()) {
        return false;
    }

    if (location->IsSick()) {
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
    VERIFY_THREAD_AFFINITY_ANY();

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
            YT_ABORT();
    }
}

TChunkStore::TPlacementInfo* TChunkStore::GetOrCreatePlacementInfo(TPlacementId placementId)
{
    VERIFY_SPINLOCK_AFFINITY(PlacementLock_);

    auto deadline = Config_->PlacementExpirationTime.ToDeadLine();
    auto it = PlacementIdToInfo_.find(placementId);
    if (it == PlacementIdToInfo_.end()) {
        TPlacementInfo placementInfo;
        placementInfo.CurrentLocationIndex = static_cast<int>(RandomNumber(Locations_.size()));
        auto pair = PlacementIdToInfo_.emplace(placementId, placementInfo);
        YT_VERIFY(pair.second);
        it = pair.first;
        YT_LOG_DEBUG("Placement info registered (PlacementId: %v)",
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
    VERIFY_SPINLOCK_AFFINITY(PlacementLock_);

    auto now = TInstant::Now();
    while (!DeadlineToPlacementId_.empty()) {
        auto it = DeadlineToPlacementId_.begin();
        if (it->first > now) {
            break;
        }
        const auto& placementId = it->second;
        YT_LOG_DEBUG("Placement info unregistered (PlacementId: %v)",
            placementId);
        YT_VERIFY(PlacementIdToInfo_.erase(placementId) == 1);
        DeadlineToPlacementId_.erase(it);
    }
}

void TChunkStore::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (const auto& location : Locations_) {
        const auto& profiler = location->GetProfiler();
        for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
            profiler.Update(location->GetPerformanceCounters().SessionCount[type], location->GetSessionCount(type));
        }
        auto& performanceCounters = location->GetPerformanceCounters();
        profiler.Update(performanceCounters.AvailableSpace, location->GetAvailableSpace());
        profiler.Update(performanceCounters.UsedSpace, location->GetUsedSpace());
        profiler.Update(performanceCounters.Full, location->IsFull() ? 1 : 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
