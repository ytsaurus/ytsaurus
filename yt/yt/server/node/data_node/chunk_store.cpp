#include "chunk_store.h"

#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "journal_chunk.h"
#include "journal_manager.h"
#include "location.h"
#include "session.h"
#include "session_manager.h"
#include "master_connector.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <util/random/random.h>

#include <limits>
#include <utility>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TChunkStoreHost
    : public IChunkStoreHost
{
public:
    explicit TChunkStoreHost(NClusterNode::IBootstrapBase* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void ScheduleMasterHeartbeat() override
    {
        if (Bootstrap_->IsDataNode()) {
            const auto& masterConnector = Bootstrap_->GetDataNodeBootstrap()->GetMasterConnector();
            masterConnector->ScheduleHeartbeat(/*immediately*/ true);
        }
    }

    NObjectClient::TCellId GetCellId() override
    {
        return Bootstrap_->GetCellId();
    }

    void SubscribePopulateAlerts(TCallback<void(std::vector<TError>*)> alerts) override
    {
        Bootstrap_->SubscribePopulateAlerts(alerts);
    }

    NClusterNode::TMasterEpoch GetMasterEpoch() override
    {
        return Bootstrap_->GetMasterEpoch();
    }

    INodeMemoryTrackerPtr GetMemoryUsageTracker() override
    {
        return Bootstrap_->GetMemoryUsageTracker();
    }

private:
    NClusterNode::IBootstrapBase* const Bootstrap_;
};

DEFINE_REFCOUNTED_TYPE(TChunkStoreHost)

IChunkStoreHostPtr CreateChunkStoreHost(NClusterNode::IBootstrapBase* bootstrap)
{
    return New<TChunkStoreHost>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    TDataNodeConfigPtr config,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IInvokerPtr controlInvoker,
    TChunkContextPtr chunkHost,
    IChunkStoreHostPtr chunkStoreHost)
    : Config_(std::move(config))
    , DynamicConfigManager_(dynamicConfigManager)
    , ControlInvoker_(controlInvoker)
    , ChunkContext_(chunkHost)
    , ChunkStoreHost_(chunkStoreHost)
    , ProfilingExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TChunkStore::OnProfiling, MakeWeak(this)),
        ProfilingPeriod))
{ }

void TChunkStore::Initialize()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Initializing chunk store");

    std::vector<TFuture<void>> futures;
    for (int index = 0; index < std::ssize(Config_->StoreLocations); ++index) {
        const auto& locationConfig = Config_->StoreLocations[index];

        auto location = New<TStoreLocation>(
            Format("store%v", index),
            locationConfig,
            DynamicConfigManager_,
            MakeStrong(this),
            ChunkContext_,
            ChunkStoreHost_);

        location->SubscribeDisabled(
            BIND(&TChunkStore::OnLocationDisabled, MakeWeak(this), MakeWeak(location)));

        futures.push_back(InitializeLocation(location));

        Locations_.push_back(std::move(location));
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();

    YT_LOG_INFO("Chunk store initialized (ChunkCount: %v)",
        GetChunkCount());

    ProfilingExecutor_->Start();
}

void TChunkStore::Shutdown()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    Locations_.clear();
}

void TChunkStore::ReconfigureLocation(const TChunkLocationPtr& location)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    auto storeLocation = DynamicPointerCast<TStoreLocation>(location);
    if (!storeLocation) {
        return;
    }

    if (!DynamicConfig_) {
        return;
    }

    const auto& staticLocationConfig = storeLocation->GetStaticConfig();;
    auto it = DynamicConfig_->StoreLocationConfigPerMedium.find(storeLocation->GetMediumName());
    auto locationConfig = it == DynamicConfig_->StoreLocationConfigPerMedium.end()
        ? staticLocationConfig
        : staticLocationConfig->ApplyDynamic(it->second);
    storeLocation->Reconfigure(locationConfig);
}

void TChunkStore::UpdateConfig(const TDataNodeDynamicConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    DynamicConfig_ = config;

    for (const auto& location : Locations_) {
        ReconfigureLocation(location);
    }
}

TFuture<void> TChunkStore::InitializeLocation(const TStoreLocationPtr& location)
{
    return location->RegisterAction(BIND([=, this, this_ = MakeStrong(this)] () {
            auto descriptors = location->Scan();
            for (const auto& descriptor : descriptors) {
                auto chunk = CreateFromDescriptor(location, descriptor);
                DoRegisterExistingChunk(chunk);
            }

            location->Start();
        })
        .AsyncVia(location->GetAuxPoolInvoker()));
}

void TChunkStore::RegisterNewChunk(
    const IChunkPtr& chunk,
    const ISessionPtr& session,
    TLockedChunkGuard lockedChunkGuard)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YT_VERIFY(lockedChunkGuard);

    // NB: The location was surely enabled the moment the chunk was created
    // but it may have got disabled later.
    const auto& location = chunk->GetLocation();
    if (!location->IsEnabled()) {
        return;
    }

    auto entry = BuildChunkEntry(chunk);

    {
        auto guard = WriterGuard(ChunkMapLock_);

        auto masterEpoch = ChunkStoreHost_->GetMasterEpoch();
        if (session && masterEpoch != session->GetMasterEpoch()) {
            THROW_ERROR_EXCEPTION("Node has reconnected to master during chunk upload")
                << TErrorAttribute("session_master_epoch", session->GetMasterEpoch())
                << TErrorAttribute("current_master_epoch", masterEpoch);
        }

        if (auto oldChunk = DoFindExistingChunk(chunk).Chunk) {
            THROW_ERROR_EXCEPTION("Attempted to register duplicate chunk")
                << TErrorAttribute("new_chunk_path", chunk->GetLocation()->GetChunkPath(chunk->GetId()))
                << TErrorAttribute("old_chunk_path", oldChunk->GetLocation()->GetChunkPath(oldChunk->GetId()));
        }

        // NB: This is multimap.
        ChunkMap_.emplace(chunk->GetId(), entry);
    }

    lockedChunkGuard.Release();

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

    auto guard = ReaderGuard(ChunkMapLock_);

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
    VERIFY_INVOKER_AFFINITY(chunk->GetLocation()->GetAuxPoolInvoker());

    IChunkPtr oldChunk;
    {
        auto guard = ReaderGuard(ChunkMapLock_);
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

            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk: {
                auto longerRowCount = chunk->AsJournalChunk()->GetFlushedRowCount();
                auto shorterRowCount = oldChunk->AsJournalChunk()->GetFlushedRowCount();

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
            auto guard = WriterGuard(ChunkMapLock_);
            ChunkMap_.emplace(chunk->GetId(), chunkEntry);
        }

        {
            auto lockedChunkGuard = chunk->GetLocation()->TryLockChunk(chunk->GetId());
            YT_VERIFY(lockedChunkGuard);
            lockedChunkGuard.Release();
        }

        OnChunkRegistered(chunk);
    }
}

void TChunkStore::ChangeLocationMedium(const TChunkLocationPtr& location, int oldMediumIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReconfigureLocation(location);

    auto guard = ReaderGuard(ChunkMapLock_);
    for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
        const auto& chunk = chunkEntry.Chunk;
        if (chunk->GetLocation() == location) {
            ChunkMediumChanged_.Fire(chunk, oldMediumIndex);
        }
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
        case EObjectType::ErasureJournalChunk:
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

    auto journalChunk = chunk->AsJournalChunk();

    TChunkEntry oldChunkEntry;
    TChunkEntry newChunkEntry;
    {
        auto guard = WriterGuard(ChunkMapLock_);

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
        auto guard = WriterGuard(ChunkMapLock_);
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

    auto guard = ReaderGuard(ChunkMapLock_);
    std::vector<IChunkPtr> result;
    result.reserve(ChunkMap_.size());
    for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
        result.push_back(chunkEntry.Chunk);
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ChunkMapLock_);
    return static_cast<int>(ChunkMap_.size());
}

TFuture<void> TChunkStore::RemoveChunk(const IChunkPtr& chunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ChunkRemovalScheduled_.Fire(chunk);

    return chunk
        ->GetLocation()
        ->RegisterAction(BIND([=, this, this_ = MakeStrong(this)] () {
            return chunk->ScheduleRemove().Apply(
                BIND(&TChunkStore::UnregisterChunk, MakeStrong(this), chunk));
        }));
}

std::tuple<TStoreLocationPtr, TLockedChunkGuard> TChunkStore::AcquireNewChunkLocation(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<int> candidateIndices;
    candidateIndices.reserve(Locations_.size());

    int minCount = std::numeric_limits<int>::max();
    for (int index = 0; index < std::ssize(Locations_); ++index) {
        const auto& location = Locations_[index];
        if (!CanStartNewSession(location, sessionId.MediumIndex)) {
            continue;
        }
        if (options.PlacementId) {
            candidateIndices.push_back(index);
        } else {
            int count = location->GetSessionCount();
            if (count < minCount) {
                candidateIndices.clear();
                minCount = count;
            }
            if (count == minCount) {
                candidateIndices.push_back(index);
            }
        }
    }

    if (candidateIndices.empty()) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoLocationAvailable,
            "No write location is available");
    }

    TStoreLocationPtr location;
    if (options.PlacementId) {
        auto guard = Guard(PlacementLock_);
        ExpirePlacementInfos();
        auto* placementInfo = GetOrCreatePlacementInfo(options.PlacementId);
        auto& currentIndex = placementInfo->CurrentLocationIndex;
        do {
            ++currentIndex;
            if (currentIndex >= std::ssize(Locations_)) {
                currentIndex = 0;
            }
        } while (std::find(candidateIndices.begin(), candidateIndices.end(), currentIndex) == candidateIndices.end());
        location = Locations_[currentIndex];
        YT_LOG_DEBUG("Next round-robin location is chosen for chunk (PlacementId: %v, ChunkId: %v, LocationId: %v)",
            options.PlacementId,
            sessionId,
            location->GetId());
    } else {
        location = Locations_[candidateIndices[RandomNumber(candidateIndices.size())]];
        YT_LOG_DEBUG("Random location is chosen for chunk (ChunkId: %v, LocationId: %v)",
            sessionId,
            location->GetId());
    }

    auto lockedChunkGuard = location->TryLockChunk(sessionId.ChunkId);
    if (!lockedChunkGuard) {
        THROW_ERROR_EXCEPTION("Failed to lock chunk %v at chosen location %Qv",
            sessionId,
            location->GetId());
    }

    return {location, std::move(lockedChunkGuard)};
}

bool TChunkStore::CanStartNewSession(
    const TStoreLocationPtr& location,
    int mediumIndex)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!location->IsWritable()) {
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
                ChunkContext_,
                location,
                descriptor);

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return New<TJournalChunk>(
                ChunkContext_,
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
        for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
            location->GetPerformanceCounters().SessionCount[type] = location->GetSessionCount(type);
        }

        auto& performanceCounters = location->GetPerformanceCounters();
        performanceCounters.AvailableSpace.Update(location->GetAvailableSpace());
        performanceCounters.UsedSpace.Update(location->GetUsedSpace());
        performanceCounters.ChunkCount.Update(location->GetChunkCount());
        performanceCounters.Full.Update(location->IsFull() ? 1 : 0);
    }
}

bool TChunkStore::ShouldPublishDisabledLocations()
{
    return DynamicConfig_
        ? DynamicConfig_->PublishDisabledLocations.value_or(Config_->PublishDisabledLocations)
        : Config_->PublishDisabledLocations;
}

void TChunkStore::OnLocationDisabled(const TWeakPtr<TStoreLocation>& weakLocation)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto location = weakLocation.Lock();
    if (!location) {
        return;
    }

    WaitFor(BIND([=, this, this_ = MakeStrong(this)] {
        auto guard = WriterGuard(ChunkMapLock_);

        YT_VERIFY(location->GetState() == ELocationState::Disabling);

        YT_LOG_INFO("Location is disabled; unregistering all the chunks in it (LocationId: %v)",
            location->GetId());

        YT_VERIFY(!location->IsEnabled());

        THashMultiMap<TChunkId, TChunkEntry> newChunkMap;
        for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
            const auto& chunk = chunkEntry.Chunk;
            if (chunk->GetLocation() == location) {
                ChunkRemoved_.Fire(chunk);
                location->RemoveChunkFiles(chunk->GetId(), false);
            } else {
                newChunkMap.emplace(chunkId, chunkEntry);
            }
        }

        ChunkMap_ = std::move(newChunkMap);
    })
        .AsyncVia(ControlInvoker_)
        .Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
