#include "chunk_store.h"

#include "bootstrap.h"
#include "private.h"
#include "blob_chunk.h"
#include "config.h"
#include "journal_chunk.h"
#include "location.h"
#include "session.h"
#include "session_manager.h"
#include "master_connector.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

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
using namespace NNode;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DataNodeLogger;
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
            masterConnector->ScheduleHeartbeat();
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

    INodeMemoryTrackerPtr GetNodeMemoryUsageTracker() override
    {
        return Bootstrap_->GetNodeMemoryUsageTracker();
    }

    void CancelLocationSessions(const TChunkLocationPtr& location) override
    {
        auto sessionManager = Bootstrap_->GetDataNodeBootstrap()->GetSessionManager();

        if (sessionManager) {
            sessionManager->CancelLocationSessions(location);
        }
    }

    bool CanPassSessionOutOfTurn(TChunkId chunkId) override
    {
        if (auto sessionManager = Bootstrap_->GetDataNodeBootstrap()->GetSessionManager()) {
            return sessionManager->CanPassSessionOutOfTurn(chunkId);
        } else {
            return false;
        }
    }

    void RemoveChunkFromCache(TChunkId chunkId) override
    {
        if (auto blockCache = Bootstrap_->GetBlockCache()) {
            blockCache->RemoveChunkBlocks(chunkId);
        }
    }

    const TFairShareHierarchicalSchedulerPtr<std::string>& GetFairShareHierarchicalScheduler() override
    {
        return Bootstrap_->GetFairShareHierarchicalScheduler();
    }

    const NIO::IHugePageManagerPtr& GetHugePageManager() override
    {
        return Bootstrap_->GetHugePageManager();
    }

    THashSet<NObjectClient::TCellTag> GetMasterCellTags() const override
    {
        return Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector()->GetMasterCellTags();
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
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

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
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    Locations_.clear();
}

void TChunkStore::ReconfigureLocation(const TChunkLocationPtr& location)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    auto storeLocation = DynamicPointerCast<TStoreLocation>(location);
    if (!storeLocation) {
        return;
    }

    if (!DynamicConfig_) {
        return;
    }

    const auto& staticLocationConfig = storeLocation->GetStaticConfig();
    auto it = DynamicConfig_->StoreLocationConfigPerMedium.find(storeLocation->GetMediumName());
    auto locationConfig = it == DynamicConfig_->StoreLocationConfigPerMedium.end()
        ? staticLocationConfig
        : staticLocationConfig->ApplyDynamic(it->second);
    storeLocation->Reconfigure(locationConfig);
}

void TChunkStore::UpdateConfig(const TDataNodeDynamicConfigPtr& config)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    DynamicConfig_ = config;

    for (const auto& location : Locations_) {
        ReconfigureLocation(location);
    }
}

TFuture<void> TChunkStore::InitializeLocation(const TStoreLocationPtr& location)
{
    return location->RegisterAction(
        BIND([=, this, this_ = MakeStrong(this)] {
            auto descriptors = location->Scan();

            location->InitializeIds();

            if (location->GetState() == ELocationState::Crashed) {
                return;
            }

            for (const auto& descriptor : descriptors) {
                auto chunk = CreateFromDescriptor(location, descriptor);
                DoRegisterExistingChunk(chunk);
            }

            location->Start();
        }).AsyncVia(location->GetAuxPoolInvoker()));
}

void TChunkStore::RegisterNewChunk(
    const IChunkPtr& chunk,
    const ISessionPtr& session,
    TLockedChunkGuard lockedChunkGuard)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
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

        OnChunkRegistered(chunk);
    }

    lockedChunkGuard.Release();
}

TChunkStore::TChunkEntry TChunkStore::DoFindExistingChunk(const IChunkPtr& chunk) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(ChunkMapLock_);

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
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
                    lhs.second.Chunk->GetLocation()->GetMediumDescriptor()->GetPriority() <
                    rhs.second.Chunk->GetLocation()->GetMediumDescriptor()->GetPriority();
            });

        return resultIt->second.Chunk;
    }

    for (auto it = itRange.first; it != itRange.second; ++it) {
        if (it->second.Chunk->GetLocation()->GetMediumDescriptor()->GetIndex() == mediumIndex) {
            return it->second.Chunk;
        }
    }

    return nullptr;
}

IChunkPtr TChunkStore::FindChunk(TChunkId chunkId, TChunkLocationUuid locationUuid) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ChunkMapLock_);

    auto itRange = ChunkMap_.equal_range(chunkId);
    if (itRange.first == itRange.second) {
        return nullptr;
    }

    for (auto it = itRange.first; it != itRange.second; ++it) {
        if (it->second.Chunk->GetLocation()->GetUuid() == locationUuid) {
            return it->second.Chunk;
        }
    }

    return nullptr;
}

TChunkStore::TChunkEntry TChunkStore::DoUpdateChunk(const IChunkPtr& oldChunk, const IChunkPtr& newChunk)
{
    YT_ASSERT_SPINLOCK_AFFINITY(ChunkMapLock_);
    YT_ASSERT(oldChunk->GetId() == newChunk->GetId());
    YT_ASSERT(oldChunk->GetLocation()->GetMediumDescriptor()->GetIndex() == newChunk->GetLocation()->GetMediumDescriptor()->GetIndex());

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
    YT_ASSERT_SPINLOCK_AFFINITY(ChunkMapLock_);

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
    YT_ASSERT_INVOKER_AFFINITY(chunk->GetLocation()->GetAuxPoolInvoker());

    {
        auto lockedChunkGuard = chunk->GetLocation()->TryLockChunk(chunk->GetId());

        YT_LOG_FATAL_IF(
            !lockedChunkGuard,
            "Location lock chunk failed (LocationId: %v, ChunkId: %v)",
            chunk->GetLocation()->GetId(),
            chunk->GetId());

        lockedChunkGuard.Release();
    }

    IChunkPtr oldChunk;
    {
        auto guard = ReaderGuard(ChunkMapLock_);
        oldChunk = DoFindExistingChunk(chunk).Chunk;
    }

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
                // NB: Unfortunately we cannot ensure size equality of duplicate chunks
                // since different replicas may have different chunk meta formats.

                // Remove duplicate replica.
                YT_LOG_WARNING("Removing duplicate blob chunk: %v vs %v",
                    currentPath,
                    oldPath);
                chunk->SyncRemove(true);
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
                    // But register new chunk.
                    UnregisterChunk(oldChunk);
                    FinishChunkRegistration(chunk);
                }
                break;
            }

            default:
                YT_ABORT();
        }
    } else {
        FinishChunkRegistration(chunk);
    }
}

void TChunkStore::FinishChunkRegistration(const IChunkPtr& chunk)
{
    auto chunkEntry = BuildChunkEntry(chunk);

    {
        auto guard = WriterGuard(ChunkMapLock_);
        ChunkMap_.emplace(chunk->GetId(), chunkEntry);
        OnChunkRegistered(chunk);
    }
}

void TChunkStore::ChangeLocationMedium(const TChunkLocationPtr& location, int oldMediumIndex)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

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
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_ASSERT_SPINLOCK_AFFINITY(ChunkMapLock_);

    auto diskSpace = chunk->GetInfo().disk_space();

    const auto& location = chunk->GetLocation();
    location->UpdateChunkCount(+1);
    location->UpdateUsedSpace(+diskSpace);

    switch (TypeFromId(DecodeChunkId(chunk->GetId()).Id)) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            YT_LOG_DEBUG("Blob chunk registered (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, DiskSpace: %v)",
                chunk->GetId(),
                location->GetId(),
                location->GetUuid(),
                location->GetIndex(),
                diskSpace);
            break;

        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            YT_LOG_DEBUG("Journal chunk registered (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v, Version: %v, Sealed: %v, Active: %v)",
                chunk->GetId(),
                location->GetId(),
                location->GetUuid(),
                location->GetIndex(),
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
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
            YT_LOG_DEBUG(
                "Journal chunk no longer exists and will not be updated (ChunkId: %v, Version: %v, JournalChunkSealed: %v, JournalChunkActive: %v)",
                journalChunk->GetId(),
                version,
                journalChunk->IsSealed(),
                journalChunk->IsActive());
            return;
        }

        newChunkEntry = DoUpdateChunk(oldChunkEntry.Chunk, chunk);

        location->UpdateUsedSpace(newChunkEntry.DiskSpace - oldChunkEntry.DiskSpace);

        ChunkAdded_.Fire(chunk);
    }
}

void TChunkStore::UnregisterChunk(const IChunkPtr& chunk)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    const auto& location = chunk->GetLocation();
    auto state = location->GetState();

    // 1. Enabled - default location state for unregister chunk.
    // 2. Disabling - remove registered chunks during location disabling.
    // 3. Enabling - remove old journal chunks during location initialization. See DoRegisterExistingChunk method.
    YT_VERIFY(state == ELocationState::Enabled ||
        state == ELocationState::Disabling ||
        state == ELocationState::Enabling);

    TChunkEntry chunkEntry;
    {
        auto guard = WriterGuard(ChunkMapLock_);
        chunkEntry = DoEraseChunk(chunk);
        // NB: Concurrent chunk removals are possible.
        if (!chunkEntry.Chunk) {
            return;
        }

        location->UpdateChunkCount(-1);
        location->UpdateUsedSpace(-chunkEntry.DiskSpace);

        ChunkRemoved_.Fire(chunk);
    }

    YT_LOG_DEBUG("Chunk unregistered (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        chunk->GetId(),
        location->GetId(),
        location->GetUuid(),
        location->GetIndex());

    ChunkStoreHost_->RemoveChunkFromCache(chunk->GetId());
}

TStoreLocationPtr TChunkStore::GetChunkLocationByUuid(TChunkLocationUuid locationUuid)
{
    for (const auto& location : Locations_) {
        if (location->GetUuid() == locationUuid) {
            return location;
        }
    }

    return nullptr;
}

void TChunkStore::SetChunkLocationIndexes(const NChunkClient::NProto::TLocationIndexes& locationIndexes) {
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    for (const auto& locationIndex : locationIndexes.locations()) {
        auto uuid = FromProto<TChunkLocationUuid>(locationIndex.uuid());
        auto index = FromProto<TChunkLocationIndex>(locationIndex.index());
        auto location = GetChunkLocationByUuid(uuid);

        if (location) {
            location->SetIndex(index);
            YT_LOG_INFO("Setting index for location (LocationUuid: %v, LocationIndex: %v)", uuid, index);
        } else {
            YT_LOG_ALERT("Trying to set index for unknown location (LocationUuid: %v, LocationIndex: %v)", uuid, index);
        }
    }

    for (const auto& location : Locations_) {
        if (location->GetIndex() == NNodeTrackerClient::InvalidChunkLocationIndex) {
            YT_LOG_ALERT("Location has no index set (LocationUuid: %v)", location->GetUuid());
        }
    }
}

void TChunkStore::RemoveNonexistentChunk(TChunkId chunkId, TChunkLocationUuid locationUuid)
{
    auto location = GetChunkLocationByUuid(locationUuid);
    if (!location) {
        YT_LOG_ERROR("Chunk location is missing during nonexistent chunk removal (ChunkId: %v, LocationUuid: %v)",
            chunkId,
            locationUuid);
        return;
    }

    TChunkDescriptor descriptor(chunkId);
    auto chunk = CreateFromDescriptor(location, descriptor);

    YT_LOG_DEBUG("Nonexistent chunk unregistered (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
        chunkId,
        location->GetId(),
        location->GetUuid(),
        location->GetIndex());

    {
        auto guard = ReaderGuard(ChunkMapLock_);
        ChunkRemoved_.Fire(chunk);
    }

    ChunkStoreHost_->RemoveChunkFromCache(chunk->GetId());
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
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto chunk = FindChunk(chunkId, mediumIndex);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v on medium %v",
            chunkId,
            mediumIndex);
    }

    return chunk;
}
IChunkPtr TChunkStore::GetChunkOrThrow(TChunkId chunkId, TChunkLocationUuid locationUuid) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto chunk = FindChunk(chunkId, locationUuid);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v on location %v",
            chunkId,
            locationUuid);
    }

    return chunk;
}

const TDataNodeConfigPtr& TChunkStore::GetStaticDataNodeConfig() const
{
    return Config_;
}

std::vector<IChunkPtr> TChunkStore::GetChunks() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(ChunkMapLock_);
    return std::ssize(ChunkMap_);
}

std::vector<IChunkPtr> TChunkStore::GetLocationChunks(const TChunkLocationPtr& location)
{
    auto guard = ReaderGuard(ChunkMapLock_);

    std::vector<IChunkPtr> chunks;
    for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
        const auto& chunk = chunkEntry.Chunk;
        if (chunk->GetLocation() == location) {
            chunks.push_back(chunk);
        }
    }

    return chunks;
}

TChunkStore::TPerLocationChunkMap TChunkStore::GetPerLocationChunks()
{
    auto guard = ReaderGuard(ChunkMapLock_);
    return GetPerLocationChunksUnsafe(std::move(guard));
}

TChunkStore::TPerLocationChunkMap TChunkStore::GetPerLocationChunksUnsafe(
    NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock> /*guard*/)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_ASSERT_SPINLOCK_AFFINITY(ChunkMapLock_);

    // TODO(danilalexeev): Initialize once for class instance.
    THashMap<TChunkLocationUuid, TStoreLocationPtr> locations;
    locations.reserve(Locations_.size());

    TPerLocationChunkMap result;
    for (auto location : Locations_) {
        EmplaceOrCrash(locations, location->GetUuid(), location);
        EmplaceOrCrash(result, location, std::vector<IChunkPtr>());
    }

    for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
        const auto& chunk = chunkEntry.Chunk;
        const auto& location = GetOrCrash(locations, chunk->GetLocation()->GetUuid());
        result[location].push_back(chunk);
    }
    return result;
}

void TChunkStore::CheckAllChunksHaveValidCellTags(const THashSet<NObjectClient::TCellTag>& masterCellTags) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    THashMap<NObjectClient::TCellTag, int> invalidCellTagToChunkCount;

    {
        auto guard = ReaderGuard(ChunkMapLock_);
        for (const auto& [chunkId, chunkEntry] : ChunkMap_) {
            auto chunkCellTag = CellTagFromId(chunkEntry.Chunk->GetId());
            if (!masterCellTags.contains(chunkCellTag)) {
                invalidCellTagToChunkCount[chunkCellTag]++;
            }
        }
    }

    int totalInvalidChunkCount = 0;
    for (auto [cellTag, count] : invalidCellTagToChunkCount) {
        totalInvalidChunkCount += count;
        YT_LOG_ALERT("Invalid master cell tag found for chunks (CellTag: %v, InvalidChunkCount: %v)", cellTag, count);
    }

    YT_LOG_INFO("Chunks cell tags are checked (InvalidCells: %v, InvalidChunkCount: %v)", invalidCellTagToChunkCount.size(), totalInvalidChunkCount);
}

TFuture<void> TChunkStore::RemoveChunk(const IChunkPtr& chunk, std::optional<TDuration> startRemoveDelay)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return chunk
        ->GetLocation()
        ->RegisterAction(
            BIND([=, this, this_ = MakeStrong(this)] {
                ChunkRemovalScheduled_.Fire(chunk);

                if (startRemoveDelay) {
                    TDelayedExecutor::WaitForDuration(*startRemoveDelay);
                }

                return chunk->ScheduleRemove()
                    .Apply(BIND(&TChunkStore::UnregisterChunk, MakeStrong(this), chunk));
            }).AsyncVia(ControlInvoker_));
}

std::tuple<TStoreLocationPtr, TLockedChunkGuard> TChunkStore::AcquireNewChunkLocation(
    TSessionId sessionId,
    const TSessionOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    std::vector<int> candidateIndices;
    candidateIndices.reserve(Locations_.size());

    std::vector<TStoreLocationPtr> throttledLocations;
    std::vector<TError> throttledLocationErrors;

    int minCount = std::numeric_limits<int>::max();
    for (int index = 0; index < std::ssize(Locations_); ++index) {
        const auto& location = Locations_[index];
        if (location->GetMediumDescriptor()->GetIndex() != sessionId.MediumIndex) {
            continue;
        }

        if (auto error = location->CheckWritable(); !error.IsOK()) {
            throttledLocations.push_back(location);
            throttledLocationErrors.push_back(error);
            continue;
        }

        if (options.MinLocationAvailableSpace) {
            if (!location->HasEnoughSpace(*options.MinLocationAvailableSpace)) {
                throttledLocations.push_back(location);
                throttledLocationErrors.push_back(TError("Session cannot be started due to lack of free space")
                    << TErrorAttribute("location_id", location->GetId())
                    << TErrorAttribute("needed_space", *options.MinLocationAvailableSpace)
                    << TErrorAttribute("available_space", location->GetAvailableSpace()));
                continue;
            }
        }

        if (!options.UseProbePutBlocks) {
            auto memoryLimitFractionForStartingNewSessions = location->GetMemoryLimitFractionForStartingNewSessions();
            auto usedMemory = location->GetUsedMemory(/*useLegacyUsedMemory*/ true, EIODirection::Write);
            auto memoryLimit = location->GetLegacyWriteMemoryLimit() * memoryLimitFractionForStartingNewSessions;
            if (memoryLimitFractionForStartingNewSessions &&
                usedMemory > memoryLimit)
            {
                throttledLocations.push_back(location);
                throttledLocationErrors.push_back(TError("Session cannot be started due to lack of memory")
                    << TErrorAttribute("location_id", location->GetId())
                    << TErrorAttribute("used_memory", usedMemory)
                    << TErrorAttribute("memory_limit", memoryLimit));
                continue;
            }

            auto trackedMemory = location->GetWriteMemoryTracker()->GetUsed();
            auto totalMemoryLimit  = location->GetWriteMemoryTracker()->GetLimit() * memoryLimitFractionForStartingNewSessions;

            if (memoryLimitFractionForStartingNewSessions &&
                trackedMemory > totalMemoryLimit)
            {
                throttledLocations.push_back(location);
                throttledLocationErrors.push_back(TError("Session cannot be started due to lack of memory")
                    << TErrorAttribute("location_id", location->GetId())
                    << TErrorAttribute("category_memory_used", trackedMemory)
                    << TErrorAttribute("category_memory_limit", totalMemoryLimit));
                continue;
            }
        }

        auto sessionCount = location->GetSessionCount();
        auto sessionCountLimit = location->GetSessionCountLimit();
        if (sessionCount >= sessionCountLimit) {
            throttledLocations.push_back(location);
            throttledLocationErrors.push_back(TError("Session cannot be started because of too many concurrent sessions")
                << TErrorAttribute("location_id", location->GetId())
                << TErrorAttribute("session_count", sessionCount)
                << TErrorAttribute("session_count_limit", sessionCountLimit));
            continue;
        }

        if (ShouldSkipWriteThrottlingLocations()) {
            auto diskThrottlingResult = location->CheckWriteThrottling(options.WorkloadDescriptor, true, options.UseProbePutBlocks);
            if (diskThrottlingResult.Enabled || diskThrottlingResult.MemoryOvercommit) {
                throttledLocations.push_back(location);
                throttledLocationErrors.push_back(TError("Session cannot be started because of disk throttling")
                    << diskThrottlingResult.Error);
                continue;
            }
        }

        if (options.PlacementId) {
            candidateIndices.push_back(index);
        } else {
            int count;
            auto ioWeight = location->GetIOWeight();
            if (ShouldChooseLocationBasedOnIOWeight()) {
                if (ioWeight > 0) {
                    // To schedule sessions on locations with bigger io_weight, when there are only locations with zero sessions.
                    count = static_cast<int>((location->GetSessionCount() + 1) / ioWeight);
                } else {
                    count = std::numeric_limits<int>::max();
                }
            } else {
                count = location->GetSessionCount();
            }
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
        auto error = TError(
            NChunkClient::EErrorCode::NoLocationAvailable,
            "No write location is available")
            << TErrorAttribute("session_id", ToString(sessionId));

        if (!throttledLocations.empty()) {
            auto size = throttledLocations.size();
            auto index = RandomNumber(size);
            throttledLocations[index]->ReportThrottledWrite();
            error <<= throttledLocationErrors[index];
        }

        THROW_ERROR_EXCEPTION(error);
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
        YT_LOG_DEBUG("Next round-robin location is chosen for chunk (PlacementId: %v, ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
            options.PlacementId,
            sessionId,
            location->GetId(),
            location->GetUuid(),
            location->GetIndex());
    } else {
        location = Locations_[candidateIndices[RandomNumber(candidateIndices.size())]];
        YT_LOG_DEBUG("Random location is chosen for chunk (ChunkId: %v, LocationId: %v, LocationUuid: %v, LocationIndex: %v)",
            sessionId,
            location->GetId(),
            location->GetUuid(),
            location->GetIndex());
    }

    auto lockedChunkGuard = location->TryLockChunk(sessionId.ChunkId);
    if (!lockedChunkGuard) {
        THROW_ERROR_EXCEPTION("Failed to lock chunk %v at chosen location %Qv",
            sessionId,
            location->GetId());
    }

    return {location, std::move(lockedChunkGuard)};
}

IChunkPtr TChunkStore::CreateFromDescriptor(
    const TStoreLocationPtr& location,
    const TChunkDescriptor& descriptor)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
    YT_ASSERT_SPINLOCK_AFFINITY(PlacementLock_);

    auto deadline = Config_->PlacementExpirationTime.ToDeadLine();
    auto it = PlacementIdToInfo_.find(placementId);
    if (it == PlacementIdToInfo_.end()) {
        TPlacementInfo placementInfo;
        placementInfo.CurrentLocationIndex = RandomNumber(Locations_.size());
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
    YT_ASSERT_SPINLOCK_AFFINITY(PlacementLock_);

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
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    for (const auto& location : Locations_) {
        for (auto type : TEnumTraits<ESessionType>::GetDomainValues()) {
            location->GetPerformanceCounters().SessionCount[type] = location->GetSessionCount(type);
        }

        auto& performanceCounters = location->GetPerformanceCounters();
        performanceCounters.IOWeight.Update(location->GetIOWeight());
        performanceCounters.AvailableSpace.Update(location->GetAvailableSpace());
        performanceCounters.UsedSpace.Update(location->GetUsedSpace());
        performanceCounters.ChunkCount.Update(location->GetChunkCount());
        performanceCounters.TrashChunkCount.Update(location->GetTrashChunkCount());
        performanceCounters.TrashSpace.Update(location->GetTrashSpace());
        performanceCounters.Full.Update(location->IsFull() ? 1 : 0);
    }
}

bool TChunkStore::ShouldPublishDisabledLocations()
{
    return DynamicConfig_
        ? DynamicConfig_->PublishDisabledLocations.value_or(Config_->PublishDisabledLocations)
        : Config_->PublishDisabledLocations;
}

bool TChunkStore::ShouldChooseLocationBasedOnIOWeight()
{
    return DynamicConfig_
        ? DynamicConfig_->ChooseLocationBasedOnIOWeight.value_or(Config_->ChooseLocationBasedOnIOWeight)
        : Config_->ChooseLocationBasedOnIOWeight;
}

bool TChunkStore::ShouldSkipWriteThrottlingLocations()
{
    return DynamicConfig_
        ? DynamicConfig_->SkipWriteThrottlingLocations.value_or(Config_->SkipWriteThrottlingLocations)
        : Config_->SkipWriteThrottlingLocations;
}

NThreading::TReaderGuard<NThreading::TReaderWriterSpinLock> TChunkStore::AcquireChunkMapReaderLock()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ReaderGuard(ChunkMapLock_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
