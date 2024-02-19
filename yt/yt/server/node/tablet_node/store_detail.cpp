#include "automaton.h"
#include "bootstrap.h"
#include "hint_manager.h"
#include "in_memory_manager.h"
#include "private.h"
#include "store_detail.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "hunk_chunk.h"
#include "versioned_chunk_meta_manager.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/data_node/chunk_registry.h>
#include <yt/yt/server/node/data_node/chunk.h>
#include <yt/yt/server/node/data_node/chunk_registry.h>
#include <yt/yt/server/node/data_node/local_chunk_reader.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/chunk_column_mapping.h>
#include <yt/yt/ytlib/table_client/chunk_lookup_hash_table.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/versioned_offloading_reader.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NClusterNode;
using namespace NDataNode;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TMiscExt;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto ChunkReaderEvictionTimeout = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

class TBackendChunkReadersHolder
    : public IBackendChunkReadersHolder
{
public:
    TBackendChunkReadersHolder(
        IBootstrap* bootstrap,
        NNative::IClientPtr client,
        TNodeDescriptor localNodeDescriptor,
        IChunkRegistryPtr chunkRegistry,
        TTabletStoreReaderConfigPtr readerConfig)
        : Bootstrap_(bootstrap)
        , Client_(std::move(client))
        , LocalNodeDescriptor_(std::move(localNodeDescriptor))
        , ChunkRegistry_(std::move(chunkRegistry))
        , ReaderConfig_(std::move(readerConfig))
    { }

    TBackendReaders GetBackendReaders(
        TChunkStoreBase* owner,
        std::optional<EWorkloadCategory> workloadCategory) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& Logger = owner->GetLogger();

        auto hasValidCachedRemoteReaderAdapter = [&] {
            if (!ChunkReader_) {
                return false;
            }
            if (CachedReadersLocal_) {
                return false;
            }
            if (!CachedRemoteReaderAdapters_.contains(workloadCategory)) {
                return false;
            }
            return true;
        };

        auto hasValidCachedLocalReader = [&] {
            if (!ReaderConfig_->PreferLocalReplicas) {
                return false;
            }
            if (!ChunkReader_) {
                return false;
            }
            if (!CachedReadersLocal_) {
                return false;
            }
            auto chunk = CachedWeakChunk_.Lock();
            if (!IsLocalChunkValid(chunk)) {
                return false;
            }
            return true;
        };

        auto setCachedReaders = [&] (bool local, IChunkReaderPtr chunkReader) {
            CachedReadersLocal_ = local;

            ChunkReader_ = std::move(chunkReader);
            OffloadingReader_ = dynamic_cast<IOffloadingReader*>(ChunkReader_.Get());
        };

        auto createLocalReaders = [&] (const IChunkPtr& chunk, IBlockCachePtr blockCache) {
            CachedRemoteReaderAdapters_.clear();

            CachedWeakChunk_ = chunk;
            setCachedReaders(
                true,
                CreateLocalChunkReader(
                    ReaderConfig_,
                    chunk,
                    std::move(blockCache),
                    /*blockMetaCache*/ nullptr));

            YT_LOG_DEBUG("Local chunk reader created and cached");
        };

        auto createRemoteReaders = [&] (IBlockCachePtr blockCache) {
            TChunkSpec chunkSpec;
            ToProto(chunkSpec.mutable_chunk_id(), owner->GetChunkId());
            chunkSpec.set_erasure_codec(ToProto<int>(owner->GetErasureCodecId()));
            chunkSpec.set_striped_erasure(owner->IsStripedErasure());
            *chunkSpec.mutable_chunk_meta() = owner->GetChunkMeta();
            CachedWeakChunk_.Reset();
            auto nodeStatusDirectory = Bootstrap_
                ? Bootstrap_->GetHintManager()
                : nullptr;

            auto chunkReaderHost = New<TChunkReaderHost>(
                Client_,
                LocalNodeDescriptor_,
                std::move(blockCache),
                /*chunkMetaCache*/ nullptr,
                std::move(nodeStatusDirectory),
                /*bandwidthThrottler*/ GetUnlimitedThrottler(),
                /*rpsThrottler*/ GetUnlimitedThrottler(),
                /*trafficMeter*/ nullptr);

            // NB: Bandwidth throttler will be set in createRemoteReaderAdapter.
            setCachedReaders(
                false,
                CreateRemoteReader(
                    chunkSpec,
                    ReaderConfig_,
                    New<TRemoteReaderOptions>(),
                    std::move(chunkReaderHost)));

            YT_LOG_DEBUG("Remote chunk reader created and cached");
        };

        auto createRemoteReaderAdapter = [&] {
            auto bandwidthThrottler = workloadCategory
                ? Bootstrap_->GetInThrottler(*workloadCategory)
                : GetUnlimitedThrottler();

            TBackendReaders backendReaders;
            backendReaders.ChunkReader = CreateRemoteReaderThrottlingAdapter(
                owner->GetChunkId(),
                ChunkReader_,
                std::move(bandwidthThrottler),
                /*rpsThrottler*/ GetUnlimitedThrottler());
            backendReaders.OffloadingReader = dynamic_cast<IOffloadingReader*>(backendReaders.ChunkReader.Get());
            backendReaders.ReaderConfig = ReaderConfig_;

            CachedRemoteReaderAdapters_.emplace(
                workloadCategory,
                std::move(backendReaders));
        };

        auto now = NProfiling::GetCpuInstant();
        auto createCachedReader = [&] (IBlockCachePtr blockCache) {
            ChunkReaderEvictionDeadline_ = now + NProfiling::DurationToCpuDuration(ChunkReaderEvictionTimeout);

            if (hasValidCachedLocalReader()) {
                return;
            }

            if (CachedReadersLocal_) {
                CachedReadersLocal_ = false;
                ChunkReader_.Reset();
                OffloadingReader_.Reset();
                CachedWeakChunk_.Reset();
                YT_LOG_DEBUG("Cached local chunk reader is no longer valid");
            }

            if (ChunkRegistry_ && ReaderConfig_->PreferLocalReplicas) {
                auto chunk = ChunkRegistry_->FindChunk(owner->GetChunkId());
                if (IsLocalChunkValid(chunk)) {
                    createLocalReaders(chunk, std::move(blockCache));
                    return;
                }
            }

            if (!ChunkReader_) {
                createRemoteReaders(std::move(blockCache));
            }
            if (!CachedRemoteReaderAdapters_.contains(workloadCategory)) {
                createRemoteReaderAdapter();
            }
        };

        auto makeResult = [&] {
            if (CachedReadersLocal_) {
                return TBackendReaders{
                    .ChunkReader = ChunkReader_,
                    .OffloadingReader = OffloadingReader_,
                    .ReaderConfig = ReaderConfig_
                };
            } else {
                return GetOrCrash(CachedRemoteReaderAdapters_, workloadCategory);
            }
        };

        // Fast lane.
        {
            auto guard = ReaderGuard(ReaderLock_);
            if (now < ChunkReaderEvictionDeadline_ && (hasValidCachedLocalReader() || hasValidCachedRemoteReaderAdapter())) {
                return makeResult();
            }
        }

        // Slow lane.
        {
            // NB: We do this off of guarded scope as this call is blocking and could interfere with ReaderLock_.
            auto blockCache = owner->GetBlockCache();

            auto guard = WriterGuard(ReaderLock_);
            createCachedReader(std::move(blockCache));
            return makeResult();
        }
    }

    TChunkReplicaWithMediumList GetReplicas(
        TChunkStoreBase* owner,
        TNodeId localNodeId) const override
    {
        const auto& chunkReplicaCache = Bootstrap_->GetConnection()->GetChunkReplicaCache();
        auto replicasList = chunkReplicaCache->FindReplicas({owner->GetChunkId()});
        YT_VERIFY(replicasList.size() == 1);
        auto replicasOrError = std::move(replicasList[0]);

        if (replicasOrError.IsOK()) {
            if (auto replicas = std::move(replicasOrError.Value())) {
                return replicas.Replicas;
            }
        }

        // Erasure chunks do not have local readers.
        if (TypeFromId(owner->GetChunkId()) != EObjectType::Chunk) {
            return {};
        }

        auto guard = ReaderGuard(ReaderLock_);

        auto makeLocalReplicas = [&] {
            return TChunkReplicaWithMediumList{
                TChunkReplicaWithMedium(localNodeId, GenericChunkReplicaIndex, GenericMediumIndex)
            };
        };

        if (CachedReadersLocal_) {
            return makeLocalReplicas();
        }

        auto localChunk = CachedWeakChunk_.Lock();
        if (ChunkRegistry_ && !localChunk) {
            localChunk = ChunkRegistry_->FindChunk(owner->GetChunkId());
        }

        if (IsLocalChunkValid(localChunk)) {
            return makeLocalReplicas();
        }

        return {};
    }

    void InvalidateCachedReadersAndTryResetConfig(
        const TTabletStoreReaderConfigPtr& config) override
    {
        auto guard = WriterGuard(ReaderLock_);

        ChunkReader_.Reset();
        OffloadingReader_.Reset();
        CachedRemoteReaderAdapters_.clear();
        CachedReadersLocal_ = false;
        CachedWeakChunk_.Reset();

        if (config) {
            ReaderConfig_ = config;
        }
    }

    TTabletStoreReaderConfigPtr GetReaderConfig() override
    {
        auto guard = ReaderGuard(ReaderLock_);

        return ReaderConfig_;
    }

private:
    IBootstrap* const Bootstrap_;
    const NApi::NNative::IClientPtr Client_;
    const TNodeDescriptor LocalNodeDescriptor_;
    const IChunkRegistryPtr ChunkRegistry_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ReaderLock_);
    NProfiling::TCpuInstant ChunkReaderEvictionDeadline_ = 0;
    IChunkReaderPtr ChunkReader_;
    IOffloadingReaderPtr OffloadingReader_;
    THashMap<std::optional<EWorkloadCategory>, TBackendReaders> CachedRemoteReaderAdapters_;
    bool CachedReadersLocal_ = false;
    TWeakPtr<IChunk> CachedWeakChunk_;
    TTabletStoreReaderConfigPtr ReaderConfig_;


    static bool IsLocalChunkValid(const IChunkPtr& chunk)
    {
        if (!chunk) {
            return false;
        }
        if (chunk->IsRemoveScheduled()) {
            return false;
        }
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IBackendChunkReadersHolderPtr CreateBackendChunkReadersHolder(
    IBootstrap* bootstrap,
    NNative::IClientPtr client,
    TNodeDescriptor localNodeDescriptor,
    IChunkRegistryPtr chunkRegistry,
    TTabletStoreReaderConfigPtr readerConfig)
{
    return New<TBackendChunkReadersHolder>(
        bootstrap,
        std::move(client),
        std::move(localNodeDescriptor),
        std::move(chunkRegistry),
        std::move(readerConfig));
}

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TTablet* tablet)
    : Config_(std::move(config))
    , StoreId_(id)
    , Tablet_(tablet)
    , PerformanceCounters_(Tablet_->PerformanceCounters())
    , RuntimeData_(Tablet_->RuntimeData())
    , TabletId_(Tablet_->GetId())
    , TablePath_(Tablet_->GetTablePath())
    , Schema_(Tablet_->GetPhysicalSchema())
    , KeyColumnCount_(Tablet_->GetPhysicalSchema()->GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->GetPhysicalSchema()->GetColumnCount())
    , ColumnLockCount_(Tablet_->GetColumnLockCount())
    , LockIndexToName_(Tablet_->LockIndexToName())
    , ColumnIndexToLockIndex_(Tablet_->ColumnIndexToLockIndex())
    , Logger(TabletNodeLogger.WithTag("StoreId: %v, TabletId: %v",
        StoreId_,
        TabletId_))
{
    UpdateTabletDynamicMemoryUsage(+1);
}

TStoreBase::~TStoreBase()
{
    YT_LOG_DEBUG("Store destroyed");
    UpdateTabletDynamicMemoryUsage(-1);
}

void TStoreBase::Initialize()
{ }

void TStoreBase::SetMemoryTracker(INodeMemoryTrackerPtr memoryTracker)
{
    YT_VERIFY(!MemoryTracker_);
    MemoryTracker_ = std::move(memoryTracker);
    DynamicMemoryTrackerGuard_ = TMemoryUsageTrackerGuard::Acquire(
        MemoryTracker_->WithCategory(
            GetMemoryCategory(),
            Tablet_->GetPoolTagByMemoryCategory(GetMemoryCategory())),
        DynamicMemoryUsage_,
        MemoryUsageGranularity);
}

TStoreId TStoreBase::GetId() const
{
    return StoreId_;
}

TTablet* TStoreBase::GetTablet() const
{
    return Tablet_;
}

bool TStoreBase::IsEmpty() const
{
    return GetRowCount() == 0;
}

EStoreState TStoreBase::GetStoreState() const
{
    return StoreState_;
}

void TStoreBase::SetStoreState(EStoreState state)
{
    UpdateTabletDynamicMemoryUsage(-1);
    StoreState_ = state;
    UpdateTabletDynamicMemoryUsage(+1);
}

i64 TStoreBase::GetDynamicMemoryUsage() const
{
    return DynamicMemoryUsage_;
}

TLegacyOwningKey TStoreBase::RowToKey(TUnversionedRow row) const
{
    return NTableClient::RowToKey(*Schema_, row);
}

TLegacyOwningKey TStoreBase::RowToKey(TSortedDynamicRow row) const
{
    return NTabletNode::RowToKey(*Schema_, row);
}

void TStoreBase::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StoreState_);
}

void TStoreBase::Load(TLoadContext& context)
{
    using NYT::Load;
    // NB: Beware of overloads!
    TStoreBase::SetStoreState(Load<EStoreState>(context));
}

void TStoreBase::BuildOrchidYson(TFluentMap fluent)
{
    fluent
        .Item("store_state").Value(StoreState_)
        .Item("min_timestamp").Value(GetMinTimestamp())
        .Item("max_timestamp").Value(GetMaxTimestamp());
}

void TStoreBase::SetDynamicMemoryUsage(i64 value)
{
    RuntimeData_->DynamicMemoryUsagePerType[DynamicMemoryTypeFromState(StoreState_)] +=
        (value - DynamicMemoryUsage_);
    DynamicMemoryUsage_ = value;
    if (DynamicMemoryTrackerGuard_) {
        DynamicMemoryTrackerGuard_.SetSize(value);
    }
}

ETabletDynamicMemoryType TStoreBase::DynamicMemoryTypeFromState(EStoreState state)
{
    switch (state) {
        case EStoreState::ActiveDynamic:
            return ETabletDynamicMemoryType::Active;

        case EStoreState::PassiveDynamic:
            return ETabletDynamicMemoryType::Passive;

        case EStoreState::Removed:
            return ETabletDynamicMemoryType::Backing;

        default:
            return ETabletDynamicMemoryType::Other;
    }
}

void TStoreBase::UpdateTabletDynamicMemoryUsage(i64 multiplier)
{
    RuntimeData_->DynamicMemoryUsagePerType[DynamicMemoryTypeFromState(StoreState_)] +=
        DynamicMemoryUsage_ * multiplier;
}

const NLogging::TLogger& TStoreBase::GetLogger() const
{
    return Logger;
}

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStoreBufferTag
{ };

TDynamicStoreBase::TDynamicStoreBase(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
    , Atomicity_(Tablet_->GetAtomicity())
    , RowBuffer_(New<TRowBuffer>(
        TDynamicStoreBufferTag(),
        Config_->PoolChunkSize))
{
    SetStoreState(EStoreState::ActiveDynamic);
}

EMemoryCategory TDynamicStoreBase::GetMemoryCategory() const
{
    return EMemoryCategory::TabletDynamic;
}

i64 TDynamicStoreBase::GetLockCount() const
{
    return StoreLockCount_;
}

i64 TDynamicStoreBase::Lock()
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);

    auto result = ++StoreLockCount_;
    YT_LOG_TRACE("Store locked (Count: %v)",
        result);
    return result;
}

i64 TDynamicStoreBase::Unlock()
{
    YT_ASSERT(Atomicity_ == EAtomicity::Full);
    YT_ASSERT(StoreLockCount_ > 0);

    auto result = --StoreLockCount_;
    YT_LOG_TRACE("Store unlocked (Count: %v)",
        result);
    return result;
}

TTimestamp TDynamicStoreBase::GetMinTimestamp() const
{
    return MinTimestamp_.load();
}

TTimestamp TDynamicStoreBase::GetMaxTimestamp() const
{
    return MaxTimestamp_.load();
}

void TDynamicStoreBase::SetStoreState(EStoreState state)
{
    if (StoreState_ == EStoreState::ActiveDynamic && state == EStoreState::PassiveDynamic) {
        OnSetPassive();
    }
    if (state == EStoreState::Removed) {
        OnSetRemoved();
    }
    TStoreBase::SetStoreState(state);
}

i64 TDynamicStoreBase::GetDataWeight() const
{
    return GetPoolCapacity();
}

i64 TDynamicStoreBase::GetCompressedDataSize() const
{
    return GetPoolCapacity();
}

i64 TDynamicStoreBase::GetUncompressedDataSize() const
{
    return GetPoolCapacity();
}

EStoreFlushState TDynamicStoreBase::GetFlushState() const
{
    return FlushState_;
}

void TDynamicStoreBase::SetFlushState(EStoreFlushState state)
{
    FlushState_ = state;
}

i64 TDynamicStoreBase::GetValueCount() const
{
    return StoreValueCount_;
}

i64 TDynamicStoreBase::GetPoolSize() const
{
    return RowBuffer_->GetSize();
}

i64 TDynamicStoreBase::GetPoolCapacity() const
{
    return RowBuffer_->GetCapacity();
}

void TDynamicStoreBase::BuildOrchidYson(TFluentMap fluent)
{
    TStoreBase::BuildOrchidYson(fluent);

    fluent
        .Item("flush_state").Value(GetFlushState())
        .Item("row_count").Value(GetRowCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("pool_size").Value(GetPoolSize())
        .Item("pool_capacity").Value(GetPoolCapacity())
        .Item("dynamic_memory_usage").Value(GetDynamicMemoryUsage())
        .Item("last_flush_attempt_time").Value(GetLastFlushAttemptTimestamp());
}

bool TDynamicStoreBase::IsDynamic() const
{
    return true;
}

IDynamicStorePtr TDynamicStoreBase::AsDynamic()
{
    return this;
}

TInstant TDynamicStoreBase::GetLastFlushAttemptTimestamp() const
{
    return LastFlushAttemptTimestamp_;
}

void TDynamicStoreBase::UpdateFlushAttemptTimestamp()
{
    LastFlushAttemptTimestamp_ = Now();
}

void TDynamicStoreBase::UpdateTimestampRange(TTimestamp commitTimestamp)
{
    // NB: Don't update min/max timestamps for passive stores since
    // others are relying on these values to remain constant.
    // See, e.g., TSortedStoreManager::MaxTimestampToStore_.
    if (StoreState_ == EStoreState::ActiveDynamic) {
        MinTimestamp_.store(std::min(MinTimestamp_.load(), commitTimestamp));
        MaxTimestamp_.store(std::max(MaxTimestamp_.load(), commitTimestamp));
    }
}

void TDynamicStoreBase::SetBackupCheckpointTimestamp(TTimestamp /*timestamp*/)
{ }

void TDynamicStoreBase::LockHunkStores(const NTableClient::THunkChunksInfo& /*hunkChunksInfo*/)
{ }

////////////////////////////////////////////////////////////////////////////////

class TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TIntrusivePtr<TChunkStoreBase> owner,
        TInMemoryChunkDataPtr chunkData,
        TChunkId chunkId,
        IBlockCachePtr underlyingCache)
        : Owner_(std::move(owner))
        , ChunkData_(std::move(chunkData))
        , ChunkId_(chunkId)
        , NativeBlockType_(GetBlockTypeFromInMemoryMode(ChunkData_->InMemoryMode))
        , UnderlyingCache_(std::move(underlyingCache))
    { }

    void PutBlock(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data) override
    {
        UnderlyingCache_->PutBlock(id, type, data);
    }

    TCachedBlock FindBlock(
        const TBlockId& id,
        EBlockType type) override
    {
        if (Any(type & NativeBlockType_)) {
            YT_ASSERT(id.ChunkId == ChunkId_);
            int blockIndex = id.BlockIndex - ChunkData_->StartBlockIndex;
            YT_VERIFY(blockIndex >= 0 && blockIndex < std::ssize(ChunkData_->Blocks));
            return TCachedBlock(ChunkData_->Blocks[blockIndex]);
        } else {
            return UnderlyingCache_->FindBlock(id, type);
        }
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& id,
        EBlockType type) override
    {
        if (Any(type & NativeBlockType_)) {
            YT_ASSERT(id.ChunkId == ChunkId_);
            int blockIndex = id.BlockIndex - ChunkData_->StartBlockIndex;
            YT_VERIFY(blockIndex >= 0 && blockIndex < std::ssize(ChunkData_->Blocks));
            return CreatePresetCachedBlockCookie(TCachedBlock(ChunkData_->Blocks[blockIndex]));
        } else {
            return UnderlyingCache_->GetBlockCookie(id, type);
        }
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return NativeBlockType_ | UnderlyingCache_->GetSupportedBlockTypes();
    }

    bool IsBlockTypeActive(EBlockType blockType) const override
    {
        return Any(blockType & NativeBlockType_) ||
            UnderlyingCache_->IsBlockTypeActive(blockType);
    }

private:
    const TWeakPtr<TChunkStoreBase> Owner_;
    const TInMemoryChunkDataPtr ChunkData_;
    const TChunkId ChunkId_;
    const EBlockType NativeBlockType_;
    const IBlockCachePtr UnderlyingCache_;
};

DEFINE_REFCOUNTED_TYPE(TPreloadedBlockCache)

////////////////////////////////////////////////////////////////////////////////

TChunkStoreBase::TChunkStoreBase(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TChunkId chunkId,
    TTimestamp overrideTimestamp,
    TTablet* tablet,
    const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
    IBlockCachePtr blockCache,
    IVersionedChunkMetaManagerPtr chunkMetaManager,
    IBackendChunkReadersHolderPtr backendReadersHolder)
    : TStoreBase(std::move(config), id, tablet)
    , BlockCache_(std::move(blockCache))
    , ChunkMetaManager_(std::move(chunkMetaManager))
    , ChunkMeta_(New<TRefCountedChunkMeta>())
    , ChunkId_(chunkId)
    , OverrideTimestamp_(overrideTimestamp)
    , BackendReadersHolder_(std::move(backendReadersHolder))
{
    /* If store is over chunk, chunkId == storeId.
     * If store is over chunk view, chunkId and storeId are different,
     * because storeId represents the id of the chunk view.
     *
     * ChunkId is null during recovery of a sorted chunk store because it is is not known
     * at the moment of store creation and will be loaded separately.
     * Consider TTabletManagerImpl::DoCreateStore, TSortedChunkStore::Load.
     */
    YT_VERIFY(
        !ChunkId_ ||
        TypeFromId(ChunkId_) == EObjectType::Chunk ||
        TypeFromId(ChunkId_) == EObjectType::ErasureChunk);

    YT_VERIFY(TypeFromId(StoreId_) == EObjectType::ChunkView || StoreId_ == ChunkId_ || !ChunkId_);

    YT_VERIFY(ChunkMetaManager_);

    SetStoreState(EStoreState::Persistent);

    if (addStoreDescriptor) {
        ChunkMeta_->CopyFrom(addStoreDescriptor->chunk_meta());
    }
}

void TChunkStoreBase::Initialize()
{
    TStoreBase::Initialize();

    SetInMemoryMode(Tablet_->GetSettings().MountConfig->InMemoryMode);
    MiscExt_ = GetProtoExtension<TMiscExt>(ChunkMeta_->extensions());

    if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(ChunkMeta_->extensions())) {
        HunkChunkRefs_.reserve(optionalHunkChunkRefsExt->refs_size());
        for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
            HunkChunkRefs_.push_back({
                .HunkChunk = Tablet_->GetHunkChunk(FromProto<TChunkId>(ref.chunk_id())),
                .HunkCount = ref.hunk_count(),
                .TotalHunkLength = ref.total_hunk_length()
            });
        }
    }
}

const TChunkMeta& TChunkStoreBase::GetChunkMeta() const
{
    return *ChunkMeta_;
}

i64 TChunkStoreBase::GetDataWeight() const
{
    return MiscExt_.data_weight();
}

i64 TChunkStoreBase::GetCompressedDataSize() const
{
    return MiscExt_.compressed_data_size();
}

i64 TChunkStoreBase::GetUncompressedDataSize() const
{
    return MiscExt_.uncompressed_data_size();
}

i64 TChunkStoreBase::GetMemoryUsage() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);
    i64 result = 0;

    switch (InMemoryMode_) {
        case EInMemoryMode::Compressed:
            result = GetCompressedDataSize();
            break;

        case EInMemoryMode::Uncompressed:
            result = GetUncompressedDataSize();
            break;

        case EInMemoryMode::None:
            break;

        default:
            YT_ABORT();
    }

    if (ChunkState_ && ChunkState_->LookupHashTable) {
        result += ChunkState_->LookupHashTable->GetByteSize();
    }

    return result;
}

i64 TChunkStoreBase::GetRowCount() const
{
    return MiscExt_.row_count();
}

NErasure::ECodec TChunkStoreBase::GetErasureCodecId() const
{
    return CheckedEnumCast<NErasure::ECodec>(MiscExt_.erasure_codec());
}

TTimestamp TChunkStoreBase::GetMinTimestamp() const
{
    return OverrideTimestamp_ != NullTimestamp ? OverrideTimestamp_ : MiscExt_.min_timestamp();
}

TTimestamp TChunkStoreBase::GetMaxTimestamp() const
{
    return OverrideTimestamp_ != NullTimestamp ? OverrideTimestamp_ : MiscExt_.max_timestamp();
}

bool TChunkStoreBase::IsStripedErasure() const
{
    return MiscExt_.striped_erasure();
}

void TChunkStoreBase::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, OverrideTimestamp_);
}

void TChunkStoreBase::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, OverrideTimestamp_);
}

TCallback<void(TSaveContext&)> TChunkStoreBase::AsyncSave()
{
    return BIND([chunkMeta = ChunkMeta_] (TSaveContext& context) {
        using NYT::Save;
        Save(context, *chunkMeta);
    });
}

void TChunkStoreBase::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;
    Load(context, *ChunkMeta_);
}

void TChunkStoreBase::BuildOrchidYson(TFluentMap fluent)
{
    TStoreBase::BuildOrchidYson(fluent);

    auto backingStore = GetBackingStore();
    fluent
        .Item("preload_state").Value(GetPreloadState())
        .Item("compaction_state").Value(GetCompactionState())
        .Item("compressed_data_size").Value(GetCompressedDataSize())
        .Item("uncompressed_data_size").Value(GetUncompressedDataSize())
        .Item("row_count").Value(GetRowCount())
        .Item("creation_time").Value(GetCreationTime())
        .Item("last_compaction_timestamp").Value(GetLastCompactionTimestamp())
        .DoIf(backingStore.operator bool(), [&] (auto fluent) {
            fluent
                .Item("backing_store").DoMap([&] (auto fluent) {
                    fluent
                        .Item(ToString(backingStore->GetId()))
                        .DoMap(BIND(&IStore::BuildOrchidYson, backingStore));
                });
        })
        .Item("hunk_chunk_refs").Value(HunkChunkRefs_);
}

IDynamicStorePtr TChunkStoreBase::GetBackingStore() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);
    return BackingStore_;
}

void TChunkStoreBase::SetBackingStore(IDynamicStorePtr store)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);
    std::swap(store, BackingStore_);
}

EStorePreloadState TChunkStoreBase::GetPreloadState() const
{
    return PreloadState_;
}

void TChunkStoreBase::SetPreloadState(EStorePreloadState state)
{
    YT_LOG_INFO("Set preload state (Current: %v, New: %v)", PreloadState_, state);
    PreloadState_ = state;
}

TFuture<void> TChunkStoreBase::GetPreloadFuture() const
{
    return PreloadFuture_;
}

void TChunkStoreBase::SetPreloadFuture(TFuture<void> future)
{
    PreloadFuture_ = std::move(future);
}

EStoreCompactionState TChunkStoreBase::GetCompactionState() const
{
    return CompactionState_;
}

void TChunkStoreBase::SetCompactionState(EStoreCompactionState state)
{
    CompactionState_ = state;
}

bool TChunkStoreBase::IsChunk() const
{
    return true;
}

IChunkStorePtr TChunkStoreBase::AsChunk()
{
    return this;
}

TBackendReaders TChunkStoreBase::GetBackendReaders(
    std::optional<EWorkloadCategory> workloadCategory)
{
    return BackendReadersHolder_->GetBackendReaders(this, workloadCategory);
}

TTabletStoreReaderConfigPtr TChunkStoreBase::GetReaderConfig()
{
    return BackendReadersHolder_->GetReaderConfig();
}

void TChunkStoreBase::InvalidateCachedReaders(const TTableSettings& settings)
{
    {
        auto guard = WriterGuard(WeakCachedVersionedChunkMetaEntryLock_);
        auto oldEntry = std::move(WeakCachedVersionedChunkMetaEntry_);
        // Prevent destroying oldCachedWeakVersionedChunkMeta under spinlock.
        guard.Release();
    }

    BackendReadersHolder_->InvalidateCachedReadersAndTryResetConfig(settings.StoreReaderConfig);
}

TCachedVersionedChunkMetaPtr TChunkStoreBase::FindCachedVersionedChunkMeta(
    bool prepareColumnarMeta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(WeakCachedVersionedChunkMetaEntryLock_);
    if (auto entry = WeakCachedVersionedChunkMetaEntry_.Lock()) {
        const auto& meta = entry->Meta();
        if (prepareColumnarMeta || !meta->IsColumnarMetaPrepared()) {
            ChunkMetaManager_->Touch(entry);
            return meta;
        }
    }

    return nullptr;
}

TFuture<TCachedVersionedChunkMetaPtr> TChunkStoreBase::GetCachedVersionedChunkMeta(
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    bool prepareColumnarMeta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TWallTimer metaWaitTimer;

    return ChunkMetaManager_->GetMeta(
        chunkReader,
        Schema_,
        chunkReadOptions,
        prepareColumnarMeta)
        .Apply(BIND([
            =,
            this,
            this_ = MakeStrong(this),
            chunkReaderStatistics = chunkReadOptions.ChunkReaderStatistics
        ] (const TVersionedChunkMetaCacheEntryPtr& entry) {
            chunkReaderStatistics->RecordMetaWaitTime(
                metaWaitTimer.GetElapsedTime());

            {
                auto guard = WriterGuard(WeakCachedVersionedChunkMetaEntryLock_);
                auto oldEntry = std::move(WeakCachedVersionedChunkMetaEntry_);
                WeakCachedVersionedChunkMetaEntry_ = entry;
                // Prevent destroying oldEntry under spinlock.
                guard.Release();
            }

            return entry->Meta();
        })
        .AsyncVia(GetCurrentInvoker()));
}

const std::vector<THunkChunkRef>& TChunkStoreBase::HunkChunkRefs() const
{
    return HunkChunkRefs_;
}

EMemoryCategory TChunkStoreBase::GetMemoryCategory() const
{
    return EMemoryCategory::TabletStatic;
}

bool TChunkStoreBase::IsPreloadAllowed() const
{
    return Now() > AllowedPreloadTimestamp_;
}

void TChunkStoreBase::UpdatePreloadAttempt(bool isBackoff)
{
    AllowedPreloadTimestamp_ = Now() + (isBackoff ? Config_->PreloadBackoffTime : TDuration::Zero());
}

void TChunkStoreBase::UpdateCompactionAttempt()
{
    LastCompactionTimestamp_ = Now();
}

TInstant TChunkStoreBase::GetLastCompactionTimestamp() const
{
    return LastCompactionTimestamp_;
}

EInMemoryMode TChunkStoreBase::GetInMemoryMode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);
    return InMemoryMode_;
}

void TChunkStoreBase::SetInMemoryMode(EInMemoryMode mode)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);

    if (InMemoryMode_ != mode) {
        YT_LOG_INFO("Changed in-memory mode (CurrentMode: %v, NewMode: %v)",
            InMemoryMode_,
            mode);

        InMemoryMode_ = mode;

        ChunkState_.Reset();
        PreloadedBlockCache_.Reset();
        BackendReadersHolder_->InvalidateCachedReadersAndTryResetConfig(/*config*/ nullptr);

        if (PreloadFuture_) {
            PreloadFuture_.Cancel(TError("Preload canceled due to in-memory mode change"));
        }
        PreloadFuture_.Reset();

        PreloadState_ = EStorePreloadState::None;
    }

    if (PreloadState_ == EStorePreloadState::None && mode != EInMemoryMode::None) {
        PreloadState_ = EStorePreloadState::Scheduled;
    }

    YT_VERIFY((mode == EInMemoryMode::None) == (PreloadState_ == EStorePreloadState::None));
}

void TChunkStoreBase::Preload(TInMemoryChunkDataPtr chunkData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(SpinLock_);

    // Otherwise action must be cancelled.
    YT_VERIFY(chunkData->InMemoryMode == InMemoryMode_);
    YT_VERIFY(chunkData->ChunkMeta);

    PreloadedBlockCache_ = New<TPreloadedBlockCache>(
        this,
        chunkData,
        ChunkId_,
        BlockCache_);

    ChunkState_ = New<TChunkState>(TChunkState{
        .BlockCache = PreloadedBlockCache_,
        .ChunkMeta = chunkData->ChunkMeta,
        .OverrideTimestamp = OverrideTimestamp_,
        .LookupHashTable = chunkData->LookupHashTable,
        .KeyComparer = GetKeyComparer(),
        .TableSchema = Schema_,
        .ChunkColumnMapping =  New<TChunkColumnMapping>(
            Schema_,
            chunkData->ChunkMeta->ChunkSchema()),
    });
}

TChunkId TChunkStoreBase::GetChunkId() const
{
    return ChunkId_;
}

TTimestamp TChunkStoreBase::GetOverrideTimestamp() const
{
    return OverrideTimestamp_;
}

TChunkReplicaWithMediumList TChunkStoreBase::GetReplicas(NNodeTrackerClient::TNodeId localNodeId)
{
    return BackendReadersHolder_->GetReplicas(this, localNodeId);
}

IBlockCachePtr TChunkStoreBase::GetBlockCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);
    return DoGetBlockCache();
}

IBlockCachePtr TChunkStoreBase::DoGetBlockCache()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock_);

    return PreloadedBlockCache_ ? PreloadedBlockCache_ : BlockCache_;
}

TChunkStatePtr TChunkStoreBase::FindPreloadedChunkState() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(SpinLock_);

    if (InMemoryMode_ == EInMemoryMode::None) {
        return nullptr;
    }

    if (!ChunkState_) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::ChunkIsNotPreloaded,
            "Chunk data is not preloaded yet")
            << TErrorAttribute("tablet_id", TabletId_)
            << TErrorAttribute("table_path", TablePath_)
            << TErrorAttribute("store_id", StoreId_)
            << TErrorAttribute("chunk_id", ChunkId_);
    }

    YT_VERIFY(ChunkState_);
    YT_VERIFY(ChunkState_->ChunkMeta);

    return ChunkState_;
}

TInstant TChunkStoreBase::GetCreationTime() const
{
    return TInstant::MicroSeconds(MiscExt_.creation_time());
}

////////////////////////////////////////////////////////////////////////////////

TPartition* TSortedStoreBase::GetPartition() const
{
    return Partition_;
}

void TSortedStoreBase::SetPartition(TPartition* partition)
{
    Partition_ = partition;
}

bool TSortedStoreBase::IsSorted() const
{
    return true;
}

ISortedStorePtr TSortedStoreBase::AsSorted()
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

bool TOrderedStoreBase::IsOrdered() const
{
    return true;
}

IOrderedStorePtr TOrderedStoreBase::AsOrdered()
{
    return this;
}

i64 TOrderedStoreBase::GetStartingRowIndex() const
{
    return StartingRowIndex_;
}

void TOrderedStoreBase::SetStartingRowIndex(i64 value)
{
    YT_VERIFY(value >= 0);
    StartingRowIndex_ = value;
}

void TOrderedStoreBase::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StartingRowIndex_);
}

void TOrderedStoreBase::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, StartingRowIndex_);
}

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey RowToKey(const TTableSchema& schema, TSortedDynamicRow row)
{
    if (!row) {
        return TLegacyOwningKey();
    }

    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        builder.AddValue(GetUnversionedKeyValue(row, index, schema.Columns()[index].GetWireType()));
    }
    return builder.FinishRow();
}

TTimestamp CalculateRetainedTimestamp(TTimestamp currentTimestamp, TDuration minDataTtl)
{
    return std::min(
        InstantToTimestamp(TimestampToInstant(currentTimestamp).second - minDataTtl).second,
        currentTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
