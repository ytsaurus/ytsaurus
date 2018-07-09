#include "store_detail.h"
#include "private.h"
#include "automaton.h"
#include "tablet.h"
#include "config.h"
#include "in_memory_manager.h"
#include "store_manager.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/chunk.h>
#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/local_chunk_reader.h>

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/block_cache.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_state.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
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

using NTabletNode::NProto::TAddStoreDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const auto LocalChunkRecheckPeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

TStoreBase::TStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : Config_(std::move(config))
    , ReaderConfig_(tablet->GetReaderConfig())
    , StoreId_(id)
    , Tablet_(tablet)
    , PerformanceCounters_(Tablet_->PerformanceCounters())
    , RuntimeData_(Tablet_->RuntimeData())
    , TabletId_(Tablet_->GetId())
    , TablePath_(Tablet_->GetTablePath())
    , Schema_(Tablet_->PhysicalSchema())
    , KeyColumnCount_(Tablet_->PhysicalSchema().GetKeyColumnCount())
    , SchemaColumnCount_(Tablet_->PhysicalSchema().GetColumnCount())
    , ColumnLockCount_(Tablet_->GetColumnLockCount())
    , LockIndexToName_(Tablet_->LockIndexToName())
    , ColumnIndexToLockIndex_(Tablet_->ColumnIndexToLockIndex())
    , Logger(NLogging::TLogger(TabletNodeLogger)
        .AddTag("StoreId: %v, TabletId: %v",
            StoreId_,
            TabletId_))
{ }

TStoreBase::~TStoreBase()
{
    i64 delta = -MemoryUsage_;
    MemoryUsage_ = 0;
    MemoryUsageUpdated_.Fire(delta);
    RuntimeData_->DynamicMemoryPoolSize += delta;
}

TStoreId TStoreBase::GetId() const
{
    return StoreId_;
}

TTablet* TStoreBase::GetTablet() const
{
    return Tablet_;
}

EStoreState TStoreBase::GetStoreState() const
{
    return StoreState_;
}

void TStoreBase::SetStoreState(EStoreState state)
{
    StoreState_ = state;
}

i64 TStoreBase::GetMemoryUsage() const
{
    return MemoryUsage_;
}

void TStoreBase::SubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback)
{
    MemoryUsageUpdated_.Subscribe(callback);
    callback.Run(+GetMemoryUsage());
}

void TStoreBase::UnsubscribeMemoryUsageUpdated(const TCallback<void(i64 delta)>& callback)
{
    MemoryUsageUpdated_.Unsubscribe(callback);
    callback.Run(-GetMemoryUsage());
}

void TStoreBase::SetMemoryUsage(i64 value)
{
    if (std::abs(value - MemoryUsage_) > MemoryUsageGranularity) {
        i64 delta = value - MemoryUsage_;
        MemoryUsage_ = value;
        MemoryUsageUpdated_.Fire(delta);
        RuntimeData_->DynamicMemoryPoolSize += delta;
    }
}

TOwningKey TStoreBase::RowToKey(TUnversionedRow row) const
{
    return NTableClient::RowToKey(Schema_, row);
}

TOwningKey TStoreBase::RowToKey(TSortedDynamicRow row) const
{
    return NTabletNode::RowToKey(Schema_, row);
}

void TStoreBase::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StoreState_);
}

void TStoreBase::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, StoreState_);
}

void TStoreBase::BuildOrchidYson(TFluentMap fluent)
{
    fluent
        .Item("store_state").Value(StoreState_)
        .Item("min_timestamp").Value(GetMaxTimestamp())
        .Item("max_timestamp").Value(GetMaxTimestamp());
}

bool TStoreBase::IsDynamic() const
{
    return false;
}

IDynamicStorePtr TStoreBase::AsDynamic()
{
    Y_UNREACHABLE();
}

bool TStoreBase::IsChunk() const
{
    return false;
}

IChunkStorePtr TStoreBase::AsChunk()
{
    Y_UNREACHABLE();
}

bool TStoreBase::IsSorted() const
{
    return false;
}

ISortedStorePtr TStoreBase::AsSorted()
{
    Y_UNREACHABLE();
}

TSortedDynamicStorePtr TStoreBase::AsSortedDynamic()
{
    Y_UNREACHABLE();
}

TSortedChunkStorePtr TStoreBase::AsSortedChunk()
{
    Y_UNREACHABLE();
}

bool TStoreBase::IsOrdered() const
{
    return false;
}

IOrderedStorePtr TStoreBase::AsOrdered()
{
    Y_UNREACHABLE();
}

TOrderedDynamicStorePtr TStoreBase::AsOrderedDynamic()
{
    Y_UNREACHABLE();
}

TOrderedChunkStorePtr TStoreBase::AsOrderedChunk()
{
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStoreBufferTag
{ };

TDynamicStoreBase::TDynamicStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
    , Atomicity_(Tablet_->GetAtomicity())
    , RowBuffer_(New<TRowBuffer>(
        TDynamicStoreBufferTag(),
        Config_->PoolChunkSize,
        Config_->MaxPoolSmallBlockRatio))
{
    StoreState_ = EStoreState::ActiveDynamic;
}

i64 TDynamicStoreBase::GetLockCount() const
{
    return StoreLockCount_;
}

i64 TDynamicStoreBase::Lock()
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);

    auto result = ++StoreLockCount_;
    LOG_TRACE("Store locked (Count: %v)",
        result);
    return result;
}

i64 TDynamicStoreBase::Unlock()
{
    Y_ASSERT(Atomicity_ == EAtomicity::Full);
    Y_ASSERT(StoreLockCount_ > 0);

    auto result = --StoreLockCount_;
    LOG_TRACE("Store unlocked (Count: %v)",
        result);
    return result;
}

TTimestamp TDynamicStoreBase::GetMinTimestamp() const
{
    return MinTimestamp_;
}

TTimestamp TDynamicStoreBase::GetMaxTimestamp() const
{
    return MaxTimestamp_;
}

void TDynamicStoreBase::SetStoreState(EStoreState state)
{
    if (StoreState_ == EStoreState::ActiveDynamic && state == EStoreState::PassiveDynamic) {
        OnSetPassive();
    }
    TStoreBase::SetStoreState(state);
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
        .Item("flush_state").Value(FlushState_)
        .Item("row_count").Value(GetRowCount())
        .Item("lock_count").Value(GetLockCount())
        .Item("value_count").Value(GetValueCount())
        .Item("pool_size").Value(GetPoolSize())
        .Item("pool_capacity").Value(GetPoolCapacity())
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
        MinTimestamp_ = std::min(MinTimestamp_, commitTimestamp);
        MaxTimestamp_ = std::max(MaxTimestamp_, commitTimestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TIntrusivePtr<TChunkStoreBase> owner,
        const TChunkId& chunkId,
        EInMemoryMode mode,
        IBlockCachePtr underlyingCache)
        : Owner_(owner)
        , ChunkId_(chunkId)
        , BlockType_(MapInMemoryModeToBlockType(mode))
        , UnderlyingCache_(std::move(underlyingCache))
    { }

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& source) override
    {
        UnderlyingCache_->Put(id, type, data, source);
    }

    virtual TBlock Find(
        const TBlockId& id,
        EBlockType type) override
    {
        if (type == BlockType_ && IsPreloaded()) {
            Y_ASSERT(id.ChunkId == ChunkId_);
            Y_ASSERT(id.BlockIndex >= 0 && id.BlockIndex < ChunkData_->Blocks.size());
            return ChunkData_->Blocks[id.BlockIndex];
        } else {
            return UnderlyingCache_->Find(id, type);
        }
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return BlockType_;
    }

    void Preload(TInMemoryChunkDataPtr chunkData)
    {
        // This method must be idempotent: preloads may be retried due to synchronization
        // with the config version. However, once the store is preloaded -- we keep it that way.
        auto preloaded = Preloaded_.load(std::memory_order_relaxed);
        if (preloaded != EState::NotPreloaded) {
            return;
        }
        if (Preloaded_.compare_exchange_strong(preloaded, EState::InProgress)) {
            ChunkData_ = std::move(chunkData);
            YCHECK(Preloaded_.exchange(EState::Preloaded) == EState::InProgress);
        }
    }

    bool IsPreloaded() const
    {
        return Preloaded_.load(std::memory_order_acquire) == EState::Preloaded;
    }

private:
    const TWeakPtr<TChunkStoreBase> Owner_;
    const TChunkId ChunkId_;
    const EBlockType BlockType_;
    const IBlockCachePtr UnderlyingCache_;

    TInMemoryChunkDataPtr ChunkData_;

    enum EState {
        NotPreloaded,
        InProgress,
        Preloaded,
    };

    std::atomic<EState> Preloaded_ = {EState::NotPreloaded};
};

DEFINE_REFCOUNTED_TYPE(TPreloadedBlockCache)

////////////////////////////////////////////////////////////////////////////////

TChunkStoreBase::TChunkStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor)
    : TStoreBase(std::move(config), id, tablet)
    , BlockCache_(std::move(blockCache))
    , ChunkRegistry_(std::move(chunkRegistry))
    , ChunkBlockManager_(std::move(chunkBlockManager))
    , Client_(std::move(client))
    , LocalDescriptor_(localDescriptor)
    , ChunkMeta_(New<TRefCountedChunkMeta>())
{
    YCHECK(
        TypeFromId(StoreId_) == EObjectType::Chunk ||
        TypeFromId(StoreId_) == EObjectType::ErasureChunk);

    StoreState_ = EStoreState::Persistent;
}

void TChunkStoreBase::Initialize(const TAddStoreDescriptor* descriptor)
{
    auto inMemoryMode = Tablet_->GetConfig()->InMemoryMode;
    ui64 inMemoryConfigRevision = 0;

    // When recovering from a snapshot, the store manager is not installed yet.
    const auto& storeManager = Tablet_->GetStoreManager();
    if (storeManager) {
        inMemoryConfigRevision = storeManager->GetInMemoryConfigRevision();
    }

    SetInMemoryMode(inMemoryMode, inMemoryConfigRevision);

    if (descriptor) {
        ChunkMeta_->CopyFrom(descriptor->chunk_meta());
        PrecacheProperties();
    }
}

const TChunkMeta& TChunkStoreBase::GetChunkMeta() const
{
    return *ChunkMeta_;
}

i64 TChunkStoreBase::GetCompressedDataSize() const
{
    return MiscExt_.compressed_data_size();
}

i64 TChunkStoreBase::GetUncompressedDataSize() const
{
    return MiscExt_.uncompressed_data_size();
}

i64 TChunkStoreBase::GetRowCount() const
{
    return MiscExt_.row_count();
}

TTimestamp TChunkStoreBase::GetMinTimestamp() const
{
    return MiscExt_.min_timestamp();
}

TTimestamp TChunkStoreBase::GetMaxTimestamp() const
{
    return MiscExt_.max_timestamp();
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

    PrecacheProperties();
}

void TChunkStoreBase::BuildOrchidYson(TFluentMap fluent)
{
    TStoreBase::BuildOrchidYson(fluent);

    auto backingStore = GetBackingStore();
    fluent
        .Item("preload_state").Value(PreloadState_)
        .Item("compaction_state").Value(CompactionState_)
        .Item("compressed_data_size").Value(MiscExt_.compressed_data_size())
        .Item("uncompressed_data_size").Value(MiscExt_.uncompressed_data_size())
        .Item("row_count").Value(MiscExt_.row_count())
        .Item("creation_time").Value(TInstant::MicroSeconds(MiscExt_.creation_time()))
        .DoIf(backingStore.operator bool(), [&] (TFluentMap fluent) {
            fluent.Item("backing_store_id").Value(backingStore->GetId());
        });
}

IDynamicStorePtr TChunkStoreBase::GetBackingStore()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return BackingStore_;
}

void TChunkStoreBase::SetBackingStore(IDynamicStorePtr store)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);
    BackingStore_ = std::move(store);
}

bool TChunkStoreBase::HasBackingStore() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return BackingStore_.operator bool();
}

EStorePreloadState TChunkStoreBase::GetPreloadState() const
{
    return PreloadState_;
}

void TChunkStoreBase::SetPreloadState(EStorePreloadState state)
{
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

IChunkReaderPtr TChunkStoreBase::GetChunkReader()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto now = NProfiling::GetCpuInstant();

    auto locateLocalChunk = [&] () -> IChunkPtr {
        if (!ReaderConfig_->PreferLocalReplicas) {
            return nullptr;
        }
        auto chunk = ChunkRegistry_->FindChunk(StoreId_);
        if (!chunk) {
            return nullptr;
        }
        if (chunk->IsRemoveScheduled()) {
            return nullptr;
        }
        return chunk;
    };

    auto updateLocalChunkRecheckDedline = [&] {
        LocalChunkCheckDeadline_.store(now + NProfiling::DurationToCpuDuration(LocalChunkRecheckPeriod));
    };

    IChunkPtr chunk;
    {
        TReaderGuard guard(SpinLock_);

        // Check if a cached reader exists.
        if (ChunkReader_) {
            // If the reader is local then just return it.
            if (ChunkReaderIsLocal_) {
                return ChunkReader_;
            }

            // Otherwise the reader is remote.
            // Don't check for local chunks too often.
            if (now < LocalChunkCheckDeadline_.load()) {
                return ChunkReader_;
            }

            // A cached reader is known but is remote; it's time to check for a local chunk.
            chunk = locateLocalChunk();

            // If no local chunk is returned then just deal with the remote one.
            if (!chunk) {
                updateLocalChunkRecheckDedline();
                return ChunkReader_;
            }
        }
    }

    // Try to find a local chunk (if not found already).
    if (!chunk) {
        chunk = locateLocalChunk();
    }

    IChunkReaderPtr chunkReader;
    bool chunkReaderIsLocal;
    if (chunk) {
        chunkReader = CreateLocalChunkReader(
            ReaderConfig_,
            chunk,
            ChunkBlockManager_,
            GetBlockCache(),
            nullptr /* blockMetaCache */,
            BIND(&TChunkStoreBase::OnLocalReaderFailed, MakeWeak(this)));
        chunkReaderIsLocal = true;
    } else {
        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), StoreId_);
        chunkSpec.set_erasure_codec(MiscExt_.erasure_codec());
        *chunkSpec.mutable_chunk_meta() = *ChunkMeta_;
        chunkReader = CreateRemoteReader(
            chunkSpec,
            ReaderConfig_,
            New<TRemoteReaderOptions>(),
            Client_,
            New<TNodeDirectory>(),
            LocalDescriptor_,
            GetBlockCache(),
            /* trafficMeter */ nullptr,
            GetUnlimitedThrottler());
        chunkReaderIsLocal = false;
    }

    {
        TWriterGuard guard(SpinLock_);

        ChunkReader_ = chunkReader;
        ChunkReaderIsLocal_ = chunkReaderIsLocal;
        updateLocalChunkRecheckDedline();
    }

    return chunkReader;
}

void TChunkStoreBase::OnLocalReaderFailed()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    ChunkReader_.Reset();
    ChunkReaderIsLocal_ = false;
}

void TChunkStoreBase::PrecacheProperties()
{
    MiscExt_ = GetProtoExtension<TMiscExt>(ChunkMeta_->extensions());
}

bool TChunkStoreBase::IsPreloadAllowed() const
{
    return Now() > AllowedPreloadTimestamp_;
}

void TChunkStoreBase::UpdatePreloadAttempt()
{
    AllowedPreloadTimestamp_ = Now() + Config_->PreloadBackoffTime;
}

bool TChunkStoreBase::IsCompactionAllowed() const
{
    return Now() > AllowedCompactionTimestamp_;
}

void TChunkStoreBase::UpdateCompactionAttempt()
{
    AllowedCompactionTimestamp_ = Now() + Config_->CompactionBackoffTime;
}

EInMemoryMode TChunkStoreBase::GetInMemoryMode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return InMemoryMode_;
}

void TChunkStoreBase::SetInMemoryMode(EInMemoryMode mode, ui64 configRevision)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    InMemoryConfigRevision_ = configRevision; // Unconditionally. Consumes the new revision.

    if (InMemoryMode_ == mode) {
        return;
    }

    InMemoryMode_ = mode;

    ChunkState_.Reset();
    PreloadedBlockCache_.Reset();

    if (PreloadFuture_) {
        PreloadFuture_.Cancel();
        PreloadFuture_.Reset();
    }

    if (mode == EInMemoryMode::None) {
        PreloadState_ = EStorePreloadState::Disabled;
    } else {
        PreloadedBlockCache_ = New<TPreloadedBlockCache>(
            this,
            StoreId_,
            mode,
            BlockCache_);

        switch (PreloadState_) {
            case EStorePreloadState::Disabled:
            case EStorePreloadState::Running:
            case EStorePreloadState::Complete:
                PreloadState_ = EStorePreloadState::None;
                break;
            case EStorePreloadState::None:
            case EStorePreloadState::Scheduled:
                break;
        }
    }

    ChunkReader_.Reset();
}

void TChunkStoreBase::Preload(TInMemoryChunkDataPtr chunkData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    YCHECK(chunkData->InMemoryMode == InMemoryMode_);
    YCHECK(chunkData->InMemoryConfigRevision == InMemoryConfigRevision_);

    PreloadedBlockCache_->Preload(chunkData);
    ChunkState_ = New<TChunkState>(
        PreloadedBlockCache_,
        chunkData->ChunkSpec,
        chunkData->ChunkMeta,
        chunkData->LookupHashTable,
        PerformanceCounters_,
        GetKeyComparer());
}

IBlockCachePtr TChunkStoreBase::GetBlockCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return PreloadedBlockCache_ ? PreloadedBlockCache_ : BlockCache_;
}


bool TChunkStoreBase::ValidateBlockCachePreloaded()
{
    if (InMemoryMode_ == EInMemoryMode::None) {
        return false;
    }

    if (!PreloadedBlockCache_ || !PreloadedBlockCache_->IsPreloaded()) {
        THROW_ERROR_EXCEPTION("Chunk data is not preloaded yet")
            << TErrorAttribute("tablet_id", TabletId_)
            << TErrorAttribute("table_path", TablePath_)
            << TErrorAttribute("store_id", StoreId_);
    }

    return true;
}

TInstant TChunkStoreBase::GetCreationTime() const
{
    return TInstant::MicroSeconds(MiscExt_.creation_time());
}

////////////////////////////////////////////////////////////////////////////////

TSortedStoreBase::TSortedStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
{ }

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

TOrderedStoreBase::TOrderedStoreBase(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(std::move(config), id, tablet)
{ }

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
    YCHECK(value >= 0);
    StartingRowIndex_ = value;
}

void TOrderedStoreBase::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);

    using NYT::Save;
    Save(context, StartingRowIndex_);
}

void TOrderedStoreBase::Load(TLoadContext& context)
{
    TStoreBase::Load(context);

    using NYT::Load;
    Load(context, StartingRowIndex_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

