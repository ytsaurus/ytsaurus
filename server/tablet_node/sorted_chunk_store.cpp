#include "sorted_chunk_store.h"
#include "automaton.h"
#include "config.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/query_agent/config.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_reader.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NApi;
using namespace NDataNode;
using namespace NCellNode;
using namespace NQueryAgent;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkStore::TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TSortedChunkStorePtr owner,
        const TChunkId& chunkId,
        EBlockType type,
        IBlockCachePtr underlyingCache)
        : Owner_(owner)
        , ChunkId_(chunkId)
        , Type_(type)
        , UnderlyingCache_(std::move(underlyingCache))
    { }

    DEFINE_BYVAL_RO_PROPERTY(IChunkLookupHashTablePtr, LookupHashTable);

    ~TPreloadedBlockCache()
    {
        auto owner = Owner_.Lock();
        if (!owner)
            return;

        owner->SetMemoryUsage(0);
    }

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TSharedRef& data,
        const TNullable<NNodeTrackerClient::TNodeDescriptor>& source) override
    {
        UnderlyingCache_->Put(id, type, data, source);
    }

    virtual TSharedRef Find(
        const TBlockId& id,
        EBlockType type) override
    {
        Y_ASSERT(id.ChunkId == ChunkId_);

        if (type == Type_ && IsPreloaded()) {
            Y_ASSERT(id.BlockIndex >= 0 && id.BlockIndex < Blocks_.size());
            return Blocks_[id.BlockIndex];
        } else {
            return UnderlyingCache_->Find(id, type);
        }
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return Type_;
    }

    void Preload(TInMemoryChunkDataPtr chunkData)
    {
        auto owner = Owner_.Lock();
        if (!owner)
            return;

        Blocks_ = std::move(chunkData->Blocks);
        LookupHashTable_ = chunkData->LookupHashTable;

        i64 dataSize = GetByteSize(Blocks_);
        if (LookupHashTable_) {
            dataSize += LookupHashTable_->GetByteSize();
        }

        owner->SetMemoryUsage(dataSize);

        Preloaded_ = true;
    }

    bool IsPreloaded() const
    {
        return Preloaded_.load();
    }

private:
    const TWeakPtr<TSortedChunkStore> Owner_;
    const TChunkId ChunkId_;
    const EBlockType Type_;
    const IBlockCachePtr UnderlyingCache_;

    std::vector<TSharedRef> Blocks_;
    std::atomic<bool> Preloaded_ = {false};
};

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor)
    : TStoreBase(config, id, tablet)
    , TChunkStoreBase(
        config,
        id,
        tablet,
        blockCache,
        chunkRegistry,
        chunkBlockManager,
        client,
        localDescriptor)
    , TSortedStoreBase(config, id, tablet)
    , KeyComparer_(tablet->GetRowKeyComparer())
{
    LOG_DEBUG("Sorted chunk store created");
}

TSortedChunkStore::~TSortedChunkStore()
{
    LOG_DEBUG("Sorted chunk store destroyed");
}

TSortedChunkStorePtr TSortedChunkStore::AsSortedChunk()
{
    return this;
}

EInMemoryMode TSortedChunkStore::GetInMemoryMode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return InMemoryMode_;
}

void TSortedChunkStore::SetInMemoryMode(EInMemoryMode mode)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    if (InMemoryMode_ == mode) {
        return;
    }

    ChunkState_.Reset();
    PreloadedBlockCache_.Reset();

    if (PreloadFuture_) {
        PreloadFuture_.Cancel();
        PreloadFuture_.Reset();
    }
    if (PreloadBackoffFuture_) {
        PreloadBackoffFuture_.Cancel();
        PreloadBackoffFuture_.Reset();
    }

    if (mode == EInMemoryMode::None) {
        PreloadState_ = EStorePreloadState::Disabled;
    } else {
        auto blockType =
               mode == EInMemoryMode::Compressed      ? EBlockType::CompressedData :
            /* mode == EInMemoryMode::Uncompressed */   EBlockType::UncompressedData;

        PreloadedBlockCache_ = New<TPreloadedBlockCache>(
            this,
            StoreId_,
            blockType,
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
            default:
                Y_UNREACHABLE();
        }
    }

    ChunkReader_.Reset();

    InMemoryMode_ = mode;
}

void TSortedChunkStore::Preload(TInMemoryChunkDataPtr chunkData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    if (chunkData->InMemoryMode != InMemoryMode_) {
        return;
    }

    PreloadedBlockCache_->Preload(chunkData);
    CachedVersionedChunkMeta_ = chunkData->ChunkMeta;
    ChunkState_ = New<TCacheBasedChunkState>(
        PreloadedBlockCache_,
        CachedVersionedChunkMeta_,
        PreloadedBlockCache_->GetLookupHashTable(),
        PerformanceCounters_,
        KeyComparer_);
}

EStoreType TSortedChunkStore::GetType() const
{
    return EStoreType::SortedChunk;
}

TOwningKey TSortedChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TSortedChunkStore::GetMaxKey() const
{
    return MaxKey_;
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        ranges,
        timestamp,
        produceAllVersions,
        columnFilter,
        tabletSnapshot->TableSchema);
    if (reader) {
        return reader;
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        return backingStore->CreateReader(
            tabletSnapshot,
            ranges,
            timestamp,
            produceAllVersions,
            columnFilter,
            workloadDescriptor);
    }

    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader();
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);

    auto config = CloneYsonSerializable(ReaderConfig_);
    config->WorkloadDescriptor = workloadDescriptor;

    return CreateVersionedChunkReader(
        std::move(config),
        std::move(chunkReader),
        std::move(blockCache),
        std::move(cachedVersionedChunkMeta),
        std::move(ranges),
        columnFilter,
        PerformanceCounters_,
        timestamp,
        produceAllVersions);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TTableSchema& schema)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);

    if (!ValidateBlockCachePreloaded()) {
        return nullptr;
    }

    YCHECK(ChunkState_);
    YCHECK(ChunkState_->ChunkMeta);

    return CreateCacheBasedVersionedChunkReader(
        ChunkState_,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions);
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        keys,
        timestamp,
        produceAllVersions,
        columnFilter,
        tabletSnapshot->TableSchema);
    if (reader) {
        return reader;
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        return backingStore->CreateReader(
            std::move(tabletSnapshot),
            keys,
            timestamp,
            produceAllVersions,
            columnFilter,
            workloadDescriptor);
    }

    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader();
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);
    auto config = CloneYsonSerializable(ReaderConfig_);
    config->WorkloadDescriptor = workloadDescriptor;

    return CreateVersionedChunkReader(
        std::move(config),
        std::move(chunkReader),
        std::move(blockCache),
        std::move(cachedVersionedChunkMeta),
        keys,
        columnFilter,
        PerformanceCounters_,
        KeyComparer_,
        timestamp,
        produceAllVersions);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TTableSchema& schema)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);

    if (!ValidateBlockCachePreloaded()) {
        return nullptr;
    }

    YCHECK(ChunkState_);
    YCHECK(ChunkState_->ChunkMeta);

    return CreateCacheBasedVersionedChunkReader(
        ChunkState_,
        keys,
        columnFilter,
        timestamp,
        produceAllVersions);
}

TError TSortedChunkStore::CheckRowLocks(
    TUnversionedRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        return backingStore->CheckRowLocks(row, transaction, lockMask);
    }

    return TError(
        "Checking for transaction conflicts against chunk stores is not supported; "
        "consider reducing transaction duration or increasing store retention time")
        << TErrorAttribute("transaction_id", transaction->GetId())
        << TErrorAttribute("transaction_start_time", transaction->GetStartTime())
        << TErrorAttribute("tablet_id", TabletId_)
        << TErrorAttribute("store_id", StoreId_)
        << TErrorAttribute("key", RowToKey(row));
}

TCachedVersionedChunkMetaPtr TSortedChunkStore::PrepareCachedVersionedChunkMeta(IChunkReaderPtr chunkReader)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TReaderGuard guard(SpinLock_);
        if (CachedVersionedChunkMeta_) {
            return CachedVersionedChunkMeta_;
        }
    }

    // TODO(babenko): do we need to make this workload descriptor configurable?
    auto asyncCachedMeta = TCachedVersionedChunkMeta::Load(
        chunkReader,
        TWorkloadDescriptor(EWorkloadCategory::UserBatch),
        Schema_);
    auto cachedMeta = WaitFor(asyncCachedMeta)
        .ValueOrThrow();

    {
        TWriterGuard guard(SpinLock_);
        CachedVersionedChunkMeta_ = cachedMeta;
    }

    return cachedMeta;
}

IBlockCachePtr TSortedChunkStore::GetBlockCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return PreloadedBlockCache_ ? PreloadedBlockCache_ : BlockCache_;
}

void TSortedChunkStore::PrecacheProperties()
{
    TChunkStoreBase::PrecacheProperties();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_->extensions());
    MinKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), KeyColumnCount_);
    MaxKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), KeyColumnCount_);
}

bool TSortedChunkStore::ValidateBlockCachePreloaded()
{
    if (InMemoryMode_ == EInMemoryMode::None) {
        return false;
    }

    if (!PreloadedBlockCache_ || !PreloadedBlockCache_->IsPreloaded()) {
        THROW_ERROR_EXCEPTION("Chunk data is not preloaded yet")
            << TErrorAttribute("tablet_id", TabletId_)
            << TErrorAttribute("store_id", StoreId_);
    }

    return true;
}

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore()
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

