#include "chunk_store.h"
#include "automaton.h"
#include "config.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/chunk.h>
#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/local_chunk_reader.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/server/query_agent/config.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_reader.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/common.h>
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
using namespace NDataNode;
using namespace NCellNode;
using namespace NQueryAgent;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

static const auto ChunkExpirationTimeout = TDuration::Seconds(15);
static const auto ChunkReaderExpirationTimeout = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

class TChunkStore::TPreloadedBlockCache
    : public IBlockCache
{
public:
    TPreloadedBlockCache(
        TChunkStorePtr owner,
        const TChunkId& chunkId,
        EBlockType type,
        IBlockCachePtr underlyingCache)
        : Owner_(owner)
        , ChunkId_(chunkId)
        , Type_(type)
        , UnderlyingCache_(std::move(underlyingCache))
    { }

    DEFINE_BYVAL_RO_PROPERTY(TVersionedChunkLookupHashTablePtr, LookupHashTable);

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
        YASSERT(id.ChunkId == ChunkId_);

        if (type == Type_ && IsPreloaded()) {
            YASSERT(id.BlockIndex >= 0 && id.BlockIndex < Blocks_.size());
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
    const TWeakPtr<TChunkStore> Owner_;
    const TChunkId ChunkId_;
    const EBlockType Type_;
    const IBlockCachePtr UnderlyingCache_;

    std::vector<TSharedRef> Blocks_;
    std::atomic<bool> Preloaded_ = {false};

};

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    const TStoreId& id,
    TTablet* tablet,
    const TChunkMeta* chunkMeta,
    TBootstrap* boostrap)
    : TStoreBase(
        id,
        tablet)
    , PreloadState_(EStorePreloadState::Disabled)
    , CompactionState_(EStoreCompactionState::None)
    , Bootstrap_(boostrap)
    , ChunkMeta_(New<TRefCountedChunkMeta>())
    , KeyComparer_(tablet->GetRowKeyComparer())
    , RequireChunkPreload_(tablet->GetConfig()->RequireChunkPreload)
{
    YCHECK(
        TypeFromId(StoreId_) == EObjectType::Chunk ||
        TypeFromId(StoreId_) == EObjectType::ErasureChunk);

    StoreState_ = EStoreState::Persistent;

    if (chunkMeta) {
        ChunkMeta_->CopyFrom(*chunkMeta);
        PrecacheProperties();
    }

    LOG_DEBUG("Static chunk store created (TabletId: %v)",
        TabletId_);
}

TChunkStore::~TChunkStore()
{
    LOG_DEBUG("Static chunk store destroyed");
}

const TChunkMeta& TChunkStore::GetChunkMeta() const
{
    return *ChunkMeta_;
}

IStorePtr TChunkStore::GetBackingStore()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return BackingStore_;
}

void TChunkStore::SetBackingStore(IStorePtr store)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);
    BackingStore_ = store;
}

bool TChunkStore::HasBackingStore() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return BackingStore_.operator bool();
}

EInMemoryMode TChunkStore::GetInMemoryMode() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return InMemoryMode_;
}

void TChunkStore::SetInMemoryMode(EInMemoryMode mode)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    if (InMemoryMode_ == mode)
        return;

    PreloadedBlockCache_.Reset();

    if (PreloadFuture_) {
        PreloadFuture_.Cancel();
        PreloadFuture_.Reset();
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
            Bootstrap_->GetBlockCache());

        switch (PreloadState_) {
            case EStorePreloadState::Disabled:
            case EStorePreloadState::Failed:
            case EStorePreloadState::Running:
            case EStorePreloadState::Complete:
                PreloadState_ = EStorePreloadState::None;
                break;
            case EStorePreloadState::None:
            case EStorePreloadState::Scheduled:
                break;
            default:
                YUNREACHABLE();
        }
    }

    RealtimeChunkReader_.Reset();
    BatchChunkReader_.Reset();

    InMemoryMode_ = mode;
}

void TChunkStore::Preload(TInMemoryChunkDataPtr chunkData)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);

    if (chunkData->InMemoryMode != InMemoryMode_)
        return;

    PreloadedBlockCache_->Preload(chunkData);
    CachedVersionedChunkMeta_ = chunkData->ChunkMeta;
}

IChunkReaderPtr TChunkStore::GetChunkReader(const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunk = PrepareChunk();
    auto chunkReader = PrepareChunkReader(std::move(chunk), workloadDescriptor);

    return chunkReader;
}

EStoreType TChunkStore::GetType() const
{
    return EStoreType::Chunk;
}

i64 TChunkStore::GetUncompressedDataSize() const
{
    return DataSize_;
}

i64 TChunkStore::GetRowCount() const
{
    return RowCount_;
}

TOwningKey TChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TChunkStore::GetMaxKey() const
{
    return MaxKey_;
}

TTimestamp TChunkStore::GetMinTimestamp() const
{
    return MinTimestamp_;
}

TTimestamp TChunkStore::GetMaxTimestamp() const
{
    return MaxTimestamp_;
}

IVersionedReaderPtr TChunkStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (upperKey <= MinKey_ || lowerKey > MaxKey_) {
        return nullptr;
    }

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        lowerKey,
        upperKey,
        timestamp,
        columnFilter);
    if (reader) {
        return reader;
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetBackingStore();
    if (backingStore) {
        return backingStore->CreateReader(
            std::move(lowerKey),
            std::move(upperKey),
            timestamp,
            columnFilter,
            workloadDescriptor);
    }

    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader(workloadDescriptor);
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);

    TReadLimit lowerLimit;
    lowerLimit.SetKey(std::move(lowerKey));

    TReadLimit upperLimit;
    upperLimit.SetKey(std::move(upperKey));

    return CreateVersionedChunkReader(
        Bootstrap_->GetConfig()->TabletNode->ChunkReader,
        std::move(chunkReader),
        std::move(blockCache),
        std::move(cachedVersionedChunkMeta),
        lowerLimit,
        upperLimit,
        columnFilter,
        PerformanceCounters_,
        timestamp);
}

IVersionedReaderPtr TChunkStore::CreateCacheBasedReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);

    if (!ValidateBlockCachePreloaded()) {
        return nullptr;
    }

    YCHECK(CachedVersionedChunkMeta_);

    return CreateCacheBasedVersionedChunkReader(
        PreloadedBlockCache_,
        CachedVersionedChunkMeta_,
        std::move(lowerKey),
        std::move(upperKey),
        columnFilter,
        PerformanceCounters_,
        timestamp);
}

IVersionedReaderPtr TChunkStore::CreateReader(
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        keys,
        timestamp,
        columnFilter);
    if (reader) {
        return reader;
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetBackingStore();
    if (backingStore) {
        return backingStore->CreateReader(
            keys,
            timestamp,
            columnFilter,
            workloadDescriptor);
    }

    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader(workloadDescriptor);
    auto cachedVersionedChunkMeta = PrepareCachedVersionedChunkMeta(chunkReader);

    return CreateVersionedChunkReader(
        Bootstrap_->GetConfig()->TabletNode->ChunkReader,
        std::move(chunkReader),
        std::move(blockCache),
        std::move(cachedVersionedChunkMeta),
        keys,
        columnFilter,
        PerformanceCounters_,
        KeyComparer_,
        timestamp);
}

IVersionedReaderPtr TChunkStore::CreateCacheBasedReader(
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);

    if (!ValidateBlockCachePreloaded()) {
        return nullptr;
    }

    YCHECK(CachedVersionedChunkMeta_);

    return CreateCacheBasedVersionedChunkReader(
        PreloadedBlockCache_,
        CachedVersionedChunkMeta_,
        PreloadedBlockCache_->GetLookupHashTable(),
        keys,
        columnFilter,
        PerformanceCounters_,
        KeyComparer_,
        timestamp);
}

void TChunkStore::CheckRowLocks(
    TUnversionedRow row,
    TTransaction* transaction,
    ui32 lockMask)
{
    auto backingStore = GetBackingStore();
    if (backingStore) {
        return backingStore->CheckRowLocks(row, transaction, lockMask);
    }

    THROW_ERROR_EXCEPTION(
        "Checking for transaction conflicts against chunk stores is not supported; "
        "consider reducing transaction duration or increasing store retention time")
        << TErrorAttribute("transaction_id", transaction->GetId())
        << TErrorAttribute("transaction_start_time", transaction->GetStartTime())
        << TErrorAttribute("transaction_register_time", transaction->GetRegisterTime())
        << TErrorAttribute("tablet_id", TabletId_)
        << TErrorAttribute("store_id", StoreId_)
        << TErrorAttribute("key", RowToKey(row));
}

void TChunkStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);
}

void TChunkStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);
}

TCallback<void(TSaveContext&)> TChunkStore::AsyncSave()
{
    return BIND([chunkMeta = ChunkMeta_] (TSaveContext& context) {
        using NYT::Save;

        Save(context, *chunkMeta);
    });
}

void TChunkStore::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    Load(context, *ChunkMeta_);

    PrecacheProperties();
}

void TChunkStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    TStoreBase::BuildOrchidYson(consumer);

    auto backingStore = GetBackingStore();
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_->extensions());
    BuildYsonMapFluently(consumer)
        .Item("preload_state").Value(PreloadState_)
        .Item("compaction_state").Value(CompactionState_)
        .Item("compressed_data_size").Value(miscExt.compressed_data_size())
        .Item("uncompressed_data_size").Value(miscExt.uncompressed_data_size())
        .Item("key_count").Value(miscExt.row_count())
        .DoIf(backingStore.operator bool(), [&] (TFluentMap fluent) {
            fluent.Item("backing_store_id").Value(backingStore->GetId());
        });
}

IChunkPtr TChunkStore::PrepareChunk()
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TReaderGuard guard(SpinLock_);
        if (ChunkInitialized_) {
            return Chunk_;
        }
    }

    auto chunkRegistry = Bootstrap_->GetChunkRegistry();
    auto chunk = chunkRegistry->FindChunk(StoreId_);

    {
        TWriterGuard guard(SpinLock_);
        ChunkInitialized_ = true;
        Chunk_ = chunk;
    }

    TDelayedExecutor::Submit(
        BIND(&TChunkStore::OnChunkExpired, MakeWeak(this)),
        ChunkExpirationTimeout);

    return chunk;
}

IChunkReaderPtr TChunkStore::PrepareChunkReader(
    IChunkPtr chunk,
    const TWorkloadDescriptor& workloadDescriptor)
{
    VERIFY_THREAD_AFFINITY_ANY();

    bool isRT = false;
    switch (workloadDescriptor.Category) {
        case EWorkloadCategory::SystemRealtime:
        case EWorkloadCategory::UserRealtime:
            isRT = true;
            break;
        default:
            break;
    }

    {
        TReaderGuard guard(SpinLock_);
        if (isRT) {
            if (RealtimeChunkReader_) {
                return RealtimeChunkReader_;
            }
        } else {
            if (BatchChunkReader_) {
                return BatchChunkReader_;
            }
        }
    }

    auto readerConfig = Bootstrap_->GetConfig()->TabletNode->ChunkReader;
    auto readerConfigCopy = CloneYsonSerializable(readerConfig);

    readerConfigCopy->WorkloadDescriptor = workloadDescriptor;
    readerConfigCopy->PopulateCache = isRT;

    IChunkReaderPtr chunkReader;
    if (chunk && !chunk->IsRemoveScheduled()) {
        chunkReader = CreateLocalChunkReader(
            Bootstrap_,
            readerConfigCopy,
            chunk,
            GetBlockCache(),
            BIND(&TChunkStore::OnLocalReaderFailed, MakeWeak(this)));
    } else {
        // TODO(babenko): provide seed replicas
        auto options = New<TRemoteReaderOptions>();
        chunkReader = CreateReplicationReader(
            readerConfigCopy,
            options,
            Bootstrap_->GetMasterClient(),
            New<TNodeDirectory>(),
            Bootstrap_->GetMasterConnector()->GetLocalDescriptor(),
            StoreId_,
            TChunkReplicaList(),
            GetBlockCache());
    }

    {
        TWriterGuard guard(SpinLock_);
        if (isRT) {
            RealtimeChunkReader_ = chunkReader;
        } else {
            BatchChunkReader_ = chunkReader;
        }
    }

    TDelayedExecutor::Submit(
        BIND(&TChunkStore::OnChunkReaderExpired, MakeWeak(this)),
        ChunkReaderExpirationTimeout);

    return chunkReader;
}

TCachedVersionedChunkMetaPtr TChunkStore::PrepareCachedVersionedChunkMeta(IChunkReaderPtr chunkReader)
{
    VERIFY_THREAD_AFFINITY_ANY();

    {
        TReaderGuard guard(SpinLock_);
        if (CachedVersionedChunkMeta_) {
            return CachedVersionedChunkMeta_;
        }
    }

    auto cachedMeta = WaitFor(TCachedVersionedChunkMeta::Load(chunkReader, Schema_, KeyColumns_))
        .ValueOrThrow();

    {
        TWriterGuard guard(SpinLock_);
        CachedVersionedChunkMeta_ = cachedMeta;
    }

    return cachedMeta;
}

IBlockCachePtr TChunkStore::GetBlockCache()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);
    return PreloadedBlockCache_
        ? PreloadedBlockCache_
        : Bootstrap_->GetBlockCache();
}

void TChunkStore::PrecacheProperties()
{
    // Precache frequently used values.
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_->extensions());
    DataSize_ = miscExt.uncompressed_data_size();
    RowCount_ = miscExt.row_count();
    MinTimestamp_ = miscExt.min_timestamp();
    MaxTimestamp_ = miscExt.max_timestamp();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_->extensions());
    MinKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), KeyColumnCount_);
    MaxKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), KeyColumnCount_);
}

void TChunkStore::OnLocalReaderFailed()
{
    VERIFY_THREAD_AFFINITY_ANY();

    OnChunkExpired();
    OnChunkReaderExpired();
}

void TChunkStore::OnChunkExpired()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);
    ChunkInitialized_ = false;
    Chunk_.Reset();
}

void TChunkStore::OnChunkReaderExpired()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(SpinLock_);
    RealtimeChunkReader_.Reset();
    BatchChunkReader_.Reset();
}

bool TChunkStore::ValidateBlockCachePreloaded()
{
    if (!PreloadedBlockCache_ || !PreloadedBlockCache_->IsPreloaded()) {
        if (RequireChunkPreload_) {
            THROW_ERROR_EXCEPTION("Chunk data is not preloaded yet")
                << TErrorAttribute("tablet_id", TabletId_)
                << TErrorAttribute("store_id", StoreId_);
        }
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

