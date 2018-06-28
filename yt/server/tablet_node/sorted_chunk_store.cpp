#include "sorted_chunk_store.h"
#include "automaton.h"
#include "config.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"
#include "versioned_chunk_meta_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/query_agent/config.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_state.h>
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

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    TVersionedChunkMetaManagerPtr chunkMetaManager,
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
        chunkMetaManager,
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
    const TClientBlockReadOptions& blockReadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        ranges,
        timestamp,
        produceAllVersions,
        columnFilter,
        blockReadOptions,
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
            blockReadOptions);
    }

    auto chunkReader = GetChunkReader(GetUnlimitedThrottler());
    auto chunkState = PrepareChunkState(chunkReader, blockReadOptions);

    ValidateBlockSize(tabletSnapshot, chunkState, blockReadOptions.WorkloadDescriptor);

    return CreateVersionedChunkReader(
        ReaderConfig_,
        std::move(chunkReader),
        chunkState,
        chunkState->ChunkMeta,
        blockReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientBlockReadOptions& blockReadOptions,
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
        blockReadOptions,
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
    const TClientBlockReadOptions& blockReadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        keys,
        timestamp,
        produceAllVersions,
        columnFilter,
        blockReadOptions,
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
            blockReadOptions);
    }

    auto blockCache = GetBlockCache();
    auto chunkReader = GetChunkReader(GetUnlimitedThrottler());
    auto chunkState = PrepareChunkState(chunkReader, blockReadOptions);

    ValidateBlockSize(tabletSnapshot, chunkState, blockReadOptions.WorkloadDescriptor);

    return CreateVersionedChunkReader(
        ReaderConfig_,
        std::move(chunkReader),
        chunkState,
        chunkState->ChunkMeta,
        blockReadOptions,
        keys,
        columnFilter,
        timestamp,
        produceAllVersions);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientBlockReadOptions& blockReadOptions,
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
        blockReadOptions,
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
        << TErrorAttribute("table_path", TablePath_)
        << TErrorAttribute("store_id", StoreId_)
        << TErrorAttribute("key", RowToKey(row));
}

TChunkStatePtr TSortedChunkStore::PrepareChunkState(
    IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), StoreId_);
    auto asyncChunkMeta = ChunkMetaManager_->GetMeta(
        chunkReader,
        Schema_,
        blockReadOptions);
    auto chunkMeta = WaitFor(asyncChunkMeta)
        .ValueOrThrow();

    return New<TChunkState>(
        BlockCache_,
        std::move(chunkSpec),
        std::move(chunkMeta),
        nullptr,
        PerformanceCounters_,
        GetKeyComparer());
}

void TSortedChunkStore::ValidateBlockSize(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TChunkStatePtr& chunkState,
    const TWorkloadDescriptor& workloadDescriptor)
{
    if ((workloadDescriptor.Category == EWorkloadCategory::UserInteractive ||
        workloadDescriptor.Category == EWorkloadCategory::UserRealtime) &&
        (chunkState->ChunkMeta->GetChunkFormat() == ETableChunkFormat::SchemalessHorizontal ||
        chunkState->ChunkMeta->GetChunkFormat() == ETableChunkFormat::UnversionedColumnar))
    {
        // For unversioned chunks verify that block size is correct
        if (auto blockSizeLimit = tabletSnapshot->Config->MaxUnversionedBlockSize) {
            auto miscExt = FindProtoExtension<TMiscExt>(chunkState->ChunkSpec.chunk_meta().extensions());
            if (miscExt && miscExt->max_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("chunk_id", GetId())
                    << TErrorAttribute("block_size", miscExt->max_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }
}

TKeyComparer TSortedChunkStore::GetKeyComparer()
{
    return KeyComparer_;
}

void TSortedChunkStore::PrecacheProperties()
{
    TChunkStoreBase::PrecacheProperties();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_->extensions());
    MinKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), KeyColumnCount_);
    MaxKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), KeyColumnCount_);
}

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore()
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

