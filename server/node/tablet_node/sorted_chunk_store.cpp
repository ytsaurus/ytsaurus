#include "sorted_chunk_store.h"
#include "automaton.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"
#include "versioned_chunk_meta_manager.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>

#include <yt/server/node/query_agent/config.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/client/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/client/misc/workload.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_state.h>
#include <yt/ytlib/table_client/lookup_chunk_reader.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/client/table_client/versioned_reader.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

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
using namespace NClusterNode;
using namespace NQueryAgent;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TFilteringReader
    : public IVersionedReader
{
public:
    TFilteringReader(
        IVersionedReaderPtr underlyingReader,
        int skipBefore,
        int skipAfter)
        : CurrentReaderIndex_(0)
        , FakeRowsRead_(0)
        , UnderlyingReader_(underlyingReader.Get())
    {
        if (skipBefore > 0) {
            Readers_.push_back(CreateEmptyVersionedReader(skipBefore));
        }
        Readers_.push_back(std::move(underlyingReader));
        if (skipAfter > 0) {
            Readers_.push_back(CreateEmptyVersionedReader(skipAfter));
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto statistics = UnderlyingReader_->GetDataStatistics();
        statistics.set_row_count(statistics.row_count() + FakeRowsRead_);
        return statistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual TFuture<void> Open() override
    {
        YT_VERIFY(CurrentReaderIndex_ == 0);
        for (auto& reader : Readers_) {
            reader->Open();
        }
        return Readers_[CurrentReaderIndex_]->GetReadyEvent();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (CurrentReaderIndex_ == Readers_.size()) {
            return VoidFuture;
        }
        return Readers_[CurrentReaderIndex_]->GetReadyEvent();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    virtual bool Read(std::vector<TVersionedRow>* rows) override
    {
        if (CurrentReaderIndex_ == Readers_.size()) {
            return false;
        }

        rows->clear();

        if (Readers_[CurrentReaderIndex_]->Read(rows)) {
            if (Readers_[CurrentReaderIndex_].Get() != UnderlyingReader_) {
                FakeRowsRead_ += rows->size();
            }
        } else {
            ++CurrentReaderIndex_;
        }

        return true;
    }

private:
    SmallVector<IVersionedReaderPtr, 3> Readers_;
    int CurrentReaderIndex_;
    int FakeRowsRead_;

    IVersionedReader* UnderlyingReader_;
};

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    TStoreId id,
    NChunkClient::TChunkId chunkId,
    const NChunkClient::TReadRange& readRange,
    TTimestamp chunkTimestamp,
    TTablet* tablet,
    IBlockCachePtr blockCache,
    TChunkRegistryPtr chunkRegistry,
    TChunkBlockManagerPtr chunkBlockManager,
    TVersionedChunkMetaManagerPtr chunkMetaManager,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor)
    : TChunkStoreBase(
        config,
        id,
        chunkId,
        chunkTimestamp,
        tablet,
        blockCache,
        chunkRegistry,
        chunkBlockManager,
        chunkMetaManager,
        client,
        localDescriptor)
    , KeyComparer_(tablet->GetRowKeyComparer())
{
    TKey lowerBound;
    TKey upperBound;

    if (readRange.LowerLimit().HasKey()) {
        lowerBound = readRange.LowerLimit().GetKey();
    }

    if (readRange.UpperLimit().HasKey()) {
        upperBound = readRange.UpperLimit().GetKey();
    }

    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);

    YT_LOG_DEBUG("Sorted chunk store created (Id: %v, ChunkId: %v, Type: %v, LowerBound: %v, UpperBound: %v)",
        id,
        ChunkId_,
        TypeFromId(id),
        lowerBound,
        upperBound);
}

TSortedChunkStore::~TSortedChunkStore()
{
    YT_LOG_DEBUG("Sorted chunk store destroyed");
}

EStoreType TSortedChunkStore::GetType() const
{
    return EStoreType::SortedChunk;
}

TSortedChunkStorePtr TSortedChunkStore::AsSortedChunk()
{
    return this;
}

void TSortedChunkStore::BuildOrchidYson(TFluentMap fluent)
{
    TChunkStoreBase::BuildOrchidYson(fluent);

    fluent
        .Item("min_key").Value(GetMinKey())
        .Item("upper_bound_key").Value(GetUpperBoundKey());
}

TOwningKey TSortedChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TSortedChunkStore::GetUpperBoundKey() const
{
    return UpperBoundKey_;
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientBlockReadOptions& blockReadOptions,
    IThroughputThrottlerPtr throttler)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ranges = FilterRowRangesByReadRange(ranges);

    // Fast lane: ranges do not intersect with chunk view.
    if (ranges.Empty()) {
        return CreateEmptyVersionedReader();
    }

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        ranges,
        timestamp,
        produceAllVersions,
        columnFilter,
        blockReadOptions,
        tabletSnapshot->TableSchema,
        ReadRange_);
    if (reader) {
        return reader;
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        YT_VERIFY(!HasNontrivialReadRange());
        return backingStore->CreateReader(
            tabletSnapshot,
            ranges,
            timestamp,
            produceAllVersions,
            columnFilter,
            blockReadOptions);
    }

    auto chunkReader = GetChunkReader(throttler);
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
        produceAllVersions,
        ReadRange_);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientBlockReadOptions& blockReadOptions,
    const TTableSchema& schema,
    const TSharedRange<TRowRange>& singletonClippingRange)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(SpinLock_);

    if (!ValidateBlockCachePreloaded()) {
        return nullptr;
    }

    YT_VERIFY(ChunkState_);
    YT_VERIFY(ChunkState_->ChunkMeta);

    return CreateCacheBasedVersionedChunkReader(
        ChunkState_,
        blockReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions,
        singletonClippingRange);
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientBlockReadOptions& blockReadOptions,
    IThroughputThrottlerPtr throttler)
{
    VERIFY_THREAD_AFFINITY_ANY();

    int skippedBefore = 0;
    int skippedAfter = 0;
    auto filteredKeys = FilterKeysByReadRange(keys, &skippedBefore, &skippedAfter);

    if (filteredKeys.Empty()) {
        return CreateEmptyVersionedReader(keys.Size());
    }

    auto createFilteringReader = [&] (IVersionedReaderPtr underlyingReader) -> IVersionedReaderPtr {
        if (skippedBefore == 0 && skippedAfter == 0) {
            return std::move(underlyingReader);
        }
        return New<TFilteringReader>(std::move(underlyingReader), skippedBefore, skippedAfter);
    };

    // Fast lane: check for in-memory reads.
    auto reader = CreateCacheBasedReader(
        filteredKeys,
        timestamp,
        produceAllVersions,
        columnFilter,
        blockReadOptions,
        tabletSnapshot->TableSchema);
    if (reader) {
        return createFilteringReader(reader);
    }

    // Another fast lane: check for backing store.
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        YT_VERIFY(!HasNontrivialReadRange());
        return backingStore->CreateReader(
            std::move(tabletSnapshot),
            filteredKeys,
            timestamp,
            produceAllVersions,
            columnFilter,
            blockReadOptions);
    }

    auto chunkReader = GetChunkReader(throttler);

    if (tabletSnapshot->Config->EnableDataNodeLookup && chunkReader->IsLookupSupported()) {
        return CreateRowLookupReader(
            std::move(chunkReader),
            blockReadOptions,
            filteredKeys,
            tabletSnapshot,
            columnFilter,
            timestamp,
            produceAllVersions);
    }

    auto chunkState = PrepareChunkState(chunkReader, blockReadOptions);
    ValidateBlockSize(tabletSnapshot, chunkState, blockReadOptions.WorkloadDescriptor);

    return createFilteringReader(CreateVersionedChunkReader(
        ReaderConfig_,
        std::move(chunkReader),
        chunkState,
        chunkState->ChunkMeta,
        blockReadOptions,
        filteredKeys,
        columnFilter,
        timestamp,
        produceAllVersions));
}

TSharedRange<TKey> TSortedChunkStore::FilterKeysByReadRange(
    const TSharedRange<TKey>& keys,
    int* skippedBefore,
    int* skippedAfter) const
{
    return NTabletNode::FilterKeysByReadRange(ReadRange_.Front(), keys, skippedBefore, skippedAfter);
}

TSharedRange<TRowRange> TSortedChunkStore::FilterRowRangesByReadRange(
    const TSharedRange<TRowRange>& ranges) const
{
    return NTabletNode::FilterRowRangesByReadRange(ReadRange_.Front(), ranges);
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

    YT_VERIFY(ChunkState_);
    YT_VERIFY(ChunkState_->ChunkMeta);

    return CreateCacheBasedVersionedChunkReader(
        ChunkState_,
        blockReadOptions,
        keys,
        columnFilter,
        timestamp,
        produceAllVersions);
}

bool TSortedChunkStore::CheckRowLocks(
    TUnversionedRow row,
    TLockMask lockMask,
    TWriteContext* context)
{
    auto backingStore = GetSortedBackingStore();
    if (backingStore) {
        return backingStore->CheckRowLocks(row, lockMask, context);
    }

    auto* transaction = context->Transaction;
    context->Error = TError(
        "Checking for transaction conflicts against chunk stores is not supported; "
        "consider reducing transaction duration or increasing store retention time")
        << TErrorAttribute("transaction_id", transaction->GetId())
        << TErrorAttribute("transaction_start_time", transaction->GetStartTime())
        << TErrorAttribute("tablet_id", TabletId_)
        << TErrorAttribute("table_path", TablePath_)
        << TErrorAttribute("store_id", StoreId_)
        << TErrorAttribute("key", RowToKey(row));
    return false;
}

void TSortedChunkStore::Save(TSaveContext& context) const
{
    TStoreBase::Save(context);

    using NYT::Save;
    Save(context, ChunkId_);
    Save(context, TOwningKey(ReadRange_[0].first));
    Save(context, TOwningKey(ReadRange_[0].second));
}

void TSortedChunkStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);

    using NYT::Load;
    Load(context, ChunkId_);
    auto lowerBound = Load<TOwningKey>(context);
    auto upperBound = Load<TOwningKey>(context);
    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);
}

TChunkStatePtr TSortedChunkStore::PrepareChunkState(
    IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    NProfiling::TWallTimer metaWaitTimer;
    TFuture<TCachedVersionedChunkMetaPtr> asyncChunkMeta;

    if (ChunkMetaManager_) {
        asyncChunkMeta = ChunkMetaManager_->GetMeta(
            chunkReader,
            Schema_,
            blockReadOptions);
    } else {
        asyncChunkMeta = TCachedVersionedChunkMeta::Load(
            chunkReader,
            blockReadOptions,
            Schema_,
            {},
            nullptr);
    }

    auto chunkMeta = WaitFor(asyncChunkMeta)
        .ValueOrThrow();
    blockReadOptions.ChunkReaderStatistics->MetaWaitTime += metaWaitTimer.GetElapsedValue();

    return New<TChunkState>(
        BlockCache_,
        std::move(chunkSpec),
        std::move(chunkMeta),
        ChunkTimestamp_,
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

    MinKey_ = FromProto<TOwningKey>(boundaryKeysExt.min());
    const auto& chunkViewLowerBound = ReadRange_.Front().first;
    if (chunkViewLowerBound && chunkViewLowerBound > MinKey_) {
        MinKey_ = TOwningKey(chunkViewLowerBound);
    }
    MinKey_ = WidenKey(MinKey_, KeyColumnCount_);

    UpperBoundKey_ = FromProto<TOwningKey>(boundaryKeysExt.max());
    const auto& chunkViewUpperBound = ReadRange_.Front().second;
    if (chunkViewUpperBound && chunkViewUpperBound <= UpperBoundKey_) {
        UpperBoundKey_ = TOwningKey(chunkViewUpperBound);
    } else {
        UpperBoundKey_ = WidenKeySuccessor(UpperBoundKey_, KeyColumnCount_);
    }
}

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore()
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

bool TSortedChunkStore::HasNontrivialReadRange() const
{
    return ReadRange_.Front().first || ReadRange_.Front().second;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TKey> FilterKeysByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<TKey>& keys,
    int* skippedBefore,
    int* skippedAfter)
{
    int begin = 0;
    int end = keys.Size();

    if (const auto& lowerLimit = readRange.first) {
        begin = std::lower_bound(
            keys.begin(),
            keys.end(),
            lowerLimit) - keys.begin();
    }

    if (const auto& upperLimit = readRange.second) {
        end = std::lower_bound(
            keys.begin(),
            keys.end(),
            upperLimit) - keys.begin();
    }

    *skippedBefore = begin;
    *skippedAfter = keys.Size() - end;

    return keys.Slice(begin, end);
}

TSharedRange<NTableClient::TRowRange> FilterRowRangesByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<NTableClient::TRowRange>& ranges)
{
    int begin = 0;
    int end = ranges.Size();

    if (const auto& lowerLimit = readRange.first) {
        begin = std::lower_bound(
            ranges.begin(),
            ranges.end(),
            lowerLimit,
            [] (const auto& range, const auto& key) {
                return range.second <= key;
            }) - ranges.begin();
    }

    if (const auto& upperLimit = readRange.second) {
        end = std::lower_bound(
            ranges.begin(),
            ranges.end(),
            upperLimit,
            [] (const auto& range, const auto& key) {
                return range.first < key;
            }) - ranges.begin();
    }

    return ranges.Slice(begin, end);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

