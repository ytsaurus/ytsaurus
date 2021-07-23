#include "sorted_chunk_store.h"
#include "automaton.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "transaction.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/query_agent/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/lookup_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/new_table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

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

using NChunkClient::TLegacyReadLimit;

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

    virtual TFuture<void> GetReadyEvent() const override
    {
        if (CurrentReaderIndex_ == std::ssize(Readers_)) {
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

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (CurrentReaderIndex_ == std::ssize(Readers_)) {
            return nullptr;
        }

        if (auto batch = Readers_[CurrentReaderIndex_]->Read(options)) {
            if (Readers_[CurrentReaderIndex_].Get() != UnderlyingReader_) {
                FakeRowsRead_ += batch->GetRowCount();
            }
            return batch;
        } else {
            ++CurrentReaderIndex_;
            return CreateEmptyVersionedRowBatch();
        }
    }

private:
    SmallVector<IVersionedReaderPtr, 3> Readers_;
    int CurrentReaderIndex_;
    int FakeRowsRead_;

    IVersionedReader* UnderlyingReader_;
};

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    IBootstrap* bootstrap,
    TTabletManagerConfigPtr config,
    TStoreId id,
    NChunkClient::TChunkId chunkId,
    const NChunkClient::TLegacyReadRange& readRange,
    TTimestamp chunkTimestamp,
    TTablet* tablet,
    const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
    IBlockCachePtr blockCache,
    IVersionedChunkMetaManagerPtr chunkMetaManager,
    IChunkRegistryPtr chunkRegistry,
    IChunkBlockManagerPtr chunkBlockManager,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor)
    : TChunkStoreBase(
        bootstrap,
        config,
        id,
        chunkId,
        chunkTimestamp,
        tablet,
        addStoreDescriptor,
        blockCache,
        chunkMetaManager,
        chunkRegistry,
        chunkBlockManager,
        client,
        localDescriptor)
    , KeyComparer_(tablet->GetRowKeyComparer())
{
    TLegacyKey lowerBound;
    TLegacyKey upperBound;

    if (readRange.LowerLimit().HasLegacyKey()) {
        lowerBound = readRange.LowerLimit().GetLegacyKey();
    }

    if (readRange.UpperLimit().HasLegacyKey()) {
        upperBound = readRange.UpperLimit().GetLegacyKey();
    }

    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);
}

void TSortedChunkStore::Initialize()
{
    TChunkStoreBase::Initialize();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_->extensions());

    MinKey_ = FromProto<TLegacyOwningKey>(boundaryKeysExt.min());
    const auto& chunkViewLowerBound = ReadRange_.Front().first;
    if (chunkViewLowerBound && chunkViewLowerBound > MinKey_) {
        MinKey_ = TLegacyOwningKey(chunkViewLowerBound);
    }
    MinKey_ = WidenKey(MinKey_, KeyColumnCount_);

    UpperBoundKey_ = FromProto<TLegacyOwningKey>(boundaryKeysExt.max());
    const auto& chunkViewUpperBound = ReadRange_.Front().second;
    if (chunkViewUpperBound && chunkViewUpperBound <= UpperBoundKey_) {
        UpperBoundKey_ = TLegacyOwningKey(chunkViewUpperBound);
    } else {
        UpperBoundKey_ = WidenKeySuccessor(UpperBoundKey_, KeyColumnCount_);
    }
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

TLegacyOwningKey TSortedChunkStore::GetMinKey() const
{
    return MinKey_;
}

TLegacyOwningKey TSortedChunkStore::GetUpperBoundKey() const
{
    return UpperBoundKey_;
}

bool TSortedChunkStore::HasNontrivialReadRange() const
{
    return ReadRange_.Front().first || ReadRange_.Front().second;
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<EWorkloadCategory> workloadCategory)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ranges = FilterRowRangesByReadRange(ranges);

    // Fast lane: ranges do not intersect with chunk view.
    if (ranges.Empty()) {
        return CreateEmptyVersionedReader();
    }

    // Fast lane: check for in-memory reads.
    if (auto reader = TryCreateCacheBasedReader(
        ranges,
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions,
        ReadRange_))
    {
        return reader;
    }

    // Another fast lane: check for backing store.
    if (auto backingStore = GetSortedBackingStore()) {
        YT_VERIFY(!HasNontrivialReadRange());
        return backingStore->CreateReader(
            tabletSnapshot,
            ranges,
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            /*workloadCategory*/ std::nullopt);
    }

    auto chunkReader = GetReaders(workloadCategory).ChunkReader;
    auto chunkState = PrepareChunkState(chunkReader, chunkReadOptions);

    ValidateBlockSize(tabletSnapshot, chunkState, chunkReadOptions.WorkloadDescriptor);

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    if (mountConfig->EnableNewScanReaderForSelect &&
        chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar &&
        timestamp != AllCommittedTimestamp)
    {
        // Chunk view support.
        ranges = NNewTableClient::ClipRanges(
            ranges,
            ReadRange_.Size() > 0 ? ReadRange_.Front().first : TUnversionedRow(),
            ReadRange_.Size() > 0 ? ReadRange_.Front().second : TUnversionedRow(),
            ReadRange_.GetHolder());

        return NNewTableClient::CreateVersionedChunkReader(
            std::move(ranges),
            timestamp,
            chunkState->ChunkMeta,
            Schema_,
            columnFilter,
            chunkState->BlockCache,
            ReaderConfig_,
            chunkReader,
            chunkState->PerformanceCounters,
            chunkReadOptions,
            produceAllVersions);
    }

    return CreateVersionedChunkReader(
        ReaderConfig_,
        std::move(chunkReader),
        chunkState,
        chunkState->ChunkMeta,
        chunkReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions,
        ReadRange_);
}

IVersionedReaderPtr TSortedChunkStore::TryCreateCacheBasedReader(
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TRowRange>& singletonClippingRange)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunkState = FindPreloadedChunkState();
    if (!chunkState) {
        return nullptr;
    }

    return CreateCacheBasedVersionedChunkReader(
        std::move(chunkState),
        chunkReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions,
        singletonClippingRange);
}

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TSharedRange<TLegacyKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    std::optional<EWorkloadCategory> workloadCategory)
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
            return underlyingReader;
        }
        return New<TFilteringReader>(std::move(underlyingReader), skippedBefore, skippedAfter);
    };

    // Fast lane: check for in-memory reads.
    if (auto reader = TryCreateCacheBasedReader(
        filteredKeys,
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions))
    {
        return createFilteringReader(std::move(reader));
    }

    // Another fast lane: check for backing store.
    if (auto backingStore = GetSortedBackingStore()) {
        YT_VERIFY(!HasNontrivialReadRange());
        return backingStore->CreateReader(
            std::move(tabletSnapshot),
            filteredKeys,
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            /*workloadCategory*/ std::nullopt);
    }

    auto readers = GetReaders(workloadCategory);

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    if (mountConfig->EnableDataNodeLookup && readers.LookupReader) {
        return createFilteringReader(CreateRowLookupReader(
            std::move(readers.LookupReader),
            chunkReadOptions,
            filteredKeys,
            tabletSnapshot,
            columnFilter,
            timestamp,
            produceAllVersions,
            ChunkTimestamp_,
            mountConfig->EnablePeerProbingInDataNodeLookup,
            mountConfig->EnableRejectsInDataNodeLookupIfThrottling));
    }

    auto chunkState = PrepareChunkState(readers.ChunkReader, chunkReadOptions);
    ValidateBlockSize(tabletSnapshot, chunkState, chunkReadOptions.WorkloadDescriptor);

    if (mountConfig->EnableNewScanReaderForLookup &&
        chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar)
    {
        return createFilteringReader(NNewTableClient::CreateVersionedChunkReader(
            filteredKeys,
            timestamp,
            chunkState->ChunkMeta,
            Schema_,
            columnFilter,
            chunkState->BlockCache,
            ReaderConfig_,
            readers.ChunkReader,
            chunkState->PerformanceCounters,
            chunkReadOptions,
            produceAllVersions));
    }

    return createFilteringReader(CreateVersionedChunkReader(
        ReaderConfig_,
        std::move(readers.ChunkReader),
        chunkState,
        chunkState->ChunkMeta,
        chunkReadOptions,
        filteredKeys,
        columnFilter,
        timestamp,
        produceAllVersions));
}

TSharedRange<TLegacyKey> TSortedChunkStore::FilterKeysByReadRange(
    const TSharedRange<TLegacyKey>& keys,
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

IVersionedReaderPtr TSortedChunkStore::TryCreateCacheBasedReader(
    const TSharedRange<TLegacyKey>& keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions)
{
    auto chunkState = FindPreloadedChunkState();
    if (!chunkState) {
        return nullptr;
    }

    return CreateCacheBasedVersionedChunkReader(
        std::move(chunkState),
        chunkReadOptions,
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
    if (auto backingStore = GetSortedBackingStore()) {
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
    TChunkStoreBase::Save(context);

    using NYT::Save;
    Save(context, ChunkId_);
    Save(context, TLegacyOwningKey(ReadRange_[0].first));
    Save(context, TLegacyOwningKey(ReadRange_[0].second));
}

void TSortedChunkStore::Load(TLoadContext& context)
{
    TStoreBase::Load(context);
    TChunkStoreBase::Load(context);

    using NYT::Load;
    Load(context, ChunkId_);
    auto lowerBound = Load<TLegacyOwningKey>(context);
    auto upperBound = Load<TLegacyOwningKey>(context);
    ReadRange_ = MakeSingletonRowRange(lowerBound, upperBound);
}

TChunkStatePtr TSortedChunkStore::PrepareChunkState(
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkMeta = GetCachedVersionedChunkMeta(chunkReader, chunkReadOptions);

    return New<TChunkState>(
        BlockCache_,
        std::move(chunkSpec),
        std::move(chunkMeta),
        ChunkTimestamp_,
        nullptr,
        PerformanceCounters_,
        GetKeyComparer(),
        nullptr,
        Schema_);
}

void TSortedChunkStore::ValidateBlockSize(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TChunkStatePtr& chunkState,
    const TWorkloadDescriptor& workloadDescriptor)
{
    if ((workloadDescriptor.Category == EWorkloadCategory::UserInteractive ||
        workloadDescriptor.Category == EWorkloadCategory::UserRealtime) &&
        (chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableSchemalessHorizontal ||
        chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableUnversionedColumnar))
    {
        // For unversioned chunks verify that block size is correct.
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
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

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore()
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TLegacyKey> FilterKeysByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<TLegacyKey>& keys,
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

