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
#include <yt/yt/ytlib/chunk_client/cache_reader.h>
#include <yt/yt/ytlib/chunk_client/block_tracking_chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/ref_counted_proto.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/cache_based_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_column_mapping.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/versioned_offloading_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_reader_adapter.h>

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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

bool IsNewScanReaderEnabled(const TTableMountConfigPtr& mountConfig)
{
    return mountConfig->EnableNewScanReaderForLookup || mountConfig->EnableNewScanReaderForSelect;
}

////////////////////////////////////////////////////////////////////////////////

class TFilteringReader
    : public IVersionedReader
{
public:
    TFilteringReader(
        IVersionedReaderPtr underlyingReader,
        int skipBefore,
        int skipAfter)
        : UnderlyingReader_(std::move(underlyingReader))
        , SkipBefore_(skipBefore)
        , SkipAfter_(skipAfter)
    { }

    TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (SkipBefore_ > 0) {
            return VoidFuture;
        }

        if (!SkippingAfter_) {
            return UnderlyingReader_->GetReadyEvent();
        }

        return VoidFuture;
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (SkipBefore_ > 0) {
            return MakeSentinelRowset(options.MaxRowsPerRead, &SkipBefore_);
        }

        if (!SkippingAfter_) {
            if (auto result = UnderlyingReader_->Read(options)) {
                return result;
            }
            SkippingAfter_ = true;
        }

        if (SkipAfter_ > 0) {
            return MakeSentinelRowset(options.MaxRowsPerRead, &SkipAfter_);
        }

        return nullptr;
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;

    int SkipBefore_;
    int SkipAfter_;
    bool SkippingAfter_ = false;

    static IVersionedRowBatchPtr MakeSentinelRowset(i64 maxRowsPerRead, int* counter)
    {
        std::vector<TVersionedRow> rows;
        int rowCount = std::min<i64>(maxRowsPerRead, *counter);
        rows.reserve(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(TVersionedRow());
        }

        *counter -= rowCount;

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows)));
    }
};

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    TStoreId id,
    NChunkClient::TChunkId chunkId,
    const NChunkClient::TLegacyReadRange& readRange,
    TTimestamp overrideTimestamp,
    TTimestamp maxClipTimestamp,
    TTablet* tablet,
    const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
    IBlockCachePtr blockCache,
    IVersionedChunkMetaManagerPtr chunkMetaManager,
    IBackendChunkReadersHolderPtr backendReadersHolder)
    : TChunkStoreBase(
        config,
        id,
        chunkId,
        overrideTimestamp,
        tablet,
        addStoreDescriptor,
        blockCache,
        chunkMetaManager,
        std::move(backendReadersHolder))
    , KeyComparer_(tablet->GetRowKeyComparer().UUComparer)
    , MaxClipTimestamp_(maxClipTimestamp)
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
        .Item("upper_bound_key").Value(GetUpperBoundKey())
        .Item("max_clip_timestamp").Value(MaxClipTimestamp_);
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

    if (MaxClipTimestamp_) {
        timestamp = std::min(timestamp, MaxClipTimestamp_);
    }

    ranges = FilterRowRangesByReadRange(ranges);

    // Fast lane:
    // - ranges do not intersect with chunk view;
    // - chunk timestamp is greater than requested timestamp.
    if (ranges.Empty() || (OverrideTimestamp_ && OverrideTimestamp_ > timestamp))
    {
        return CreateEmptyVersionedReader();
    }

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    bool enableNewScanReader = IsNewScanReaderEnabled(mountConfig);

    // Fast lane: check for in-memory reads.
    if (auto reader = TryCreateCacheBasedReader(
        ranges,
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions,
        ReadRange_,
        enableNewScanReader))
    {
        return MaybeWrapWithTimestampResettingAdapter(std::move(reader));
    }

    // Another fast lane: check for backing store.
    if (auto backingStore = GetSortedBackingStore()) {
        YT_VERIFY(!HasNontrivialReadRange());
        YT_VERIFY(!OverrideTimestamp_);
        return backingStore->CreateReader(
            tabletSnapshot,
            ranges,
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            /*workloadCategory*/ std::nullopt);
    }

    auto backendReaders = GetBackendReaders(workloadCategory);
    auto chunkReader = backendReaders.ChunkReader;

    if (chunkReadOptions.MemoryReferenceTracker) {
        chunkReader = CreateBlockTrackingChunkReader(
            chunkReader,
            chunkReadOptions.MemoryReferenceTracker);
    }

    auto chunkState = PrepareChunkState(
        std::move(chunkReader),
        chunkReadOptions,
        enableNewScanReader);

    const auto& chunkMeta = chunkState->ChunkMeta;

    ValidateBlockSize(tabletSnapshot, chunkState, chunkReadOptions.WorkloadDescriptor);

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NNewTableClient::ClipRanges(
            ranges,
            ReadRange_.Size() > 0 ? ReadRange_.Front().first : TUnversionedRow(),
            ReadRange_.Size() > 0 ? ReadRange_.Front().second : TUnversionedRow(),
            ReadRange_.GetHolder());

        return MaybeWrapWithTimestampResettingAdapter(NNewTableClient::CreateVersionedChunkReader(
            std::move(ranges),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            chunkState->BlockCache,
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            chunkState->PerformanceCounters,
            chunkReadOptions,
            produceAllVersions,
            nullptr,
            GetCurrentInvoker()));
    }

    // Reader can handle chunk timestamp itself if needed, no need to wrap with
    // timestamp resetting adapter.
    return CreateVersionedChunkReader(
        std::move(backendReaders.ReaderConfig),
        std::move(backendReaders.ChunkReader),
        chunkState,
        chunkMeta,
        chunkReadOptions,
        std::move(ranges),
        columnFilter,
        timestamp,
        produceAllVersions,
        ReadRange_,
        nullptr,
        GetCurrentInvoker());
}

IVersionedReaderPtr TSortedChunkStore::TryCreateCacheBasedReader(
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TRowRange>& singletonClippingRange,
    bool enableNewScanReader)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto chunkState = FindPreloadedChunkState();
    if (!chunkState) {
        return nullptr;
    }

    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NNewTableClient::ClipRanges(
            ranges,
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().first : TUnversionedRow(),
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().second : TUnversionedRow(),
            singletonClippingRange.GetHolder());

        auto chunkReader = CreateCacheReader(
            ChunkId_,
            chunkState->BlockCache);

        if (chunkReadOptions.MemoryReferenceTracker) {
            chunkReader = CreateBlockTrackingChunkReader(
                chunkReader,
                chunkReadOptions.MemoryReferenceTracker);
        }

        return NNewTableClient::CreateVersionedChunkReader(
            std::move(ranges),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            chunkState->BlockCache,
            GetReaderConfig(),
            chunkReader,
            chunkState->PerformanceCounters,
            chunkReadOptions,
            produceAllVersions,
            nullptr,
            GetCurrentInvoker());
    }

    return CreateCacheBasedVersionedChunkReader(
        ChunkId_,
        chunkState,
        chunkState->ChunkMeta,
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

    if (MaxClipTimestamp_) {
        timestamp = std::min(timestamp, MaxClipTimestamp_);
    }

    if (OverrideTimestamp_ && OverrideTimestamp_ > timestamp) {
        return CreateEmptyVersionedReader(keys.Size());
    }

    int skippedBefore = 0;
    int skippedAfter = 0;
    auto filteredKeys = FilterKeysByReadRange(keys, &skippedBefore, &skippedAfter);

    if (filteredKeys.Empty()) {
        return CreateEmptyVersionedReader(keys.Size());
    }

    auto wrapReader = [&] (
        IVersionedReaderPtr underlyingReader,
        bool needSetTimestamp) -> IVersionedReaderPtr
    {
        if (skippedBefore > 0 || skippedAfter > 0) {
            underlyingReader = New<TFilteringReader>(
                std::move(underlyingReader),
                skippedBefore,
                skippedAfter);
        }
        if (needSetTimestamp) {
            return MaybeWrapWithTimestampResettingAdapter(std::move(underlyingReader));
        } else {
            return underlyingReader;
        }
    };

    const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
    bool enableNewScanReader = IsNewScanReaderEnabled(mountConfig);

    // Fast lane: check for in-memory reads.
    if (auto reader = TryCreateCacheBasedReader(
        filteredKeys,
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions,
        enableNewScanReader))
    {
        return wrapReader(std::move(reader), /*needSetTimestamp*/ true);
    }

    // Another fast lane: check for backing store.
    if (auto backingStore = GetSortedBackingStore()) {
        YT_VERIFY(!HasNontrivialReadRange());
        YT_VERIFY(!OverrideTimestamp_);
        return backingStore->CreateReader(
            std::move(tabletSnapshot),
            filteredKeys,
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            /*workloadCategory*/ std::nullopt);
    }

    auto backendReaders = GetBackendReaders(workloadCategory);

    if (mountConfig->EnableDataNodeLookup && backendReaders.OffloadingReader) {
        auto options = New<TOffloadingReaderOptions>(TOffloadingReaderOptions{
            .ChunkReadOptions = chunkReadOptions,
            .TableId = tabletSnapshot->TableId,
            .MountRevision = tabletSnapshot->MountRevision,
            .TableSchema = tabletSnapshot->TableSchema,
            .ColumnFilter = columnFilter,
            .Timestamp = timestamp,
            .ProduceAllVersions = produceAllVersions,
            .OverrideTimestamp = OverrideTimestamp_,
            .EnableHashChunkIndex = mountConfig->EnableHashChunkIndexForLookup
        });
        return wrapReader(
            CreateVersionedOffloadingLookupReader(
                std::move(backendReaders.OffloadingReader),
                std::move(options),
                filteredKeys),
            /*needSetTimestamp*/ true);
    }

    auto chunkState = PrepareChunkState(
        backendReaders.ChunkReader,
        chunkReadOptions,
        enableNewScanReader);
    const auto& chunkMeta = chunkState->ChunkMeta;

    ValidateBlockSize(tabletSnapshot, chunkState, chunkReadOptions.WorkloadDescriptor);

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        auto reader = NNewTableClient::CreateVersionedChunkReader(
            filteredKeys,
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            BlockCache_,
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            PerformanceCounters_,
            chunkReadOptions,
            produceAllVersions);
        return wrapReader(std::move(reader), /*needSetTimestamp*/ true);
    }

    auto reader = CreateVersionedChunkReader(
        std::move(backendReaders.ReaderConfig),
        std::move(backendReaders.ChunkReader),
        chunkState,
        chunkMeta,
        chunkReadOptions,
        filteredKeys,
        columnFilter,
        timestamp,
        produceAllVersions);

    // Reader can handle chunk timestamp itself if needed, no need to wrap with
    // timestamp resetting adapter.
    return wrapReader(std::move(reader), /*needSetTimestamp*/ false);
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
    const TClientChunkReadOptions& chunkReadOptions,
    bool enableNewScanReader)
{
    auto chunkState = FindPreloadedChunkState();
    if (!chunkState) {
        return nullptr;
    }

    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        auto chunkReader = CreateCacheReader(
            ChunkId_,
            chunkState->BlockCache);

        if (chunkReadOptions.MemoryReferenceTracker) {
            chunkReader = CreateBlockTrackingChunkReader(
                chunkReader,
                chunkReadOptions.MemoryReferenceTracker);
        }

        return NNewTableClient::CreateVersionedChunkReader(
            std::move(keys),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            chunkState->BlockCache,
            GetReaderConfig(),
            chunkReader,
            chunkState->PerformanceCounters,
            chunkReadOptions,
            produceAllVersions,
            nullptr,
            GetCurrentInvoker());
    }

    return CreateCacheBasedVersionedChunkReader(
        ChunkId_,
        chunkState,
        chunkState->ChunkMeta,
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
        NTabletClient::EErrorCode::CannotCheckConflictsAgainstChunkStore,
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
    Save(context, MaxClipTimestamp_);
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

    Load(context, MaxClipTimestamp_);
}

IVersionedReaderPtr TSortedChunkStore::MaybeWrapWithTimestampResettingAdapter(
    IVersionedReaderPtr underlyingReader) const
{
    if (OverrideTimestamp_) {
        return CreateTimestampResettingAdapter(
            std::move(underlyingReader),
            OverrideTimestamp_);
    } else {
        return underlyingReader;
    }
}

NTableClient::TChunkColumnMappingPtr TSortedChunkStore::GetChunkColumnMapping(
    const NTableClient::TTableSchemaPtr& tableSchema,
    const NTableClient::TTableSchemaPtr& chunkSchema)
{
    // Fast lane.
    {
        auto guard = ReaderGuard(ChunkColumnMappingLock_);
        if (ChunkColumnMapping_) {
            return ChunkColumnMapping_;
        }
    }

    auto chunkColumnMapping = New<TChunkColumnMapping>(tableSchema, chunkSchema);

    {
        auto guard = WriterGuard(ChunkColumnMappingLock_);
        ChunkColumnMapping_ = chunkColumnMapping;
    }

    return chunkColumnMapping;
}

TChunkStatePtr TSortedChunkStore::PrepareChunkState(
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    bool prepareColumnarMeta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkMeta = GetCachedVersionedChunkMeta(chunkReader, chunkReadOptions, prepareColumnarMeta);

    auto chunkState = New<TChunkState>(
        BlockCache_,
        std::move(chunkSpec),
        chunkMeta,
        OverrideTimestamp_,
        /*lookupHashTable*/ nullptr,
        PerformanceCounters_,
        GetKeyComparer(),
        /*virtualValueDirectory*/ nullptr,
        Schema_,
        GetChunkColumnMapping(Schema_, chunkMeta->GetChunkSchema()));

    return chunkState;
}

void TSortedChunkStore::ValidateBlockSize(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TChunkStatePtr& chunkState,
    const TWorkloadDescriptor& workloadDescriptor)
{
    auto chunkMeta = chunkState->ChunkMeta;
    auto chunkFormat = chunkMeta->GetChunkFormat();

    if ((workloadDescriptor.Category == EWorkloadCategory::UserInteractive ||
        workloadDescriptor.Category == EWorkloadCategory::UserRealtime) &&
        (chunkFormat == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        chunkFormat == EChunkFormat::TableUnversionedColumnar))
    {
        // For unversioned chunks verify that block size is correct.
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
            auto miscExt = FindProtoExtension<TMiscExt>(chunkState->ChunkSpec.chunk_meta().extensions());
            if (miscExt && miscExt->max_data_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("chunk_id", GetId())
                    << TErrorAttribute("block_size", miscExt->max_data_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }
}

TKeyComparer TSortedChunkStore::GetKeyComparer() const
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

