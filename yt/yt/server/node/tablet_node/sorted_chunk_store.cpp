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
#include <yt/yt/ytlib/table_client/chunk_index_read_controller.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/indexed_versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/key_filter.h>
#include <yt/yt/ytlib/table_client/performance_counters.h>
#include <yt/yt/ytlib/table_client/versioned_offloading_reader.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/versioned_reader_adapter.h>

#include <yt/yt/ytlib/columnar_chunk_format/versioned_chunk_reader.h>

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

#include <library/cpp/iterator/enumerate.h>

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

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

bool IsNewScanReaderEnabled(const TTableMountConfigPtr& mountConfig)
{
    return mountConfig->EnableNewScanReaderForLookup || mountConfig->EnableNewScanReaderForSelect;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TCompactionHints& compactionHints,
    NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_view_size").Value(compactionHints.ChunkViewSize)
            .Item("row_digest").Value(compactionHints.RowDigest)
        .EndMap();
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

    std::vector<TChunkId> GetFailedChunkIds() const override
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

class TKeyFilteringReader
    : public IVersionedReader
{
public:
    TKeyFilteringReader(
        IVersionedReaderPtr underlyingReader,
        std::vector<ui8> missingKeyMask,
        TKeyFilterStatisticsPtr keyFilterStatistics)
        : UnderlyingReader_(std::move(underlyingReader))
        , MissingKeyMask_(std::move(missingKeyMask))
        , KeyFilterStatistics_(std::move(keyFilterStatistics))
        , ReadRowCount_(0)
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
        return UnderlyingReader_->GetReadyEvent();
    }

    bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (ReadRowCount_ == ssize(MissingKeyMask_)) {
            return nullptr;
        }

        int batchEndIndex = ReadRowCount_ + std::min(options.MaxRowsPerRead, ssize(MissingKeyMask_) - ReadRowCount_);
        int neededUnderlyingRowCount = std::count(
            MissingKeyMask_.begin() + ReadRowCount_,
            MissingKeyMask_.begin() + batchEndIndex,
            0);

        auto underlyingOptions = options;
        underlyingOptions.MaxRowsPerRead = neededUnderlyingRowCount;

        auto underlyingBatch = neededUnderlyingRowCount > 0
            ? UnderlyingReader_->Read(underlyingOptions)
            : nullptr;

        auto underlyingRows = underlyingBatch
            ? underlyingBatch->MaterializeRows()
            : TSharedRange<TVersionedRow>();

        int underlyingRowIndex = 0;
        std::vector<TVersionedRow> result;

        i64 falsePositiveCount = 0;
        while (ReadRowCount_ < batchEndIndex) {
            if (MissingKeyMask_[ReadRowCount_]) {
                result.emplace_back();
                ++ReadRowCount_;
            } else if (underlyingRows && underlyingRowIndex < std::ssize(underlyingRows)) {
                result.push_back(underlyingRows[underlyingRowIndex++]);
                ++ReadRowCount_;
                falsePositiveCount += !static_cast<bool>(result.back());
            } else {
                break;
            }
        }

        if (KeyFilterStatistics_) {
            KeyFilterStatistics_->FalsePositiveEntryCount.fetch_add(falsePositiveCount, std::memory_order::relaxed);
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(result)));
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;
    const std::vector<ui8> MissingKeyMask_;
    const TKeyFilterStatisticsPtr KeyFilterStatistics_;

    i64 ReadRowCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSortedChunkStore::TSortedChunkStore(
    TTabletManagerConfigPtr config,
    TStoreId id,
    TChunkId chunkId,
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
        .Item("max_clip_timestamp").Value(MaxClipTimestamp_)
        .Item("compaction_hints").Value(CompactionHints_);
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

std::optional<NChunkClient::TReadLimit> TSortedChunkStore::GetChunkViewLowerLimit() const
{
    if (ReadRange_.Front().first) {
        return NChunkClient::TReadLimit(KeyBoundFromLegacyRow(MinKey_, /*isUpper*/ false, KeyColumnCount_));
    }
    return std::nullopt;
}

std::optional<NChunkClient::TReadLimit> TSortedChunkStore::GetChunkViewUpperLimit() const
{
    if (ReadRange_.Front().second) {
        return NChunkClient::TReadLimit(KeyBoundFromLegacyRow(UpperBoundKey_, /*isUpper*/ true, KeyColumnCount_));
    }
    return std::nullopt;
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

    auto wrapReaderWithPerformanceCounting = [&] (IVersionedReaderPtr underlyingReader)
    {
        return CreateVersionedPerformanceCountingReader(
            underlyingReader,
            PerformanceCounters_,
            NTableClient::EDataSource::ChunkStore,
            ERequestType::Read);
    };

    // Fast lane: check for in-memory reads.
    if (auto chunkState = FindPreloadedChunkState()) {
        return wrapReaderWithPerformanceCounting(
            MaybeWrapWithTimestampResettingAdapter(
                CreateCacheBasedReader(
                    chunkState,
                    ranges,
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    ReadRange_,
                    enableNewScanReader)));
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

    auto chunkMeta = FindCachedVersionedChunkMeta(enableNewScanReader);
    if (!chunkMeta) {
        chunkMeta = WaitForFast(GetCachedVersionedChunkMeta(
            backendReaders.ChunkReader,
            chunkReadOptions,
            enableNewScanReader))
            .ValueOrThrow();
    }

    auto chunkState = PrepareChunkState(std::move(chunkMeta));

    ValidateBlockSize(tabletSnapshot, chunkState->ChunkMeta, chunkReadOptions.WorkloadDescriptor);

    bool keyFilterUsed = false;
    ranges = MaybePerformXorRangeFiltering(
        tabletSnapshot,
        chunkReader,
        chunkReadOptions,
        chunkState,
        std::move(ranges),
        &keyFilterUsed);

    auto keyFilterStatistics = keyFilterUsed ? chunkReadOptions.KeyFilterStatistics : nullptr;

    if (enableNewScanReader && chunkState->ChunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NColumnarChunkFormat::ClipRanges(
            ranges,
            ReadRange_.Size() > 0 ? ReadRange_.Front().first : TUnversionedRow(),
            ReadRange_.Size() > 0 ? ReadRange_.Front().second : TUnversionedRow(),
            ReadRange_.GetHolder());

        auto blockManagerFactory = NColumnarChunkFormat::CreateAsyncBlockWindowManagerFactory(
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            chunkState->BlockCache,
            chunkReadOptions,
            chunkState->ChunkMeta,
            // Enable current invoker for range reads.
            GetCurrentInvoker());

        return wrapReaderWithPerformanceCounting(
            MaybeWrapWithTimestampResettingAdapter(
                NColumnarChunkFormat::CreateVersionedChunkReader(
                    std::move(ranges),
                    timestamp,
                    chunkState->ChunkMeta,
                    Schema_,
                    columnFilter,
                    chunkState->ChunkColumnMapping,
                    blockManagerFactory,
                    produceAllVersions,
                    /*readerStatistics*/ nullptr,
                    std::move(keyFilterStatistics))));
    }

    // Reader can handle chunk timestamp itself if needed, no need to wrap with
    // timestamp resetting adapter.
    return wrapReaderWithPerformanceCounting(
        CreateVersionedChunkReader(
            std::move(backendReaders.ReaderConfig),
            std::move(backendReaders.ChunkReader),
            chunkState,
            chunkState->ChunkMeta,
            chunkReadOptions,
            std::move(ranges),
            columnFilter,
            timestamp,
            produceAllVersions,
            ReadRange_,
            nullptr,
            GetCurrentInvoker(),
            std::move(keyFilterStatistics)));
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TChunkStatePtr& chunkState,
    TSharedRange<TRowRange> ranges,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TRowRange>& singletonClippingRange,
    bool enableNewScanReader) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        // Chunk view support.
        ranges = NColumnarChunkFormat::ClipRanges(
            ranges,
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().first : TUnversionedRow(),
            singletonClippingRange.Size() > 0 ? singletonClippingRange.Front().second : TUnversionedRow(),
            singletonClippingRange.GetHolder());

        auto blockManagerFactory = NColumnarChunkFormat::CreateSyncBlockWindowManagerFactory(
            chunkState->BlockCache,
            chunkMeta,
            ChunkId_);

        return NColumnarChunkFormat::CreateVersionedChunkReader(
            std::move(ranges),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            blockManagerFactory,
            produceAllVersions,
            /*readerStatistics*/ nullptr,
            chunkReadOptions.KeyFilterStatistics);
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

class TSortedChunkStore::TSortedChunkStoreVersionedReader
    : public IVersionedReader
{
public:
    TSortedChunkStoreVersionedReader(
        int skippedBefore,
        int skippedAfter,
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory)
        : SkippedBefore_(skippedBefore)
        , SkippedAfter_(skippedAfter)
    {
        InitializationFuture_ = InitializeUnderlyingReader(
            chunk,
            tabletSnapshot,
            std::move(keys),
            timestamp,
            produceAllVersions,
            columnFilter,
            chunkReadOptions,
            workloadCategory);
    }

    TDataStatistics GetDataStatistics() const override
    {
        if (!UnderlyingReaderInitialized_.load()) {
            return {};
        }
        return UnderlyingReader_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        if (!UnderlyingReaderInitialized_.load()) {
            return {};
        }
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    TFuture<void> Open() override
    {
        if (UnderlyingReaderInitialized_.load()) {
            // InitializationFuture_ may actually be unset yet.
            return UnderlyingReader_->Open();
        }

        return InitializationFuture_.Apply(BIND([=, this, this_ = MakeStrong(this)] {
            YT_VERIFY(UnderlyingReaderInitialized_.load());
            return UnderlyingReader_->Open();
        })
            .AsyncVia(GetCurrentInvoker()));
    }

    TFuture<void> GetReadyEvent() const override
    {
        YT_VERIFY(UnderlyingReaderInitialized_.load());
        return UnderlyingReader_->GetReadyEvent();
    }

    bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(UnderlyingReaderInitialized_.load());
        return UnderlyingReader_->Read(options);
    }

private:
    const int SkippedBefore_;
    const int SkippedAfter_;

    TFuture<void> InitializationFuture_;
    std::atomic<bool> UnderlyingReaderInitialized_ = false;
    IVersionedReaderPtr UnderlyingReader_;

    std::vector<ui8> MissingKeyMask_;


    TFuture<void> InitializeUnderlyingReader(
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory)
    {
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        bool enableNewScanReader = IsNewScanReaderEnabled(mountConfig);

        // Check for in-memory reads.
        if (auto chunkState = chunk->FindPreloadedChunkState()) {
            MaybeWrapUnderlyingReader(
                chunk,
                chunk->CreateCacheBasedReader(
                    chunkState,
                    std::move(keys),
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    enableNewScanReader),
                chunkReadOptions);
            return VoidFuture;
        }

        // Check for backing store.
        if (auto backingStore = chunk->GetSortedBackingStore()) {
            YT_VERIFY(!chunk->HasNontrivialReadRange());
            YT_VERIFY(!chunk->OverrideTimestamp_);
            UnderlyingReader_ = backingStore->CreateReader(
                std::move(tabletSnapshot),
                std::move(keys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                /*workloadCategory*/ std::nullopt);
            UnderlyingReaderInitialized_.store(true);
            return VoidFuture;
        }

        auto backendReaders = chunk->GetBackendReaders(workloadCategory);

        if (mountConfig->EnableDataNodeLookup && backendReaders.OffloadingReader) {
            auto options = New<TOffloadingReaderOptions>(TOffloadingReaderOptions{
                .ChunkReadOptions = chunkReadOptions,
                .TableId = tabletSnapshot->TableId,
                .MountRevision = tabletSnapshot->MountRevision,
                .TableSchema = tabletSnapshot->TableSchema,
                .ColumnFilter = columnFilter,
                .Timestamp = timestamp,
                .ProduceAllVersions = produceAllVersions,
                .OverrideTimestamp = chunk->OverrideTimestamp_,
                .EnableHashChunkIndex = mountConfig->EnableHashChunkIndexForLookup
            });
            MaybeWrapUnderlyingReader(
                chunk,
                CreateVersionedOffloadingLookupReader(
                    std::move(backendReaders.OffloadingReader),
                    std::move(options),
                    std::move(keys)),
                chunkReadOptions);
            return VoidFuture;
        }

        if (auto chunkMeta = chunk->FindCachedVersionedChunkMeta(enableNewScanReader)) {
            return OnGotChunkMeta(
                chunk,
                tabletSnapshot,
                std::move(keys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                std::move(backendReaders),
                std::move(chunkMeta));
        } else {
            return chunk->GetCachedVersionedChunkMeta(
                backendReaders.ChunkReader,
                chunkReadOptions,
                enableNewScanReader)
                .ApplyUnique(BIND([
                    =,
                    this,
                    this_ = MakeStrong(this),
                    // NB: Here as we initiate asynchronous execution we need to ref chunk.
                    chunk = MakeStrong(chunk),
                    keys = std::move(keys),
                    backendReaders = std::move(backendReaders)
                ] (TCachedVersionedChunkMetaPtr&& chunkMeta) mutable {
                    return OnGotChunkMeta(
                        chunk.Get(),
                        tabletSnapshot,
                        std::move(keys),
                        timestamp,
                        produceAllVersions,
                        // FIXME(akozhikhov): Heavy copy here.
                        columnFilter,
                        chunkReadOptions,
                        std::move(backendReaders),
                        std::move(chunkMeta));
                })
                .AsyncVia(GetCurrentInvoker()));
        }
    }

    TFuture<void> OnGotChunkMeta(
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        TBackendReaders backendReaders,
        TCachedVersionedChunkMetaPtr chunkMeta)
    {
        TFuture<TKeyFilteringResult> filteringResultFuture;
        if (tabletSnapshot->Settings.MountConfig->EnableKeyFilterForLookup) {
            if (auto* xorFilterMeta = chunkMeta->FindXorFilterByLength(keys[0].GetCount())) {
                filteringResultFuture = chunk->PerformXorKeyFiltering(
                    chunkMeta,
                    backendReaders.ChunkReader,
                    chunkReadOptions,
                    *xorFilterMeta,
                    std::move(keys));
            }
        }

        TSharedRange<TLegacyKey> filteredKeys;
        if (!filteringResultFuture) {
            filteredKeys = std::move(keys);
        } else if (filteringResultFuture.IsSet() && filteringResultFuture.Get().IsOK()) {
            auto filteringResult = std::move(filteringResultFuture.Get().Value());
            filteredKeys = std::move(filteringResult.FilteredKeys);
            MissingKeyMask_ = std::move(filteringResult.MissingKeyMask);
        }

        if (filteredKeys) {
            OnKeysFiltered(
                chunk,
                tabletSnapshot,
                std::move(filteredKeys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                std::move(backendReaders),
                std::move(chunkMeta));
            return VoidFuture;
        }

        return filteringResultFuture.ApplyUnique(BIND([
            =,
            this,
            this_ = MakeStrong(this),
            // NB: Here as we initiate asynchronous execution we need to ref chunk.
            chunk = MakeStrong(chunk),
            backendReaders = std::move(backendReaders),
            chunkMeta = std::move(chunkMeta)
        ] (TKeyFilteringResult&& filteringResult) mutable {
            MissingKeyMask_ = std::move(filteringResult.MissingKeyMask);
            OnKeysFiltered(
                chunk.Get(),
                tabletSnapshot,
                std::move(filteringResult.FilteredKeys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                std::move(backendReaders),
                std::move(chunkMeta));
        }));
        // NB: Do not append AsyncVia here because PerformXorKeyFiltering already does.
    }

    void OnKeysFiltered(
        TSortedChunkStore* const chunk,
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        TBackendReaders backendReaders,
        TCachedVersionedChunkMetaPtr chunkMeta)
    {
        i64 filteredOutKeyCount = std::count(MissingKeyMask_.begin(), MissingKeyMask_.end(), 1);

        if (const auto& keyFilterStatistics = chunkReadOptions.KeyFilterStatistics) {
            keyFilterStatistics->InputEntryCount.fetch_add(ssize(MissingKeyMask_), std::memory_order::relaxed);
            keyFilterStatistics->FilteredOutEntryCount.fetch_add(filteredOutKeyCount, std::memory_order::relaxed);
        }

        if (!MissingKeyMask_.empty() && filteredOutKeyCount == ssize(MissingKeyMask_)) {
            int initialKeyCount = SkippedBefore_ + SkippedAfter_ + std::ssize(MissingKeyMask_);
            UnderlyingReader_ = CreateEmptyVersionedReader(initialKeyCount);
            UnderlyingReaderInitialized_.store(true);
            return;
        }

        chunk->ValidateBlockSize(tabletSnapshot, chunkMeta, chunkReadOptions.WorkloadDescriptor);

        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

        if (mountConfig->EnableHashChunkIndexForLookup && chunkMeta->HashTableChunkIndexMeta()) {
            auto controller = CreateChunkIndexReadController(
                chunk->ChunkId_,
                columnFilter,
                std::move(chunkMeta),
                std::move(keys),
                chunk->GetKeyComparer(),
                tabletSnapshot->TableSchema,
                timestamp,
                produceAllVersions,
                chunk->BlockCache_,
                /*testingOptions*/ std::nullopt,
                TabletNodeLogger);

            MaybeWrapUnderlyingReader(
                chunk,
                CreateIndexedVersionedChunkReader(
                    std::move(chunkReadOptions),
                    std::move(controller),
                    std::move(backendReaders.ChunkReader),
                    tabletSnapshot->ChunkFragmentReader),
                chunkReadOptions);
            return;
        }

        auto chunkState = chunk->PrepareChunkState(chunkMeta);

        if (IsNewScanReaderEnabled(mountConfig) &&
            chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar)
        {
            auto blockManagerFactory = NColumnarChunkFormat::CreateAsyncBlockWindowManagerFactory(
                std::move(backendReaders.ReaderConfig),
                std::move(backendReaders.ChunkReader),
                chunkState->BlockCache,
                std::move(chunkReadOptions),
                chunkMeta);

            MaybeWrapUnderlyingReader(
                chunk,
                NColumnarChunkFormat::CreateVersionedChunkReader(
                    std::move(keys),
                    timestamp,
                    std::move(chunkMeta),
                    chunkState->TableSchema,
                    std::move(columnFilter),
                    chunkState->ChunkColumnMapping,
                    std::move(blockManagerFactory),
                    produceAllVersions,
                    /*readerStatistics*/ nullptr,
                    MissingKeyMask_.empty() ? nullptr : chunkReadOptions.KeyFilterStatistics),
                chunkReadOptions);
            return;
        }

        MaybeWrapUnderlyingReader(
            chunk,
            CreateVersionedChunkReader(
                std::move(backendReaders.ReaderConfig),
                std::move(backendReaders.ChunkReader),
                std::move(chunkState),
                std::move(chunkMeta),
                chunkReadOptions,
                std::move(keys),
                columnFilter,
                timestamp,
                produceAllVersions),
            chunkReadOptions,
            /*needSetTimestamp*/ false);
        return;
    }

    void MaybeWrapUnderlyingReader(
        TSortedChunkStore* const chunk,
        IVersionedReaderPtr underlyingReader,
        const TClientChunkReadOptions& chunkReaderOptions,
        bool needSetTimestamp = true)
    {
        // TODO(akozhikhov): Avoid extra wrappers TKeyFilteringReader and TFilteringReader,
        // implement their logic in this reader.
        if (!MissingKeyMask_.empty()) {
            underlyingReader = New<TKeyFilteringReader>(
                std::move(underlyingReader),
                MissingKeyMask_,
                chunkReaderOptions.KeyFilterStatistics);
        }

        if (SkippedBefore_ > 0 || SkippedAfter_ > 0) {
            underlyingReader = New<TFilteringReader>(
                std::move(underlyingReader),
                SkippedBefore_,
                SkippedAfter_);
        }

        if (needSetTimestamp && chunk->OverrideTimestamp_) {
            underlyingReader = CreateTimestampResettingAdapter(
                std::move(underlyingReader),
                chunk->OverrideTimestamp_);
        }

        UnderlyingReader_ = CreateVersionedPerformanceCountingReader(
            std::move(underlyingReader),
            chunk->PerformanceCounters_,
            NTableClient::EDataSource::ChunkStore,
            ERequestType::Lookup);
        UnderlyingReaderInitialized_.store(true);
    }
};

IVersionedReaderPtr TSortedChunkStore::CreateReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    TSharedRange<TLegacyKey> keys,
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

    int initialKeyCount = keys.Size();

    if (OverrideTimestamp_ && OverrideTimestamp_ > timestamp) {
        return CreateEmptyVersionedReader(initialKeyCount);
    }

    int skippedBefore = 0;
    int skippedAfter = 0;
    auto filteredKeys = FilterKeysByReadRange(std::move(keys), &skippedBefore, &skippedAfter);
    YT_VERIFY(skippedBefore + skippedAfter + std::ssize(filteredKeys) == initialKeyCount);

    if (filteredKeys.Empty()) {
        return CreateEmptyVersionedReader(initialKeyCount);
    }

    // NB: This is a fast path for in-memory readers to avoid extra wrapper.
    // We could do the same for ext-memory readers but there is no need.
    bool needKeyRangeFiltering = skippedBefore > 0 || skippedAfter > 0;
    bool needTimestampResetting = OverrideTimestamp_;
    if (!needKeyRangeFiltering && !needTimestampResetting) {
        // Check for in-memory reads.
        if (auto chunkState = FindPreloadedChunkState()) {
            return CreateVersionedPerformanceCountingReader(
                CreateCacheBasedReader(
                    chunkState,
                    std::move(filteredKeys),
                    timestamp,
                    produceAllVersions,
                    columnFilter,
                    chunkReadOptions,
                    IsNewScanReaderEnabled(tabletSnapshot->Settings.MountConfig)),
                PerformanceCounters_,
                NTableClient::EDataSource::ChunkStore,
                ERequestType::Lookup);
        }

        // Check for backing store.
        if (auto backingStore = GetSortedBackingStore()) {
            YT_VERIFY(!HasNontrivialReadRange());
            YT_VERIFY(!OverrideTimestamp_);
            // NB: Performance counting reader is created within this call.
            return backingStore->CreateReader(
                std::move(tabletSnapshot),
                std::move(filteredKeys),
                timestamp,
                produceAllVersions,
                columnFilter,
                chunkReadOptions,
                /*workloadCategory*/ std::nullopt);
        }
    }

    return New<TSortedChunkStoreVersionedReader>(
        skippedBefore,
        skippedAfter,
        this,
        tabletSnapshot,
        std::move(filteredKeys),
        timestamp,
        produceAllVersions,
        columnFilter,
        chunkReadOptions,
        workloadCategory);
}

TSharedRange<TLegacyKey> TSortedChunkStore::FilterKeysByReadRange(
    TSharedRange<TLegacyKey> keys,
    int* skippedBefore,
    int* skippedAfter) const
{
    return NTabletNode::FilterKeysByReadRange(ReadRange_.Front(), std::move(keys), skippedBefore, skippedAfter);
}

TSharedRange<TRowRange> TSortedChunkStore::FilterRowRangesByReadRange(
    const TSharedRange<TRowRange>& ranges) const
{
    return NTabletNode::FilterRowRangesByReadRange(ReadRange_.Front(), ranges);
}

IVersionedReaderPtr TSortedChunkStore::CreateCacheBasedReader(
    const TChunkStatePtr& chunkState,
    TSharedRange<TLegacyKey> keys,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TColumnFilter& columnFilter,
    const TClientChunkReadOptions& chunkReadOptions,
    bool enableNewScanReader) const
{
    const auto& chunkMeta = chunkState->ChunkMeta;

    if (enableNewScanReader && chunkMeta->GetChunkFormat() == EChunkFormat::TableVersionedColumnar) {
        auto blockManagerFactory = NColumnarChunkFormat::CreateSyncBlockWindowManagerFactory(
            chunkState->BlockCache,
            chunkMeta,
            ChunkId_);

        if (InMemoryMode_ == NTabletClient::EInMemoryMode::Uncompressed) {
            if (auto* lookupHashTable = chunkState->LookupHashTable.Get()) {
                auto keysWithHints = NColumnarChunkFormat::BuildKeyHintsUsingLookupTable(
                    *lookupHashTable,
                    std::move(keys));

                return NColumnarChunkFormat::CreateVersionedChunkReader(
                    std::move(keysWithHints),
                    timestamp,
                    chunkMeta,
                    Schema_,
                    columnFilter,
                    chunkState->ChunkColumnMapping,
                    std::move(blockManagerFactory),
                    produceAllVersions);
            }
        }

        return NColumnarChunkFormat::CreateVersionedChunkReader(
            std::move(keys),
            timestamp,
            chunkMeta,
            Schema_,
            columnFilter,
            chunkState->ChunkColumnMapping,
            std::move(blockManagerFactory),
            produceAllVersions);
    }

    return CreateCacheBasedVersionedChunkReader(
        ChunkId_,
        chunkState,
        chunkState->ChunkMeta,
        chunkReadOptions,
        std::move(keys),
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

TChunkStatePtr TSortedChunkStore::PrepareChunkState(TCachedVersionedChunkMetaPtr meta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), ChunkId_);

    auto chunkColumnMapping = GetChunkColumnMapping(Schema_, meta->ChunkSchema());

    return New<TChunkState>(TChunkState{
        .BlockCache = BlockCache_,
        .ChunkSpec = std::move(chunkSpec),
        .ChunkMeta = std::move(meta),
        .OverrideTimestamp = OverrideTimestamp_,
        .KeyComparer = GetKeyComparer(),
        .TableSchema = Schema_,
        .ChunkColumnMapping = std::move(chunkColumnMapping),
    });
}

void TSortedChunkStore::ValidateBlockSize(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TWorkloadDescriptor& workloadDescriptor)
{
    auto chunkFormat = chunkMeta->GetChunkFormat();

    if ((workloadDescriptor.Category == EWorkloadCategory::UserInteractive ||
        workloadDescriptor.Category == EWorkloadCategory::UserRealtime) &&
        (chunkFormat == EChunkFormat::TableUnversionedSchemalessHorizontal ||
        chunkFormat == EChunkFormat::TableUnversionedColumnar))
    {
        // For unversioned chunks verify that block size is correct.
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;
        if (auto blockSizeLimit = mountConfig->MaxUnversionedBlockSize) {
            const auto& miscExt = chunkMeta->Misc();
            if (miscExt.max_data_block_size() > *blockSizeLimit) {
                THROW_ERROR_EXCEPTION("Maximum block size limit violated")
                    << TErrorAttribute("tablet_id", TabletId_)
                    << TErrorAttribute("chunk_id", GetId())
                    << TErrorAttribute("block_size", miscExt.max_data_block_size())
                    << TErrorAttribute("block_size_limit", *blockSizeLimit);
            }
        }
    }
}

const TKeyComparer& TSortedChunkStore::GetKeyComparer() const
{
    return KeyComparer_;
}

ISortedStorePtr TSortedChunkStore::GetSortedBackingStore() const
{
    auto backingStore = GetBackingStore();
    return backingStore ? backingStore->AsSorted() : nullptr;
}

TFuture<TSortedChunkStore::TKeyFilteringResult> TSortedChunkStore::PerformXorKeyFiltering(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    const TXorFilterMeta& xorFilterMeta,
    TSharedRange<TLegacyKey> keys) const
{
    int chunkKeyColumnCount = chunkMeta->GetChunkKeyColumnCount();
    auto codecId = CheckedEnumCast<NCompression::ECodec>(chunkMeta->Misc().compression_codec());

    int currentBlockIndex = 0;
    int currentKeyIndex = 0;
    std::vector<TXorFilterBlockInfo> blockInfos;

    while (currentKeyIndex < std::ssize(keys)) {
        currentBlockIndex = BinarySearch(
            currentBlockIndex,
            std::ssize(xorFilterMeta.BlockMetas),
            [&] (int blockIndex) {
                return CompareWithWidening(
                    ToKeyRef(xorFilterMeta.BlockMetas[blockIndex].BlockLastKey, chunkKeyColumnCount),
                    ToKeyRef(keys[currentKeyIndex])) < 0;
            });

        if (currentBlockIndex == std::ssize(xorFilterMeta.BlockMetas)) {
            break;
        }

        int nextKeyIndex = BinarySearch(
            currentKeyIndex,
            std::ssize(keys),
            [&] (int keyIndex) {
                return CompareWithWidening(
                    ToKeyRef(xorFilterMeta.BlockMetas[currentBlockIndex].BlockLastKey, chunkKeyColumnCount),
                    ToKeyRef(keys[keyIndex])) >= 0;
            });
        YT_VERIFY(currentKeyIndex < nextKeyIndex);

        blockInfos.push_back({
            .BlockIndex = xorFilterMeta.BlockMetas[currentBlockIndex].BlockIndex,
            .KeyPrefixLength = xorFilterMeta.KeyPrefixLength,
            .Keys = TRange(keys).Slice(currentKeyIndex, nextKeyIndex),
        });

        currentKeyIndex = nextKeyIndex;
        ++currentBlockIndex;
    }

    std::vector<int> requestedBlockIndexes;
    for (auto& blockInfo : blockInfos) {
        auto blockData = BlockCache_->FindBlock(
            NChunkClient::TBlockId(ChunkId_, blockInfo.BlockIndex),
            EBlockType::XorFilter)
            .Data;
        if (blockData) {
            blockInfo.XorFilter.Initialize(std::move(blockData));
        } else {
            requestedBlockIndexes.push_back(blockInfo.BlockIndex);
        }
    }

    if (requestedBlockIndexes.empty()) {
        return MakeFuture<TKeyFilteringResult>(OnXorKeyFilterBlocksRead(
            codecId,
            std::move(blockInfos),
            std::move(keys),
            /*requestedBlocks*/ {}));
    }

    return chunkReader->ReadBlocks(
        IChunkReader::TReadBlocksOptions{
            .ClientOptions = chunkReadOptions,
        },
        requestedBlockIndexes)
        .ApplyUnique(BIND(
            &TSortedChunkStore::OnXorKeyFilterBlocksRead,
            MakeStrong(this),
            codecId,
            Passed(std::move(blockInfos)),
            Passed(std::move(keys)))
        .AsyncVia(GetCurrentInvoker()));
}

TSortedChunkStore::TKeyFilteringResult TSortedChunkStore::OnXorKeyFilterBlocksRead(
    NCompression::ECodec codecId,
    std::vector<TXorFilterBlockInfo> blockInfos,
    TSharedRange<TLegacyKey> keys,
    std::vector<TBlock>&& requestedBlocks) const
{
    TKeyFilteringResult filteringResult;
    filteringResult.MissingKeyMask.reserve(keys.size());

    int requestedBlockIndex = 0;
    std::vector<TLegacyKey> filteredKeys;
    for (auto& blockInfo : blockInfos) {
        auto& xorFilter = blockInfo.XorFilter;
        if (!xorFilter.IsInitialized()) {
            YT_VERIFY(requestedBlockIndex < std::ssize(requestedBlocks));

            auto* codec = NCompression::GetCodec(codecId);
            auto decompressedBlock = codec->Decompress(std::move(requestedBlocks[requestedBlockIndex].Data));
            ++requestedBlockIndex;

            BlockCache_->PutBlock(
                NChunkClient::TBlockId(ChunkId_, blockInfo.BlockIndex),
                EBlockType::XorFilter,
                TBlock(decompressedBlock));
            xorFilter.Initialize(std::move(decompressedBlock));
        }

        for (auto key : blockInfo.Keys) {
            if (Contains(xorFilter, key, blockInfo.KeyPrefixLength)) {
                filteredKeys.push_back(key);
                filteringResult.MissingKeyMask.push_back(0);
            } else {
                filteringResult.MissingKeyMask.push_back(1);
            }
        }
    }

    YT_VERIFY(requestedBlockIndex == std::ssize(requestedBlocks));

    // NB: Account keys that are behind last key of last xor filter block.
    while (filteringResult.MissingKeyMask.size() < keys.size()) {
        filteringResult.MissingKeyMask.push_back(1);
    }

    filteringResult.FilteredKeys = MakeSharedRange(
        std::move(filteredKeys),
        std::move(keys.ReleaseHolder()));

    return filteringResult;
}

TSharedRange<TRowRange> TSortedChunkStore::MaybePerformXorRangeFiltering(
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::IChunkReaderPtr& chunkReader,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TChunkStatePtr& chunkState,
    TSharedRange<TRowRange> ranges,
    bool* keyFilterUsed) const
{
    if (!tabletSnapshot->Settings.MountConfig->EnableKeyFilterForLookup) {
        return ranges;
    }

    struct TFilterAndRangesInfo
    {
        const TXorFilterMeta* XorFilterMeta;
        std::vector<int> RangeIndexes;
    };

    THashMap<int, TFilterAndRangesInfo> keyPrefixLengthToInfo;
    for (int rangeIndex = 0; rangeIndex < std::ssize(ranges); ++rangeIndex) {
        auto range = ranges[rangeIndex];
        int commonPrefixLength = 0;
        while (
            commonPrefixLength < static_cast<int>(range.first.GetCount()) &&
            commonPrefixLength + 1 < static_cast<int>(range.second.GetCount()) &&
            range.first[commonPrefixLength] == range.second[commonPrefixLength])
        {
            ++commonPrefixLength;
        }

        if (auto* xorFilterMeta = chunkState->ChunkMeta->FindXorFilterByLength(commonPrefixLength)) {
            auto [it, _] = keyPrefixLengthToInfo.try_emplace(
                xorFilterMeta->KeyPrefixLength,
                TFilterAndRangesInfo{
                    .XorFilterMeta = xorFilterMeta,
                });
            it->second.RangeIndexes.push_back(rangeIndex);
        }
    }

    if (!keyPrefixLengthToInfo.empty()) {
        *keyFilterUsed = true;

        std::vector<int> futureIndexToKeyPrefixLength;
        std::vector<TFuture<TSortedChunkStore::TKeyFilteringResult>> filteringFutures;
        futureIndexToKeyPrefixLength.reserve(keyPrefixLengthToInfo.size());
        filteringFutures.reserve(keyPrefixLengthToInfo.size());

        for (const auto& [keyPrefixLength, filterInfo] : keyPrefixLengthToInfo) {
            std::vector<TLegacyKey> keys;
            for (auto rangeIndex : filterInfo.RangeIndexes) {
                // NB: No difference which bound to take because it will be cut down
                // to the size of the filter that does not exceed common prefix size.
                keys.push_back(ranges[rangeIndex].first);
            }
            futureIndexToKeyPrefixLength.push_back(keyPrefixLength);
            filteringFutures.push_back(PerformXorKeyFiltering(
                chunkState->ChunkMeta,
                chunkReader,
                chunkReadOptions,
                *filterInfo.XorFilterMeta,
                MakeSharedRange(std::move(keys))));
        }

        // TODO(akozhikhov): Get rid of WairFor TSortedChunkStoreVersionedReader-style.
        auto filteringResults = WaitForFast(AllSucceeded(std::move(filteringFutures)))
            .ValueOrThrow();

        std::vector<int> filteredOutRangeFlags(ranges.Size());
        for (int resultIndex = 0; resultIndex < std::ssize(filteringResults); ++resultIndex) {
            const auto& filteringResult = filteringResults[resultIndex];
            const auto& filterInfo = GetOrCrash(keyPrefixLengthToInfo, futureIndexToKeyPrefixLength[resultIndex]);
            YT_VERIFY(filteringResult.MissingKeyMask.size() == filterInfo.RangeIndexes.size());

            for (int localRangeIndex = 0; localRangeIndex < std::ssize(filterInfo.RangeIndexes); ++localRangeIndex) {
                filteredOutRangeFlags[filterInfo.RangeIndexes[localRangeIndex]] = filteringResult.MissingKeyMask[localRangeIndex];
            }
        }

        std::vector<TRowRange> filteredRanges;
        for (int rangeIndex = 0; rangeIndex < std::ssize(ranges); ++rangeIndex) {
            if (!filteredOutRangeFlags[rangeIndex]) {
                filteredRanges.push_back(ranges[rangeIndex]);
            }
        }

        YT_LOG_DEBUG("Performed range filtering "
            "(InitialRangeCount: %v, FilteredRangeCount: %v, ChunkId: %v, ReadSessionId: %v)",
            ranges.Size(),
            filteredRanges.size(),
            ChunkId_,
            chunkReadOptions.ReadSessionId);

        if (const auto& keyFilterStatistics = chunkReadOptions.KeyFilterStatistics) {
            keyFilterStatistics->InputEntryCount.fetch_add(ssize(ranges), std::memory_order::relaxed);
            keyFilterStatistics->FilteredOutEntryCount.fetch_add(ssize(ranges) - ssize(filteredRanges), std::memory_order::relaxed);
        }

        ranges = MakeSharedRange(std::move(filteredRanges), std::move(ranges.ReleaseHolder()));
    }

    return ranges;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TLegacyKey> FilterKeysByReadRange(
    const NTableClient::TRowRange& readRange,
    TSharedRange<TLegacyKey> keys,
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

    return MakeSharedRange(
        static_cast<TRange<TLegacyKey>>(keys).Slice(begin, end),
        std::move(keys.ReleaseHolder()));
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

