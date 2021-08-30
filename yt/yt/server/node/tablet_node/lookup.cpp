#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/row_merger.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profile_manager.h>
#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/small_vector.h>
#include <yt/yt/core/misc/tls_cache.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;
static const i64 RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

struct TLookupSessionBufferTag
{ };

class TLookupSession
{
public:
    TLookupSession(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool useLookupCache,
        const TColumnFilter& columnFilter,
        const TClientChunkReadOptions& chunkReadOptions,
        TSharedRange<TUnversionedRow> lookupKeys)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , UseLookupCache_(useLookupCache && TabletSnapshot_->RowCache)
        , ColumnFilter_(columnFilter)
        , ChunkReadOptions_(chunkReadOptions)
        , LookupKeys_(std::move(lookupKeys))
    { }

    template <
        class TAddPartialRow,
        class TFinishRow
    >
    void Run(
        const TAddPartialRow& addPartialRow,
        const TFinishRow& finishRow)
    {
        YT_LOG_DEBUG("Tablet lookup started (TabletId: %v, CellId: %v, KeyCount: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            LookupKeys_.Size(),
            ChunkReadOptions_.ReadSessionId);

        ThrowUponThrottlerOverdraft(
            ETabletDistributedThrottlerKind::Lookup,
            TabletSnapshot_,
            ChunkReadOptions_);

        std::vector<TUnversionedRow> chunkLookupKeys;

        // Lookup in dynamic stores always and merge with cache.
        if (UseLookupCache_) {
            YT_LOG_DEBUG("Looking up in row cache (ReadSessionId: %v)",
                ChunkReadOptions_.ReadSessionId);

            const auto& rowCache = TabletSnapshot_->RowCache;

            auto flushIndex = rowCache->GetFlushIndex();

            CacheLookuper_ = rowCache->GetCache()->GetLookuper();
            for (auto key : LookupKeys_) {
                auto foundItemRef = CacheLookuper_(key);
                auto foundItem = foundItemRef.Get();

                if (foundItem) {
                    // If table is frozen both revisions are zero.
                    if (foundItem->Revision.load(std::memory_order_acquire) >= flushIndex) {
                        ++CacheHits_;
                        YT_LOG_TRACE("Row found (Key: %v)", key);
                        RowsFromCache_.push_back(std::move(foundItemRef));
                        continue;
                    } else {
                        ++CacheOutdated_;
                    }
                } else {
                    ++CacheMisses_;
                    YT_LOG_TRACE("Row not found (Key: %v)", key);
                }

                chunkLookupKeys.push_back(key);
                RowsFromCache_.emplace_back();
            }
        } else {
            chunkLookupKeys = LookupKeys_.ToVector();
            RowsFromCache_.resize(LookupKeys_.Size());
        }

        ChunkLookupKeys_ = MakeSharedRange(std::move(chunkLookupKeys), LookupKeys_);

        RetainedTimestamp_ = TabletSnapshot_->RetainedTimestamp;
        StoreFlushIndex_ = TabletSnapshot_->StoreFlushIndex;

        auto edenStores = TabletSnapshot_->GetEdenStores();
        std::vector<ISortedStorePtr> dynamicEdenStores;
        std::vector<ISortedStorePtr> chunkEdenStores;

        for (const auto& store : edenStores) {
            if (store->IsDynamic()) {
                // Can not check store state via GetStoreState.
                if (TabletSnapshot_->ActiveStore == store) {
                    YT_VERIFY(ActiveStoreIndex_ == -1);
                    ActiveStoreIndex_ = dynamicEdenStores.size();
                }

                dynamicEdenStores.push_back(store);
            } else {
                chunkEdenStores.push_back(store);
            }
        }

        const auto& mountConfig = TabletSnapshot_->Settings.MountConfig;

        if (UseLookupCache_) {
            auto compactionTimestamp = NTransactionClient::InstantToTimestamp(
                NTransactionClient::TimestampToInstant(RetainedTimestamp_).first + mountConfig->MinDataTtl).first;

            YT_LOG_DEBUG("Creating cache row merger (CompactionTimestamp: %llx, ReadSessionId: %v)",
                compactionTimestamp,
                ChunkReadOptions_.ReadSessionId);

            CacheRowMerger_ = std::make_unique<TVersionedRowMerger>(
                New<TRowBuffer>(TLookupSessionBufferTag()),
                TabletSnapshot_->PhysicalSchema->GetColumnCount(),
                TabletSnapshot_->PhysicalSchema->GetKeyColumnCount(),
                TColumnFilter::MakeUniversal(),
                mountConfig,
                compactionTimestamp,
                MaxTimestamp, // Do not consider major timestamp.
                TabletSnapshot_->ColumnEvaluator,
                /*lookup*/ true, // Do not produce sentinel rows.
                /*mergeRowsOnFlush*/ true); // Always merge rows on flush.
        }

        // NB: We allow more parallelization in case of data node lookup
        // due to lower cpu and memory usage on tablet nodes.
        if (mountConfig->MaxParallelPartitionLookups && mountConfig->EnableDataNodeLookup) {
            auto readSessionFutures = CreateReadSessions(
                &DynamicEdenSessions_,
                dynamicEdenStores,
                LookupKeys_);
            auto otherReadSessionFutures = CreateReadSessions(
                &ChunkEdenSessions_,
                chunkEdenStores,
                ChunkLookupKeys_);
            readSessionFutures.insert(
                readSessionFutures.end(),
                otherReadSessionFutures.begin(),
                otherReadSessionFutures.end());
            ParallelLookupInPartitions(
                std::move(readSessionFutures),
                addPartialRow,
                finishRow);
        } else {
            auto dynamicEdenReadSessions = CreateReadSessions(
                &DynamicEdenSessions_,
                dynamicEdenStores,
                LookupKeys_);
            if (!dynamicEdenReadSessions.empty()) {
                WaitFor(AllSucceeded(dynamicEdenReadSessions))
                    .ThrowOnError();
            }
            auto chunkEdenReadSessions = CreateReadSessions(
                &ChunkEdenSessions_,
                chunkEdenStores,
                ChunkLookupKeys_);
            if (!chunkEdenReadSessions.empty()) {
                WaitFor(AllSucceeded(chunkEdenReadSessions))
                    .ThrowOnError();
            }

            PartitionSessions_.resize(1);
            auto currentIt = LookupKeys_.Begin();
            int startChunkKeyIndex = 0;
            while (currentIt != LookupKeys_.End()) {
                auto sessionInfo = CreatePartitionSession(&currentIt, &startChunkKeyIndex, 0);

                if (!sessionInfo.ReadSessionFutures.empty()) {
                    WaitFor(AllSucceeded(sessionInfo.ReadSessionFutures))
                        .ThrowOnError();
                }

                LookupInPartition(
                    sessionInfo.StartKeyIndex,
                    sessionInfo.EndKeyIndex,
                    &PartitionSessions_[0],
                    addPartialRow,
                    finishRow);
            }
        }

        UpdateUnmergedStatistics(DynamicEdenSessions_);
        UpdateUnmergedStatistics(ChunkEdenSessions_);

        if (const auto& throttler = TabletSnapshot_->DistributedThrottlers[ETabletDistributedThrottlerKind::Lookup]) {
            throttler->Acquire(FoundDataWeight_);
        }

        YT_LOG_DEBUG(
            "Tablet lookup completed "
            "(TabletId: %v, CellId: %v, CacheHits: %v, CacheOutdated: %v, CacheMisses: %v, "
            "FoundRowCount: %v, FoundDataWeight: %v, DecompressionCpuTime: %v, "
            "ReadSessionId: %v, EnableDetailedProfiling: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            CacheHits_,
            CacheOutdated_,
            CacheMisses_,
            FoundRowCount_,
            FoundDataWeight_,
            DecompressionCpuTime_,
            ChunkReadOptions_.ReadSessionId,
            mountConfig->EnableDetailedProfiling);
    }

    void UpdateProfilingCounters(TDuration cpuTime, TDuration wallTime)
    {
        auto counters = TabletSnapshot_->TableProfiler->GetLookupCounters(GetCurrentProfilingUser());

        counters->CacheHits.Increment(CacheHits_);
        counters->CacheOutdated.Increment(CacheOutdated_);
        counters->CacheMisses.Increment(CacheMisses_);
        counters->CacheInserts.Increment(CacheInserts_);

        counters->RowCount.Increment(FoundRowCount_);
        counters->MissingKeyCount.Increment(LookupKeys_.size() - FoundRowCount_);
        counters->DataWeight.Increment(FoundDataWeight_);
        counters->UnmergedRowCount.Increment(UnmergedRowCount_);
        counters->UnmergedDataWeight.Increment(UnmergedDataWeight_);

        counters->CpuTime.Add(cpuTime);
        counters->DecompressionCpuTime.Add(DecompressionCpuTime_);

        counters->ChunkReaderStatisticsCounters.Increment(ChunkReadOptions_.ChunkReaderStatistics);

        counters->HunkChunkReaderCounters.Increment(ChunkReadOptions_.HunkChunkReaderStatistics);

        if (TabletSnapshot_->Settings.MountConfig->EnableDetailedProfiling) {
            counters->LookupDuration.Record(wallTime);
        }
    }

private:
    struct TPartitionSessionInfo
    {
        const TPartitionSnapshotPtr& PartitionSnapshot;
        int StartKeyIndex;
        int EndKeyIndex;
        std::vector<TFuture<void>> ReadSessionFutures;
    };

    class TReadSession
    {
    public:
        explicit TReadSession(IVersionedReaderPtr reader)
            : Reader_(std::move(reader))
        { }

        TReadSession(const TReadSession& otherSession) = delete;
        TReadSession(TReadSession&& otherSession) = default;

        TReadSession& operator=(const TReadSession& otherSession) = delete;
        TReadSession& operator=(TReadSession&& otherSession)
        {
            YT_VERIFY(!Reader_);
            YT_VERIFY(!otherSession.Reader_);
            return *this;
        }

        TVersionedRow FetchRow()
        {
            ++RowIndex_;
            if (!RowBatch_ || RowIndex_ >= RowBatch_->GetRowCount()) {
                RowIndex_ = 0;
                while (true) {
                    TRowBatchReadOptions options{
                        .MaxRowsPerRead = RowBufferCapacity
                    };
                    RowBatch_ = Reader_->Read(options);
                    YT_VERIFY(RowBatch_);
                    if (!RowBatch_->IsEmpty()) {
                        break;
                    }
                    WaitFor(Reader_->GetReadyEvent())
                        .ThrowOnError();
                }
            }
            return RowBatch_->MaterializeRows()[RowIndex_];
        }

        NChunkClient::NProto::TDataStatistics GetDataStatistics() const
        {
            return Reader_->GetDataStatistics();
        }

        TCodecStatistics GetDecompressionStatistics() const
        {
            return Reader_->GetDecompressionStatistics();
        }

    private:
        const IVersionedReaderPtr Reader_;

        IVersionedRowBatchPtr RowBatch_;
        int RowIndex_ = -1;
    };

    const TTabletSnapshotPtr TabletSnapshot_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const bool UseLookupCache_;
    const TColumnFilter& ColumnFilter_;
    const TClientChunkReadOptions& ChunkReadOptions_;
    const TSharedRange<TUnversionedRow> LookupKeys_;
    TSharedRange<TUnversionedRow> ChunkLookupKeys_;
    // Holds references to lookup tables.
    TConcurrentCache<TCachedRow>::TLookuper CacheLookuper_;
    std::vector<TConcurrentCache<TCachedRow>::TCachedItemRef> RowsFromCache_;
    ui32 StoreFlushIndex_;
    TTimestamp RetainedTimestamp_;
    int ActiveStoreIndex_ = -1;
    std::unique_ptr<TVersionedRowMerger> CacheRowMerger_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList DynamicEdenSessions_;
    TReadSessionList ChunkEdenSessions_;
    SmallVector<TReadSessionList, MaxParallelPartitionLookupsLimit> PartitionSessions_;

    int CacheHits_ = 0;
    int CacheMisses_ = 0;
    int CacheOutdated_ = 0;
    int CacheInserts_ = 0;
    int FoundRowCount_ = 0;
    i64 FoundDataWeight_ = 0;
    int UnmergedRowCount_ = 0;
    i64 UnmergedDataWeight_ = 0;
    TDuration DecompressionCpuTime_;

    std::vector<TFuture<void>> CreateReadSessions(
        TReadSessionList* sessions,
        const std::vector<ISortedStorePtr>& stores,
        const TSharedRange<TLegacyKey>& keys)
    {
        sessions->clear();

        // When using lookup cache we must read all versions.
        // It is safe to change fixed timestamp to SyncLastCommitted and drop newer than timestamp versions
        // in row merger.
        auto readTimestamp = UseLookupCache_ && Timestamp_ != AsyncLastCommittedTimestamp
            ? SyncLastCommittedTimestamp
            : Timestamp_;

        // NB: Will remain empty for in-memory tables.
        std::vector<TFuture<void>> futures;
        for (const auto& store : stores) {
            YT_LOG_DEBUG("Creating reader (Store: %v, KeysCount: %v, ReadSessionId: %v)",
                store->GetId(),
                keys.Size(),
                ChunkReadOptions_.ReadSessionId);

            auto reader = store->CreateReader(
                TabletSnapshot_,
                keys,
                readTimestamp,
                UseLookupCache_ || ProduceAllVersions_,
                UseLookupCache_ ? TColumnFilter::MakeUniversal() : ColumnFilter_,
                ChunkReadOptions_,
                /*workloadCategory*/ std::nullopt);
            auto future = reader->Open();
            if (auto optionalError = future.TryGet()) {
                optionalError->ThrowOnError();
            } else {
                futures.emplace_back(std::move(future));
            }
            sessions->emplace_back(std::move(reader));
        }

        return futures;
    }

    TPartitionSessionInfo CreatePartitionSession(
        decltype(LookupKeys_)::iterator* currentIt,
        int* startChunkKeyIndex,
        int sessionIndex)
    {
        auto nextPartitionIt = std::upper_bound(
            TabletSnapshot_->PartitionList.begin(),
            TabletSnapshot_->PartitionList.end(),
            **currentIt,
            [] (TLegacyKey lhs, const TPartitionSnapshotPtr& rhs) {
                return lhs < rhs->PivotKey;
            });
        YT_VERIFY(nextPartitionIt != TabletSnapshot_->PartitionList.begin());
        const auto& partitionSnapshot = *(nextPartitionIt - 1);

        auto nextIt = nextPartitionIt == TabletSnapshot_->PartitionList.end()
            ? LookupKeys_.End()
            : std::lower_bound(*currentIt, LookupKeys_.End(), (*nextPartitionIt)->PivotKey);
        int startKeyIndex = *currentIt - LookupKeys_.Begin();
        int endKeyIndex = nextIt - LookupKeys_.Begin();
        int endChunkKeyIndex = *startChunkKeyIndex;
        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            endChunkKeyIndex += !RowsFromCache_[index];
        }

        auto readSessionFutures = CreateReadSessions(
            &PartitionSessions_[sessionIndex],
            partitionSnapshot->Stores,
            ChunkLookupKeys_.Slice(*startChunkKeyIndex, endChunkKeyIndex));

        *startChunkKeyIndex = endChunkKeyIndex;
        *currentIt = nextIt;

        return {
            partitionSnapshot,
            startKeyIndex,
            endKeyIndex,
            std::move(readSessionFutures)
        };
    }

    template <
        class TAddPartialRow,
        class TFinishRow
    >
    void LookupInPartition(
        int startKeyIndex,
        int endKeyIndex,
        TReadSessionList* partitionSessions,
        const TAddPartialRow& addPartialRow,
        const TFinishRow& finishRow)
    {
        std::vector<TVersionedRow> versionedRows;

        auto processSessions = [&] (TReadSessionList& sessions, int excludedStoreIndex) {
            int sessionIndex = 0;
            for (auto& session : sessions) {
                auto row = session.FetchRow();
                addPartialRow(row, Timestamp_ + 1);
                if (UseLookupCache_) {
                    versionedRows.push_back(row);

                    // Do not add values from active dynamic store. Add only key.
                    auto upperTimestampLimit = excludedStoreIndex == sessionIndex
                        ? MinTimestamp
                        : MaxTimestamp;
                    CacheRowMerger_->AddPartialRow(row, upperTimestampLimit);
                }
                ++sessionIndex;
            }
        };

        TConcurrentCache<TCachedRow>::TInserter inserter;

        const auto& rowCache = TabletSnapshot_->RowCache;

        if (UseLookupCache_) {
            inserter = rowCache->GetCache()->GetInserter();
        }

        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            // Need to insert rows into cache even from active dynamic store.
            // Otherwise cache misses will occur.
            // Process dynamic store rows firstly.
            processSessions(DynamicEdenSessions_, ActiveStoreIndex_);

            auto cachedItemRef = std::move(RowsFromCache_[index]);

            if (auto cachedItemHead = cachedItemRef.Get()) {
                auto cachedItem = GetLatestRow(cachedItemHead);

                if (Timestamp_ < cachedItem->RetainedTimestamp) {
                    THROW_ERROR_EXCEPTION("Timestamp %llx is less than retained timestamp %llx of cached row in tablet %v",
                        Timestamp_,
                        cachedItem->RetainedTimestamp,
                        TabletSnapshot_->TabletId);
                }

                YT_LOG_TRACE("Using row from cache (CacheRow: %v, Revision: %v, ReadTimestamp: %v, DynamicRows: %v)",
                    cachedItem->GetVersionedRow(),
                    cachedItem->Revision.load(),
                    Timestamp_,
                    versionedRows);

                addPartialRow(cachedItem->GetVersionedRow(), Timestamp_ + 1);

                // Reinsert row here.
                // TODO(lukyan): Move into function UpdateRow(cachedItemRef, inserter, cachedItem)
                auto lookupTable = inserter.GetTable();
                if (lookupTable == cachedItemRef.Origin) {
                    YT_LOG_TRACE("Updating row");
                    cachedItemRef.Update(std::move(cachedItem), cachedItemHead.Get());
                } else {
                    YT_LOG_TRACE("Reinserting row");
                    lookupTable->Insert(std::move(cachedItem));
                }

                // Cleanup row merger.
                CacheRowMerger_->BuildMergedRow();
            } else {
                processSessions(*partitionSessions, -1);
                processSessions(ChunkEdenSessions_, -1);

                if (UseLookupCache_) {
                    auto mergedRow = CacheRowMerger_->BuildMergedRow();

                    auto cachedItem = CachedRowFromVersionedRow(
                        rowCache->GetAllocator(),
                        mergedRow,
                        RetainedTimestamp_);

                    if (cachedItem) {
                        YT_VERIFY(cachedItem->GetVersionedRow().GetKeyCount() > 0);

                        auto revision = StoreFlushIndex_;
                        cachedItem->Revision.store(revision, std::memory_order_release);

                        YT_LOG_TRACE("Populating cache (Row: %v, Revision: %v)",
                            cachedItem->GetVersionedRow(),
                            revision);
                        inserter.GetTable()->Insert(cachedItem);

                        auto flushIndex = rowCache->GetFlushIndex();

                        // Row revision is equal to flushRevision if the last passive dynamic store has started flushing.
                        if (revision >= flushIndex) {
                            cachedItem->Revision.compare_exchange_strong(revision, std::numeric_limits<ui32>::max());
                        }

                        ++CacheInserts_;
                    }
                }
            }

            versionedRows.clear();

            auto [found, dataWeight] = finishRow();
            FoundRowCount_ += found ? 1 : 0;
            FoundDataWeight_ += dataWeight;
        }

        UpdateUnmergedStatistics(*partitionSessions);
    }

    void UpdateUnmergedStatistics(const TReadSessionList& sessions)
    {
        for (const auto& session : sessions) {
            auto statistics = session.GetDataStatistics();
            UnmergedRowCount_ += statistics.row_count();
            UnmergedDataWeight_ += statistics.data_weight();
            DecompressionCpuTime_ += session.GetDecompressionStatistics().GetTotalDuration();
        }
    }

    template <
        class TAddPartialRow,
        class TFinishRow
    >
    void ParallelLookupInPartitions(
        std::vector<TFuture<void>> readSessionFutures,
        const TAddPartialRow& addPartialRow,
        const TFinishRow& finishRow)
    {
        const auto& mountConfig = TabletSnapshot_->Settings.MountConfig;

        YT_VERIFY(mountConfig->MaxParallelPartitionLookups);
        PartitionSessions_.resize(*mountConfig->MaxParallelPartitionLookups);

        auto currentIt = LookupKeys_.Begin();
        int startChunkKeyIndex = 0;
        while (currentIt != LookupKeys_.End()) {
            std::vector<TPartitionSessionInfo> partitionSessionInfos;

            while (std::ssize(partitionSessionInfos) < *mountConfig->MaxParallelPartitionLookups &&
                currentIt != LookupKeys_.End())
            {
                auto sessionInfo = CreatePartitionSession(&currentIt, &startChunkKeyIndex, partitionSessionInfos.size());

                for (auto& readSession : sessionInfo.ReadSessionFutures) {
                    readSessionFutures.push_back(std::move(readSession));
                }
                sessionInfo.ReadSessionFutures.clear();

                partitionSessionInfos.push_back(std::move(sessionInfo));
            }

            YT_LOG_DEBUG("Starting parallel lookups "
                "(PartitionCount: %v, MaxPartitionCount: %v, ReadSessionId: %v, PartitionSessionInfos: %v)",
                partitionSessionInfos.size(),
                *mountConfig->MaxParallelPartitionLookups,
                ChunkReadOptions_.ReadSessionId,
                MakeFormattableView(partitionSessionInfos, [] (auto* builder, const TPartitionSessionInfo& partitionInfo) {
                    builder->AppendFormat("{Id: %v, KeyCount: %v}",
                        partitionInfo.PartitionSnapshot->Id,
                        partitionInfo.EndKeyIndex - partitionInfo.StartKeyIndex);
                }));

            WaitFor(AllSucceeded(std::move(readSessionFutures)))
                .ThrowOnError();

            // NB: Processing of partitions is sequential.
            for (int index = 0; index < std::ssize(partitionSessionInfos); ++index) {
                LookupInPartition(
                    partitionSessionInfos[index].StartKeyIndex,
                    partitionSessionInfos[index].EndKeyIndex,
                    &PartitionSessions_[index],
                    addPartialRow,
                    finishRow);
            }
        }
    }
};

namespace {

NTableClient::TColumnFilter DecodeColumnFilter(
    std::unique_ptr<NTableClient::NProto::TColumnFilter> protoColumnFilter,
    int columnCount)
{
    auto columnFilter = protoColumnFilter
        ? TColumnFilter(FromProto<TColumnFilter::TIndexes>(protoColumnFilter->indexes()))
        : TColumnFilter();
    ValidateColumnFilter(columnFilter, columnCount);
    return columnFilter;
}

TFuture<TSharedRange<TMutableUnversionedRow>> DecodeHunks(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows)
{
    return DecodeHunksInSchemafulUnversionedRows(
        schema,
        columnFilter,
        std::move(chunkFragmentReader),
        std::move(options),
        std::move(rows));
}

TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunks(
    const TTableSchemaPtr& /*schema*/,
    const TColumnFilter& /*columnFilter*/,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows)
{
    return DecodeHunksInVersionedRows(
        std::move(chunkFragmentReader),
        std::move(options),
        std::move(rows));
}

template <
    class TRowMerger,
    class TRowWriter
>
void DoLookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestamp,
    bool produceAllVersions,
    bool useLookupCache,
    const TColumnFilter& columnFilter,
    TClientChunkReadOptions chunkReadOptions,
    TSharedRange<TUnversionedRow> lookupKeys,
    const TRowBufferPtr& rowBuffer,
    TRowMerger& rowMerger,
    const TRowWriter& rowWriter)
{
    tabletSnapshot->WaitOnLocks(timestamp);

    THazardPtrFlushGuard flushGuard;

    if (!chunkReadOptions.ChunkReaderStatistics) {
        chunkReadOptions.ChunkReaderStatistics = New<NChunkClient::TChunkReaderStatistics>();
    }

    const auto& schema = tabletSnapshot->PhysicalSchema;
    chunkReadOptions.HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
        tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
        schema);

    TLookupSession session(
        tabletSnapshot,
        timestamp,
        produceAllVersions,
        useLookupCache,
        columnFilter,
        chunkReadOptions,
        std::move(lookupKeys));

    TFiberWallTimer cpuTimer;
    TWallTimer wallTimer;
    // NB: Hunk reader statistics will also be accounted with this guard.
    auto updateProfiling = Finally([&] {
        session.UpdateProfilingCounters(cpuTimer.GetElapsedTime(), wallTimer.GetElapsedTime());
    });

    auto addPartialRow = [&] (TVersionedRow partialRow, TTimestamp timestamp) {
        rowMerger.AddPartialRow(partialRow, timestamp);
    };

    using TRow = decltype(rowMerger.BuildMergedRow());
    auto getMergedRowStatistics = [] (TRow mergedRow) {
        return std::make_pair(static_cast<bool>(mergedRow), GetDataWeight(mergedRow));
    };

    if (schema->HasHunkColumns()) {
        std::vector<TRow> rows;
        rows.reserve(lookupKeys.Size());

        session.Run(
            addPartialRow,
            [&] {
                auto mergedRow = rowMerger.BuildMergedRow();
                rowBuffer->CaptureValues(mergedRow);
                rows.push_back(mergedRow);
                return getMergedRowStatistics(mergedRow);
            });

        auto sharedRows = MakeSharedRange(std::move(rows), std::move(rowBuffer));

        auto hunkReaderFuture = DecodeHunks(
            schema,
            columnFilter,
            tabletSnapshot->ChunkFragmentReader,
            chunkReadOptions,
            sharedRows);
        if (hunkReaderFuture) {
            sharedRows = WaitFor(std::move(hunkReaderFuture))
                .ValueOrThrow();
        }

        for (auto row : sharedRows) {
            rowWriter(row);
        }
    } else {
        session.Run(
            addPartialRow,
            [&] {
                auto mergedRow = rowMerger.BuildMergedRow();
                rowWriter(mergedRow);
                return getMergedRowStatistics(mergedRow);
            });
    }

    YT_LOG_DEBUG("Tablet lookup timing statistics "
        "(CpuTime: %v, WallTime: %v, TabletId: %v, ReadSessionId: %v)",
        cpuTimer.GetElapsedTime(),
        wallTimer.GetElapsedTime(),
        tabletSnapshot->TabletId,
        chunkReadOptions.ReadSessionId);
}

} // namespace

void LookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const TClientChunkReadOptions& chunkReadOptions,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    NTableClient::NProto::TReqLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTableClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema->GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(*tabletSnapshot->PhysicalSchema->ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    auto rowBuffer = New<TRowBuffer>(TLookupSessionBufferTag());

    TSchemafulRowMerger rowMerger(
        rowBuffer,
        tabletSnapshot->PhysicalSchema->GetColumnCount(),
        tabletSnapshot->PhysicalSchema->GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    DoLookupRows(
        tabletSnapshot,
        timestamp,
        /*produceAllVersions*/ false,
        useLookupCache,
        columnFilter,
        chunkReadOptions,
        std::move(lookupKeys),
        rowBuffer,
        rowMerger,
        [&] (TUnversionedRow row) {
            writer->WriteSchemafulRow(row);
        });
}

void VersionedLookupRows(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TRetentionConfigPtr& retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    NTableClient::NProto::TReqVersionedLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTableClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema->GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(*tabletSnapshot->PhysicalSchema->ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    auto rowBuffer = New<TRowBuffer>(TLookupSessionBufferTag());

    TVersionedRowMerger rowMerger(
        rowBuffer,
        tabletSnapshot->PhysicalSchema->GetColumnCount(),
        tabletSnapshot->PhysicalSchema->GetKeyColumnCount(),
        columnFilter,
        retentionConfig,
        timestamp,
        MinTimestamp,
        tabletSnapshot->ColumnEvaluator,
        /*lookup*/ true,
        /*mergeRowsOnFlush*/ false);

    DoLookupRows(
        tabletSnapshot,
        timestamp,
        /*produceAllVersions*/ true,
        useLookupCache,
        TColumnFilter::MakeUniversal(),
        chunkReadOptions,
        std::move(lookupKeys),
        rowBuffer,
        rowMerger,
        [&] (TVersionedRow row) {
            writer->WriteVersionedRow(row);
        });
}

void ExecuteSingleRead(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TRetentionConfigPtr& retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::LookupRows:
            LookupRows(
                tabletSnapshot,
                timestamp,
                useLookupCache,
                chunkReadOptions,
                reader,
                writer);
            break;

        case EWireProtocolCommand::VersionedLookupRows:
            VersionedLookupRows(
                tabletSnapshot,
                timestamp,
                useLookupCache,
                chunkReadOptions,
                std::move(retentionConfig),
                reader,
                writer);
            break;

        default:
            THROW_ERROR_EXCEPTION("Unknown read command %v",
                command);
    }
}

void LookupRead(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TRetentionConfigPtr& retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ValidateReadTimestamp(timestamp);
    ValidateTabletRetainedTimestamp(tabletSnapshot, timestamp);

    tabletSnapshot->TabletRuntimeData->AccessTime = NProfiling::GetInstant();

    while (!reader->IsFinished()) {
        ExecuteSingleRead(
            tabletSnapshot,
            timestamp,
            useLookupCache,
            chunkReadOptions,
            retentionConfig,
            reader,
            writer);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
