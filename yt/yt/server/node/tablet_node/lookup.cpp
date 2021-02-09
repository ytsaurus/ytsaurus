#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/server/lib/tablet_node/config.h>

#include <yt/server/lib/misc/profiling_helpers.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/row_merger.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/versioned_reader.h>

#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/proto/wire_protocol.pb.h>

#include <yt/client/transaction_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/tls_cache.h>

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
static const size_t RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

struct TLookupSessionBufferTag
{ };

static const TColumnFilter UniversalColumnFilter;

class TLookupSession
{
public:
    TLookupSession(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool useLookupCache,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        TSharedRange<TUnversionedRow> lookupKeys)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , UseLookupCache_(useLookupCache && TabletSnapshot_->RowCache)
        , ColumnFilter_(columnFilter)
        , BlockReadOptions_(blockReadOptions)
        , LookupKeys_(std::move(lookupKeys))
    { }

    void Run(
        const std::function<void(TVersionedRow, TTimestamp)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        YT_LOG_DEBUG("Tablet lookup started (TabletId: %v, CellId: %v, KeyCount: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            LookupKeys_.Size(),
            BlockReadOptions_.ReadSessionId);

        TFiberWallTimer timer;

        ThrottleUponOverdraft(ETabletDistributedThrottlerKind::Lookup, TabletSnapshot_, BlockReadOptions_);

        std::vector<TUnversionedRow> chunkLookupKeys;

        // Lookup in dynamic stores always and merge with cache.
        if (UseLookupCache_) {
            YT_LOG_DEBUG("Looking up in row cache");

            auto flushIndex = TabletSnapshot_->RowCache->FlushIndex.load(std::memory_order_acquire);

            CacheLookuper_ = TabletSnapshot_->RowCache->Cache.GetLookuper();
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

        ChunkLookupKeys_ = MakeSharedRange(chunkLookupKeys, LookupKeys_);

        auto edenStores = TabletSnapshot_->GetEdenStores();
        std::vector<ISortedStorePtr> dynamicEdenStores;
        std::vector<ISortedStorePtr> chunkEdenStores;

        for (const auto& store : edenStores) {
            if (store->IsDynamic()) {
                dynamicEdenStores.push_back(store);
            } else {
                chunkEdenStores.push_back(store);
            }
        }

        RetainedTimestamp_ = TabletSnapshot_->RetainedTimestamp;
        StoreFlushIndex_ = TabletSnapshot_->StoreFlushIndex;

        // NB: We allow more parallelization in case of data node lookup
        // due to lower cpu and memory usage on tablet nodes.
        if (TabletSnapshot_->Config->MaxParallelPartitionLookups &&
            TabletSnapshot_->Config->EnableDataNodeLookup)
        {
            auto asyncReadSessions = CreateReadSessions(
                &DynamicEdenSessions_,
                dynamicEdenStores,
                LookupKeys_,
                false);
            auto otherAsyncReadSessions = CreateReadSessions(
                &ChunkEdenSessions_,
                chunkEdenStores,
                ChunkLookupKeys_,
                UseLookupCache_);
            asyncReadSessions.insert(asyncReadSessions.end(), otherAsyncReadSessions.begin(), otherAsyncReadSessions.end());

            ParallelLookupInPartitions(std::move(asyncReadSessions), onPartialRow, onRow);
        } else {
            auto dynamicEdenReadSessions = CreateReadSessions(
                &DynamicEdenSessions_,
                dynamicEdenStores,
                LookupKeys_,
                false);
            if (!dynamicEdenReadSessions.empty()) {
                WaitFor(AllSucceeded(dynamicEdenReadSessions))
                    .ThrowOnError();
            }
            auto chunkEdenReadSessions = CreateReadSessions(
                &ChunkEdenSessions_,
                chunkEdenStores,
                ChunkLookupKeys_,
                UseLookupCache_);
            if (!chunkEdenReadSessions.empty()) {
                WaitFor(AllSucceeded(chunkEdenReadSessions))
                    .ThrowOnError();
            }

            PartitionSessions_.resize(1);
            auto currentIt = LookupKeys_.Begin();
            int startChunkKeyIndex = 0;
            while (currentIt != LookupKeys_.End()) {
                auto sessionInfo = CreatePartitionSession(&currentIt, &startChunkKeyIndex, 0);

                if (!sessionInfo.AsyncReadSessions.empty()) {
                    WaitFor(AllSucceeded(sessionInfo.AsyncReadSessions))
                        .ThrowOnError();
                }

                LookupInPartition(
                    sessionInfo.PartitionSnapshot,
                    sessionInfo.StartKeyIndex,
                    sessionInfo.EndKeyIndex,
                    &PartitionSessions_[0],
                    onPartialRow,
                    onRow);
            }
        }

        UpdateUnmergedStatistics(DynamicEdenSessions_);
        UpdateUnmergedStatistics(ChunkEdenSessions_);

        auto cpuTime = timer.GetElapsedTime();

        {
            auto counters = TabletSnapshot_->TableProfiler->GetLookupCounters(GetCurrentProfilingUser());

            counters->CacheHits.Increment(CacheHits_);
            counters->CacheOutdated.Increment(CacheOutdated_);
            counters->CacheMisses.Increment(CacheMisses_);
            counters->CacheInserts.Increment(CacheInserts_);
            counters->RowCount.Increment(FoundRowCount_);
            counters->DataWeight.Increment(FoundDataWeight_);
            counters->UnmergedRowCount.Increment(UnmergedRowCount_);
            counters->UnmergedDataWeight.Increment(UnmergedDataWeight_);

            counters->CpuTime.Add(cpuTime);
            counters->DecompressionCpuTime.Add(DecompressionCpuTime_);

            counters->ChunkReaderStatisticsCounters.Increment(BlockReadOptions_.ChunkReaderStatistics);
        }

        if (const auto& throttler = TabletSnapshot_->DistributedThrottlers[ETabletDistributedThrottlerKind::Lookup]) {
            throttler->Acquire(FoundDataWeight_);
        }

        YT_LOG_DEBUG("Tablet lookup completed (TabletId: %v, CellId: %v, CacheHits: %v, CacheOutdated: %v, CacheMisses: %v, "
            "FoundRowCount: %v, FoundDataWeight: %v, CpuTime: %v, DecompressionCpuTime: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            CacheHits_,
            CacheOutdated_,
            CacheMisses_,
            FoundRowCount_,
            FoundDataWeight_,
            cpuTime,
            DecompressionCpuTime_,
            BlockReadOptions_.ReadSessionId);
    }

private:
    struct TPartitionSessionInfo
    {
        const TPartitionSnapshotPtr& PartitionSnapshot;
        int StartKeyIndex;
        int EndKeyIndex;
        std::vector<TFuture<void>> AsyncReadSessions;
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
    const TClientBlockReadOptions& BlockReadOptions_;
    const TSharedRange<TUnversionedRow> LookupKeys_;
    TSharedRange<TUnversionedRow> ChunkLookupKeys_;
    // Holds references to lookup tables.
    TConcurrentCache<TCachedRow>::TLookuper CacheLookuper_;
    std::vector<TConcurrentCache<TCachedRow>::TCachedItemRef> RowsFromCache_;
    ui32 StoreFlushIndex_;
    TTimestamp RetainedTimestamp_;

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
    size_t FoundDataWeight_ = 0;
    int UnmergedRowCount_ = 0;
    size_t UnmergedDataWeight_ = 0;
    TDuration DecompressionCpuTime_;

    std::vector<TFuture<void>> CreateReadSessions(
        TReadSessionList* sessions,
        const std::vector<ISortedStorePtr>& stores,
        const TSharedRange<TLegacyKey>& keys,
        bool produceAllValues)
    {
        sessions->clear();

        // NB: Will remain empty for in-memory tables.
        std::vector<TFuture<void>> asyncFutures;
        for (const auto& store : stores) {
            YT_LOG_DEBUG("Creating reader (Store: %v, KeysCount: %v, ReadSessionId: %v)",
                store->GetId(),
                keys.Size(),
                BlockReadOptions_.ReadSessionId);

            auto reader = store->CreateReader(
                TabletSnapshot_,
                keys,
                produceAllValues ? AllCommittedTimestamp : Timestamp_,
                produceAllValues || ProduceAllVersions_,
                produceAllValues ? UniversalColumnFilter : ColumnFilter_,
                BlockReadOptions_);
            auto future = reader->Open();
            if (auto optionalError = future.TryGet()) {
                optionalError->ThrowOnError();
            } else {
                asyncFutures.emplace_back(std::move(future));
            }
            sessions->emplace_back(std::move(reader));
        }

        return asyncFutures;
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

        auto asyncReadSessions = CreateReadSessions(
            &PartitionSessions_[sessionIndex],
            partitionSnapshot->Stores,
            ChunkLookupKeys_.Slice(*startChunkKeyIndex, endChunkKeyIndex),
            UseLookupCache_);

        *startChunkKeyIndex = endChunkKeyIndex;
        *currentIt = nextIt;

        return {partitionSnapshot, startKeyIndex, endKeyIndex, std::move(asyncReadSessions)};
    }

    void LookupInPartition(
        const TPartitionSnapshotPtr& partitionSnapshot,
        int startKeyIndex,
        int endKeyIndex,
        TReadSessionList* partitionSessions,
        const std::function<void(TVersionedRow, TTimestamp)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        std::vector<TVersionedRow> versionedRows;

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                auto row = session.FetchRow();
                onPartialRow(row, MaxTimestamp);
                if (UseLookupCache_) {
                    versionedRows.push_back(row);
                }
            }
        };

        TConcurrentCache<TCachedRow>::TInserter inserter;

        if (UseLookupCache_) {
            inserter = TabletSnapshot_->RowCache->Cache.GetInserter();
        }

        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            // Need to insert rows into cache even from active dynamic store.
            // Otherwise cache misses will occur.
            // Process dynamic store rows firstly.
            processSessions(DynamicEdenSessions_);

            auto cachedItemRef = std::move(RowsFromCache_[index]);

            if (auto cachedItemHead = cachedItemRef.Get()) {
                auto cachedItem = GetLatestRow(cachedItemHead);

                if (Timestamp_ < cachedItem->RetainedTimestamp) {
                    THROW_ERROR_EXCEPTION("Timestamp %llx is less than retained timestamp %llx of cached row in tablet %v",
                        Timestamp_,
                        cachedItem->RetainedTimestamp,
                        TabletSnapshot_->TabletId);
                }

                auto minDynamicTimestamp = MaxTimestamp;
                for (auto row : versionedRows) {
                    minDynamicTimestamp = std::min(minDynamicTimestamp, GetMinTimestamp(row));
                }

                YT_LOG_TRACE("Using row from cache (Row: %v, Revision: %v, MinDynamicTimestamp: %v, ReadTimestamp: %v)",
                    cachedItem->GetVersionedRow(),
                    cachedItem->Revision.load(),
                    minDynamicTimestamp,
                    Timestamp_);

                // Consider only versions before versions from dynamic rows.
                onPartialRow(cachedItem->GetVersionedRow(), minDynamicTimestamp);

                // Reinsert row here.
                YT_LOG_TRACE("Reinserting row (Row: %v)", cachedItem->GetVersionedRow());
                auto lookupTable = inserter.GetTable();
                if (lookupTable == cachedItemRef.Origin) {
                    cachedItemRef.Update(std::move(cachedItem), cachedItemHead.Get());
                } else {
                    lookupTable->Insert(std::move(cachedItem));
                }
            } else {
                processSessions(*partitionSessions);
                processSessions(ChunkEdenSessions_);

                if (UseLookupCache_) {
                    auto cachedItem = BuildCachedRow(
                        &TabletSnapshot_->RowCache->Allocator,
                        versionedRows,
                        RetainedTimestamp_);

                    if (cachedItem) {
                        YT_VERIFY(cachedItem->GetVersionedRow().GetKeyCount() > 0);

                        auto revision = StoreFlushIndex_;
                        cachedItem->Revision.store(revision, std::memory_order_release);

                        YT_LOG_TRACE("Populating cache (Row: %v, Revision: %v)", cachedItem->GetVersionedRow(), revision);
                        inserter.GetTable()->Insert(cachedItem);

                        auto flushIndex = TabletSnapshot_->RowCache->FlushIndex.load(std::memory_order_acquire);

                        // Row revision is equal to flushRevsion if last passive dynamic store is already flushing.
                        if (revision >= flushIndex) {
                            cachedItem->Revision.compare_exchange_strong(revision, std::numeric_limits<ui32>::max());
                        }

                        ++CacheInserts_;
                    }
                }
            }

            versionedRows.clear();

            auto statistics = onRow();
            FoundRowCount_ += statistics.first;
            FoundDataWeight_ += statistics.second;
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

    void ParallelLookupInPartitions(
        std::vector<TFuture<void>> asyncReadSessions,
        const std::function<void(TVersionedRow, TTimestamp)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        YT_VERIFY(TabletSnapshot_->Config->MaxParallelPartitionLookups);
        PartitionSessions_.resize(*TabletSnapshot_->Config->MaxParallelPartitionLookups);

        auto currentIt = LookupKeys_.Begin();
        int startChunkKeyIndex = 0;
        while (currentIt != LookupKeys_.End()) {
            std::vector<TPartitionSessionInfo> partitionSessionInfos;

            while (partitionSessionInfos.size() < *TabletSnapshot_->Config->MaxParallelPartitionLookups &&
                currentIt != LookupKeys_.End())
            {
                auto sessionInfo = CreatePartitionSession(&currentIt, &startChunkKeyIndex, partitionSessionInfos.size());

                for (auto& readSession : sessionInfo.AsyncReadSessions) {
                    asyncReadSessions.push_back(std::move(readSession));
                }
                sessionInfo.AsyncReadSessions.clear();

                partitionSessionInfos.push_back(std::move(sessionInfo));
            }

            YT_LOG_DEBUG("Starting parallel lookups "
                "(PartitionCount: %v, MaxPartitionCount: %v, ReadSessionId: %v, partitionSessionInfos: %v)",
                partitionSessionInfos.size(),
                *TabletSnapshot_->Config->MaxParallelPartitionLookups,
                BlockReadOptions_.ReadSessionId,
                MakeFormattableView(partitionSessionInfos, [] (auto* builder, const TPartitionSessionInfo& partitionInfo) {
                    builder->AppendFormat("{Id: %v, KeyCount: %v}",
                        partitionInfo.PartitionSnapshot->Id,
                        partitionInfo.EndKeyIndex - partitionInfo.StartKeyIndex);
                }));

            WaitFor(AllSucceeded(asyncReadSessions))
                .ThrowOnError();
            asyncReadSessions.clear();

            // NB: Processing of partitions is sequential.
            for (int index = 0; index < partitionSessionInfos.size(); ++index) {
                LookupInPartition(
                    partitionSessionInfos[index].PartitionSnapshot,
                    partitionSessionInfos[index].StartKeyIndex,
                    partitionSessionInfos[index].EndKeyIndex,
                    &PartitionSessions_[index],
                    onPartialRow,
                    onRow);
            }
        }
    }
};

static NTableClient::TColumnFilter DecodeColumnFilter(
    std::unique_ptr<NTableClient::NProto::TColumnFilter> protoColumnFilter,
    int columnCount)
{
    auto columnFilter = !protoColumnFilter
        ? TColumnFilter()
        : TColumnFilter(FromProto<TColumnFilter::TIndexes>(protoColumnFilter->indexes()));
    ValidateColumnFilter(columnFilter, columnCount);
    return columnFilter;
}

void LookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    tabletSnapshot->WaitOnLocks(timestamp);

    NTableClient::NProto::TReqLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTableClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema->GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(*tabletSnapshot->PhysicalSchema->ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TSchemafulRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema->Columns().size(),
        tabletSnapshot->PhysicalSchema->GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    THazardPtrFlushGuard flushGuard;

    TLookupSession session(
        std::move(tabletSnapshot),
        timestamp,
        false,
        useLookupCache,
        columnFilter,
        blockReadOptions,
        std::move(lookupKeys));

    session.Run(
        [&] (TVersionedRow partialRow, TTimestamp timestamp) {
            merger.AddPartialRow(partialRow, timestamp);
        },
        [&] {
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteSchemafulRow(mergedRow);
            return std::make_pair(static_cast<bool>(mergedRow), GetDataWeight(mergedRow));
        });
}

void VersionedLookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TRetentionConfigPtr retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    tabletSnapshot->WaitOnLocks(timestamp);

    NTableClient::NProto::TReqVersionedLookupRows req;
    reader->ReadMessage(&req);

    auto columnFilter = DecodeColumnFilter(
        std::unique_ptr<NTableClient::NProto::TColumnFilter>(req.release_column_filter()),
        tabletSnapshot->PhysicalSchema->GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(*tabletSnapshot->PhysicalSchema->ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TVersionedRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema->GetColumnCount(),
        tabletSnapshot->PhysicalSchema->GetKeyColumnCount(),
        columnFilter,
        std::move(retentionConfig),
        timestamp,
        MinTimestamp,
        tabletSnapshot->ColumnEvaluator,
        true,
        false);

    THazardPtrFlushGuard flushGuard;

    // NB: TLookupSession captures TColumnFilter by const ref.
    static const TColumnFilter UniversalColumnFilter;
    TLookupSession session(
        std::move(tabletSnapshot),
        timestamp,
        true,
        useLookupCache,
        UniversalColumnFilter,
        blockReadOptions,
        std::move(lookupKeys));

    session.Run(
        [&] (TVersionedRow partialRow, TTimestamp timestamp) {
            merger.AddPartialRow(partialRow, timestamp);
        },
        [&] {
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteVersionedRow(mergedRow);
            return std::make_pair(static_cast<bool>(mergedRow), GetDataWeight(mergedRow));
        });
}

void ExecuteSingleRead(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TRetentionConfigPtr retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    auto command = reader->ReadCommand();
    switch (command) {
        case EWireProtocolCommand::LookupRows:
            LookupRows(
                std::move(tabletSnapshot),
                timestamp,
                useLookupCache,
                blockReadOptions,
                reader,
                writer);
            break;

        case EWireProtocolCommand::VersionedLookupRows:
            VersionedLookupRows(
                std::move(tabletSnapshot),
                timestamp,
                useLookupCache,
                blockReadOptions,
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
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    bool useLookupCache,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TRetentionConfigPtr retentionConfig,
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
            blockReadOptions,
            retentionConfig,
            reader,
            writer);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
