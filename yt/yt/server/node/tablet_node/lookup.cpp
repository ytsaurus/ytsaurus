#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
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

struct TLookupCounters
{
    explicit TLookupCounters(const TTagIdList& list)
        : CacheHits("/lookup/cache_hits", list)
        , CacheMisses("/lookup/cache_misses", list)
        , RowCount("/lookup/row_count", list)
        , DataWeight("/lookup/data_weight", list)
        , UnmergedRowCount("/lookup/unmerged_row_count", list)
        , UnmergedDataWeight("/lookup/unmerged_data_weight", list)
        , CpuTime("/lookup/cpu_time", list)
        , DecompressionCpuTime("/lookup/decompression_cpu_time", list)
        , ChunkReaderStatisticsCounters("/lookup/chunk_reader_statistics", list)
    { }

    TShardedMonotonicCounter CacheHits;
    TShardedMonotonicCounter CacheMisses;
    TShardedMonotonicCounter RowCount;
    TShardedMonotonicCounter DataWeight;
    TShardedMonotonicCounter UnmergedRowCount;
    TShardedMonotonicCounter UnmergedDataWeight;
    TShardedMonotonicCounter CpuTime;
    TShardedMonotonicCounter DecompressionCpuTime;
    TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

using TLookupProfilerTrait = TTagListProfilerTrait<TLookupCounters>;

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
    {
        if (TabletSnapshot_->IsProfilingEnabled()) {
            Tags_ = AddCurrentUserTag(TabletSnapshot_->ProfilerTags);
        }
    }

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

        std::vector<TUnversionedRow> chunkLookupKeys;

        // Lookup in dynamic stores always and merge with cache.
        if (UseLookupCache_) {
            YT_LOG_DEBUG("Looking up in row cache");
            auto accessor = TabletSnapshot_->RowCache->Cache.GetLookupAccessor();
            for (auto key : LookupKeys_) {
                auto found = accessor.Lookup(key, true);

                if (found) {
                    YT_VERIFY(found->GetVersionedRow().GetKeyCount() > 0);
                    ++CacheHits_;
                    YT_LOG_TRACE("Row found (Key: %v, Row: %v)",
                        key,
                        found->GetVersionedRow());
                } else {
                    chunkLookupKeys.push_back(key);
                    ++CacheMisses_;
                    YT_LOG_TRACE("Row not found (Key: %v)", key);
                }
                RowsFromCache_.push_back(std::move(found));
            }
        } else {
            chunkLookupKeys = LookupKeys_.ToVector();
            RowsFromCache_.resize(LookupKeys_.Size(), nullptr);
        }

        ChunkLookupKeys_ = MakeSharedRange(chunkLookupKeys, LookupKeys_);

        auto edenStores = TabletSnapshot_->GetEdenStores();
        std::vector<ISortedStorePtr> dynamicEdenStores;
        std::vector<ISortedStorePtr> chunkEdenStores;

        auto unflushedTimestamp = MaxTimestamp;
        auto latestTimestamp = MinTimestamp;

        for (const auto& store : edenStores) {
            if (store->IsDynamic()) {
                dynamicEdenStores.push_back(store);
                unflushedTimestamp = std::min(unflushedTimestamp, store->GetMinTimestamp());
                latestTimestamp = std::min(latestTimestamp, store->GetMaxTimestamp());
            } else {
                chunkEdenStores.push_back(store);
            }
        }

        UnflushedTimestamp_ = unflushedTimestamp;

        auto majorTimestamp = std::min(unflushedTimestamp, TabletSnapshot_->RetainedTimestamp);

        CacheRowMerger_ = std::make_unique<TVersionedRowMerger>(
            New<TRowBuffer>(TLookupSessionBufferTag()),
            TabletSnapshot_->PhysicalSchema->GetColumnCount(),
            TabletSnapshot_->PhysicalSchema->GetKeyColumnCount(),
            UniversalColumnFilter,
            TabletSnapshot_->Config,
            latestTimestamp, // compaction timestamp
            majorTimestamp,
            TabletSnapshot_->ColumnEvaluator,
            /*lookup*/ false,
            /*mergeRowsOnFlush*/ false);

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

        auto cpuTime = timer.GetElapsedValue();

        if (IsProfilingEnabled()) {
            auto& counters = GetLocallyGloballyCachedValue<TLookupProfilerTrait>(Tags_);
            TabletNodeProfiler.Increment(counters.CacheHits, CacheHits_);
            TabletNodeProfiler.Increment(counters.CacheMisses, CacheMisses_);
            TabletNodeProfiler.Increment(counters.RowCount, FoundRowCount_);
            TabletNodeProfiler.Increment(counters.DataWeight, FoundDataWeight_);
            TabletNodeProfiler.Increment(counters.UnmergedRowCount, UnmergedRowCount_);
            TabletNodeProfiler.Increment(counters.UnmergedDataWeight, UnmergedDataWeight_);
            TabletNodeProfiler.Increment(counters.CpuTime, cpuTime);
            TabletNodeProfiler.Increment(counters.DecompressionCpuTime, DurationToValue(DecompressionCpuTime_));
            counters.ChunkReaderStatisticsCounters.Increment(TabletNodeProfiler, BlockReadOptions_.ChunkReaderStatistics);
        }

        YT_LOG_DEBUG("Tablet lookup completed (TabletId: %v, CellId: %v, CacheHits: %v, CacheMisses: %v, "
            "FoundRowCount: %v, FoundDataWeight: %v, CpuTime: %v, DecompressionCpuTime: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            CacheHits_,
            CacheMisses_,
            FoundRowCount_,
            FoundDataWeight_,
            ValueToDuration(cpuTime),
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
        {
            Rows_.reserve(RowBufferCapacity);
        }

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
            if (RowIndex_ >= Rows_.size()) {
                RowIndex_ = 0;
                while (true) {
                    YT_VERIFY(Reader_->Read(&Rows_));
                    if (!Rows_.empty()) {
                        break;
                    }
                    WaitFor(Reader_->GetReadyEvent())
                        .ThrowOnError();
                }
            }
            return Rows_[RowIndex_];
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

        std::vector<TVersionedRow> Rows_;

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
    std::vector<TCachedRowPtr> RowsFromCache_;
    std::unique_ptr<TVersionedRowMerger> CacheRowMerger_;
    TTimestamp UnflushedTimestamp_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList DynamicEdenSessions_;
    TReadSessionList ChunkEdenSessions_;
    SmallVector<TReadSessionList, MaxParallelPartitionLookupsLimit> PartitionSessions_;

    int CacheHits_ = 0;
    int CacheMisses_ = 0;
    int FoundRowCount_ = 0;
    size_t FoundDataWeight_ = 0;
    int UnmergedRowCount_ = 0;
    size_t UnmergedDataWeight_ = 0;
    TDuration DecompressionCpuTime_;

    TTagIdList Tags_;

    bool IsProfilingEnabled() const
    {
        return !Tags_.empty();
    }

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
        std::optional<TConcurrentCache<TCachedRow>::TInsertAccessor> accessor;

        if (UseLookupCache_) {
            accessor.emplace(TabletSnapshot_->RowCache->Cache.GetInsertAccessor());
        }

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                auto row = session.FetchRow();
                onPartialRow(row, MaxTimestamp);
                if (accessor) {
                    CacheRowMerger_->AddPartialRow(row);
                }
            }
        };

        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            auto rowFromCache = std::move(RowsFromCache_[index]);
            if (rowFromCache) {
                YT_LOG_TRACE("Using row from cache (Row: %v)", rowFromCache->GetVersionedRow());
                // Consider only versions before unflushed timestamp.
                onPartialRow(rowFromCache->GetVersionedRow(), UnflushedTimestamp_);
            } else {
                processSessions(*partitionSessions);
                processSessions(ChunkEdenSessions_);

                if (accessor) {
                    auto mergedRow = CacheRowMerger_->BuildMergedRow();
                    if (mergedRow) {
                        YT_VERIFY(mergedRow.GetKeyCount() > 0);

                        auto cachedRow = CachedRowFromVersionedRow(
                            &TabletSnapshot_->RowCache->Allocator,
                            mergedRow);

                        YT_LOG_TRACE("Populating cache (Row: %v)", cachedRow->GetVersionedRow());

                        accessor->Insert(std::move(cachedRow));
                    }
                }
            }

            for (auto& session : DynamicEdenSessions_) {
                auto row = session.FetchRow();
                onPartialRow(row, MaxTimestamp);

                // Row not present in cache but present in dynamic store.
                if (row && accessor && !rowFromCache) {
                    auto cachedRow = CachedKeyFromVersionedRow(
                        &TabletSnapshot_->RowCache->Allocator,
                        row);

                    YT_LOG_TRACE("Populating cache (Row: %v)", cachedRow->GetVersionedRow());

                    accessor->Insert(std::move(cachedRow));
                }
            }

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
