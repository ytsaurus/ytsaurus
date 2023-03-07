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

    TMonotonicCounter CacheHits;
    TMonotonicCounter CacheMisses;
    TMonotonicCounter RowCount;
    TMonotonicCounter DataWeight;
    TMonotonicCounter UnmergedRowCount;
    TMonotonicCounter UnmergedDataWeight;
    TMonotonicCounter CpuTime;
    TMonotonicCounter DecompressionCpuTime;
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
        const TString& user,
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
        , Merger_(
            New<TRowBuffer>(TLookupSessionBufferTag()),
            TabletSnapshot_->PhysicalSchema.GetColumnCount(),
            TabletSnapshot_->PhysicalSchema.GetKeyColumnCount(),
            UniversalColumnFilter,
            TabletSnapshot_->Config,
            AllCommittedTimestamp,
            TabletSnapshot_->RetainedTimestamp,
            TabletSnapshot_->ColumnEvaluator,
            false /*lookup*/,
            false /*mergeRowsOnFlush*/)
    {
        if (TabletSnapshot_->IsProfilingEnabled()) {
            Tags_ = AddUserTag(user, TabletSnapshot_->ProfilerTags);
        }
    }

    void Run(
        const std::function<void(TVersionedRow)>& onPartialRow,
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

        for (auto&& store : edenStores) {
            if (store->IsDynamic()) {
                dynamicEdenStores.push_back(store);
            } else {
                chunkEdenStores.push_back(store);
            }
        }

        CreateReadSessions(&DynamicEdenSessions_, dynamicEdenStores, LookupKeys_);
        CreateReadSessions(&ChunkEdenSessions_, chunkEdenStores, ChunkLookupKeys_);

        auto currentIt = LookupKeys_.Begin();
        int startChunkKeyIndex = 0;
        while (currentIt != LookupKeys_.End()) {
            auto nextPartitionIt = std::upper_bound(
                TabletSnapshot_->PartitionList.begin(),
                TabletSnapshot_->PartitionList.end(),
                *currentIt,
                [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                    return lhs < rhs->PivotKey;
                });
            YT_VERIFY(nextPartitionIt != TabletSnapshot_->PartitionList.begin());
            auto nextIt = nextPartitionIt == TabletSnapshot_->PartitionList.end()
                ? LookupKeys_.End()
                : std::lower_bound(currentIt, LookupKeys_.End(), (*nextPartitionIt)->PivotKey);

            startChunkKeyIndex = LookupInPartition(
                *(nextPartitionIt - 1),
                currentIt - LookupKeys_.Begin(),
                nextIt - LookupKeys_.Begin(),
                startChunkKeyIndex,
                onPartialRow,
                onRow);

            currentIt = nextIt;
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
    class TReadSession
    {
    public:
        explicit TReadSession(IVersionedReaderPtr reader)
            : Reader_(std::move(reader))
        {
            Rows_.reserve(RowBufferCapacity);
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
    TVersionedRowMerger Merger_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList DynamicEdenSessions_;
    TReadSessionList ChunkEdenSessions_;
    TReadSessionList PartitionSessions_;

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

    void CreateReadSessions(
        TReadSessionList* sessions,
        const std::vector<ISortedStorePtr>& stores,
        const TSharedRange<TKey>& keys)
    {
        sessions->clear();

        // NB: Will remain empty for in-memory tables.
        std::vector<TFuture<void>> asyncFutures;
        for (const auto& store : stores) {
            YT_LOG_DEBUG("Creating reader (Store: %v, KeysCount: %v)", store->GetId(), keys.Size());

            bool populateCache = UseLookupCache_;
            auto reader = store->CreateReader(
                TabletSnapshot_,
                keys,
                Timestamp_,
                populateCache ? true : ProduceAllVersions_,
                populateCache ? UniversalColumnFilter : ColumnFilter_,
                BlockReadOptions_);
            auto future = reader->Open();
            auto optionalError = future.TryGet();
            if (optionalError) {
                optionalError->ThrowOnError();
            } else {
                asyncFutures.emplace_back(std::move(future));
            }
            sessions->emplace_back(std::move(reader));
        }

        if (!asyncFutures.empty()) {
            WaitFor(Combine(asyncFutures))
                .ThrowOnError();
        }
    }

    int LookupInPartition(
        const TPartitionSnapshotPtr& partitionSnapshot,
        int startKeyIndex,
        int endKeyIndex,
        int startChunkKeyIndex,
        const std::function<void(TVersionedRow)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        int endChunkKeyIndex = startChunkKeyIndex;
        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            endChunkKeyIndex += !RowsFromCache_[index];
        }

        CreateReadSessions(
            &PartitionSessions_,
            partitionSnapshot->Stores,
            ChunkLookupKeys_.Slice(startChunkKeyIndex, endChunkKeyIndex));

        auto processSessions = [&] (TReadSessionList& sessions, bool populateCache) {
            for (auto& session : sessions) {
                auto row = session.FetchRow();
                onPartialRow(row);
                if (populateCache) {
                    Merger_.AddPartialRow(row);
                }
            }
        };

        std::optional<TConcurrentCache<TCachedRow>::TInsertAccessor> accessor;

        bool populateCache = UseLookupCache_;

        if (populateCache) {
            accessor.emplace(TabletSnapshot_->RowCache->Cache.GetInsertAccessor());
        }

        for (int index = startKeyIndex; index < endKeyIndex; ++index) {
            auto rowFromCache = std::move(RowsFromCache_[index]);
            if (rowFromCache) {
                YT_LOG_TRACE("Using row from cache (Row: %v)", rowFromCache->GetVersionedRow());
                onPartialRow(rowFromCache->GetVersionedRow());
            } else {
                processSessions(PartitionSessions_, populateCache);
                processSessions(ChunkEdenSessions_, populateCache);

                if (accessor) {
                    auto mergedRow = Merger_.BuildMergedRow();
                    if (mergedRow) {
                        auto cachedRow = CachedRowFromVersionedRow(
                            &TabletSnapshot_->RowCache->Allocator,
                            mergedRow);

                        YT_LOG_TRACE("Populating cache (Row: %v)", cachedRow->GetVersionedRow());

                        accessor->Insert(std::move(cachedRow));
                    }
                }
            }

            processSessions(DynamicEdenSessions_, false);

            auto statistics = onRow();
            FoundRowCount_ += statistics.first;
            FoundDataWeight_ += statistics.second;
        }

        UpdateUnmergedStatistics(PartitionSessions_);
        return endChunkKeyIndex;
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
    const TString& user,
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
        tabletSnapshot->PhysicalSchema.GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(tabletSnapshot->PhysicalSchema.ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TSchemafulRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema.Columns().size(),
        tabletSnapshot->PhysicalSchema.GetKeyColumnCount(),
        columnFilter,
        tabletSnapshot->ColumnEvaluator);

    THazardPtrFlushGuard flushGuard;

    TLookupSession session(
        std::move(tabletSnapshot),
        timestamp,
        user,
        false,
        useLookupCache,
        columnFilter,
        blockReadOptions,
        std::move(lookupKeys));

    session.Run(
        [&] (TVersionedRow partialRow) { merger.AddPartialRow(partialRow, timestamp); },
        [&] {
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteSchemafulRow(mergedRow);
            return std::make_pair(static_cast<bool>(mergedRow), GetDataWeight(mergedRow));
        });
}

void VersionedLookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TString& user,
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
        tabletSnapshot->PhysicalSchema.GetColumnCount());
    auto schemaData = TWireProtocolReader::GetSchemaData(tabletSnapshot->PhysicalSchema.ToKeys());
    auto lookupKeys = reader->ReadSchemafulRowset(schemaData, false);

    TVersionedRowMerger merger(
        New<TRowBuffer>(TLookupSessionBufferTag()),
        tabletSnapshot->PhysicalSchema.GetColumnCount(),
        tabletSnapshot->PhysicalSchema.GetKeyColumnCount(),
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
        user,
        true,
        useLookupCache,
        UniversalColumnFilter,
        blockReadOptions,
        std::move(lookupKeys));

    session.Run(
        [&] (TVersionedRow partialRow) { merger.AddPartialRow(partialRow); },
        [&] {
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteVersionedRow(mergedRow);
            return std::make_pair(static_cast<bool>(mergedRow), GetDataWeight(mergedRow));
        });
}

void ExecuteSingleRead(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TString& user,
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
                user,
                useLookupCache,
                blockReadOptions,
                reader,
                writer);
            break;

        case EWireProtocolCommand::VersionedLookupRows:
            VersionedLookupRows(
                std::move(tabletSnapshot),
                timestamp,
                user,
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
    const TString& user,
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
            user,
            useLookupCache,
            blockReadOptions,
            retentionConfig,
            reader,
            writer);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
