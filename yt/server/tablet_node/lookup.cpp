#include "lookup.h"
#include "private.h"
#include "store.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"

#include <yt/server/tablet_node/config.h>

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
#include <yt/core/misc/variant.h>
#include <yt/core/misc/tls_cache.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NTabletNode {

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
        : RowCount("/lookup/row_count", list)
        , DataWeight("/lookup/data_weight", list)
        , UnmergedRowCount("/lookup/unmerged_row_count", list)
        , UnmergedDataWeight("/lookup/unmerged_data_weight", list)
        , CpuTime("/lookup/cpu_time", list)
        , DecompressionCpuTime("/lookup/decompression_cpu_time", list)
        , ChunkReaderStatisticsCounters("/lookup/chunk_reader_statistics", list)
    { }

    TMonotonicCounter RowCount;
    TMonotonicCounter DataWeight;
    TMonotonicCounter UnmergedRowCount;
    TMonotonicCounter UnmergedDataWeight;
    TMonotonicCounter CpuTime;
    TMonotonicCounter DecompressionCpuTime;
    TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

using TLookupProfilerTrait = TTabletProfilerTrait<TLookupCounters>;

////////////////////////////////////////////////////////////////////////////////

struct TLookupSessionBufferTag
{ };

class TLookupSession
{
public:
    TLookupSession(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        const TString& user,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        TSharedRange<TUnversionedRow> lookupKeys)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , ColumnFilter_(columnFilter)
        , BlockReadOptions_(blockReadOptions)
        , LookupKeys_(std::move(lookupKeys))
    {
        if (TabletSnapshot_->IsProfilingEnabled()) {
            Tags_ = AddUserTag(user, TabletSnapshot_->ProfilerTags);
        }
    }

    void Run(
        const std::function<void(TVersionedRow)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        LOG_DEBUG("Tablet lookup started (TabletId: %v, CellId: %v, KeyCount: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
            LookupKeys_.Size(),
            BlockReadOptions_.ReadSessionId);

        TCpuTimer timer;

        CreateReadSessions(&EdenSessions_, TabletSnapshot_->GetEdenStores(), LookupKeys_);

        auto currentIt = LookupKeys_.Begin();
        while (currentIt != LookupKeys_.End()) {
            auto nextPartitionIt = std::upper_bound(
                TabletSnapshot_->PartitionList.begin(),
                TabletSnapshot_->PartitionList.end(),
                *currentIt,
                [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                    return lhs < rhs->PivotKey;
                });
            YCHECK(nextPartitionIt != TabletSnapshot_->PartitionList.begin());
            auto nextIt = nextPartitionIt == TabletSnapshot_->PartitionList.end()
                ? LookupKeys_.End()
                : std::lower_bound(currentIt, LookupKeys_.End(), (*nextPartitionIt)->PivotKey);

            LookupInPartition(
                *(nextPartitionIt - 1),
                LookupKeys_.Slice(currentIt, nextIt),
                onPartialRow,
                onRow);

            currentIt = nextIt;
        }

        UpdateUnmergedStatistics(EdenSessions_);

        auto cpuTime = timer.GetElapsedValue();

        if (IsProfilingEnabled()) {
            auto& counters = GetLocallyGloballyCachedValue<TLookupProfilerTrait>(Tags_);
            TabletNodeProfiler.Increment(counters.RowCount, FoundRowCount_);
            TabletNodeProfiler.Increment(counters.DataWeight, FoundDataWeight_);
            TabletNodeProfiler.Increment(counters.UnmergedRowCount, UnmergedRowCount_);
            TabletNodeProfiler.Increment(counters.UnmergedDataWeight, UnmergedDataWeight_);
            TabletNodeProfiler.Increment(counters.CpuTime, cpuTime);
            TabletNodeProfiler.Increment(counters.DecompressionCpuTime, DurationToValue(DecompressionCpuTime_));
            counters.ChunkReaderStatisticsCounters.Increment(TabletNodeProfiler, BlockReadOptions_.ChunkReaderStatistics);
        }

        LOG_DEBUG("Tablet lookup completed (TabletId: %v, CellId: %v, FoundRowCount: %v, "
            "FoundDataWeight: %v, CpuTime: %v, DecompressionCpuTime: %v, ReadSessionId: %v)",
            TabletSnapshot_->TabletId,
            TabletSnapshot_->CellId,
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
                    YCHECK(Reader_->Read(&Rows_));
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
    const TColumnFilter& ColumnFilter_;
    const TClientBlockReadOptions& BlockReadOptions_;
    const TSharedRange<TUnversionedRow> LookupKeys_;

    static const int TypicalSessionCount = 16;
    using TReadSessionList = SmallVector<TReadSession, TypicalSessionCount>;
    TReadSessionList EdenSessions_;
    TReadSessionList PartitionSessions_;

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
            auto reader = store->CreateReader(
                TabletSnapshot_,
                keys,
                Timestamp_,
                ProduceAllVersions_,
                ColumnFilter_,
                BlockReadOptions_);
            auto future = reader->Open();
            auto maybeError = future.TryGet();
            if (maybeError) {
                maybeError->ThrowOnError();
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

    void LookupInPartition(
        const TPartitionSnapshotPtr& partitionSnapshot,
        const TSharedRange<TKey>& keys,
        const std::function<void(TVersionedRow)>& onPartialRow,
        const std::function<std::pair<bool, size_t>()>& onRow)
    {
        if (keys.Empty() || !partitionSnapshot) {
            return;
        }

        CreateReadSessions(&PartitionSessions_, partitionSnapshot->Stores, keys);

        auto processSessions = [&] (TReadSessionList& sessions) {
            for (auto& session : sessions) {
                onPartialRow(session.FetchRow());
            }
        };

        for (int index = 0; index < keys.Size(); ++index) {
            processSessions(PartitionSessions_);
            processSessions(EdenSessions_);

            auto statistics = onRow();
            FoundRowCount_ += statistics.first;
            FoundDataWeight_ += statistics.second;
        }

        UpdateUnmergedStatistics(PartitionSessions_);
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
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
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

    TLookupSession session(
        std::move(tabletSnapshot),
        timestamp,
        user,
        false,
        columnFilter,
        blockReadOptions,
        std::move(lookupKeys));

    session.Run(
        [&] (TVersionedRow partialRow) { merger.AddPartialRow(partialRow); },
        [&] {
            auto mergedRow = merger.BuildMergedRow();
            writer->WriteSchemafulRow(mergedRow);
            return std::make_pair(static_cast<bool>(mergedRow),GetDataWeight(mergedRow));
        });
}

void VersionedLookupRows(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TString& user,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    TRetentionConfigPtr retentionConfig,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
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

    // NB: TLookupSession captures TColumnFilter by const ref.
    static const TColumnFilter UniversalColumnFilter;
    TLookupSession session(
        std::move(tabletSnapshot),
        timestamp,
        user,
        true,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
