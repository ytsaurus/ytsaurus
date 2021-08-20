#include "private.h"
#include "tablet.h"
#include "tablet_profiling.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/core/profiling/profile_manager.h>
#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/misc/farm_hash.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/syncmap/map.h>

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TString HideDigits(const TString& path)
{
    TString pathCopy = path;
    for (auto& c : pathCopy) {
        if (std::isdigit(c)) {
            c = '_';
        }
    }
    return pathCopy;
}

////////////////////////////////////////////////////////////////////////////////

TLookupCounters::TLookupCounters(
    const TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : CacheHits(profiler.Counter("/lookup/cache_hits"))
    , CacheOutdated(profiler.Counter("/lookup/cache_outdated"))
    , CacheMisses(profiler.Counter("/lookup/cache_misses"))
    , CacheInserts(profiler.Counter("/lookup/cache_inserts"))
    , RowCount(profiler.Counter("/lookup/row_count"))
    , MissingKeyCount(profiler.Counter("/lookup/missing_key_count"))
    , DataWeight(profiler.Counter("/lookup/data_weight"))
    , UnmergedRowCount(profiler.Counter("/lookup/unmerged_row_count"))
    , UnmergedDataWeight(profiler.Counter("/lookup/unmerged_data_weight"))
    , CpuTime(profiler.TimeCounter("/lookup/cpu_time"))
    , DecompressionCpuTime(profiler.TimeCounter("/lookup/decompression_cpu_time"))
    , LookupDuration(profiler.Histogram(
        "/lookup/duration",
        TDuration::MicroSeconds(1),
        TDuration::Seconds(10)))
    , ChunkReaderStatisticsCounters(profiler.WithPrefix("/lookup/chunk_reader_statistics"))
    , HunkChunkReaderCounters(profiler.WithPrefix("/lookup/hunks"), schema)
{ }

////////////////////////////////////////////////////////////////////////////////

TSelectCpuCounters::TSelectCpuCounters(
    const TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : CpuTime(profiler.TimeCounter("/select/cpu_time"))
    , ChunkReaderStatisticsCounters(profiler.WithPrefix("/select/chunk_reader_statistics"))
    , HunkChunkReaderCounters(profiler.WithPrefix("/select/hunks"), schema)
{ }

TSelectReadCounters::TSelectReadCounters(const TProfiler& profiler)
    : RowCount(profiler.Counter("/select/row_count"))
    , DataWeight(profiler.Counter("/select/data_weight"))
    , UnmergedRowCount(profiler.Counter("/select/unmerged_row_count"))
    , UnmergedDataWeight(profiler.Counter("/select/unmerged_data_weight"))
    , DecompressionCpuTime(profiler.TimeCounter("/select/decompression_cpu_time"))
    , SelectDuration(profiler.Histogram(
        "/select/duration",
        TDuration::MicroSeconds(1),
        TDuration::Seconds(10)))
{ }

////////////////////////////////////////////////////////////////////////////////

TWriteCounters::TWriteCounters(const TProfiler& profiler)
    : RowCount(profiler.Counter("/write/row_count"))
    , DataWeight(profiler.Counter("/write/data_weight"))
{ }

////////////////////////////////////////////////////////////////////////////////

TCommitCounters::TCommitCounters(const TProfiler& profiler)
    : RowCount(profiler.Counter("/commit/row_count"))
    , DataWeight(profiler.Counter("/commit/data_weight"))
{ }

////////////////////////////////////////////////////////////////////////////////

TRemoteDynamicStoreReadCounters::TRemoteDynamicStoreReadCounters(const TProfiler& profiler)
    : RowCount(profiler.Counter("/dynamic_store_read/row_count"))
    , DataWeight(profiler.Counter("/dynamic_store_read/data_weight"))
    , CpuTime(profiler.TimeCounter("/dynamic_store_read/cpu_time"))
    , SessionRowCount(profiler.Summary("/dynamic_store_read/session_row_count"))
    , SessionDataWeight(profiler.Summary("/dynamic_store_read/session_data_weight"))
    , SessionWallTime(profiler.Timer("/dynamic_store_read/session_wall_time"))
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkReadCounters::TChunkReadCounters(
    const TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : CompressedDataSize(profiler.Counter("/chunk_reader/compressed_data_size"))
    , UnmergedDataWeight(profiler.Counter("/chunk_reader/unmerged_data_weight"))
    , DecompressionCpuTime(profiler.TimeCounter("/chunk_reader/decompression_cpu_time"))
    , ChunkReaderStatisticsCounters(profiler.WithPrefix("/chunk_reader_statistics"))
    , HunkChunkReaderCounters(profiler.WithPrefix("/chunk_reader/hunks"), schema)
{ }

TChunkWriteCounters::TChunkWriteCounters(
    const TProfiler& profiler,
    const TTableSchemaPtr& schema)
    : ChunkWriterCounters(profiler.WithPrefix("/chunk_writer"))
    , HunkChunkWriterCounters(
        profiler.WithPrefix("/chunk_writer/hunks"),
        schema)
{ }

////////////////////////////////////////////////////////////////////////////////

TTabletCounters::TTabletCounters(const TProfiler& profiler)
    : OverlappingStoreCount(profiler.GaugeSummary("/tablet/overlapping_store_count"))
    , EdenStoreCount(profiler.GaugeSummary("/tablet/eden_store_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TReplicaCounters::TReplicaCounters(const TProfiler& profiler)
    : LagRowCount(profiler.WithDense().Gauge("/replica/lag_row_count"))
    , LagTime(profiler.WithDense().TimeGaugeSummary("/replica/lag_time"))
    , ReplicationThrottleTime(profiler.Timer("/replica/replication_throttle_time"))
    , ReplicationTransactionStartTime(profiler.Timer("/replica/replication_transaction_start_time"))
    , ReplicationTransactionCommitTime(profiler.Timer("/replica/replication_transaction_commit_time"))
    , ReplicationRowsReadTime(profiler.Timer("/replica/replication_rows_read_time"))
    , ReplicationRowsWriteTime(profiler.Timer("/replica/replication_rows_write_time"))
    , ReplicationBatchRowCount(profiler.Summary("/replica/replication_batch_row_count"))
    , ReplicationBatchDataWeight(profiler.Summary("/replica/replication_batch_data_weight"))
    , ReplicationRowCount(profiler.WithDense().Counter("/replica/replication_row_count"))
    , ReplicationDataWeight(profiler.WithDense().Counter("/replica/replication_data_weight"))
    , ReplicationErrorCount(profiler.WithDense().Counter("/replica/replication_error_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

TQueryServiceCounters::TQueryServiceCounters(const TProfiler& profiler)
    : Execute(profiler.WithPrefix("/execute"))
    , Multiread(profiler.WithPrefix("/multiread"))
{ }

////////////////////////////////////////////////////////////////////////////////

void TWriterProfiler::Profile(
    const TTabletSnapshotPtr& tabletSnapshot,
    EChunkWriteProfilingMethod method,
    bool failed)
{
    auto* counters = tabletSnapshot->TableProfiler->GetWriteCounters(method, failed);

    counters->ChunkWriterCounters.Increment(
        DataStatistics_,
        CodecStatistics_,
        tabletSnapshot->Settings.StoreWriterOptions->ReplicationFactor);
    
    counters->HunkChunkWriterCounters.Increment(
        HunkChunkWriterStatistics_,
        HunkChunkDataStatistics_,
        /*codecStatistics*/ {},
        tabletSnapshot->Settings.HunkWriterOptions->ReplicationFactor);
}

void TWriterProfiler::Update(const IMultiChunkWriterPtr& writer)
{
    if (writer) {
        DataStatistics_ += writer->GetDataStatistics();
        CodecStatistics_ += writer->GetCompressionStatistics();
    }
}

void TWriterProfiler::Update(const IChunkWriterBasePtr& writer)
{
    if (writer) {
        DataStatistics_ += writer->GetDataStatistics();
        CodecStatistics_ += writer->GetCompressionStatistics();
    }
}

void TWriterProfiler::Update(
    const IHunkChunkPayloadWriterPtr& hunkChunkWriter,
    const IHunkChunkWriterStatisticsPtr& hunkChunkWriterStatistics)
{
    if (hunkChunkWriter) {
        HunkChunkDataStatistics_ += hunkChunkWriter->GetDataStatistics();
    }
    HunkChunkWriterStatistics_ = hunkChunkWriterStatistics;
}

////////////////////////////////////////////////////////////////////////////////

void TReaderProfiler::Profile(
    const TTabletSnapshotPtr& tabletSnapshot,
    EChunkReadProfilingMethod method,
    bool failed)
{
    auto compressionCpuTime = CodecStatistics_.GetTotalDuration();

    auto counters = tabletSnapshot->TableProfiler->GetReadCounters(method, failed);

    counters->CompressedDataSize.Increment(DataStatistics_.compressed_data_size());
    counters->UnmergedDataWeight.Increment(DataStatistics_.data_weight());
    counters->DecompressionCpuTime.Add(compressionCpuTime);

    counters->ChunkReaderStatisticsCounters.Increment(ChunkReaderStatistics_);

    counters->HunkChunkReaderCounters.Increment(HunkChunkReaderStatistics_);
}

void TReaderProfiler::Update(
    const IVersionedReaderPtr& reader,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics,
    const IHunkChunkReaderStatisticsPtr& hunkChunkReaderStatistics)
{
    if (reader) {
        DataStatistics_ += reader->GetDataStatistics();
        CodecStatistics_ += reader->GetDecompressionStatistics();
    }
    ChunkReaderStatistics_ = chunkReaderStatistics;
    HunkChunkReaderStatistics_ = hunkChunkReaderStatistics;
}

void TReaderProfiler::SetCompressedDataSize(i64 compressedDataSize)
{
    DataStatistics_.set_compressed_data_size(compressedDataSize);
}

void TReaderProfiler::SetCodecStatistics(const TCodecStatistics& codecStatistics)
{
    CodecStatistics_ = codecStatistics;
}

void TReaderProfiler::SetChunkReaderStatistics(const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    ChunkReaderStatistics_ = chunkReaderStatistics;
}

////////////////////////////////////////////////////////////////////////////////

class TTabletProfilerManager
{
public:
    TTabletProfilerManager()
        : ConsumedTableTags_(TabletNodeProfiler.Gauge("/consumed_table_tags"))
    { }

    TTableProfilerPtr CreateTabletProfiler(
        EDynamicTableProfilingMode profilingMode,
        const TString& bundle,
        const TString& tablePath,
        const TString& tableTag,
        const TString& account,
        const TString& medium,
        TObjectId schemaId,
        const TTableSchemaPtr& schema)
    {
        auto guard = Guard(Lock_);

        TProfilerKey key;
        switch (profilingMode) {
            case EDynamicTableProfilingMode::Path:
                key = {profilingMode, bundle, tablePath, account, medium, schemaId};
                AllTables_.insert(tablePath);
                ConsumedTableTags_.Update(AllTables_.size());
                break;

            case EDynamicTableProfilingMode::Tag:
                key = {profilingMode, bundle, tableTag, account, medium, schemaId};
                break;

            case EDynamicTableProfilingMode::PathLetters:
                key = {profilingMode, bundle, HideDigits(tablePath), account, medium, schemaId};
                AllTables_.insert(HideDigits(tablePath));
                ConsumedTableTags_.Update(AllTables_.size());
                break;

            case EDynamicTableProfilingMode::Disabled:
            default:
                key = {profilingMode, bundle, "", account, medium, schemaId};
                break;
        }

        auto& profiler = Tables_[key];
        auto p = profiler.Lock();
        if (p) {
            return p;
        }


        TTagSet tableTagSet;
        tableTagSet.AddRequiredTag({"tablet_cell_bundle", bundle});

        TTagSet diskTagSet = tableTagSet;

        switch (profilingMode) {
            case EDynamicTableProfilingMode::Path:
                tableTagSet.AddTag({"table_path", tablePath}, -1);

                diskTagSet = tableTagSet;
                diskTagSet.AddTagWithChild({"account", account}, -1);
                diskTagSet.AddTagWithChild({"medium", medium}, -2);
                break;

            case EDynamicTableProfilingMode::Tag:
                tableTagSet.AddTag({"table_tag", tableTag}, -1);

                diskTagSet = tableTagSet;
                diskTagSet.AddTagWithChild({"account", account}, -1);
                diskTagSet.AddTagWithChild({"medium", medium}, -2);
                break;

            case EDynamicTableProfilingMode::PathLetters:
                tableTagSet.AddTag({"table_path", HideDigits(tablePath)}, -1);

                diskTagSet = tableTagSet;
                diskTagSet.AddTagWithChild({"account", account}, -1);
                diskTagSet.AddTagWithChild({"medium", medium}, -2);
                break;

            case EDynamicTableProfilingMode::Disabled:
            default:
                diskTagSet.AddTag({"account", account});
                diskTagSet.AddTag({"medium", medium});
                break;
        }

        auto tableProfiler = TabletNodeProfiler
            .WithHot()
            .WithSparse()
            .WithTags(tableTagSet);

        auto diskProfiler = TabletNodeProfiler
            .WithHot()
            .WithSparse()
            .WithTags(diskTagSet);

        p = New<TTableProfiler>(tableProfiler, diskProfiler, schema);
        profiler = p;
        return p;
    }

private:
    TSpinLock Lock_;

    THashSet<TString> AllTables_;
    TGauge ConsumedTableTags_;

    using TProfilerKey = std::tuple<EDynamicTableProfilingMode, TString, TString, TString, TString, TObjectId>;

    THashMap<TProfilerKey, TWeakPtr<TTableProfiler>> Tables_;
};

////////////////////////////////////////////////////////////////////////////////

TTableProfilerPtr CreateTableProfiler(
    EDynamicTableProfilingMode profilingMode,
    const TString& tabletCellBundle,
    const TString& tablePath,
    const TString& tableTag,
    const TString& account,
    const TString& medium,
    TObjectId schemaId,
    const TTableSchemaPtr& schema)
{
    return Singleton<TTabletProfilerManager>()->CreateTabletProfiler(
        profilingMode,
        tabletCellBundle,
        tablePath,
        tableTag,
        account,
        medium,
        schemaId,
        schema);
}

template <class TCounter>
TCounter* TTableProfiler::TUserTaggedCounter<TCounter>::Get(
    bool disabled,
    const std::optional<TString>& userTag,
    const TProfiler& profiler)
{
    if (disabled) {
        static TCounter counter;
        return &counter;
    }

    return Counters.FindOrInsert(userTag, [&] {
        if (userTag) {
            return TCounter{profiler.WithTag("user", *userTag)};
        } else {
            return TCounter{profiler};
        }
    }).first;
}

template <class TCounter>
TCounter* TTableProfiler::TUserTaggedCounter<TCounter>::Get(
    bool disabled,
    const std::optional<TString>& userTag,
    const TProfiler& profiler,
    const TTableSchemaPtr& schema)
{
    if (disabled) {
        static TCounter counter;
        return &counter;
    }
 
    return Counters.FindOrInsert(userTag, [&] {
        if (userTag) {
            return TCounter(profiler.WithTag("user", *userTag), schema);
        } else {
            return TCounter(profiler, schema);
        }
    }).first;
}


TTableProfiler::TTableProfiler(
    const TProfiler& profiler,
    const TProfiler& diskProfiler,
    TTableSchemaPtr schema)
    : Disabled_(false)
    , Profiler_(profiler)
    , Schema_(std::move(schema))
{
    for (auto method : TEnumTraits<EChunkWriteProfilingMethod>::GetDomainValues()) {
        ChunkWriteCounters_[method] = {
            TChunkWriteCounters(
                diskProfiler.WithTag("method", FormatEnum(method)),
                Schema_),
            TChunkWriteCounters(
                diskProfiler.WithTag("method", FormatEnum(method) + "_failed"),
                Schema_)
        };
    }

    for (auto method : TEnumTraits<EChunkReadProfilingMethod>::GetDomainValues()) {
        ChunkReadCounters_[method] = {
            TChunkReadCounters(
                diskProfiler.WithTag("method", FormatEnum(method)),
                Schema_),
            TChunkReadCounters(
                diskProfiler.WithTag("method", FormatEnum(method) + "_failed"),
                Schema_)
        };
    }

    for (auto kind : TEnumTraits<ETabletDistributedThrottlerKind>::GetDomainValues()) {
        ThrottlerWaitTimers_[kind] = profiler.Timer(
            "/tablet/" + CamelCaseToUnderscoreCase(ToString(kind)) + "_throttler_wait_time");
    }

    for (auto kind : TEnumTraits<ETabletDistributedThrottlerKind>::GetDomainValues()) {
        ThrottlerCounters_[kind] = profiler.Counter(
            "/tablet/throttled_" + CamelCaseToUnderscoreCase(ToString(kind)) + "_count");
    }
}

TTableProfilerPtr TTableProfiler::GetDisabled()
{
    return RefCountedSingleton<TTableProfiler>();
}

TTabletCounters TTableProfiler::GetTabletCounters()
{
    if (Disabled_) {
        return {};
    }

    return TTabletCounters{Profiler_};
}

TLookupCounters* TTableProfiler::GetLookupCounters(const std::optional<TString>& userTag)
{
    return LookupCounters_.Get(Disabled_, userTag, Profiler_, Schema_);
}

TWriteCounters* TTableProfiler::GetWriteCounters(const std::optional<TString>& userTag)
{
    return WriteCounters_.Get(Disabled_, userTag, Profiler_);
}

TCommitCounters* TTableProfiler::GetCommitCounters(const std::optional<TString>& userTag)
{
    return CommitCounters_.Get(Disabled_, userTag, Profiler_);
}

TSelectCpuCounters* TTableProfiler::GetSelectCpuCounters(const std::optional<TString>& userTag)
{
    return SelectCpuCounters_.Get(Disabled_, userTag, Profiler_, Schema_);
}

TSelectReadCounters* TTableProfiler::GetSelectReadCounters(const std::optional<TString>& userTag)
{
    return SelectReadCounters_.Get(Disabled_, userTag, Profiler_);
}

TRemoteDynamicStoreReadCounters* TTableProfiler::GetRemoteDynamicStoreReadCounters(const std::optional<TString>& userTag)
{
    return DynamicStoreReadCounters_.Get(Disabled_, userTag, Profiler_);
}

TQueryServiceCounters* TTableProfiler::GetQueryServiceCounters(const std::optional<TString>& userTag)
{
    return QueryServiceCounters_.Get(Disabled_, userTag, Profiler_);
}

TReplicaCounters TTableProfiler::GetReplicaCounters(const TString& cluster)
{
    if (Disabled_) {
        return {};
    }

    return TReplicaCounters{Profiler_.WithTag("replica_cluster", cluster)};
}

TChunkWriteCounters* TTableProfiler::GetWriteCounters(EChunkWriteProfilingMethod method, bool failed)
{
    return &ChunkWriteCounters_[method][failed ? 1 : 0];
}

TChunkReadCounters* TTableProfiler::GetReadCounters(EChunkReadProfilingMethod method, bool failed)
{
    return &ChunkReadCounters_[method][failed ? 1 : 0];
}

TEventTimer* TTableProfiler::GetThrottlerTimer(ETabletDistributedThrottlerKind kind)
{
    return &ThrottlerWaitTimers_[kind];
}

TCounter* TTableProfiler::GetThrottlerCounter(ETabletDistributedThrottlerKind kind)
{
    return &ThrottlerCounters_[kind];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
