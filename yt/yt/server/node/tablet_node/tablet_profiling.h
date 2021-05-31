#pragma once

#include "public.h"
#include "yt/yt/library/profiling/sensor.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/ytalloc/core/misc/enum.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TLookupCounters
{
    TLookupCounters() = default;

    explicit TLookupCounters(const NProfiling::TProfiler& profiler)
        : CacheHits(profiler.Counter("/lookup/cache_hits"))
        , CacheOutdated(profiler.Counter("/lookup/cache_outdated"))
        , CacheMisses(profiler.Counter("/lookup/cache_misses"))
        , CacheInserts(profiler.Counter("/lookup/cache_inserts"))
        , RowCount(profiler.Counter("/lookup/row_count"))
        , DataWeight(profiler.Counter("/lookup/data_weight"))
        , UnmergedRowCount(profiler.Counter("/lookup/unmerged_row_count"))
        , UnmergedDataWeight(profiler.Counter("/lookup/unmerged_data_weight"))
        , CpuTime(profiler.TimeCounter("/lookup/cpu_time"))
        , DecompressionCpuTime(profiler.TimeCounter("/lookup/decompression_cpu_time"))
        , ChunkReaderStatisticsCounters(profiler.WithPrefix("/lookup/chunk_reader_statistics"))
        , LookupDuration(profiler.Histogram("/lookup/duration", TDuration::MicroSeconds(1), TDuration::Seconds(10)))
    { }

    NProfiling::TCounter CacheHits;
    NProfiling::TCounter CacheOutdated;
    NProfiling::TCounter CacheMisses;
    NProfiling::TCounter CacheInserts;
    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter UnmergedRowCount;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TTimeCounter CpuTime;
    NProfiling::TTimeCounter DecompressionCpuTime;
    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
    NYT::NProfiling::TEventTimer LookupDuration;
};

////////////////////////////////////////////////////////////////////////////////

struct TSelectCpuCounters
{
    TSelectCpuCounters() = default;

    explicit TSelectCpuCounters(const NProfiling::TProfiler& profiler)
        : CpuTime(profiler.TimeCounter("/select/cpu_time"))
        , ChunkReaderStatisticsCounters(profiler.WithPrefix("/select/chunk_reader_statistics"))
    { }

    NProfiling::TTimeCounter CpuTime;
    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

struct TSelectReadCounters
{
    TSelectReadCounters() = default;

    explicit TSelectReadCounters(const NProfiling::TProfiler& profiler)
        : RowCount(profiler.Counter("/select/row_count"))
        , DataWeight(profiler.Counter("/select/data_weight"))
        , UnmergedRowCount(profiler.Counter("/select/unmerged_row_count"))
        , UnmergedDataWeight(profiler.Counter("/select/unmerged_data_weight"))
        , DecompressionCpuTime(profiler.TimeCounter("/select/decompression_cpu_time"))
    { }

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter UnmergedRowCount;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TTimeCounter DecompressionCpuTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteCounters
{
    TWriteCounters() = default;

    explicit TWriteCounters(const NProfiling::TProfiler& profiler)
        : RowCount(profiler.Counter("/write/row_count"))
        , DataWeight(profiler.Counter("/write/data_weight"))
    { }

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitCounters
{
    TCommitCounters() = default;

    explicit TCommitCounters(const NProfiling::TProfiler& profiler)
        : RowCount(profiler.Counter("/commit/row_count"))
        , DataWeight(profiler.Counter("/commit/data_weight"))
    { }

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoteDynamicStoreReadCounters
{
    TRemoteDynamicStoreReadCounters() = default;

    explicit TRemoteDynamicStoreReadCounters(const NProfiling::TProfiler& profiler)
        : RowCount(profiler.Counter("/dynamic_store_read/row_count"))
        , DataWeight(profiler.Counter("/dynamic_store_read/data_weight"))
        , CpuTime(profiler.TimeCounter("/dynamic_store_read/cpu_time"))
        , SessionRowCount(profiler.Summary("/dynamic_store_read/session_row_count"))
        , SessionDataWeight(profiler.Summary("/dynamic_store_read/session_data_weight"))
        , SessionWallTime(profiler.Timer("/dynamic_store_read/session_wall_time"))
    { }

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TTimeCounter CpuTime;
    NProfiling::TSummary SessionRowCount;
    NProfiling::TSummary SessionDataWeight;
    NProfiling::TEventTimer SessionWallTime;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkReadProfilingMethod,
    (Preload)
    (Partitioning)
    (Compaction)
);

struct TChunkReadCounters
{
    TChunkReadCounters() = default;

    explicit TChunkReadCounters(const NProfiling::TProfiler& profiler)
        : CompressedDataSize(profiler.Counter("/chunk_reader/compressed_data_size"))
        , UnmergedDataWeight(profiler.Counter("/chunk_reader/unmerged_data_weight"))
        , DecompressionCpuTime(profiler.TimeCounter("/chunk_reader/decompression_cpu_time"))
        , ChunkReaderStatisticsCounters(profiler.WithPrefix("/chunk_reader_statistics"))
    { }

    NProfiling::TCounter CompressedDataSize;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TTimeCounter DecompressionCpuTime;
    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

DEFINE_ENUM(EChunkWriteProfilingMethod,
    (StoreFlush)
    (Partitioning)
    (Compaction)
);

struct TChunkWriteCounters
{
    TChunkWriteCounters() = default;

    explicit TChunkWriteCounters(const NProfiling::TProfiler& profiler)
        : DiskSpace(profiler.Counter("/chunk_writer/disk_space"))
        , DataWeight(profiler.Counter("/chunk_writer/data_weight"))
        , CompressionCpuTime(profiler.TimeCounter("/chunk_writer/compression_cpu_time"))
    { }

    NProfiling::TCounter DiskSpace;
    NProfiling::TCounter DataWeight;
    NProfiling::TTimeCounter CompressionCpuTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletCounters
{
    TTabletCounters() = default;

    explicit TTabletCounters(const NProfiling::TProfiler& profiler)
        : OverlappingStoreCount(profiler.GaugeSummary("/tablet/overlapping_store_count"))
        , EdenStoreCount(profiler.GaugeSummary("/tablet/eden_store_count"))
    { }

    NProfiling::TGauge OverlappingStoreCount;
    NProfiling::TGauge EdenStoreCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TReplicaCounters
{
    TReplicaCounters() = default;

    explicit TReplicaCounters(const NProfiling::TProfiler& profiler)
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

    NProfiling::TGauge LagRowCount;
    NProfiling::TTimeGauge LagTime;
    NProfiling::TEventTimer ReplicationThrottleTime;
    NProfiling::TEventTimer ReplicationTransactionStartTime;
    NProfiling::TEventTimer ReplicationTransactionCommitTime;
    NProfiling::TEventTimer ReplicationRowsReadTime;
    NProfiling::TEventTimer ReplicationRowsWriteTime;
    NProfiling::TSummary ReplicationBatchRowCount;
    NProfiling::TSummary ReplicationBatchDataWeight;

    NProfiling::TCounter ReplicationRowCount;
    NProfiling::TCounter ReplicationDataWeight;
    NProfiling::TCounter ReplicationErrorCount;
};

////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////

struct TQueryServiceCounters
{
    TQueryServiceCounters() = default;

    explicit TQueryServiceCounters(const NProfiling::TProfiler& profiler)
        : Execute(profiler.WithPrefix("/execute"))
        , Multiread(profiler.WithPrefix("/multiread"))
    { }

    TMethodCounters Execute;
    TMethodCounters Multiread;
};

////////////////////////////////////////////////////////////////////////////////

TTableProfilerPtr CreateTableProfiler(
    EDynamicTableProfilingMode profilingMode,
    const TString& tabletCellBundle,
    const TString& tablePath,
    const TString& tableTag,
    const TString& account,
    const TString& medium);

////////////////////////////////////////////////////////////////////////////////

using TChunkWriteCountersVector = TEnumIndexedVector<
    EChunkWriteProfilingMethod,
    std::array<TChunkWriteCounters, 2>>;

using TChunkReadCountersVector = TEnumIndexedVector<
    EChunkReadProfilingMethod,
    std::array<TChunkReadCounters, 2>>;

using TTabletDistributedThrottlerTimersVector = TEnumIndexedVector<
    ETabletDistributedThrottlerKind,
    NProfiling::TEventTimer>;

using TTabletDistributedThrottlerCounters = TEnumIndexedVector<
    ETabletDistributedThrottlerKind,
    NProfiling::TCounter>;

class TTableProfiler
    : public TRefCounted
{
public:
    TTableProfiler() = default;

    TTableProfiler(
        const NProfiling::TProfiler& profiler,
        const NProfiling::TProfiler& diskProfiler);

    static TTableProfilerPtr GetDisabled();

    TTabletCounters GetTabletCounters();

    TLookupCounters* GetLookupCounters(const std::optional<TString>& userTag);
    TWriteCounters* GetWriteCounters(const std::optional<TString>& userTag);
    TCommitCounters* GetCommitCounters(const std::optional<TString>& userTag);
    TSelectCpuCounters* GetSelectCpuCounters(const std::optional<TString>& userTag);
    TSelectReadCounters* GetSelectReadCounters(const std::optional<TString>& userTag);
    TRemoteDynamicStoreReadCounters* GetRemoteDynamicStoreReadCounters(const std::optional<TString>& userTag);
    TQueryServiceCounters* GetQueryServiceCounters(const std::optional<TString>& userTag);

    TReplicaCounters GetReplicaCounters(const TString& cluster);

    TChunkWriteCounters* GetWriteCounters(EChunkWriteProfilingMethod method, bool failed);
    TChunkReadCounters* GetReadCounters(EChunkReadProfilingMethod method, bool failed);
    NProfiling::TEventTimer* GetThrottlerTimer(ETabletDistributedThrottlerKind kind);
    NProfiling::TCounter* GetThrottlerCounter(ETabletDistributedThrottlerKind kind);

private:
    bool Disabled_ = true;
    const NProfiling::TProfiler Profiler_{};

    template <class TCounter>
    struct TUserTaggedCounter
    {
        NConcurrency::TSyncMap<std::optional<TString>, TCounter> Counters;

        TCounter* Get(
            bool disabled,
            const std::optional<TString>& userTag,
            const NProfiling::TProfiler& profiler);
    };

    TUserTaggedCounter<TLookupCounters> LookupCounters_;
    TUserTaggedCounter<TWriteCounters> WriteCounters_;
    TUserTaggedCounter<TCommitCounters> CommitCounters_;
    TUserTaggedCounter<TSelectCpuCounters> SelectCpuCounters_;
    TUserTaggedCounter<TSelectReadCounters> SelectReadCounters_;
    TUserTaggedCounter<TRemoteDynamicStoreReadCounters> DynamicStoreReadCounters_;
    TUserTaggedCounter<TQueryServiceCounters> QueryServiceCounters_;

    TChunkWriteCountersVector ChunkWriteCounters_;
    TChunkReadCountersVector ChunkReadCounters_;
    TTabletDistributedThrottlerTimersVector ThrottlerWaitTimers_;
    TTabletDistributedThrottlerCounters ThrottlerCounters_;
};

DEFINE_REFCOUNTED_TYPE(TTableProfiler)

////////////////////////////////////////////////////////////////////////////////

class TWriterProfiler
    : public TRefCounted
{
public:
    TWriterProfiler() = default;

    void Profile(
        const TTabletSnapshotPtr& tabletSnapshot,
        EChunkWriteProfilingMethod method,
        bool failed);

    void Update(const NChunkClient::IMultiChunkWriterPtr& writer);
    void Update(const NChunkClient::IChunkWriterBasePtr& writer);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TWriterProfiler)

/////////////////////////////////////////////////////////////////////////////

class TReaderProfiler
    : public TRefCounted
{
public:
    TReaderProfiler() = default;

    void Profile(
        const TTabletSnapshotPtr& tabletSnapshot,
        EChunkReadProfilingMethod method,
        bool failed);

    void Update(
        const NTableClient::IVersionedReaderPtr& reader,
        const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics);
    void SetCompressedDataSize(i64 compressedDataSize);
    void SetCodecStatistics(const NChunkClient::TCodecStatistics& codecStatistics);
    void SetChunkReaderStatistics(const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;
    NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TReaderProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
