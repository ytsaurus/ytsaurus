#pragma once

#include "public.h"
#include "yt/yt/library/profiling/sensor.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/lsm/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/syncmap/map.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TKeyFilterCounters
{
    TKeyFilterCounters() = default;

    explicit TKeyFilterCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter InputKeyCount;
    NProfiling::TCounter FilteredOutKeyCount;
    NProfiling::TCounter FalsePositiveKeyCount;
};

struct TLookupCounters
{
    TLookupCounters() = default;

    TLookupCounters(
        const NProfiling::TProfiler& tabletProfiler,
        const NProfiling::TProfiler& mediumProfiler,
        const NTableClient::TTableSchemaPtr& schema);

    NProfiling::TCounter CacheHits;
    NProfiling::TCounter CacheOutdated;
    NProfiling::TCounter CacheMisses;
    NProfiling::TCounter CacheInserts;

    NProfiling::TCounter RowCount;
    NProfiling::TCounter MissingRowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter UnmergedRowCount;
    NProfiling::TCounter UnmergedMissingRowCount;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TCounter WastedUnmergedDataWeight;

    NProfiling::TTimeCounter CpuTime;
    NProfiling::TTimeCounter DecompressionCpuTime;
    NYT::NProfiling::TEventTimer LookupDuration;

    NProfiling::TCounter RetryCount;

    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;

    NTableClient::THunkChunkReaderCounters HunkChunkReaderCounters;

    TKeyFilterCounters KeyFilterCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TRangeFilterCounters
{
    TRangeFilterCounters() = default;

    explicit TRangeFilterCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter InputRangeCount;
    NProfiling::TCounter FilteredOutRangeCount;
    NProfiling::TCounter FalsePositiveRangeCount;
};

struct TSelectRowsCounters
{
    TSelectRowsCounters() = default;

    TSelectRowsCounters(
        const NProfiling::TProfiler& tabletProfiler,
        const NProfiling::TProfiler& mediumProfiler,
        const NTableClient::TTableSchemaPtr& schema);

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter UnmergedRowCount;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TTimeCounter CpuTime;
    NProfiling::TTimeCounter DecompressionCpuTime;
    NYT::NProfiling::TEventTimer SelectDuration;
    TRangeFilterCounters RangeFilterCounters;
    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
    NTableClient::THunkChunkReaderCounters HunkChunkReaderCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TPullRowsCounters
{
    TPullRowsCounters() = default;

    explicit TPullRowsCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter DataWeight;
    NProfiling::TCounter RowCount;
    NProfiling::TCounter WastedRowCount;
    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TTablePullerCounters
{
    TTablePullerCounters() = default;

    explicit TTablePullerCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter DataWeight;
    NProfiling::TCounter RowCount;
    NProfiling::TCounter ErrorCount;
    NProfiling::TEventTimer PullRowsTime;
    NProfiling::TEventTimer WriteTime;
    NProfiling::TTimeGauge LagTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteCounters
{
    TWriteCounters() = default;

    explicit TWriteCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter BulkInsertRowCount;
    NProfiling::TCounter BulkInsertDataWeight;
    NProfiling::TEventTimer ValidateResourceWallTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TCommitCounters
{
    TCommitCounters() = default;

    explicit TCommitCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoteDynamicStoreReadCounters
{
    TRemoteDynamicStoreReadCounters() = default;

    explicit TRemoteDynamicStoreReadCounters(const NProfiling::TProfiler& profiler);

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
    (DictionaryBuilding)
);

struct TChunkReadCounters
{
    TChunkReadCounters() = default;

    TChunkReadCounters(
        const NProfiling::TProfiler& profiler,
        const NTableClient::TTableSchemaPtr& schema);

    NProfiling::TCounter CompressedDataSize;
    NProfiling::TCounter UnmergedDataWeight;
    NProfiling::TTimeCounter DecompressionCpuTime;

    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
    NTableClient::THunkChunkReaderCounters HunkChunkReaderCounters;
};

DEFINE_ENUM(EChunkWriteProfilingMethod,
    (StoreFlush)
    (Partitioning)
    (Compaction)
    (DictionaryBuilding)
);

struct TChunkWriteCounters
{
    TChunkWriteCounters() = default;

    TChunkWriteCounters(
        const NProfiling::TProfiler& profiler,
        const NTableClient::TTableSchemaPtr& schema);

    NChunkClient::TChunkWriterCounters ChunkWriterCounters;
    NTableClient::THunkChunkWriterCounters HunkChunkWriterCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletCounters
{
    TTabletCounters() = default;

    explicit TTabletCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TGauge OverlappingStoreCount;
    NProfiling::TGauge EdenStoreCount;
    NProfiling::TGauge DataWeight;
    NProfiling::TGauge UncompressedDataSize;
    NProfiling::TGauge CompressedDataSize;
    NProfiling::TGauge RowCount;
    NProfiling::TGauge ChunkCount;
    NProfiling::TGauge HunkCount;
    NProfiling::TGauge TotalHunkLength;
    NProfiling::TGauge HunkChunkCount;
    NProfiling::TGauge TabletCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TReplicaCounters
{
    TReplicaCounters() = default;

    explicit TReplicaCounters(const NProfiling::TProfiler& profiler);

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

struct TQueryServiceCounters
{
    TQueryServiceCounters() = default;

    explicit TQueryServiceCounters(const NProfiling::TProfiler& profiler);

    TMethodCounters Execute;
    TMethodCounters Multiread;
    TMethodCounters PullRows;
};

struct TTabletServiceCounters
{
    TTabletServiceCounters() = default;

    explicit TTabletServiceCounters(const NProfiling::TProfiler& profiler);

    TMethodCounters Write;
};

////////////////////////////////////////////////////////////////////////////////

struct TStoreRotationCounters
{
    TStoreRotationCounters() = default;

    explicit TStoreRotationCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter RotationCount;
    NProfiling::TSummary RotatedRowCount;
    NProfiling::TSummary RotatedMemoryUsage;
};

struct TStoreCompactionCounterGroup
{
    TStoreCompactionCounterGroup() = default;

    explicit TStoreCompactionCounterGroup(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter InDataWeight;
    NProfiling::TCounter OutDataWeight;
    NProfiling::TCounter InStoreCount;
    NProfiling::TCounter OutStoreCount;
};

struct TStoreCompactionCounters
{
    TStoreCompactionCounters() = default;

    explicit TStoreCompactionCounters(const NProfiling::TProfiler& profiler);

    TStoreCompactionCounterGroup StoreChunks;

    TStoreCompactionCounterGroup HunkChunks;
    TEnumIndexedArray<EHunkCompactionReason, NProfiling::TCounter> InHunkChunkCountByReason;
};

struct TPartitionBalancingCounters
{
    TPartitionBalancingCounters() = default;

    explicit TPartitionBalancingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter PartitionSplits;
    NProfiling::TCounter PartitionMerges;
};

class TLsmCounters
{
public:
    TLsmCounters() = default;

    explicit TLsmCounters(const NProfiling::TProfiler& profiler);

    void ProfileRotation(NLsm::EStoreRotationReason reason, i64 rowCount, i64 memoryUsage);

    void ProfileCompaction(
        NLsm::EStoreCompactionReason reason,
        TEnumIndexedArray<EHunkCompactionReason, i64> hunkChunkCountByReason,
        bool isEden,
        const NChunkClient::NProto::TDataStatistics& readerStatistics,
        const NChunkClient::NProto::TDataStatistics& writerStatistics,
        const NTableClient::IHunkChunkReaderStatisticsPtr& hunkChunkReaderStatistics,
        const NChunkClient::NProto::TDataStatistics& hunkChunkWriterStatistics);

    void ProfilePartitioning(
        NLsm::EStoreCompactionReason reason,
        TEnumIndexedArray<EHunkCompactionReason, i64> hunkChunkCountByReason,
        const NChunkClient::NProto::TDataStatistics& readerStatistics,
        const NChunkClient::NProto::TDataStatistics& writerStatistics,
        const NTableClient::IHunkChunkReaderStatisticsPtr& hunkChunkReaderStatistics,
        const NChunkClient::NProto::TDataStatistics& hunkChunkWriterStatistics);

    void ProfilePartitionSplit();
    void ProfilePartitionMerge();

private:
    TEnumIndexedArray<
        NLsm::EStoreRotationReason,
        TStoreRotationCounters> RotationCounters_;

    // Counters[reason][eden][compaction/partitioning].
    TEnumIndexedArray<
        NLsm::EStoreCompactionReason,
        std::array<
            TEnumIndexedArray<
                NLsm::EStoreCompactorActivityKind,
                TStoreCompactionCounters>, 2>> CompactionCounters_;

    TPartitionBalancingCounters PartitionBalancingCounters_;

    void DoProfileCompaction(
        TStoreCompactionCounters* counters,
        const NChunkClient::NProto::TDataStatistics& readerStatistics,
        const NChunkClient::NProto::TDataStatistics& writerStatistics,
        const NTableClient::IHunkChunkReaderStatisticsPtr& hunkChunkReaderStatistics,
        const NChunkClient::NProto::TDataStatistics& hunkChunkWriterStatistics,
        TEnumIndexedArray<EHunkCompactionReason, i64> hunkChunkCountByReason);
};

////////////////////////////////////////////////////////////////////////////////

TTableProfilerPtr CreateTableProfiler(
    EDynamicTableProfilingMode profilingMode,
    const TString& tabletCellBundle,
    const TString& tablePath,
    const TString& tableTag,
    const TString& account,
    const TString& medium,
    NObjectClient::TObjectId schemaId,
    const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

using TChunkWriteCountersVector = TEnumIndexedArray<
    EChunkWriteProfilingMethod,
    std::array<TChunkWriteCounters, 2>>;

using TChunkReadCountersVector = TEnumIndexedArray<
    EChunkReadProfilingMethod,
    std::array<TChunkReadCounters, 2>>;

using TTabletDistributedThrottlerTimersVector = TEnumIndexedArray<
    ETabletDistributedThrottlerKind,
    NProfiling::TEventTimer>;

using TTabletDistributedThrottlerCounters = TEnumIndexedArray<
    ETabletDistributedThrottlerKind,
    NProfiling::TCounter>;

class TTableProfiler
    : public TRefCounted
{
public:
    TTableProfiler() = default;

    TTableProfiler(
        const NProfiling::TProfiler& tableProfiler,
        const NProfiling::TProfiler& diskProfiler,
        const NProfiling::TProfiler& mediumProfiler,
        NTableClient::TTableSchemaPtr schema);

    static TTableProfilerPtr GetDisabled();

    TTabletCounters GetTabletCounters();

    TQueryServiceCounters* GetQueryServiceCounters(const std::optional<TString>& userTag);
    TTabletServiceCounters* GetTabletServiceCounters(const std::optional<TString>& userTag);

    TLookupCounters* GetLookupCounters(const std::optional<TString>& userTag);
    TWriteCounters* GetWriteCounters(const std::optional<TString>& userTag);
    TCommitCounters* GetCommitCounters(const std::optional<TString>& userTag);
    TSelectRowsCounters* GetSelectRowsCounters(const std::optional<TString>& userTag);
    TRemoteDynamicStoreReadCounters* GetRemoteDynamicStoreReadCounters(const std::optional<TString>& userTag);
    TPullRowsCounters* GetPullRowsCounters(const std::optional<TString>& userTag);

    TReplicaCounters GetReplicaCounters(const TString& cluster);

    TTablePullerCounters* GetTablePullerCounters();
    TChunkWriteCounters* GetWriteCounters(EChunkWriteProfilingMethod method, bool failed);
    TChunkReadCounters* GetReadCounters(EChunkReadProfilingMethod method, bool failed);
    NProfiling::TEventTimer* GetThrottlerTimer(ETabletDistributedThrottlerKind kind);
    NProfiling::TCounter* GetThrottlerCounter(ETabletDistributedThrottlerKind kind);
    TLsmCounters* GetLsmCounters();

    const NProfiling::TProfiler& GetProfiler() const;

private:
    const bool Disabled_ = true;
    const NProfiling::TProfiler Profiler_ = {};
    const NProfiling::TProfiler MediumProfiler_ = {};
    const NTableClient::TTableSchemaPtr Schema_;

    template <class TCounter>
    struct TUserTaggedCounter
    {
        NConcurrency::TSyncMap<std::optional<TString>, TCounter> Counters;

        TCounter* Get(
            bool disabled,
            const std::optional<TString>& userTag,
            const NProfiling::TProfiler& profiler);
        TCounter* Get(
            bool disabled,
            const std::optional<TString>& userTag,
            const NProfiling::TProfiler& tableProfiler,
            const NProfiling::TProfiler& mediumProfiler,
            const NTableClient::TTableSchemaPtr& schema);
    };

    TUserTaggedCounter<TQueryServiceCounters> QueryServiceCounters_;
    TUserTaggedCounter<TTabletServiceCounters> TabletServiceCounters_;

    TUserTaggedCounter<TLookupCounters> LookupCounters_;
    TUserTaggedCounter<TWriteCounters> WriteCounters_;
    TUserTaggedCounter<TCommitCounters> CommitCounters_;
    TUserTaggedCounter<TSelectRowsCounters> SelectRowsCounters_;
    TUserTaggedCounter<TRemoteDynamicStoreReadCounters> DynamicStoreReadCounters_;
    TUserTaggedCounter<TPullRowsCounters> PullRowsCounters_;

    TTablePullerCounters TablePullerCounters_;
    TChunkWriteCountersVector ChunkWriteCounters_;
    TChunkReadCountersVector ChunkReadCounters_;
    TTabletDistributedThrottlerTimersVector ThrottlerWaitTimers_;
    TTabletDistributedThrottlerCounters ThrottlerCounters_;
    TLsmCounters LsmCounters_;

    template <class TCounter>
    TCounter* GetCounterUnlessDisabled(TCounter* counter);
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
    void Update(
        const NTableClient::IHunkChunkPayloadWriterPtr& hunkChunkWriter,
        const NTableClient::IHunkChunkWriterStatisticsPtr& hunkChunkWriterStatistics);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;

    NChunkClient::NProto::TDataStatistics HunkChunkDataStatistics_;
    NTableClient::IHunkChunkWriterStatisticsPtr HunkChunkWriterStatistics_;
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
        const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics,
        const NTableClient::IHunkChunkReaderStatisticsPtr& hunkChunkReaderStatistics);

    void SetCompressedDataSize(i64 compressedDataSize);
    void SetCodecStatistics(const NChunkClient::TCodecStatistics& codecStatistics);
    void SetChunkReaderStatistics(const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics);

private:
    NChunkClient::NProto::TDataStatistics DataStatistics_;
    NChunkClient::TCodecStatistics CodecStatistics_;

    NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_;
    NTableClient::IHunkChunkReaderStatisticsPtr HunkChunkReaderStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TReaderProfiler)

////////////////////////////////////////////////////////////////////////////////

class TBulkInsertProfiler
{
public:
    explicit TBulkInsertProfiler(TTablet* tablet);

    ~TBulkInsertProfiler();

    void Update(const IStorePtr& store);

private:
    const TWriteCounters* const Counters_;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
