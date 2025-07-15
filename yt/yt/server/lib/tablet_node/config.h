#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/journal_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TRelativeReplicationThrottlerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    //! Desired ratio of replication speed to lag accumulation speed.
    double Ratio;

    //! Minimal difference between log row timestamps from successive replication
    //! batches required to activate the throttler.
    TDuration ActivationThreshold;

    //! Controls the number of successive replication timestamps used to estimate
    //! the replication speed.
    TDuration WindowSize;

    //! Maximum number of replication timestamps to keep.
    int MaxTimestampsToKeep;

    REGISTER_YSON_STRUCT(TRelativeReplicationThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRelativeReplicationThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRowDigestCompactionConfig
    : public NYTree::TYsonStruct
{
    double MaxObsoleteTimestampRatio;
    int MaxTimestampsPerValue;

    REGISTER_YSON_STRUCT(TRowDigestCompactionConfig);

    static void Register(TRegistrar registrar);
};

bool operator==(const TRowDigestCompactionConfig& lhs, const TRowDigestCompactionConfig& rhs);

DEFINE_REFCOUNTED_TYPE(TRowDigestCompactionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGradualCompactionConfig
    : public NYTree::TYsonStructLite
{
    TInstant StartTime;
    TDuration Duration;

    REGISTER_YSON_STRUCT_LITE(TGradualCompactionConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TBuiltinTableMountConfig
    : public virtual NYTree::TYsonStruct
{
    // Any fields that should not be sent to the tablet node without master
    // consent (by setting it under @mount_config attribute or by an experiment)
    // must be included into the list below.
    static constexpr std::array NonDynamicallyModifiableFields{
        "tablet_cell_bundle",
        "in_memory_mode",
        "profiling_mode",
        "profiling_tag",
        "enable_dynamic_store_read",
        "enable_consistent_chunk_replica_placement",
        "enable_detailed_profiling",
    };

    static_assert(NonDynamicallyModifiableFields.size() == 7,
        "Consider promoting master reign");

public:
    std::string TabletCellBundle;

    NTabletClient::EInMemoryMode InMemoryMode;

    std::optional<NHydra::TRevision> ForcedCompactionRevision;
    std::optional<NHydra::TRevision> ForcedStoreCompactionRevision;
    std::optional<NHydra::TRevision> ForcedHunkCompactionRevision;
    std::optional<NHydra::TRevision> ForcedChunkViewCompactionRevision;

    EDynamicTableProfilingMode ProfilingMode;
    TString ProfilingTag;

    bool EnableDynamicStoreRead;

    bool EnableConsistentChunkReplicaPlacement;

    bool EnableDetailedProfiling;

    REGISTER_YSON_STRUCT(TBuiltinTableMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBuiltinTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTestingTableMountConfig
    : public NYTree::TYsonStructLite
{
    double CompactionFailureProbability;
    double PartitioningFailureProbability;
    double FlushFailureProbability;

    TDuration SimulatedTabletSnapshotDelay;
    TDuration SimulatedStorePreloadDelay;

    TDuration SyncDelayInWriteTransactionCommit;

    double SortedStoreManagerRowHashCheckProbability;

    std::optional<int> TablePullerReplicaBanIterationCount;

    TDuration WriteResponseDelay;

    bool OpaqueStoresInOrchid;
    bool OpaqueSettingsInOrchid;

    REGISTER_YSON_STRUCT_LITE(TTestingTableMountConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TCustomTableMountConfig
    : public NTableClient::TRetentionConfig
{
    i64 MaxDynamicStoreRowCount;
    i64 MaxDynamicStoreValueCount;
    TMaxDynamicStoreTimestampCount MaxDynamicStoreTimestampCount;
    i64 MaxDynamicStorePoolSize;
    i64 MaxDynamicStoreRowDataWeight;

    double DynamicStoreOverflowThreshold;

    i64 MaxPartitionDataSize;
    i64 DesiredPartitionDataSize;
    i64 MinPartitionDataSize;

    int MaxPartitionCount;

    i64 MinPartitioningDataSize;
    int MinPartitioningStoreCount;
    i64 MaxPartitioningDataSize;
    int MaxPartitioningStoreCount;

    int MinCompactionStoreCount;
    int MaxCompactionStoreCount;
    i64 CompactionDataSizeBase;
    double CompactionDataSizeRatio;

    TGradualCompactionConfig GlobalCompaction;

    NConcurrency::TThroughputThrottlerConfigPtr PartitioningThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr CompactionThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr FlushThrottler;

    THashMap<TString, NConcurrency::TThroughputThrottlerConfigPtr> Throttlers;

    int SamplesPerPartition;

    TDuration BackingStoreRetentionTime;

    int MaxReadFanIn;

    int MaxOverlappingStoreCount;
    int OverlappingStoreImmediateSplitThreshold;

    int MaxStoresPerTablet;
    int MaxEdenStoresPerTablet;

    std::optional<TDuration> DynamicStoreAutoFlushPeriod;
    TDuration DynamicStoreFlushPeriodSplay;
    std::optional<TDuration> AutoCompactionPeriod;
    double AutoCompactionPeriodSplayRatio;
    EPeriodicCompactionMode PeriodicCompactionMode;
    TRowDigestCompactionConfigPtr RowDigestCompaction;

    bool EnableLookupHashTable;

    i64 LookupCacheRowsPerTablet;
    double LookupCacheRowsRatio;
    bool EnableLookupCacheByDefault;

    i64 RowCountToKeep;

    TDuration ReplicationTickPeriod;
    TDuration MinReplicationLogTtl;
    int MaxTimestampsPerReplicationCommit;
    int MaxRowsPerReplicationCommit;
    i64 MaxDataWeightPerReplicationCommit;
    TDuration MaxReplicationBatchSpan;
    NConcurrency::TThroughputThrottlerConfigPtr ReplicationThrottler;
    TRelativeReplicationThrottlerConfigPtr RelativeReplicationThrottler;
    bool EnableReplicationLogging;

    TDuration ReplicationProgressUpdateTickPeriod;

    bool EnableProfiling;

    bool EnableStructuredLogger;

    bool EnableCompactionAndPartitioning;
    bool EnablePartitioning;
    bool EnableStoreRotation;
    bool EnableStoreFlush;
    bool EnableLsmVerboseLogging;

    NTabletClient::ERowMergerType RowMergerType;
    bool MergeRowsOnFlush;
    bool MergeDeletionsOnFlush;

    std::optional<i64> MaxUnversionedBlockSize;
    std::optional<int> CriticalOverlappingStoreCount;

    bool PreserveTabletIndex;

    bool EnablePartitionSplitWhileEdenPartitioning;
    bool EnableDiscardingExpiredPartitions;
    bool PrioritizeEdenForcedCompaction;
    bool AlwaysFlushToEden;

    bool EnableDataNodeLookup;

    bool EnableHashChunkIndexForLookup;
    bool EnableKeyFilterForLookup;

    int LookupRpcMultiplexingParallelism;

    bool SingleColumnGroupByDefault;
    bool EnableSegmentMetaInBlocks;
    bool EnableColumnMetaInChunkMeta;

    bool EnableHunkColumnarProfiling;

    double MaxHunkCompactionGarbageRatio;

    i64 MaxHunkCompactionSize;
    i64 HunkCompactionSizeBase;
    double HunkCompactionSizeRatio;
    int MinHunkCompactionChunkCount;
    int MaxHunkCompactionChunkCount;

    bool EnableNarrowChunkViewCompaction;
    double MaxChunkViewSizeRatio;

    // COMPAT(shamteev)
    // YT-24851: Protects from stale reads in ordered tables
    bool RetryReadOnOrderedStoreRotation;

    // TODO(akozhikhov): Make these true by default.
    bool PrecacheChunkReplicasOnMount;
    bool RegisterChunkReplicasOnStoresUpdate;

    bool EnableReplicationProgressAdvanceToBarrier;

    std::optional<i64> MaxOrderedTabletDataWeight;

    NTableClient::TDictionaryCompressionConfigPtr ValueDictionaryCompression;

    bool InsertMetaUponStoreUpdate;

    //! In case of non-block reads will open partition readers instantly at the start of the lookup session
    //! so chunk read sessions corresponding to this amount of keys will be opened.
    std::optional<int> PartitionReaderPrefetchKeyLimit;

    TTestingTableMountConfig Testing;

    REGISTER_YSON_STRUCT(TCustomTableMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCustomTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableMountConfig
    : public TBuiltinTableMountConfig
    , public TCustomTableMountConfig
{
    REGISTER_YSON_STRUCT(TTableMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletStoreReaderConfig
    : public NTableClient::TChunkReaderConfig
    , public NChunkClient::TErasureReaderConfig
{
    bool PreferLocalReplicas;

    TAdaptiveHedgingManagerConfigPtr HedgingManager;

    REGISTER_YSON_STRUCT(TTabletStoreReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletHunkReaderConfig
    : public NChunkClient::TChunkFragmentReaderConfig
    , public NTableClient::TBatchHunkReaderConfig
    , public NTableClient::TDictionaryCompressionSessionConfig
{
    TAdaptiveHedgingManagerConfigPtr HedgingManager;

    REGISTER_YSON_STRUCT(TTabletHunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletHunkWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NTableClient::THunkChunkPayloadWriterConfig
{
    REGISTER_YSON_STRUCT(TTabletHunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSecurityManagerConfig
    : public NYTree::TYsonStruct
{
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    REGISTER_YSON_STRUCT(TSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSecurityManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    REGISTER_YSON_STRUCT(TSecurityManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicatorHintConfig
    : public NYTree::TYsonStruct
{
    //! Set of replica clusters that are banned to replicate to.
    THashSet<std::string> BannedReplicaClusters;

    //! If |false| replication to the cluster shall be banned by replicating clusters.
    bool EnableIncomingReplication;

    //! Set of replica clusters that are preferred for serving sync replicas.
    //! NB: This options overrides corresponding option of a replicated table.
    THashSet<std::string> PreferredSyncReplicaClusters;

    REGISTER_YSON_STRUCT(TReplicatorHintConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatorHintConfig)

////////////////////////////////////////////////////////////////////////////////

struct THunkStorageMountConfig
    : public NYTree::TYsonStruct
{
    int DesiredAllocatedStoreCount;

    TDuration StoreRotationPeriod;
    TDuration StoreRemovalGracePeriod;

    REGISTER_YSON_STRUCT(THunkStorageMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStorageMountConfig)

////////////////////////////////////////////////////////////////////////////////

struct THunkStoreWriterConfig
    : public NJournalClient::TJournalHunkChunkWriterConfig
{
    i64 DesiredHunkCountPerChunk;
    i64 DesiredChunkSize;

    REGISTER_YSON_STRUCT(THunkStoreWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStoreWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct THunkStoreWriterOptions
    : public NJournalClient::TJournalHunkChunkWriterOptions
{
    std::string MediumName;
    std::string Account;

    REGISTER_YSON_STRUCT(THunkStoreWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStoreWriterOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
