#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/journal_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TTabletHydraManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TRelativeReplicationThrottlerConfig
    : public NYTree::TYsonStruct
{
public:
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

class TRowDigestCompactionConfig
    : public NYTree::TYsonStruct
{
public:
    double MaxObsoleteTimestampRatio;
    int MaxTimestampsPerValue;

    REGISTER_YSON_STRUCT(TRowDigestCompactionConfig);

    static void Register(TRegistrar registrar);
};

bool operator==(const TRowDigestCompactionConfig& lhs, const TRowDigestCompactionConfig& rhs);

DEFINE_REFCOUNTED_TYPE(TRowDigestCompactionConfig)

////////////////////////////////////////////////////////////////////////////////

class TBuiltinTableMountConfig
    : public virtual NYTree::TYsonStruct
{
    // Any fields that should not be modified in a mounted table by the experiment
    // must be listed in ValidateNoForbiddenKeysInPatch.
public:
    TString TabletCellBundle;

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

class TCustomTableMountConfig
    : public NTableClient::TRetentionConfig
{
public:
    i64 MaxDynamicStoreRowCount;
    i64 MaxDynamicStoreValueCount;
    i64 MaxDynamicStoreTimestampCount;
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

    bool EnableNewScanReaderForLookup;
    bool EnableNewScanReaderForSelect;

    bool SingleColumnGroupByDefault;
    bool EnableSegmentMetaInBlocks;

    bool EnableHunkColumnarProfiling;

    double MaxHunkCompactionGarbageRatio;

    i64 MaxHunkCompactionSize;
    i64 HunkCompactionSizeBase;
    double HunkCompactionSizeRatio;
    int MinHunkCompactionChunkCount;
    int MaxHunkCompactionChunkCount;

    bool EnableNarrowChunkViewCompaction;
    double MaxChunkViewSizeRatio;

    // TODO(akozhikhov): Make these true by default.
    bool PrecacheChunkReplicasOnMount;
    bool RegisterChunkReplicasOnStoresUpdate;

    bool EnableReplicationProgressAdvanceToBarrier;

    // For testing purposes only.
    TDuration SimulatedTabletSnapshotDelay;
    TDuration SimulatedStorePreloadDelay;

    NTableClient::TDictionaryCompressionConfigPtr ValueDictionaryCompression;

    REGISTER_YSON_STRUCT(TCustomTableMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCustomTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public TBuiltinTableMountConfig
    , public TCustomTableMountConfig
{
public:
    REGISTER_YSON_STRUCT(TTableMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MaxTransactionTimeout;
    TDuration BarrierCheckPeriod;
    int MaxAbortedTransactionPoolSize;
    bool RejectIncorrectClockClusterTag;

    REGISTER_YSON_STRUCT(TTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletStoreReaderConfig
    : public NTableClient::TChunkReaderConfig
    , public NChunkClient::TErasureReaderConfig
{
public:
    bool PreferLocalReplicas;

    TAdaptiveHedgingManagerConfigPtr HedgingManager;

    REGISTER_YSON_STRUCT(TTabletStoreReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletStoreReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletHunkReaderConfig
    : public NChunkClient::TChunkFragmentReaderConfig
    , public NTableClient::TBatchHunkReaderConfig
{
public:
    TAdaptiveHedgingManagerConfigPtr HedgingManager;

    REGISTER_YSON_STRUCT(TTabletHunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletHunkWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NTableClient::THunkChunkPayloadWriterConfig
{
    REGISTER_YSON_STRUCT(TTabletHunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkWriterConfig)

///////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonStruct
{
public:
    i64 PoolChunkSize;

    TDuration PreloadBackoffTime;
    TDuration CompactionBackoffTime;
    TDuration PartitionSplitMergeBackoffTime;
    TDuration FlushBackoffTime;

    TDuration MaxBlockedRowWaitTime;

    NCompression::ECodec ChangelogCodec;

    //! When committing a non-atomic transaction, clients provide timestamps based
    //! on wall clock readings. These timestamps are checked for sanity using the server-side
    //! timestamp estimates.
    TDuration ClientTimestampThreshold;

    int ReplicatorThreadPoolSize;
    TDuration ReplicatorSoftBackoffTime;
    TDuration ReplicatorHardBackoffTime;

    TDuration TabletCellDecommissionCheckPeriod;
    TDuration TabletCellSuspensionCheckPeriod;

    //! Testing option. Time to (synchronously) sleep before sending a hive message to master.
    std::optional<TDuration> SleepBeforePostToMaster;

    //! Testing options. If true, locked rows of transaction are shuffled, simulating violation
    //! of the invariant of isomorphism of locked rows list and write log.
    bool ShuffleLockedRows;

    REGISTER_YSON_STRUCT(TTabletManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<int> ReplicatorThreadPoolSize;

    REGISTER_YSON_STRUCT(TTabletManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletCellWriteManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Testing option.
    //! If set, write request will fail with this probability.
    //! In case of failure write request will be equiprobably
    //! applied or not applied.
    std::optional<double> WriteFailureProbability;

    REGISTER_YSON_STRUCT(TTabletCellWriteManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellWriteManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletHunkLockManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period of time each hunks store is kept alive after it is no longer referenced.
    TDuration HunkStoreExtraLifeTime;

    TDuration UnlockCheckPeriod;

    REGISTER_YSON_STRUCT(TTabletHunkLockManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkLockManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreBackgroundActivityOrchidConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxFailedTaskCount;
    int MaxCompletedTaskCount;

    REGISTER_YSON_STRUCT(TStoreBackgroundActivityOrchidConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreBackgroundActivityOrchidConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadPoolSize;
    int MaxConcurrentFlushes;
    i64 MinForcedFlushDataSize;

    REGISTER_YSON_STRUCT(TStoreFlusherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    //! Fraction of #MemoryLimit when tablets must be forcefully flushed.
    std::optional<double> ForcedRotationMemoryRatio;

    std::optional<int> ThreadPoolSize;
    std::optional<int> MaxConcurrentFlushes;
    std::optional<i64> MinForcedFlushDataSize;

    TStoreBackgroundActivityOrchidConfigPtr Orchid;

    REGISTER_YSON_STRUCT(TStoreFlusherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactorConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadPoolSize;
    int MaxConcurrentCompactions;
    int MaxConcurrentPartitionings;

    REGISTER_YSON_STRUCT(TStoreCompactorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    std::optional<int> ThreadPoolSize;
    std::optional<int> MaxConcurrentCompactions;
    std::optional<int> MaxConcurrentPartitionings;

    TDuration ChunkViewSizeFetchPeriod;
    NConcurrency::TThroughputThrottlerConfigPtr ChunkViewSizeRequestThrottler;

    TDuration RowDigestFetchPeriod;
    NConcurrency::TThroughputThrottlerConfigPtr RowDigestRequestThrottler;
    bool UseRowDigests;

    int MaxCompactionStructuredLogEvents;
    int MaxPartitioningStructuredLogEvents;

    TStoreBackgroundActivityOrchidConfigPtr Orchid;

    REGISTER_YSON_STRUCT(TStoreCompactorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreTrimmerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    REGISTER_YSON_STRUCT(TStoreTrimmerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreTrimmerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THunkChunkSweeperDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    REGISTER_YSON_STRUCT(THunkChunkSweeperDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkChunkSweeperDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManagerConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxConcurrentPreloads;
    TDuration InterceptedDataRetentionTime;
    TDuration PingPeriod;
    TDuration ControlRpcTimeout;
    TDuration HeavyRpcTimeout;
    i64 RemoteSendBatchSize;
    TWorkloadDescriptor WorkloadDescriptor;
    // COMPAT(babenko): use /tablet_node/throttlers/static_store_preload_in instead.
    NConcurrency::TRelativeThroughputThrottlerConfigPtr PreloadThrottler;

    bool EnablePreliminaryNetworkThrottling;

    TInMemoryManagerConfigPtr ApplyDynamic(const TInMemoryManagerDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TInMemoryManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<int> MaxConcurrentPreloads;
    std::optional<TDuration> InterceptedDataRetentionTime;
    std::optional<TDuration> PingPeriod;
    std::optional<TDuration> ControlRpcTimeout;
    std::optional<TDuration> HeavyRpcTimeout;
    std::optional<i64> RemoteSendBatchSize;
    std::optional<bool> EnablePreliminaryNetworkThrottling;

    REGISTER_YSON_STRUCT(TInMemoryManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    //! Scraps unavailable chunks.
    NChunkClient::TChunkScraperConfigPtr ChunkScraper;

    //! Fetches samples from remote chunks.
    NChunkClient::TFetcherConfigPtr SamplesFetcher;

    //! Minimum number of samples needed for partitioning.
    int MinPartitioningSampleCount;

    //! Maximum number of samples to request for partitioning.
    int MaxPartitioningSampleCount;

    //! Maximum number of concurrent partition samplings.
    int MaxConcurrentSamplings;

    //! Minimum interval between resampling.
    TDuration ResamplingPeriod;

    //! Retry delay after unsuccessful partition balancing.
    TDuration SplitRetryDelay;

    REGISTER_YSON_STRUCT(TPartitionBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    REGISTER_YSON_STRUCT(TPartitionBalancerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    REGISTER_YSON_STRUCT(TSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

class TSecurityManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    REGISTER_YSON_STRUCT(TSecurityManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent tablet node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for tablet node heartbeats.
    TDuration HeartbeatPeriodSplay;

    //! Timeout of the tablet node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent tablet node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for tablet node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    //! Timeout of the tablet node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonStruct
{
public:
    //! Maximum number of Tablet Managers to run.
    int Slots;

    //! Maximum amount of memory static tablets (i.e. "in-memory tables") are allowed to occupy.
    i64 TabletStaticMemory;

    //! Maximum amount of memory dynamics tablets are allowed to occupy.
    i64 TabletDynamicMemory;

    REGISTER_YSON_STRUCT(TResourceLimitsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TBackupManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration CheckpointFeasibilityCheckBatchPeriod;
    TDuration CheckpointFeasibilityCheckBackoff;

    REGISTER_YSON_STRUCT(TBackupManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBackupManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TServiceMethod
    : public NYTree::TYsonStruct
{
public:
    TString Service;
    TString Method;

    REGISTER_YSON_STRUCT(TServiceMethod);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceMethod)

////////////////////////////////////////////////////////////////////////////////

class TServiceMethodConfig
    : public NYTree::TYsonStruct
{
public:
    TString Service;
    TString Method;

    int MaxWindow;
    double WaitingTimeoutFraction;

    REGISTER_YSON_STRUCT(TServiceMethodConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServiceMethodConfig)

////////////////////////////////////////////////////////////////////////////////

class TOverloadTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MeanWaitTimeThreshold;
    std::vector<TServiceMethodPtr> MethodsToThrottle;

    REGISTER_YSON_STRUCT(TOverloadTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOverloadTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

class TOverloadControllerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;
    THashMap<TString, TOverloadTrackerConfigPtr> Trackers;
    std::vector<TServiceMethodConfigPtr> Methods;
    TDuration LoadAdjustingPeriod;

    REGISTER_YSON_STRUCT(TOverloadControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOverloadControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TStatisticsReporterConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    int MaxTabletsPerTransaction;
    TDuration ReportBackoffTime;
    NYPath::TYPath TablePath;

    NConcurrency::TPeriodicExecutorOptions PeriodicOptions;

    REGISTER_YSON_STRUCT(TStatisticsReporterConfig);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporterConfig)

////////////////////////////////////////////////////////////////////////////////

class TErrorManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration DeduplicationCacheTimeout;
    TDuration ErrorExpirationTimeout;

    REGISTER_YSON_STRUCT(TErrorManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TErrorManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMediumThrottlersConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableChangelogThrottling;
    bool EnableBlobThrottling;

    // Defines throttling time as a fraction of the request timeout.
    double ThrottleTimeoutFraction;
    // Max allowed throttling time for a request.
    TDuration MaxThrottlingTime;

    REGISTER_YSON_STRUCT(TMediumThrottlersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumThrottlersConfig);

////////////////////////////////////////////////////////////////////////////////

class TCompressionDictionaryBuilderConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadPoolSize;
    int MaxConcurrentBuildTasks;

    REGISTER_YSON_STRUCT(TCompressionDictionaryBuilderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompressionDictionaryBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

class TCompressionDictionaryBuilderDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    std::optional<int> ThreadPoolSize;
    std::optional<int> MaxConcurrentBuildTasks;

    REGISTER_YSON_STRUCT(TCompressionDictionaryBuilderDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompressionDictionaryBuilderDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Maximum number of Tablet Managers to run.
    //! If set, overrides corresponding value in TResourceLimitsConfig.
    // COMPAT(gritukan): Drop optional.
    std::optional<int> Slots;

    TTabletManagerDynamicConfigPtr TabletManager;

    TTabletCellWriteManagerDynamicConfigPtr TabletCellWriteManager;

    TTabletHunkLockManagerDynamicConfigPtr HunkLockManager;

    TEnumIndexedArray<ETabletNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    TStoreCompactorDynamicConfigPtr StoreCompactor;
    TStoreFlusherDynamicConfigPtr StoreFlusher;
    TStoreTrimmerDynamicConfigPtr StoreTrimmer;
    THunkChunkSweeperDynamicConfigPtr HunkChunkSweeper;
    TPartitionBalancerDynamicConfigPtr PartitionBalancer;
    TInMemoryManagerDynamicConfigPtr InMemoryManager;
    TCompressionDictionaryBuilderDynamicConfigPtr CompressionDictionaryBuilder;

    TSlruCacheDynamicConfigPtr VersionedChunkMetaCache;

    NQueryClient::TColumnEvaluatorCacheDynamicConfigPtr ColumnEvaluatorCache;

    bool EnableStructuredLogger;
    TDuration FullStructuredTabletHeartbeatPeriod;
    TDuration IncrementalStructuredTabletHeartbeatPeriod;

    TMasterConnectorDynamicConfigPtr MasterConnector;
    TSecurityManagerDynamicConfigPtr SecurityManager;
    TBackupManagerDynamicConfigPtr BackupManager;

    TOverloadControllerConfigPtr OverloadController;

    TStatisticsReporterConfigPtr StatisticsReporter;

    TErrorManagerConfigPtr ErrorManager;

    bool EnableChunkFragmentReaderThrottling;

    TMediumThrottlersConfigPtr MediumThrottlers;

    TSlruCacheDynamicConfigPtr CompressionDictionaryCache;

    REGISTER_YSON_STRUCT(TTabletNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THintManagerConfig
    : public NYTree::TYsonStruct
{
public:
    NDynamicConfig::TDynamicConfigManagerConfigPtr ReplicatorHintConfigFetcher;

    REGISTER_YSON_STRUCT(THintManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THintManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public NYTree::TYsonStruct
{
public:
    // TODO(ifsmirnov): drop in favour of dynamic config.
    double ForcedRotationMemoryRatio;

    //! Limits resources consumed by tablets.
    TResourceLimitsConfigPtr ResourceLimits;

    //! Remote by default, can be set to local.
    NHydra::TSnapshotStoreConfigBasePtr Snapshots;

    //! Remote changelogs.
    NHydra::TRemoteChangelogStoreConfigPtr Changelogs;

    //! Generic configuration for all Hydra instances.
    TTabletHydraManagerConfigPtr HydraManager;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    //! Generic configuration for all Hive instances.
    NHiveServer::THiveManagerConfigPtr HiveManager;

    TTransactionManagerConfigPtr TransactionManager;
    NTransactionSupervisor::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;
    TStoreFlusherConfigPtr StoreFlusher;
    TStoreCompactorConfigPtr StoreCompactor;
    TInMemoryManagerConfigPtr InMemoryManager;
    TPartitionBalancerConfigPtr PartitionBalancer;
    TSecurityManagerConfigPtr SecurityManager;
    THintManagerConfigPtr HintManager;
    NDynamicConfig::TDynamicConfigManagerConfigPtr TableConfigManager;
    TCompressionDictionaryBuilderConfigPtr CompressionDictionaryBuilder;

    //! Cache for versioned chunk metas.
    TSlruCacheConfigPtr VersionedChunkMetaCache;

    //! Configuration for various Tablet Node throttlers.
    TEnumIndexedArray<ETabletNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    //! Interval between slots examination.
    TDuration SlotScanPeriod;

    //! Time to keep retired tablet snapshots hoping for a rapid Hydra restart.
    TDuration TabletSnapshotEvictionTimeout;

    //! Column evaluator used for handling tablet writes.
    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;

    TMasterConnectorConfigPtr MasterConnector;

    TSlruCacheConfigPtr CompressionDictionaryCache;

    REGISTER_YSON_STRUCT(TTabletNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicatorHintConfig
    : public NYTree::TYsonStruct
{
public:
    //! Set of replica clusters that are banned to replicate to.
    THashSet<TString> BannedReplicaClusters;

    //! If |false| replication to the cluster shall be banned by replicating clusters.
    bool EnableIncomingReplication;

    REGISTER_YSON_STRUCT(TReplicatorHintConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicatorHintConfig)

///////////////////////////////////////////////////////////////////////////////

class THunkStorageMountConfig
    : public NYTree::TYsonStruct
{
public:
    int DesiredAllocatedStoreCount;

    TDuration StoreRotationPeriod;
    TDuration StoreRemovalGracePeriod;

    REGISTER_YSON_STRUCT(THunkStorageMountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStorageMountConfig)

///////////////////////////////////////////////////////////////////////////////

class THunkStoreWriterConfig
    : public NJournalClient::TJournalHunkChunkWriterConfig
{
public:
    i64 DesiredHunkCountPerChunk;
    i64 DesiredChunkSize;

    REGISTER_YSON_STRUCT(THunkStoreWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStoreWriterConfig)

///////////////////////////////////////////////////////////////////////////////

class THunkStoreWriterOptions
    : public NJournalClient::TJournalHunkChunkWriterOptions
{
public:
    TString MediumName;
    TString Account;

    REGISTER_YSON_STRUCT(THunkStoreWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THunkStoreWriterOptions)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
