#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/server/lib/dynamic_config/public.h>

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;
    bool UseNewHydra;

    TTabletHydraManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableMountConfig
    : public NTableClient::TRetentionConfig
{
public:
    TString TabletCellBundle;

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

    NTabletClient::EInMemoryMode InMemoryMode;

    int MaxStoresPerTablet;
    int MaxEdenStoresPerTablet;

    std::optional<NHydra::TRevision> ForcedCompactionRevision;
    std::optional<NHydra::TRevision> ForcedStoreCompactionRevision;
    std::optional<NHydra::TRevision> ForcedHunkCompactionRevision;
    // TODO(babenko,ifsmirnov): make builtin
    std::optional<NHydra::TRevision> ForcedChunkViewCompactionRevision;

    std::optional<TDuration> DynamicStoreAutoFlushPeriod;
    TDuration DynamicStoreFlushPeriodSplay;
    std::optional<TDuration> AutoCompactionPeriod;
    double AutoCompactionPeriodSplayRatio;
    EPeriodicCompactionMode PeriodicCompactionMode;

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
    bool EnableReplicationLogging;

    bool EnableProfiling;
    EDynamicTableProfilingMode ProfilingMode;
    TString ProfilingTag;

    bool EnableStructuredLogger;

    bool EnableCompactionAndPartitioning;
    bool EnableStoreRotation;
    bool EnableLsmVerboseLogging;

    bool MergeRowsOnFlush;
    bool MergeDeletionsOnFlush;

    std::optional<i64> MaxUnversionedBlockSize;
    std::optional<int> CriticalOverlappingStoreCount;

    bool PreserveTabletIndex;

    bool EnablePartitionSplitWhileEdenPartitioning;
    bool EnableDiscardingExpiredPartitions;

    bool EnableDataNodeLookup;
    std::optional<int> MaxParallelPartitionLookups;
    bool EnablePeerProbingInDataNodeLookup;
    bool EnableRejectsInDataNodeLookupIfThrottling;

    int LookupRpcMultiplexingParallelism;

    bool EnableDynamicStoreRead;
    bool EnableNewScanReaderForLookup;
    bool EnableNewScanReaderForSelect;

    bool EnableConsistentChunkReplicaPlacement;

    bool EnableDetailedProfiling;
    bool EnableHunkColumnarProfiling;

    i64 MinHunkCompactionTotalHunkLength;
    double MaxHunkCompactionGarbageRatio;

    i64 MaxHunkCompactionSize;
    i64 HunkCompactionSizeBase;
    double HunkCompactionSizeRatio;
    int MinHunkCompactionChunkCount;
    int MaxHunkCompactionChunkCount;

    bool PrecacheChunkReplicasOnMount;
    bool RegisterChunkReplicasOnStoresUpdate;

    bool EnableReplicationProgressAdvanceToBarrier;

    TTableMountConfig();
};

DEFINE_REFCOUNTED_TYPE(TTableMountConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    TDuration BarrierCheckPeriod;
    int MaxAbortedTransactionPoolSize;
    bool RejectIncorrectClockClusterTag;

    TTransactionManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletStoreReaderConfig
    : public NTableClient::TChunkReaderConfig
    , public NChunkClient::TErasureReaderConfig
{
public:
    bool PreferLocalReplicas;

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
    // COMPAT(babenko)
    bool UseNewChunkFragmentReader;

    TTabletHunkReaderConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 PoolChunkSize;

    TDuration PreloadBackoffTime;
    TDuration CompactionBackoffTime;
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

    //! Testing option. Time to (synchronously) sleep before sending a hive message to master.
    std::optional<TDuration> SleepBeforePostToMaster;

    TTabletManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> ReplicatorThreadPoolSize;

    TTabletManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentFlushes;
    i64 MinForcedFlushDataSize;

    TStoreFlusherConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreFlusherDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    //! Fraction of #MemoryLimit when tablets must be forcefully flushed.
    std::optional<double> ForcedRotationMemoryRatio;

    std::optional<int> ThreadPoolSize;
    std::optional<int> MaxConcurrentFlushes;
    std::optional<i64> MinForcedFlushDataSize;

    TStoreFlusherDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreFlusherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactorConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;
    int MaxConcurrentCompactions;
    int MaxConcurrentPartitionings;

    TStoreCompactorConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreCompactorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    std::optional<int> ThreadPoolSize;
    std::optional<int> MaxConcurrentCompactions;
    std::optional<int> MaxConcurrentPartitionings;

    TStoreCompactorDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreTrimmerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    TStoreTrimmerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreTrimmerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THunkChunkSweeperDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    THunkChunkSweeperDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(THunkChunkSweeperDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TInMemoryManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxConcurrentPreloads;
    TDuration InterceptedDataRetentionTime;
    TDuration PingPeriod;
    TDuration ControlRpcTimeout;
    TDuration HeavyRpcTimeout;
    size_t BatchSize;
    TWorkloadDescriptor WorkloadDescriptor;
    // COMPAT(babenko): use /tablet_node/throttlers/static_store_preload_in instead.
    NConcurrency::TRelativeThroughputThrottlerConfigPtr PreloadThrottler;

    TInMemoryManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerConfig
    : public NYTree::TYsonSerializable
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

    TPartitionBalancerConfig();
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    TPartitionBalancerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TPartitionBalancerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    TSecurityManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

class TSecurityManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    TAsyncExpiringCacheConfigPtr ResourceLimitsCache;

    TSecurityManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent tablet node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for tablet node heartbeats.
    TDuration HeartbeatPeriodSplay;

    //! Timeout of the tablet node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    TMasterConnectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent tablet node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for tablet node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    //! Timeout of the tablet node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    TMasterConnectorDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum number of Tablet Managers to run.
    int Slots;

    //! Maximum amount of memory static tablets (i.e. "in-memory tables") are allowed to occupy.
    i64 TabletStaticMemory;

    //! Maximum amount of memory dynamics tablets are allowed to occupy.
    i64 TabletDynamicMemory;

    TResourceLimitsConfig();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TBackupManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CheckpointFeasibilityCheckBatchPeriod;
    TDuration CheckpointFeasibilityCheckBackoff;

    TBackupManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TBackupManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Maximum number of Tablet Managers to run.
    //! If set, overrides corresponding value in TResourceLimitsConfig.
    // COMPAT(gritukan): Drop optional.
    std::optional<int> Slots;

    TTabletManagerDynamicConfigPtr TabletManager;

    TEnumIndexedVector<ETabletNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    TStoreCompactorDynamicConfigPtr StoreCompactor;
    TStoreFlusherDynamicConfigPtr StoreFlusher;
    TStoreTrimmerDynamicConfigPtr StoreTrimmer;
    THunkChunkSweeperDynamicConfigPtr HunkChunkSweeper;
    TPartitionBalancerDynamicConfigPtr PartitionBalancer;

    TSlruCacheDynamicConfigPtr VersionedChunkMetaCache;

    NQueryClient::TColumnEvaluatorCacheDynamicConfigPtr ColumnEvaluatorCache;

    bool EnableStructuredLogger;
    TDuration FullStructuredTabletHeartbeatPeriod;
    TDuration IncrementalStructuredTabletHeartbeatPeriod;

    TMasterConnectorDynamicConfigPtr MasterConnector;
    TSecurityManagerDynamicConfigPtr SecurityManager;
    TBackupManagerDynamicConfigPtr BackupManager;

    TTabletNodeDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THintManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    NDynamicConfig::TDynamicConfigManagerConfigPtr ReplicatorHintConfigFetcher;

    THintManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(THintManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    // TODO(ifsmirnov): drop in favour of dynamic config.
    double ForcedRotationMemoryRatio;

    //! Limits resources consumed by tablets.
    TResourceLimitsConfigPtr ResourceLimits;

    //! Remote snapshots.
    NHydra::TRemoteSnapshotStoreConfigPtr Snapshots;

    //! Remote changelogs.
    NHydra::TRemoteChangelogStoreConfigPtr Changelogs;

    //! Generic configuration for all Hydra instances.
    TTabletHydraManagerConfigPtr HydraManager;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    //! Generic configuration for all Hive instances.
    NHiveServer::THiveManagerConfigPtr HiveManager;

    TTransactionManagerConfigPtr TransactionManager;
    NHiveServer::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TTabletManagerConfigPtr TabletManager;
    TStoreFlusherConfigPtr StoreFlusher;
    TStoreCompactorConfigPtr StoreCompactor;
    TInMemoryManagerConfigPtr InMemoryManager;
    TPartitionBalancerConfigPtr PartitionBalancer;
    TSecurityManagerConfigPtr SecurityManager;
    THintManagerConfigPtr HintManager;

    //! Cache for versioned chunk metas.
    TSlruCacheConfigPtr VersionedChunkMetaCache;

    //! Configuration for various Tablet Node throttlers.
    TEnumIndexedVector<ETabletNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    //! Interval between slots examination.
    TDuration SlotScanPeriod;

    //! Time to keep retired tablet snapshots hoping for a rapid Hydra restart.
    TDuration TabletSnapshotEvictionTimeout;

    //! Column evaluator used for handling tablet writes.
    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;

    TMasterConnectorConfigPtr MasterConnector;

    TTabletNodeConfig();
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicatorHintConfig
    : public NYTree::TYsonSerializable
{
public:
    THashSet<TString> BannedReplicaClusters;

    TReplicatorHintConfig();
};

DEFINE_REFCOUNTED_TYPE(TReplicatorHintConfig)

///////////////////////////////////////////////////////////////////////////////

class TTabletHunkWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NTableClient::THunkChunkPayloadWriterConfig
{
    REGISTER_YSON_STRUCT(TTabletHunkWriterConfig)

    static void Register(TRegistrar registrar)
    {
        registrar.Preprocessor([&] (TTabletHunkWriterConfig* config) {
            config->UseStripedErasureWriter = true;
        });

        registrar.Postprocessor([&] (TTabletHunkWriterConfig* config) {
            if (!config->UseStripedErasureWriter) {
                THROW_ERROR_EXCEPTION("Hunk chunk writer must use striped erasure writer");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletHunkWriterConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
