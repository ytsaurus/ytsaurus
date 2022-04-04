#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/io/config.h>

#include <yt/yt/server/lib/containers/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/journal_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TP2PConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enabled;

    TSlruCacheConfigPtr BlockCache;
    TSlruCacheDynamicConfigPtr BlockCacheOverride;

    TDuration TickPeriod;
    TDuration NodeRefreshPeriod;
    TDuration RequestTimeout;
    TDuration NodeStalenessTimeout;

    TDuration IterationWaitTimeout;
    int MaxWaitingRequests;

    TDuration SessionCleaupPeriod;
    TDuration SessionTTL;

    TSlruCacheConfigPtr RequestCache;
    TSlruCacheDynamicConfigPtr RequestCacheOverride;

    TDuration ChunkCooldownTimeout;
    int MaxDistributedBytes;
    int MaxBlockSize;
    int BlockCounterResetTicks;
    int HotBlockThreshold;
    int SecondHotBlockThreshold;
    int HotBlockReplicaCount;
    int BlockRedistributionTicks;

    TBooleanFormula NodeTagFilter;

    TP2PConfig();
};

DEFINE_REFCOUNTED_TYPE(TP2PConfig)

////////////////////////////////////////////////////////////////////////////////

class TStoreLocationConfigBase
    : public TDiskLocationConfig
{
public:
    //! Maximum space chunks are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    std::optional<i64> Quota;

    // NB: actually registered as parameter by subclasses (because default value
    // is subclass-specific).
    TString MediumName;

    //! Disk family in this location (HDD, SDD, etc.)
    TString DiskFamily;

    //! Controls outcoming location bandwidth used by replication jobs.
    NConcurrency::TThroughputThrottlerConfigPtr ReplicationOutThrottler;

    //! Controls outcoming location bandwidth used by tablet compaction and partitioning.
    NConcurrency::TThroughputThrottlerConfigPtr TabletCompactionAndPartitioningOutThrottler;

    //! Controls outcoming location bandwidth used by tablet logging.
    NConcurrency::TThroughputThrottlerConfigPtr TabletLoggingOutThrottler;

    //! Controls outcoming location bandwidth used by tablet preload.
    NConcurrency::TThroughputThrottlerConfigPtr TabletPreloadOutThrottler;

    //! Controls outcoming location bandwidth used by tablet recovery.
    NConcurrency::TThroughputThrottlerConfigPtr TabletRecoveryOutThrottler;

    //! IO Engine type.
    NIO::EIOEngineType IOEngineType;

    //! IO Engine config.
    NYTree::INodePtr IOConfig;

    //! Direct IO policy for read requests.
    NIO::EDirectIOPolicy UseDirectIOForReads;

    TDuration ThrottleDuration;

    //! Maximum number of bytes in the gap between two adjacent read locations
    //! in order to join them together during read coalescing.
    i64 CoalescedReadMaxGapSize;

    //! Block device name.
    TString DeviceName;

    //! Storage device vendor info.
    TString DeviceModel;

    i64 MaxWriteRateByDWPD;

    TStoreLocationConfigBase();
};

DEFINE_REFCOUNTED_TYPE(TStoreLocationConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TStoreLocationConfig
    : public TStoreLocationConfigBase
{
public:
    //! A currently full location is considered to be non-full again when available space grows
    //! above this limit.
    i64 LowWatermark;

    //! A location is considered to be full when available space becomes less than #HighWatermark.
    i64 HighWatermark;

    //! All writes to the location are aborted when available space becomes less than #DisableWritesWatermark.
    i64 DisableWritesWatermark;

    //! Maximum amount of time files of a deleted chunk could rest in trash directory before
    //! being permanently removed.
    TDuration MaxTrashTtl;

    //! When free space drops below this watermark, the system starts deleting files in trash directory,
    //! starting from the eldest ones.
    i64 TrashCleanupWatermark;

    //! Period between trash cleanups.
    TDuration TrashCheckPeriod;

    //! Controls incoming location bandwidth used by repair jobs.
    NConcurrency::TThroughputThrottlerConfigPtr RepairInThrottler;

    //! Controls incoming location bandwidth used by replication jobs.
    NConcurrency::TThroughputThrottlerConfigPtr ReplicationInThrottler;

    //! Controls incoming location bandwidth used by tablet compaction and partitioning.
    NConcurrency::TThroughputThrottlerConfigPtr TabletCompactionAndPartitioningInThrottler;

    //! Controls incoming location bandwidth used by tablet journals.
    NConcurrency::TThroughputThrottlerConfigPtr TabletLoggingInThrottler;

    //! Controls incoming location bandwidth used by tablet snapshots.
    NConcurrency::TThroughputThrottlerConfigPtr TabletSnapshotInThrottler;

    //! Controls incoming location bandwidth used by tablet store flush.
    NConcurrency::TThroughputThrottlerConfigPtr TabletStoreFlushInThrottler;

    //! Per-location multiplexed changelog configuration.
    NYTree::INodePtr MultiplexedChangelog;

    //! Per-location  configuration of per-chunk changelog that backs the multiplexed changelog.
    NYTree::INodePtr HighLatencySplitChangelog;

    //! Per-location configuration of per-chunk changelog that is being written directly (w/o multiplexing).
    NYTree::INodePtr LowLatencySplitChangelog;

    TStoreLocationConfig();
};

DEFINE_REFCOUNTED_TYPE(TStoreLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TCacheLocationConfig
    : public TStoreLocationConfigBase
{
public:
    //! Controls incoming location bandwidth used by cache.
    NConcurrency::TThroughputThrottlerConfigPtr InThrottler;

    TCacheLocationConfig();
};

DEFINE_REFCOUNTED_TYPE(TCacheLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiplexedChangelogConfig
    : public NHydra::TFileChangelogConfig
    , public NHydra::TFileChangelogDispatcherConfig
{
public:
    //! Multiplexed changelog record count limit.
    /*!
     *  When this limit is reached, the current multiplexed changelog is rotated.
     */
    int MaxRecordCount;

    //! Multiplexed changelog data size limit, in bytes.
    /*!
     *  See #MaxRecordCount.
     */
    i64 MaxDataSize;

    //! Interval between automatic changelog rotation (to avoid keeping too many non-clean records
    //! and speed up starup).
    TDuration AutoRotationPeriod;

    //! Maximum bytes of multiplexed changelog to read during
    //! a single iteration of replay.
    i64 ReplayBufferSize;

    //! Maximum number of clean multiplexed changelogs to keep.
    int MaxCleanChangelogsToKeep;

    //! Time to wait before marking a multiplexed changelog as clean.
    TDuration CleanDelay;

    //! Records bigger than BigRecordThreshold are not multiplexed.
    std::optional<i64> BigRecordThreshold;

    TMultiplexedChangelogConfig();
};

DEFINE_REFCOUNTED_TYPE(TMultiplexedChangelogConfig)

////////////////////////////////////////////////////////////////////////////////

class TArtifactCacheReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
    , public virtual NTableClient::TTableReaderConfig
    , public virtual NApi::TFileReaderConfig
{
public:
    REGISTER_YSON_STRUCT(TArtifactCacheReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArtifactCacheReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TLayerLocationConfig
    : public TDiskLocationConfig
{
public:
    //! The location is considered to be full when available space becomes less than #LowWatermark.
    i64 LowWatermark;

    //! Maximum space layers are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    std::optional<i64> Quota;

    bool LocationIsAbsolute;

    TLayerLocationConfig();
};

DEFINE_REFCOUNTED_TYPE(TLayerLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsLayerCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 Capacity;
    std::optional<TString> LayersDirectoryPath;
    TDuration LayersUpdatePeriod;

    TTmpfsLayerCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TTmpfsLayerCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCacheConfig
    : public TSlruCacheConfig
{
public:
    //! Timeout for table schema request.
    TDuration TableSchemaCacheRequestTimeout;

    REGISTER_YSON_STRUCT(TTableSchemaCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
public:
    std::optional<TDuration> TableSchemaCacheRequestTimeout;

    REGISTER_YSON_STRUCT(TTableSchemaCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TVolumeManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    NContainers::TPortoExecutorConfigPtr PortoExecutor;
    std::vector<TLayerLocationConfigPtr> LayerLocations;
    bool EnableLayersCache;
    double CacheCapacityFraction;
    int LayerImportConcurrency;

    bool EnableDiskQuota;

    bool ConvertLayersToSquashfs;

    //! Path to tar2squash binary.
    TString Tar2SquashToolPath;
    bool UseBundledTar2Squash;

    TTmpfsLayerCacheConfigPtr RegularTmpfsLayerCache;
    TTmpfsLayerCacheConfigPtr NirvanaTmpfsLayerCache;

    TVolumeManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TVolumeManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TRepairReaderConfig
    : public virtual NChunkClient::TReplicationReaderConfig
    , public virtual NJournalClient::TChunkReaderConfig
{
    REGISTER_YSON_STRUCT(TRepairReaderConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TRepairReaderConfig)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan): Drop all the optionals in this class after configs migration.
class TMasterConnectorConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent incremental data node heartbeats.
    std::optional<TDuration> IncrementalHeartbeatPeriod;

    //! Splay for data node heartbeats.
    TDuration IncrementalHeartbeatPeriodSplay;

    //! Period between consequent job heartbeats to a given cell.
    std::optional<TDuration> JobHeartbeatPeriod;

    //! Splay for job heartbeats.
    TDuration JobHeartbeatPeriodSplay;

    TMasterConnectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent incremental data node heartbeats.
    std::optional<TDuration> IncrementalHeartbeatPeriod;

    //! Splay for data node heartbeats.
    std::optional<TDuration> IncrementalHeartbeatPeriodSplay;

    //! Timeout for incremental data node heartbeat RPC request.
    TDuration IncrementalHeartbeatTimeout;

    //! Timeout for full data node heartbeat.
    TDuration FullHeartbeatTimeout;

    //! Period between consequent job heartbeats to a given cell.
    std::optional<TDuration> JobHeartbeatPeriod;

    //! Splay for job heartbeats.
    std::optional<TDuration> JobHeartbeatPeriodSplay;

    //! Timeout for job heartbeat RPC request.
    TDuration JobHeartbeatTimeout;

    //! Maximum number of chunk events per incremental heartbeat.
    i64 MaxChunkEventsPerIncrementalHeartbeat;

    //! Enable detailed incremental heartbeat statistics profiling.
    bool EnableProfiling;

    TMasterConnectorDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TAllyReplicaManagerDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Period between consequent requests to a certain node.
    TDuration AnnouncementBackoffTime;

    //! Maximum number of chunks per a single announcement request.
    i64 MaxChunksPerAnnouncementRequest;

    //! Timeout for AnnounceChunkReplicas request.
    TDuration AnnouncementRequestTimeout;

    TAllyReplicaManagerDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TAllyReplicaManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkAutotomizerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration RpcTimeout;

    // Testing options.
    bool FailJobs;
    bool SleepInJobs;

    TChunkAutotomizerConfig();
};

DEFINE_REFCOUNTED_TYPE(TChunkAutotomizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    //! This duration will be used to insert delays within [0, MaxDelay] after each
    //! chunk meta fetch for GetColumnarStatistics.
    std::optional<TDuration> ColumnarStatisticsChunkMetaFetchMaxDelay;

    TDataNodeTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TDataNodeTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TMediumThroughputMeterConfig
    : public NIO::TGentleLoaderConfig
{
public:
    TString MediumName;
    bool Enabled;

    TMediumThroughputMeterConfig();
};

DEFINE_REFCOUNTED_TYPE(TMediumThroughputMeterConfig)

////////////////////////////////////////////////////////////////////////////////

class TIOThroughputMeterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enabled;

    std::vector<TMediumThroughputMeterConfigPtr> Mediums;

    // Remeasure throughtput after this timeout.
    TDuration TimeBetweenTests;

    // Desired testing session duration.
    TDuration TestingTimeSoftLimit;

    // Max allowed testing duration.
    TDuration TestingTimeHardLimit;

    int MaxCongestionsPerTest;

    TIOThroughputMeterConfig();
};

DEFINE_REFCOUNTED_TYPE(TIOThroughputMeterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkMergerConfig
    : public NYTree::TYsonSerializable
{
public:
    // Testing options.
    bool FailShallowMergeValidation;

    TChunkMergerConfig();
};

DEFINE_REFCOUNTED_TYPE(TChunkMergerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkRepairJobDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    NChunkClient::TErasureReaderConfigPtr Reader;

    i64 WindowSize;

    TChunkRepairJobDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TChunkRepairJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Timeout for lease transactions.
    TDuration LeaseTransactionTimeout;

    //! Period between consequent lease transaction pings.
    TDuration LeaseTransactionPingPeriod;

    //! Period between consequent incremental heartbeats.
    TDuration IncrementalHeartbeatPeriod;

    //! Splay for incremental heartbeats.
    TDuration IncrementalHeartbeatPeriodSplay;

    //! Controls incremental heartbeats from node to master.
    NConcurrency::TThroughputThrottlerConfigPtr IncrementalHeartbeatThrottler;

    //! Period between consequent registration attempts.
    TDuration RegisterRetryPeriod;

    //! Splay for consequent registration attempts.
    TDuration RegisterRetrySplay;

    //! Timeout for RegisterNode requests.
    TDuration RegisterTimeout;

    //! Timeout for NodeTrackerService:IncrementalHeartbeat requests.
    TDuration IncrementalHeartbeatTimeout;

    //! Timeout for NodeTrackerService:FullHeartbeat requests.
    TDuration FullHeartbeatTimeout;

    //! Timeout for JobTrackerService:Heartbeat requests.
    TDuration JobHeartbeatTimeout;

    //! Cache for chunk metas.
    TSlruCacheConfigPtr ChunkMetaCache;

    //! Cache for blocks extensions.
    TSlruCacheConfigPtr BlocksExtCache;

    //! Cache for partition block metas.
    TSlruCacheConfigPtr BlockMetaCache;

    //! Cache for all types of blocks.
    NChunkClient::TBlockCacheConfigPtr BlockCache;

    //! Opened blob chunks cache.
    TSlruCacheConfigPtr BlobReaderCache;

    //! Opened changelogs cache.
    TSlruCacheConfigPtr ChangelogReaderCache;

    //! Table schema and row key comparer cache.
    TTableSchemaCacheConfigPtr TableSchemaCache;

    //! Multiplexed changelog configuration.
    TMultiplexedChangelogConfigPtr MultiplexedChangelog;

    //! Configuration of per-chunk changelog that backs the multiplexed changelog.
    NHydra::TFileChangelogConfigPtr HighLatencySplitChangelog;

    //! Configuration of per-chunk changelog that is being written directly (w/o multiplexing).
    NHydra::TFileChangelogConfigPtr LowLatencySplitChangelog;

    //! Upload session timeout.
    /*!
     * Some activity must be happening in a session regularly (i.e. new
     * blocks uploaded or sent to other data nodes). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;

    //! Timeout for "PutBlocks" requests to other data nodes.
    TDuration NodeRpcTimeout;

    //! Period between peer updates (see TBlockPeerUpdater).
    TDuration PeerUpdatePeriod;

    //! Peer update expiration time (see TBlockPeerUpdater).
    TDuration PeerUpdateExpirationTime;

    //! Read requests are throttled when the number of bytes queued at Bus layer exceeds this limit.
    //! This is a global limit.
    //! Cf. TTcpDispatcherStatistics::PendingOutBytes
    i64 NetOutThrottlingLimit;

    //! Extra limit for net queue size, that is checked after blocks are read from disk.
    i64 NetOutThrottlingExtraLimit;

    TDuration NetOutThrottleDuration;

    //! Write requests are throttled when the number of bytes queued for write exceeds this limit.
    //! This is a per-location limit.
    i64 DiskWriteThrottlingLimit;

    //! Read requests are throttled when the number of bytes scheduled for read exceeds this limit.
    //! This is a per-location limit.
    i64 DiskReadThrottlingLimit;

    //! Regular storage locations.
    std::vector<TStoreLocationConfigPtr> StoreLocations;

    //! Cached chunks location.
    std::vector<TCacheLocationConfigPtr> CacheLocations;

    //! Manages layers and root volumes for Porto job environment.
    TVolumeManagerConfigPtr VolumeManager;

    //! Writer configuration used to replicate chunks.
    NChunkClient::TReplicationWriterConfigPtr ReplicationWriter;

    //! Reader configuration used to repair chunks (both blob and journal).
    TRepairReaderConfigPtr RepairReader;

    //! Writer configuration used to repair chunks.
    NChunkClient::TReplicationWriterConfigPtr RepairWriter;

    //! Reader configuration used to seal chunks.
    NJournalClient::TChunkReaderConfigPtr SealReader;

    //! Reader configuration used to merge chunks.
    NChunkClient::TReplicationReaderConfigPtr MergeReader;

    //! Writer configuration used to merge chunks.
    NChunkClient::TMultiChunkWriterConfigPtr MergeWriter;

    //! Reader configuration used to autotomize chunks.
    NJournalClient::TChunkReaderConfigPtr AutotomyReader;

    //! Writer configuration used to autotomize chunks.
    NChunkClient::TReplicationWriterConfigPtr AutotomyWriter;

    //! Configuration for various Data Node throttlers. Used when fair throttler is not enabled.
    TEnumIndexedVector<EDataNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    //! Configuration for rps out throttler.
    NConcurrency::TThroughputThrottlerConfigPtr ReadRpsOutThrottler;

    //! Configuration for rps throttler of ally replica manager.
    NConcurrency::TThroughputThrottlerConfigPtr AnnounceChunkReplicaRpsOutThrottler;

    //! Runs periodic checks against disks.
    TDiskHealthCheckerConfigPtr DiskHealthChecker;

    //! Maximum number of concurrent balancing write sessions.
    int MaxWriteSessions;

    //! Maximum number of blocks to fetch via a single range request.
    int MaxBlocksPerRead;

    //! Maximum number of bytes to fetch via a single range request.
    i64 MaxBytesPerRead;

    //! Desired number of bytes per disk write in a blob chunks.
    i64 BytesPerWrite;

    //! Enables block checksums validation.
    bool ValidateBlockChecksums;

    //! The time after which any registered placement info expires.
    TDuration PlacementExpirationTime;

    //! Controls if cluster and cell directories are to be synchronized on connect.
    //! Useful for tests.
    bool SyncDirectoriesOnConnect;

    //! The number of threads in StorageHeavy thread pool (used for extracting chunk meta, handling
    //! chunk slices, columnar statistic etc).
    int StorageHeavyThreadCount;

    //! The number of threads in StorageLight thread pool (used for reading chunk blocks).
    int StorageLightThreadCount;

    //! Number of threads in DataNodeLookup thread pool (used for row lookups).
    int StorageLookupThreadCount;

    //! Number of replication errors sent in heartbeat.
    int MaxReplicationErrorsInHeartbeat;

    //! Number of tablet errors sent in heartbeat.
    int MaxTabletErrorsInHeartbeat;

    //! Fraction of GetBlockSet/GetBlockRange RPC timeout, after which reading routine tries
    //! to return all blocks read up to moment (in case at least one block is read; otherwise
    //! it still tries to read at least one block).
    double BlockReadTimeoutFraction;

    //! Fraction of the GetColumnarStatistics RPC timeout, after which early exit is performed and currently uncompleted
    //! chunk fetches are failed with a timeout error.
    //! The enable_early_exit field has to be set to true in the request options for this option to have any effect.
    double ColumnarStatisticsReadTimeoutFraction;

    //! Delay between node initializatin and start of background artifact validation.
    TDuration BackgroundArtifactValidationDelay;

    //! Master connector config.
    TMasterConnectorConfigPtr MasterConnector;

    //! Config for the new P2P implementation.
    TP2PConfigPtr P2P;

    //! Testing options.
    TDataNodeTestingOptionsPtr TestingOptions;

    TDataNodeConfig();

    i64 GetCacheCapacity() const;

    i64 GetNetOutThrottlingHardLimit() const;
};

DEFINE_REFCOUNTED_TYPE(TDataNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> StorageHeavyThreadCount;
    std::optional<int> StorageLightThreadCount;
    std::optional<int> StorageLookupThreadCount;

    //! Number of threads in MasterJob thread pool (used for master jobs execution).
    int MasterJobThreadCount;

    TEnumIndexedVector<EDataNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;
    NConcurrency::TThroughputThrottlerConfigPtr ReadRpsOutThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr AnnounceChunkReplicaRpsOutThrottler;

    TSlruCacheDynamicConfigPtr ChunkMetaCache;
    TSlruCacheDynamicConfigPtr BlocksExtCache;
    TSlruCacheDynamicConfigPtr BlockMetaCache;
    NChunkClient::TBlockCacheDynamicConfigPtr BlockCache;
    TSlruCacheDynamicConfigPtr BlobReaderCache;
    TSlruCacheDynamicConfigPtr ChangelogReaderCache;
    TTableSchemaCacheDynamicConfigPtr TableSchemaCache;

    TMasterConnectorDynamicConfigPtr MasterConnector;
    TAllyReplicaManagerDynamicConfigPtr AllyReplicaManager;

    //! Prepared chunk readers are kept open during this period of time after the last use.
    TDuration ChunkReaderRetentionTimeout;

    //! Reader configuration used to download chunks into cache.
    TArtifactCacheReaderConfigPtr ArtifactCacheReader;

    //! If |true|, node will abort when location becomes disabled.
    bool AbortOnLocationDisabled;

    TP2PConfigPtr P2P;

    TChunkAutotomizerConfigPtr ChunkAutotomizer;

    TDuration IOStatisticsUpdateTimeout;

    // COMPAT(gritukan, capone212)
    NChunkClient::TErasureReaderConfigPtr AdaptiveChunkRepairJob;

    TIOThroughputMeterConfigPtr IOThroughputMeter;

    TChunkMergerConfigPtr ChunkMerger;

    TChunkRepairJobDynamicConfigPtr ChunkRepairJob;

    THashMap<TString, NYTree::INodePtr> MediumIOConfig;

    TDataNodeDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TDataNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
