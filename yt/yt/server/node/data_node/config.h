#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/distributed_chunk_session_server/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/io/config.h>

#include <yt/yt/library/disk_manager/public.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/journal_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TP2PConfig
    : public NYTree::TYsonStruct
{
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
    TDuration SessionTtl;

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

    bool TrackMemoryOfChunkBlocksBuffer;

    TBooleanFormula NodeTagFilter;

    REGISTER_YSON_STRUCT(TP2PConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TP2PConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkLocationConfig
    : public NServer::TDiskLocationConfig
{
    static constexpr bool EnableHazard = true;

    //! Maximum space chunks are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    std::optional<i64> Quota;

    // NB: Actually registered as parameter by subclasses (because default value
    // is subclass-specific).
    TString MediumName;

    //! Configuration for various per-location throttlers.
    TEnumIndexedArray<EChunkLocationThrottlerKind, NConcurrency::TThroughputThrottlerConfigPtr> Throttlers;

    //! Configuration for uncategorized throttler.
    bool EnableUncategorizedThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr UncategorizedThrottler;

    //! IO engine type.
    NIO::EIOEngineType IOEngineType;

    //! IO engine config.
    NYTree::INodePtr IOConfig;

    TDuration ThrottleDuration;

    //! Maximum number of bytes in the gap between two adjacent read locations
    //! in order to join them together during read coalescing.
    i64 CoalescedReadMaxGapSize;

    i64 MaxWriteRateByDwpd;

    double IOWeight;

    bool ResetUuid;

    TEnumIndexedArray<EWorkloadCategory, std::optional<double>> FairShareWorkloadCategoryWeights;

    //! Limit on the maximum memory used of location reads.
    i64 ReadMemoryLimit;

    //! Limit on the maximum memory used of location writes.
    i64 WriteMemoryLimit;

    //! Limit on the maximum count of location write sessions.
    i64 SessionCountLimit;

    //! If the tracked memory is close to the limit, new sessions will not be started.
    double MemoryLimitFractionForStartingNewSessions;

    void ApplyDynamicInplace(const TChunkLocationDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TChunkLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkLocationDynamicConfig
    : public NServer::TDiskLocationDynamicConfig
{
    std::optional<NIO::EIOEngineType> IOEngineType;
    NYTree::INodePtr IOConfig;

    TEnumIndexedArray<EChunkLocationThrottlerKind, NConcurrency::TThroughputThrottlerConfigPtr> Throttlers;
    std::optional<TDuration> ThrottleDuration;

    std::optional<bool> EnableUncategorizedThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr UncategorizedThrottler;

    std::optional<i64> CoalescedReadMaxGapSize;

    TEnumIndexedArray<EWorkloadCategory, std::optional<double>> FairShareWorkloadCategoryWeights;

    //! Limit on the maximum memory used by location reads.
    std::optional<i64> ReadMemoryLimit;

    //! Limit on the maximum memory used by location writes.
    std::optional<i64> WriteMemoryLimit;

    //! Limit on the maximum count of location write sessions.
    std::optional<i64> SessionCountLimit;

    //! If the tracked memory is close to the limit, new sessions will not be started.
    std::optional<double> MemoryLimitFractionForStartingNewSessions;

    REGISTER_YSON_STRUCT(TChunkLocationDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkLocationDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStoreLocationConfig
    : public TChunkLocationConfig
{
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

    //! Per-location multiplexed changelog configuration.
    NYTree::INodePtr MultiplexedChangelog;

    //! Per-location  configuration of per-chunk changelog that backs the multiplexed changelog.
    NYTree::INodePtr HighLatencySplitChangelog;

    //! Per-location configuration of per-chunk changelog that is being written directly (w/o multiplexing).
    NYTree::INodePtr LowLatencySplitChangelog;

    TStoreLocationConfigPtr ApplyDynamic(const TStoreLocationDynamicConfigPtr& dynamicConfig) const;
    void ApplyDynamicInplace(const TStoreLocationDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TStoreLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TStoreLocationDynamicConfig
    : public TChunkLocationDynamicConfig
{
    std::optional<i64> LowWatermark;
    std::optional<i64> HighWatermark;
    std::optional<i64> DisableWritesWatermark;

    std::optional<TDuration> MaxTrashTtl;
    std::optional<i64> TrashCleanupWatermark;
    std::optional<TDuration> TrashCheckPeriod;

    REGISTER_YSON_STRUCT(TStoreLocationDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStoreLocationDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCacheLocationConfig
    : public TChunkLocationConfig
{
    //! Controls incoming location bandwidth used by cache.
    NConcurrency::TThroughputThrottlerConfigPtr InThrottler;

    REGISTER_YSON_STRUCT(TCacheLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCacheLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMultiplexedChangelogConfig
    : public NHydra::TFileChangelogConfig
    , public NHydra::TFileChangelogDispatcherConfig
{
    static constexpr bool EnableHazard = true;

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
    //! and speed up startup).
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

    REGISTER_YSON_STRUCT(TMultiplexedChangelogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiplexedChangelogConfig)

////////////////////////////////////////////////////////////////////////////////

struct TArtifactCacheReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
    , public virtual NTableClient::TTableReaderConfig
    , public virtual NApi::TFileReaderConfig
{
    REGISTER_YSON_STRUCT(TArtifactCacheReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TArtifactCacheReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TLayerLocationConfig
    : public NServer::TDiskLocationConfig
{
    //! The location is considered to be full when available space becomes less than #LowWatermark.
    i64 LowWatermark;

    //! Maximum space layers are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    std::optional<i64> Quota;

    bool LocationIsAbsolute;

    bool ResidesOnTmpfs;

    REGISTER_YSON_STRUCT(TLayerLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLayerLocationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTmpfsLayerCacheConfig
    : public NYTree::TYsonStruct
{
    i64 Capacity;
    std::optional<TString> LayersDirectoryPath;
    TDuration LayersUpdatePeriod;

    REGISTER_YSON_STRUCT(TTmpfsLayerCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTmpfsLayerCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaCacheConfig
    : public TSlruCacheConfig
{
    //! Timeout for table schema request.
    TDuration TableSchemaCacheRequestTimeout;

    REGISTER_YSON_STRUCT(TTableSchemaCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableSchemaCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
    std::optional<TDuration> TableSchemaCacheRequestTimeout;

    REGISTER_YSON_STRUCT(TTableSchemaCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableSchemaCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TVolumeManagerConfig
    : public NYTree::TYsonStruct
{
    NContainers::TPortoExecutorDynamicConfigPtr PortoExecutor;
    std::vector<TLayerLocationConfigPtr> LayerLocations;
    bool EnableLayersCache;
    double CacheCapacityFraction;
    int LayerImportConcurrency;

    //! Enforce disk space limits for Porto volumes.
    bool EnableDiskQuota;

    TTmpfsLayerCacheConfigPtr RegularTmpfsLayerCache;
    TTmpfsLayerCacheConfigPtr NirvanaTmpfsLayerCache;

    REGISTER_YSON_STRUCT(TVolumeManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVolumeManagerConfig)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan): Drop all the optionals in this class after configs migration.
struct TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
    //! Period between consequent incremental data node heartbeats.
    std::optional<TDuration> IncrementalHeartbeatPeriod;

    //! Splay for data node heartbeats.
    TDuration IncrementalHeartbeatPeriodSplay;

    NConcurrency::TRetryingPeriodicExecutorOptions HeartbeatExecutor;

    //! Period between consequent job heartbeats to a given cell.
    std::optional<TDuration> JobHeartbeatPeriod;

    //! Splay for job heartbeats.
    TDuration JobHeartbeatPeriodSplay;

    //! Delay before a node sends its first data heartbeat
    //! to master-server after successful registration.
    std::optional<TDuration> DelayBeforeFullHeartbeatReport;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<NConcurrency::TRetryingPeriodicExecutorOptions> HeartbeatExecutor;

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

    //! Test location disable during full heartbeat, contains location uuid.
    std::optional<TChunkLocationUuid> LocationUuidToDisableDuringFullHeartbeat;

    //! Test data node intermediate state at master during full hearbteat session.
    std::optional<TDuration> FullHeartbeatSessionSleepDuration;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAllyReplicaManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Period between consequent requests to a certain node.
    TDuration AnnouncementBackoffTime;

    //! Maximum number of chunks per a single announcement request.
    i64 MaxChunksPerAnnouncementRequest;

    //! Timeout for AnnounceChunkReplicas request.
    TDuration AnnouncementRequestTimeout;

    REGISTER_YSON_STRUCT(TAllyReplicaManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllyReplicaManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTestingOptions
    : public NYTree::TYsonStruct
{
public:
    //! This duration will be used to insert delays within [0, MaxDelay] after each
    //! chunk meta fetch for GetColumnarStatistics.
    std::optional<TDuration> ColumnarStatisticsChunkMetaFetchMaxDelay;

    bool SimulateNetworkThrottlingForGetBlockSet;

    // For testing purposes.
    bool FailReincarnationJobs;

    //! Fraction of GetBlockSet/GetBlockRange RPC timeout, after which reading routine tries
    //! to return all blocks read up to moment (in case at least one block is read; otherwise
    //! it still tries to read at least one block).
    double BlockReadTimeoutFraction;

    //! Fraction of the GetColumnarStatistics RPC timeout, after which early exit is performed and currently uncompleted
    //! chunk fetches are failed with a timeout error.
    //! The enable_early_exit field has to be set to true in the request options for this option to have any effect.
    double ColumnarStatisticsReadTimeoutFraction;

    // Delay before blob session block free.
    std::optional<TDuration> DelayBeforeBlobSessionBlockFree;

    // Delay before cancelling chunk
    std::optional<TDuration> ChunkCancellationDelay;

    // Stop trash scanning at initialization
    std::optional<bool> EnableTrashScanningBarrier;

    REGISTER_YSON_STRUCT(TDataNodeTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataNodeTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct TMediumThroughputMeterConfig
    : public NIO::TGentleLoaderConfig
{
    TString MediumName;
    bool Enabled;

    double VerificationInitialWindowFactor;
    double VerificationSegmentSizeFactor;
    TDuration VerificationWindowPeriod;
    double DWPDFactor;
    bool UseWorkloadModel;

    REGISTER_YSON_STRUCT(TMediumThroughputMeterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumThroughputMeterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TIOThroughputMeterConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;

    std::vector<TMediumThroughputMeterConfigPtr> Media;

    // Remeasure throughput after this timeout.
    TDuration TimeBetweenTests;

    // Desired estimate stage duration.
    TDuration EstimateTimeLimit;

    int MaxEstimateCongestions;

    // Max allowed overall testing duration.
    TDuration TestingTimeHardLimit;

    REGISTER_YSON_STRUCT(TIOThroughputMeterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIOThroughputMeterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TLocationHealthCheckerDynamicConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;

    bool EnableManualDiskFailures;

    bool EnableNewDiskChecker;

    TDuration HealthCheckPeriod;

    REGISTER_YSON_STRUCT(TLocationHealthCheckerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLocationHealthCheckerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRemoveChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> DelayBeforeStartRemoveChunk;

    REGISTER_YSON_STRUCT(TRemoveChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemoveChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicateChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    NChunkClient::TReplicationWriterConfigPtr Writer;

    bool UseBlockCache;

    REGISTER_YSON_STRUCT(TReplicateChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicateChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMergeWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NTableClient::TChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TMergeWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TMergeWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMergeChunksJobDynamicConfig
    : public NYTree::TYsonStruct
{
    NTableClient::TChunkReaderConfigPtr Reader;
    TMergeWriterConfigPtr Writer;

    // Testing options.
    bool FailShallowMergeValidation;
    bool FailChunkMetaValidation;
    bool TrackWriterMemory;

    i64 ReadMemoryLimit;

    // COMPAT(babenko): drop when bitwise validation proves to be just perfect
    bool EnableBitwiseRowValidation;

    REGISTER_YSON_STRUCT(TMergeChunksJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMergeChunksJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRepairReaderConfig
    : public NChunkClient::TErasureReaderConfig
    , public NJournalClient::TChunkReaderConfig
{
    REGISTER_YSON_STRUCT(TRepairReaderConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TRepairReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRepairChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    TRepairReaderConfigPtr Reader;
    NChunkClient::TReplicationWriterConfigPtr Writer;

    i64 WindowSize;

    REGISTER_YSON_STRUCT(TRepairChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRepairChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAutotomizeChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    NJournalClient::TChunkReaderConfigPtr Reader;
    NChunkClient::TReplicationWriterConfigPtr Writer;

    TDuration RpcTimeout;

    // Testing options.
    bool FailJobs;
    bool SleepInJobs;

    REGISTER_YSON_STRUCT(TAutotomizeChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAutotomizeChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReincarnateReaderConfig
    : public NChunkClient::TErasureReaderConfig
    , public NTableClient::TChunkReaderConfig
{
    REGISTER_YSON_STRUCT(TReincarnateReaderConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TReincarnateReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReincarnateWriterConfig
    : public NChunkClient::TMultiChunkWriterConfig
    , public NTableClient::TChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TReincarnateWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TReincarnateWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReincarnateChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    TReincarnateReaderConfigPtr Reader;
    TReincarnateWriterConfigPtr Writer;

    REGISTER_YSON_STRUCT(TReincarnateChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReincarnateChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSealChunkJobDynamicConfig
    : public NYTree::TYsonStruct
{
    NJournalClient::TChunkReaderConfigPtr Reader;

    REGISTER_YSON_STRUCT(TSealChunkJobDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSealChunkJobDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJournalManagerConfig
    : public virtual NYTree::TYsonStruct
{
    static constexpr bool EnableHazard = true;

    //! Configuration of multiplexed changelogs.
    TMultiplexedChangelogConfigPtr MultiplexedChangelog;

    //! Configuration of per-chunk changelogs that back the multiplexed changelog.
    NHydra::TFileChangelogConfigPtr HighLatencySplitChangelog;

    //! Configuration of per-chunk changelogs that are being written directly (w/o multiplexing).
    NHydra::TFileChangelogConfigPtr LowLatencySplitChangelog;

    REGISTER_YSON_STRUCT(TJournalManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobControllerDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration WaitingJobsTimeout;

    TDuration ProfilingPeriod;

    bool AccountMasterMemoryRequest;

    REGISTER_YSON_STRUCT(TJobControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDataNodeConfig
    : public TJournalManagerConfig
{
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

    //! Upload session timeout.
    /*!
     * Some activity must be happening in a session regularly (i.e. new
     * blocks uploaded or sent to other data nodes). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;

    //! After that time alert about long live read sessions will be sent.
    TDuration LongLiveReadSessionTreshold;

    TDuration SessionBlockReorderTimeout;

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

    //! Smoothing interval for net out limit throttling.
    TDuration NetOutThrottlingDuration;

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

    //! Configuration for various Data Node throttlers. Used when fair throttler is not enabled.
    TEnumIndexedArray<EDataNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;

    //! Configuration for RPS out throttler.
    NConcurrency::TThroughputThrottlerConfigPtr ReadRpsOutThrottler;

    //! Configuration for RPS throttler of ally replica manager.
    NConcurrency::TThroughputThrottlerConfigPtr AnnounceChunkReplicaRpsOutThrottler;

    //! Runs periodic checks against disks.
    NServer::TDiskHealthCheckerConfigPtr DiskHealthChecker;

    //! Publish disabled locations to master.
    bool PublishDisabledLocations;

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

    int MaxOutOfTurnSessions;

    //! Delay between node initialization and start of background artifact validation.
    TDuration BackgroundArtifactValidationDelay;

    //! Master connector config.
    TMasterConnectorConfigPtr MasterConnector;

    //! Config for the new P2P implementation.
    TP2PConfigPtr P2P;

    //! Distributed chunk session service config.
    NDistributedChunkSessionServer::TDistributedChunkSessionServiceConfigPtr DistributedChunkSessionService;

    //! Blocks trash scan for test purpose.
    //! Have to be static, because dynamic config loads after initialization.
    bool EnableTrashScanningBarrier;

    i64 GetCacheCapacity() const;

    REGISTER_YSON_STRUCT(TDataNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataNodeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDataNodeDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<int> StorageHeavyThreadCount;
    std::optional<int> StorageLightThreadCount;
    std::optional<int> StorageLookupThreadCount;

    //! Number of threads in MasterJob thread pool (used for master jobs execution).
    int MasterJobThreadCount;

    TEnumIndexedArray<EDataNodeThrottlerKind, NConcurrency::TRelativeThroughputThrottlerConfigPtr> Throttlers;
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

    bool TrackSystemJobsMemory;

    bool EnableGetChunkFragmentSetThrottling;
    bool EnableGetChunkFragmentSetMemoryTracking;

    //! Publish disabled locations to master.
    std::optional<bool> PublishDisabledLocations;

    bool UseDisableSendBlocks;

    TP2PConfigPtr P2P;

    //! Desired number of bytes per disk write in a blob chunks.
    std::optional<i64> BytesPerWrite;

    TDuration IOStatisticsUpdateTimeout;

    std::optional<double> ReadMetaTimeoutFraction;

    TIOThroughputMeterConfigPtr IOThroughputMeter;

    TRemoveChunkJobDynamicConfigPtr RemoveChunkJob;
    TReplicateChunkJobDynamicConfigPtr ReplicateChunkJob;
    TMergeChunksJobDynamicConfigPtr MergeChunksJob;
    TRepairChunkJobDynamicConfigPtr RepairChunkJob;
    TAutotomizeChunkJobDynamicConfigPtr AutotomizeChunkJob;
    TReincarnateChunkJobDynamicConfigPtr ReincarnateChunkJob;
    TSealChunkJobDynamicConfigPtr SealChunkJob;

    TLocationHealthCheckerDynamicConfigPtr LocationHealthChecker;

    THashMap<TString, TStoreLocationDynamicConfigPtr> StoreLocationConfigPerMedium;

    std::optional<i64> NetOutThrottlingLimit;

    std::optional<i64> DiskWriteThrottlingLimit;
    std::optional<i64> DiskReadThrottlingLimit;

    //! Testing options.
    TDataNodeTestingOptionsPtr TestingOptions;

    //! Job controller config.
    TJobControllerDynamicConfigPtr JobController;

    std::optional<double> FallbackTimeoutFraction;

    REGISTER_YSON_STRUCT(TDataNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
