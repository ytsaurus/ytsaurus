#pragma once

#include "public.h"

#include <yt/yt/core/http/config.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/public.h>

#include <optional>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkManagerConfig
    : public NYTree::TYsonStruct
{
    //! A default value for an additional bound for the global replication
    //! factor cap. The value is used when a new medium is created to initialize
    //! corresponding medium-specific setting.
    int MaxReplicationFactor;
    //! A default value for an additional bound for the number of replicas per
    //! rack for every chunk. The value is used when a new medium is created to
    //! initialize corresponding medium-specific setting.
    int MaxReplicasPerRack;
    //! Same as #MaxReplicasPerRack but only applies to regular chunks.
    int MaxRegularReplicasPerRack;
    //! Same as #MaxReplicasPerRack but only applies to journal chunks.
    int MaxJournalReplicasPerRack;
    //! Same as #MaxReplicasPerRack but only applies to erasure chunks.
    int MaxErasureReplicasPerRack;
    //! Same as #MaxReplicasPerRack but only applies to erasure journal chunks.
    int MaxErasureJournalReplicasPerRack;

    //! Enables storing more than one chunk part per node.
    //! Should only be used in local mode to enable writing erasure chunks in a cluster with just one node.
    bool AllowMultipleErasurePartsPerNode;

    //! When balancing chunk repair queues for multiple media, how often do
    //! their weights decay. (Weights are essentially repaired data sizes.)
    TDuration RepairQueueBalancerWeightDecayInterval;
    //! The number by which chunk repair queue weights are multiplied during decay.
    double RepairQueueBalancerWeightDecayFactor;

    REGISTER_YSON_STRUCT(TChunkManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDomesticMediumConfig
    : public NYTree::TYsonStruct
{
    //! An additional bound for the global replication factor cap.
    //! Useful when the number of racks is too low to interoperate meaningfully
    //! with the default cap.
    int MaxReplicationFactor;

    //! Provides an additional bound for the number of replicas per rack for every chunk.
    int MaxReplicasPerRack;

    //! Same as #MaxReplicasPerRack but only applies to regular chunks.
    int MaxRegularReplicasPerRack;

    //! Same as #MaxReplicasPerRack but only applies to journal chunks.
    int MaxJournalReplicasPerRack;

    //! Same as #MaxReplicasPerRack but only applies to erasure chunks.
    int MaxErasureReplicasPerRack;

    //! Same as #MaxReplicasPerRack but only applies to erasure journal chunks.
    int MaxErasureJournalReplicasPerRack;

    //! Default behavior for dynamic tables, living on this medium.
    bool PreferLocalHostForDynamicTables;

    REGISTER_YSON_STRUCT(TDomesticMediumConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDomesticMediumConfig)

////////////////////////////////////////////////////////////////////////////////

// TODO(gritukan): This config is copied from yt/yt/library/s3 to prevent peerdir
// from master to library reaches libiconv by dependencies.
struct TS3ConnectionConfig
    : public virtual NYTree::TYsonStruct
{
    //! Url of the S3 server, for example, http://my_bucket.s3.amazonaws.com
    TString Url;

    //! Name of the region.
    //! In some of the S3 implementations it is already included into
    //! address, in some not.
    TString Region;

    //! Name of the bucket to use.
    TString Bucket;

    //! Credentials.
    TString AccessKeyId;
    TString SecretAccessKey;

    REGISTER_YSON_STRUCT(TS3ConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TS3ClientConfig
    : public TS3ConnectionConfig
    , public NHttp::TClientConfig
{
    REGISTER_YSON_STRUCT(TS3ClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3ClientConfig)

////////////////////////////////////////////////////////////////////////////////

struct TS3MediumConfig
    : public TS3ConnectionConfig
{
    REGISTER_YSON_STRUCT(TS3MediumConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TS3MediumConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkMergerConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    int MaxChunkCount;
    int MinChunkCount;

    // If we are in auto mode and less than MinShallowMergeChunkCount chunks satisfy shallow
    // merge criteria, fallback to deep merge right away.
    int MinShallowMergeChunkCount;

    i64 MaxRowCount;
    i64 MaxDataWeight;
    i64 MaxUncompressedDataSize;
    i64 MaxCompressedDataSize;
    i64 MaxInputChunkDataWeight;

    i64 MaxBlockCount;
    i64 MaxJobsPerChunkList;
    int MaxChunkListCountPerMergeSession;

    TDuration SchedulePeriod;
    TDuration CreateChunksPeriod;
    TDuration TransactionUpdatePeriod;
    TDuration SessionFinalizationPeriod;
    TDuration ScheduleChunkReplacePeriod;

    int CreateChunksBatchSize;
    int SessionFinalizationBatchSize;

    int QueueSizeLimit;
    int MaxRunningJobCount;

    //! Fraction (in percents) of shallow merge jobs for which validation is run.
    int ShallowMergeValidationProbability;

    bool EnableChunkMetaExtensionsValidation;

    bool RescheduleMergeOnSuccess;
    bool AllowSettingChunkMergerMode;

    // COMPAT(aleksandra-zh)
    bool EnableQueueSizeLimitChanges;

    // COMPAT(aleksandra-zh)
    bool RespectAccountSpecificToggle;

    // COMPAT(aleksandra-zh)
    bool EnableCarefulRequisitionUpdate;

    int MaxNodesBeingMerged;

    int MaxChunkListsWithChunksBeingReplaced;

    int MaxAllowedBackoffReschedulingsPerSession;

    TDuration MinBackoffPeriod;
    TDuration MaxBackoffPeriod;

    // For testing purposes.
    std::optional<int> MaxChunksPerIteration;
    std::optional<TDuration> DelayBetweenIterations;

    int MaxChunkMetaSize;

    REGISTER_YSON_STRUCT(TDynamicChunkMergerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkMergerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicMasterCellChunkStatisticsCollectorConfig
    : public NYTree::TYsonStruct
{
    int MaxChunksPerScan;
    TDuration ChunkScanPeriod;

    std::vector<TInstant> CreationTimeHistogramBucketBounds;

    REGISTER_YSON_STRUCT(TDynamicMasterCellChunkStatisticsCollectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicMasterCellChunkStatisticsCollectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkReincarnatorConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    TDuration ChunkScanPeriod;
    int MaxChunksPerScan;

    //! Either chunk lists or chunk owners.
    //! Exceeding this limit means chunk cannot be reincarnated.
    int MaxVisitedChunkAncestorsPerChunk;

    TInstant MinAllowedCreationTime;

    int MaxRunningJobCount;
    int ReplacedChunkBatchSize;

    TDuration TransactionUpdatePeriod;

    int MaxFailedJobs;

    //! Max chunk count for which failed jobs are tracked.
    int MaxTrackedChunks;

    TDuration MulticellReincarnationTransactionTimeout;

    bool IgnoreAccountSettings;

    bool EnableVerboseLogging;

    TDuration ForcedUnderfilledBatchReplacementPeriod;

    bool SkipVersionedChunks;

    bool ShouldRescheduleAfterChange(
        const TDynamicChunkReincarnatorConfig& that) const noexcept;

    REGISTER_YSON_STRUCT(TDynamicChunkReincarnatorConfig);

    static void Register(TRegistrar registrar);

};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkReincarnatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDanglingLocationCleanerConfig
    : public NYTree::TYsonStruct
{
    static constexpr int DefaultMaxLocationsToCleanPerIteration = 10;

    // COMPAT(koloshmet)
    std::optional<TInstant> DefaultLastSeenTime;

    TDuration CleanupPeriod;
    TDuration ExpirationTimeout;
    int MaxLocationsToCleanPerIteration;
    bool Enable;

    REGISTER_YSON_STRUCT(TDanglingLocationCleanerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDanglingLocationCleanerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicDataNodeTrackerConfig
    : public NYTree::TYsonStruct
{
    // COMPAT(danilalexeev): YT-23781.
    int MaxConcurrentFullHeartbeats;

    int MaxConcurrentLocationFullHeartbeats;

    int MaxConcurrentIncrementalHeartbeats;

    TDanglingLocationCleanerConfigPtr DanglingLocationCleaner;

    // COMPAT(danilalexeev): YT-23781.
    bool EnablePerLocationFullHeartbeats;

    REGISTER_YSON_STRUCT(TDynamicDataNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicDataNodeTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicDataCenterFaultThresholdsConfig
    : public NYTree::TYsonStruct
{
    int OnlineNodeCountToDisable;
    int OnlineNodeCountToEnable;
    double OnlineNodeFractionToDisable;
    double OnlineNodeFractionToEnable;

    REGISTER_YSON_STRUCT(TDynamicDataCenterFaultThresholdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicDataCenterFaultThresholdsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicDataCenterFailureDetectorConfig
    : public NYTree::TYsonStruct
{
    TDynamicDataCenterFaultThresholdsConfigPtr DefaultThresholds;
    THashMap<std::string, TDynamicDataCenterFaultThresholdsConfigPtr> DataCenterThresholds;
    bool Enable;

    REGISTER_YSON_STRUCT(TDynamicDataCenterFailureDetectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicDataCenterFailureDetectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkTreeBalancerSettings
    : public NYTree::TYsonStruct
{
    int MaxChunkTreeRank = 32;
    int MinChunkListSize = 1024;
    int MaxChunkListSize = 2048;
    double MinChunkListToChunkRatio = 0.01;

    REGISTER_YSON_STRUCT(TChunkTreeBalancerSettings);

    static void Register(TRegistrar)
    { }

protected:
    static void RegisterParameters(
        TRegistrar registrar,
        int maxChunkTreeRank,
        int minChunkListSize,
        int maxChunkListSize,
        double minChunkListToChunkRatio);
};

struct TStrictChunkTreeBalancerSettings
    : public TChunkTreeBalancerSettings
{
    REGISTER_YSON_STRUCT(TStrictChunkTreeBalancerSettings);

    static void Register(TRegistrar registrar);
};

struct TPermissiveChunkTreeBalancerSettings
    : public TChunkTreeBalancerSettings
{
    REGISTER_YSON_STRUCT(TPermissiveChunkTreeBalancerSettings);

    static void Register(TRegistrar registrar);
};

struct TDynamicChunkTreeBalancerConfig
    : public NYTree::TYsonStruct
{
    using TChunkTreeBalancerSettingsPtr = TIntrusivePtr<TChunkTreeBalancerSettings>;

    TChunkTreeBalancerSettingsPtr StrictSettings;
    TChunkTreeBalancerSettingsPtr PermissiveSettings;

    TChunkTreeBalancerSettingsPtr GetSettingsForMode(EChunkTreeBalancerMode mode);

    REGISTER_YSON_STRUCT(TDynamicChunkTreeBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkTreeBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAllyReplicaManagerConfig
    : public NYTree::TYsonStruct
{
    //! When a chunk is not fully replicated by approved replicas, its new replicas
    //! still announce replicas to allies but with a certain delay.
    TDuration UnderreplicatedChunkAnnouncementRequestDelay;

    //! Override of |SafeOnlineNodeCount| for replica announcements and endorsements.
    std::optional<int> SafeOnlineNodeCount;

    //! Override of |SafeLostChunkCount| for replica announcements and endorsements.
    std::optional<int> SafeLostChunkCount;

    REGISTER_YSON_STRUCT(TDynamicAllyReplicaManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAllyReplicaManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkAutotomizerConfig
    : public NYTree::TYsonStruct
{
    TDuration TransactionUpdatePeriod;

    TDuration RefreshPeriod;

    TDuration ChunkUnstagePeriod;

    int TailChunksPerAllocation;

    int MaxChunksPerUnstage;

    int MaxChunksPerRefresh;

    int MaxConcurrentJobsPerChunk;

    int MaxChangedChunksPerRefresh;

    TDuration JobSpeculationTimeout;

    TDuration JobTimeout;

    bool ScheduleUrgentJobs;

    REGISTER_YSON_STRUCT(TDynamicChunkAutotomizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkAutotomizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkManagerTestingConfig
    : public NYTree::TYsonStruct
{
    //! If true, seal will always be unreliable.
    bool ForceUnreliableSeal;

    //! If true, removed replicas won't be removed from DestroyedReplicas_.
    bool DisableRemovingReplicasFromDestroyedQeueue;

    //! If true, Sequoia refresh queues are not drained.
    bool DisableSequoiaChunkRefresh;

    REGISTER_YSON_STRUCT(TDynamicChunkManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkManagerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSequoiaChunkReplicasConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    TDuration RemovalPeriod;
    int RemovalBatchSize;

    //! Probability (in percents) that chunk replicas will be Sequoia.
    int ReplicasPercentage;

    bool FetchReplicasFromSequoia;
    bool StoreSequoiaReplicasOnMaster;
    bool ProcessRemovedSequoiaReplicasOnMaster;

    bool EnableChunkPurgatory;

    bool EnableSequoiaChunkRefresh;
    TDuration SequoiaChunkRefreshPeriod;
    int SequoiaChunkCountToFetchFromRefreshQueue;

    bool ClearMasterRequest;

    std::vector<TErrorCode> RetriableErrorCodes;

    REGISTER_YSON_STRUCT(TDynamicSequoiaChunkReplicasConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSequoiaChunkReplicasConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicConsistentReplicaPlacementConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    bool EnablePullReplication;

    int TokenDistributionBucketCount;

    // NB: Nullability is for testing purposes.
    std::optional<TDuration> TokenRedistributionPeriod;

    int TokensPerNode;

    // Keep this larger than TokensPerNode * TokenDistributionBucketCount * maximum replication factor.
    int ReplicasPerChunk;

    REGISTER_YSON_STRUCT(TDynamicConsistentReplicaPlacementConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConsistentReplicaPlacementConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkManagerConfig
    : public NYTree::TYsonStruct
{
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(1000);
    static constexpr auto DefaultMaxLostVitalChunksSampleSizePerCell = 10;

    //! If set to false, disables scheduling new chunk jobs (replication, removal).
    bool EnableChunkReplicator;

    //! If set to false, disables scheduling new chunk seal jobs.
    bool EnableChunkSealer;

    //! If set to false, chunks seal unreliably instead of autotomy.
    bool EnableChunkAutotomizer;

    TDuration ReplicaApproveTimeout;

    //! Controls the maximum number of unsuccessful attempts to schedule a replication job.
    int MaxMisscheduledReplicationJobsPerHeartbeat;
    //! Controls the maximum number of unsuccessful attempts to schedule a repair job.
    int MaxMisscheduledRepairJobsPerHeartbeat;
    //! Controls the maximum number of unsuccessful attempts to schedule a removal job.
    int MaxMisscheduledRemovalJobsPerHeartbeat;
    //! Controls the maximum number of unsuccessful attempts to schedule a seal job.
    int MaxMisscheduledSealJobsPerHeartbeat;
    //! Controls the maximum number of unsuccessful attempts to schedule a merge job.
    int MaxMisscheduledMergeJobsPerHeartbeat;

    //! Maximum number of running replication jobs for CRP chunks per target node
    //! (where the chunk will be put).
    int MaxRunningReplicationJobsPerTargetNode;

    //! If set to false, fully disables background chunk refresh.
    //! Only use during bulk node restarts to save leaders' CPU.
    //! Don't forget to turn it on afterwards.
    bool EnableChunkRefresh;
    //! Graceful delay before chunk refresh.
    TDuration ChunkRefreshDelay;
    //! Interval between consequent chunk refresh iterations.
    std::optional<TDuration> ChunkRefreshPeriod;

    //! Maximum number of chunks to process during a refresh iteration.
    int MaxBlobChunksPerRefresh;

    //! Maximum number of chunks to process during a refresh iteration.
    int MaxJournalChunksPerRefresh;

    // Alert if the number of chunk refresh attempts reaches that number.
    int MaxUnsuccessfullRefreshAttempts;

    //! Interval between consequent replicator state checks.
    TDuration ReplicatorEnabledCheckPeriod;

    //! If set to false, fully disables background chunk requisition updates;
    //! see #EnableChunkRefresh for a rationale.
    bool EnableChunkRequisitionUpdate;
    //! Interval between consequent chunk requisition update iterations.
    std::optional<TDuration> ChunkRequisitionUpdatePeriod;
    //! Chunks are propagated to incumbent peers for requisition update within this period.
    TDuration ScheduledChunkRequisitionUpdatesFlushPeriod;
    //! Maximum number of chunks to delegate to incumbents for requisition update per flush.
    int MaxChunksPerRequisitionUpdateScheduling;
    //! Maximum number of chunks to process during a requisition update iteration.
    int MaxBlobChunksPerRequisitionUpdate;
    //! Maximum amount of time allowed to spend during a requisition update iteration.
    TDuration MaxTimePerBlobChunkRequisitionUpdate;
    //! Maximum number of chunks to process during a requisition update iteration.
    int MaxJournalChunksPerRequisitionUpdate;
    //! Maximum amount of time allowed to spend during a requisition update iteration.
    TDuration MaxTimePerJournalChunkRequisitionUpdate;
    //! Chunk requisition update finish mutations are batched within this period.
    TDuration FinishedChunkListsRequisitionTraverseFlushPeriod;
    //! Chunks sample are propagated to primary master within this period.
    TDuration LostVitalChunksSampleUpdatePeriod;
    //! Maximum amount of сhunks in sample.
    int MaxLostVitalChunksSampleSizePerCell;

    //! Interval between consequent seal attempts.
    TDuration ChunkSealBackoffTime;
    //! Timeout for RPC requests to nodes during journal operations.
    TDuration JournalRpcTimeout;
    //! Quorum session waits for this period of time even if quorum is already reached
    //! in order to receive responses from all the replicas and then run fast path
    //! during chunk seal.
    TDuration QuorumSessionDelay;
    //! Maximum number of chunks to process during a seal scan.
    int MaxChunksPerSeal;
    //! Maximum number of chunks that can be sealed concurrently.
    int MaxConcurrentChunkSeals;

    //! Maximum number of chunks to report per single fetch request.
    int MaxChunksPerFetch;

    //! Maximum duration a job can run before it is considered dead.
    TDuration JobTimeout;

    //! When the number of online nodes drops below this margin,
    //! replicator gets disabled. Also ally replica announcements are done lazily
    //! and endorsements are not flushed.
    int SafeOnlineNodeCount;
    //! When the fraction of lost chunks grows above this margin,
    //! replicator gets disabled.
    double SafeLostChunkFraction;
    //! When the number of lost chunks grows above this margin,
    //! replicator gets disabled.
    int SafeLostChunkCount;

    //! Memory usage assigned to every repair job.
    i64 RepairJobMemoryUsage;

    //! Throttles all chunk jobs combined.
    NConcurrency::TThroughputThrottlerConfigPtr JobThrottler;

    //! Throttles chunk jobs per type.
    THashMap<EJobType, NConcurrency::TThroughputThrottlerConfigPtr> JobTypeToThrottler;

    //! Maximum number of heavy columns in chunk approximate statistics.
    int MaxHeavyColumns;

    //! Deprecated codec ids, used values from yt/core/compression by default.
    std::optional<THashSet<NCompression::ECodec>> ForbiddenCompressionCodecs;

    //! Deprecated codec names and their alises, used values from yt/core/compression by default.
    std::optional<THashMap<std::string, std::string>> ForbiddenCompressionCodecNameToAlias;

    //! Forbidden erasure codec ids, empty by default.
    THashSet<NErasure::ECodec> ForbiddenErasureCodecs;

    //! The number of oldest part-missing chunks to be remembered by the replicator.
    int MaxOldestPartMissingChunks;

    int FinishedJobsQueueSize;

    bool AbortJobsOnEpochFinish;

    //! Controls if node={{node}} tag should be added to incremental heartbeat sensors.
    bool EnablePerNodeIncrementalHeartbeatProfiling;

    TDynamicDataNodeTrackerConfigPtr DataNodeTracker;

    TDynamicChunkTreeBalancerConfigPtr ChunkTreeBalancer;

    TDynamicChunkMergerConfigPtr ChunkMerger;

    TDynamicMasterCellChunkStatisticsCollectorConfigPtr MasterCellChunkStatisticsCollector;

    TDynamicChunkReincarnatorConfigPtr ChunkReincarnator;

    TDynamicAllyReplicaManagerConfigPtr AllyReplicaManager;

    TDynamicConsistentReplicaPlacementConfigPtr ConsistentReplicaPlacement;

    TDuration DestroyedReplicasProfilingPeriod;

    TDynamicChunkAutotomizerConfigPtr ChunkAutotomizer;

    TDynamicSequoiaChunkReplicasConfigPtr SequoiaChunkReplicas;

    TDynamicChunkManagerTestingConfigPtr Testing;

    //! If true, replicator is aware of data centers when placing replicas.
    bool UseDataCenterAwareReplicator;

    //! Set of data centers that are used for chunk storage.
    THashSet<std::string> StorageDataCenters;

    //! Set of storage data centers on which replica placement is forbidden.
    THashSet<std::string> BannedStorageDataCenters;

    TDynamicDataCenterFailureDetectorConfigPtr DataCenterFailureDetector;

    TDuration ProfilingPeriod;

    //! When set of active chunk replicator shards is changed, no removal jobs
    //! will be scheduled within this period.
    TDuration RemovalJobScheduleDelay;

    TDuration DisposedPendingRestartNodeChunkRefreshDelay;

    // COMPAT(kvk1920): YT-17756.
    bool EnableFixRequisitionUpdateOnMerge;

    bool EnableChunkSchemas;

    //! Forces rack awareness for erasure parts during write targets allocation.
    bool ForceRackAwarenessForErasureParts;

    bool EnableTwoRandomChoicesWriteTargetAllocation;
    int NodesToCheckBeforeGivingUpOnWriteTargetAllocation;

    // COMPAT(danilalexeev)
    bool ValidateResourceUsageIncreaseOnPrimaryMediumChange;

    // COMPAT(shakurov)
    bool UseHunkSpecificMediaForRequisitionUpdates;

    bool EnableRepairViaReplication;

    REGISTER_YSON_STRUCT(TDynamicChunkManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicChunkServiceConfig
    : public NYTree::TYsonStruct
{
    bool EnablePerUserRequestWeightThrottling;
    bool EnablePerUserRequestBytesThrottling;

    NConcurrency::TThroughputThrottlerConfigPtr DefaultRequestWeightThrottler;

    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserRequestWeightThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserRequestBytesThrottler;

    REGISTER_YSON_STRUCT(TDynamicChunkServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
