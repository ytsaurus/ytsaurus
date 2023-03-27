#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/public.h>

#include <optional>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManagerConfig
    : public NYTree::TYsonStruct
{
public:
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

    //! Enables storing more than one chunk part per node.
    //! Should only be used in local mode to enable writing erasure chunks in a cluster with just one node.
    bool AllowMultipleErasurePartsPerNode;

    //! Interval between consequent replicator state checks.
    TDuration ReplicatorEnabledCheckPeriod;

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

class TMediumConfig
    : public NYTree::TYsonStruct
{
public:
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

    //! Default behavior for dynamic tables, living on this medium.
    bool PreferLocalHostForDynamicTables;

    REGISTER_YSON_STRUCT(TMediumConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChunkMergerConfig
    : public NYTree::TYsonStruct
{
public:
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

    TDuration SchedulePeriod;
    TDuration CreateChunksPeriod;
    TDuration TransactionUpdatePeriod;
    TDuration SessionFinalizationPeriod;

    int CreateChunksBatchSize;
    int SessionFinalizationBatchSize;

    int QueueSizeLimit;
    int MaxRunningJobCount;

    //! Fraction (in percents) of shallow merge jobs for which validation is run.
    int ShallowMergeValidationProbability;

    bool RescheduleMergeOnSuccess;
    bool AllowSettingChunkMergerMode;

    // COMPAT(shakurov)
    bool EnableNodeStatisticsFix;

    // COMPAT(aleksandra-zh)
    bool EnableQueueSizeLimitChanges;

    int MaxNodesBeingMerged;

    // For testing purposes.
    std::optional<int> MaxChunksPerIteration;
    std::optional<TDuration> DelayBetweenIterations;

    REGISTER_YSON_STRUCT(TDynamicChunkMergerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkMergerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChunkReincarnatorConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    TDuration ChunkScanPeriod;
    int MaxChunksPerScan;
    int MaxVisitedChunkListsPerScan;

    TInstant MinAllowedCreationTime;

    int MaxRunningJobCount;
    int ReplacedChunkBatchSize;

    TDuration TransactionUpdatePeriod;

    int MaxFailedJobs;
    int MaxTrackedChunks;

    bool ShouldRescheduleAfterChange(const TDynamicChunkReincarnatorConfig& that) const noexcept;

    REGISTER_YSON_STRUCT(TDynamicChunkReincarnatorConfig)

    static void Register(TRegistrar registrar);

};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkReincarnatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicDataNodeTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxConcurrentFullHeartbeats;

    int MaxConcurrentIncrementalHeartbeats;

    REGISTER_YSON_STRUCT(TDynamicDataNodeTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicDataNodeTrackerConfig)

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

class TDynamicChunkTreeBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    using TChunkTreeBalancerSettingsPtr = TIntrusivePtr<TChunkTreeBalancerSettings>;

    TChunkTreeBalancerSettingsPtr StrictSettings;
    TChunkTreeBalancerSettingsPtr PermissiveSettings;

    TChunkTreeBalancerSettingsPtr GetSettingsForMode(EChunkTreeBalancerMode mode);

    REGISTER_YSON_STRUCT(TDynamicChunkTreeBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkTreeBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicAllyReplicaManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Enables scheduling of ally replica announce requests and endorsements.
    bool EnableAllyReplicaAnnouncement;

    //! If |false|, ally replica endorsements will not be stored.
    /*!
     *  WARNING: setting this from |true| to |false| will trigger immediate
     *  cleanup of existing endorsement queues and may stall automaton thread
     *  for a while.
     */
    bool EnableEndorsements;

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

class TDynamicChunkAutotomizerConfig
    : public NYTree::TYsonStruct
{
public:
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

class TDynamicChunkManagerTestingConfig
    : public NYTree::TYsonStruct
{
public:
    //! If true, seal will always be unreliable.
    bool ForceUnreliableSeal;

    REGISTER_YSON_STRUCT(TDynamicChunkManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkManagerTestingConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicConsistentReplicaPlacementConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    bool EnablePullReplication;

    int TokenDistributionBucketCount;

    // NB: nullability is for testing purposes.
    std::optional<TDuration> TokenRedistributionPeriod;

    int TokensPerNode;

    // Keep this larger than TokensPerNode * TokenDistributionBucketCount * maximum replication factor.
    int ReplicasPerChunk;

    REGISTER_YSON_STRUCT(TDynamicConsistentReplicaPlacementConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConsistentReplicaPlacementConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChunkManagerConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr auto DefaultProfilingPeriod = TDuration::MilliSeconds(1000);

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
    //! Maximum amount of time allowed to spend during a refresh iteration.
    TDuration MaxTimePerBlobChunkRefresh;

    //! Maximum number of chunks to process during a refresh iteration.
    int MaxJournalChunksPerRefresh;
    //! Maximum amount of time allowed to spend during a refresh iteration.
    TDuration MaxTimePerJournalChunkRefresh;

    //! If set to false, fully disables background chunk requisition updates;
    //! see #EnableChunkRefresh for a rationale.
    bool EnableChunkRequisitionUpdate;
    //! Interval between consequent chunk requisition update iterations.
    std::optional<TDuration> ChunkRequisitionUpdatePeriod;
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
    std::optional<THashSet<NCompression::ECodec>> DeprecatedCodecIds;

    //! Deprecated codec names and their alises, used values from yt/core/compression by default.
    std::optional<THashMap<TString, TString>> DeprecatedCodecNameToAlias;

    //! The number of oldest part-missing chunks to be remembered by the replicator.
    int MaxOldestPartMissingChunks;

    //! When a node executes a chunk removal job, it will keep the set of known
    //! chunk replicas (and suggest these to others) for some time.
    TDuration ChunkRemovalJobReplicasExpirationTime;

    int FinishedJobsQueueSize;

    bool AbortJobsOnEpochFinish;

    //! Controls if node={{node}} tag should be added to incremental heartbeat sensors.
    bool EnablePerNodeIncrementalHeartbeatProfiling;

    TDynamicDataNodeTrackerConfigPtr DataNodeTracker;

    TDynamicChunkTreeBalancerConfigPtr ChunkTreeBalancer;

    TDynamicChunkMergerConfigPtr ChunkMerger;

    TDynamicChunkReincarnatorConfigPtr ChunkReincarnator;

    TDynamicAllyReplicaManagerConfigPtr AllyReplicaManager;

    TDynamicConsistentReplicaPlacementConfigPtr ConsistentReplicaPlacement;

    TDuration DestroyedReplicasProfilingPeriod;

    TDynamicChunkAutotomizerConfigPtr ChunkAutotomizer;

    TDynamicChunkManagerTestingConfigPtr Testing;

    //! If true, replicator is aware of data centers when placing replicas.
    bool UseDataCenterAwareReplicator;

    //! Set of data centers that are used for chunk storage.
    THashSet<TString> StorageDataCenters;

    //! Set of storage data centers on which replica placement is forbidden.
    THashSet<TString> BannedStorageDataCenters;

    TDuration ProfilingPeriod;

    //! Probability (in percents) that newly created chunk will be Sequoia.
    int SequoiaChunkProbability;

    //! When set of active chunk replicator shards is changed, no removal jobs
    //! will be scheduled within this period.
    TDuration RemovalJobScheduleDelay;

    // COMPAT(kvk1920): YT-17756.
    bool EnableFixRequisitionUpdateOnMerge;

    // COMPAT(kvk1920)
    // Just for rolling purposes. Can be removed after 22.3.
    bool EnableMoreChunkConfirmationChecks;
    // For full description see TDynamicChunkManagerConfig::Register().
    bool EnableChunkConfirmationWithoutLocationUuid;

    bool EnablePerLocationNodeDisposal;

    REGISTER_YSON_STRUCT(TDynamicChunkManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChunkServiceConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnablePerUserRequestWeightThrottling;
    bool EnablePerUserRequestBytesThrottling;

    NConcurrency::TThroughputThrottlerConfigPtr DefaultRequestWeightThrottlerConfig;

    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserRequestWeightThrottlerConfig;
    NConcurrency::TThroughputThrottlerConfigPtr DefaultPerUserRequestBytesThrottlerConfig;

    // COMPAT(h0pless): Remove after release when per user throttlers will be live on all clusters.
    std::optional<double> ExecuteRequestWeightThrottlerLimit;
    std::optional<double> ExecuteRequestBytesThrottlerLimit;

    REGISTER_YSON_STRUCT(TDynamicChunkServiceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicChunkServiceConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
