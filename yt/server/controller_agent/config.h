#pragma once

#include "public.h"

#include <yt/server/misc/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/event_log/config.h>

#include <yt/ytlib/node_tracker_client/config.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/ytree/yson_serializable.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjusterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    TDuration MaxJobTime;

    double ExecToPrepareTimeRatio;

    TJobSizeAdjusterConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobSizeAdjusterConfig)

////////////////////////////////////////////////////////////////////////////////

class TIntermediateChunkScraperConfig
    : public NChunkClient::TChunkScraperConfig
{
public:
    TDuration RestartTimeout;

    TIntermediateChunkScraperConfig();
};

DEFINE_REFCOUNTED_TYPE(TIntermediateChunkScraperConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    // Testing option that enables snapshot build/load cycle after operation materialization.
    bool EnableSnapshotCycleAfterMaterialization;

    TTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationAlertsConfig
    : public NYTree::TYsonSerializable
{
public:
    // Maximum allowed ratio of unused tmpfs size.
    double TmpfsAlertMaxUnusedSpaceRatio;

    // Min unused space threshold. If unutilized space is less than
    // this threshold then operation alert will not be set.
    i64 TmpfsAlertMinUnusedSpaceThreshold;

    // Maximum allowed aborted jobs time. If it is violated
    // then operation alert will be set.
    i64 AbortedJobsAlertMaxAbortedTime;

    // Maximum allowed aborted jobs time ratio.
    double AbortedJobsAlertMaxAbortedTimeRatio;

    // Minimum desired job duration.
    TDuration ShortJobsAlertMinJobDuration;

    // Minimum number of completed jobs after which alert can be set.
    i64 ShortJobsAlertMinJobCount;

    // Minimum partition size to enable data skew check.
    i64 IntermediateDataSkewAlertMinPartitionSize;

    // Minimum interquartile range to consider data to be skewed.
    i64 IntermediateDataSkewAlertMinInterquartileRange;

    // Job spec throttling alert is triggered if throttler activation
    // count is above this threshold.
    i64 JobSpecThrottlingAlertActivationCountThreshold;

    // Minimum total time sum to analyze operation
    TDuration LowCpuUsageAlertMinExecTime;

    // Minimum average job time to analyze operation
    TDuration LowCpuUsageAlertMinAverageJobTime;

    // Cpu usage threshold to send an alert
    double LowCpuUsageAlertCpuUsageThreshold;

    // Minimum wall time of operation duration
    TDuration OperationTooLongAlertMinWallTime;

    // Threshold for estimate duration of operation
    TDuration OperationTooLongAlertEstimateDurationThreshold;

    TOperationAlertsConfig();
};

DEFINE_REFCOUNTED_TYPE(TOperationAlertsConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobSplitterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MinJobTime;
    double ExecToPrepareTimeRatio;
    i64 MinTotalDataWeight;
    TDuration UpdatePeriod;
    TDuration MedianExcessDuration;
    double CandidatePercentile;
    int MaxJobsPerSplit;
    int MaxInputTableCount;

    TJobSplitterConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSuspiciousJobsOptions
    : public NYTree::TYsonSerializable
{
public:
    //! Duration of no activity by job to be considered as suspicious.
    TDuration InactivityTimeout;

    //! Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 CpuUsageThreshold;

    //! Time fraction spent in idle state of JobProxy -> UserJob pipe enough for job to be considered suspicious.
    double InputPipeIdleTimeFraction;

    //! Time fraction spent in idle state of UserJob -> JobProxy pipe enough for job to be considered suspicious.
    double OutputPipeIdleTimeFraction;

    //! Suspicious jobs per operation recalculation period.
    TDuration UpdatePeriod;

    //! Maximum number of suspicious jobs that are reported in Orchid for each job type.
    i64 MaxOrchidEntryCountPerType;

    TSuspiciousJobsOptions();
};

DEFINE_REFCOUNTED_TYPE(TSuspiciousJobsOptions)

////////////////////////////////////////////////////////////////////////////////

class TOperationOptions
    : public NYTree::TYsonSerializable
    , public virtual NPhoenix::TDynamicTag
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOperationOptions, 0x6d2a0bdd);

public:
    NYTree::INodePtr SpecTemplate;

    //! Controls finer initial slicing of input data to ensure even distribution of data split sizes among jobs.
    double SliceDataWeightMultiplier;

    //! Maximum number of primary data slices per job.
    int MaxDataSlicesPerJob;

    i64 MaxSliceDataWeight;
    i64 MinSliceDataWeight;

    int MaxInputTableCount;

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

    TJobSplitterConfigPtr JobSplitter;

    TOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationOptions
    : public TOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationOptions, 0x875251fa);

public:
    int MaxJobCount;
    i64 DataWeightPerJob;

    TSimpleOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapOperationOptions, 0x5d08252b);

public:
    NControllerAgent::TJobSizeAdjusterConfigPtr JobSizeAdjuster;

    TMapOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TMapOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationOptions, 0x28332598);
};

DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationOptions, 0xc71863e6);
};

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeOperationOptions, 0x9089b24a);
};

DEFINE_REFCOUNTED_TYPE(TSortedMergeOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationOptions
    : public TSortedMergeOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceOperationOptions, 0x91371bf5);

public:
    TReduceOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationOptions
    : public TOrderedMergeOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseOperationOptions, 0x73cb9f3b);
};

DEFINE_REFCOUNTED_TYPE(TEraseOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptionsBase
    : public TOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptionsBase, 0x1f448453);

public:
    int MaxPartitionJobCount;
    int MaxPartitionCount;
    i32 MaxSampleSize;
    i64 CompressedBlockSize;
    i64 MinPartitionWeight;
    i64 MinUncompressedBlockSize;
    NControllerAgent::TJobSizeAdjusterConfigPtr PartitionJobSizeAdjuster;

    TSortOperationOptionsBase();
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptionsBase)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationOptions
    : public TSortOperationOptionsBase
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationOptions, 0xc11251c0);
};

DEFINE_REFCOUNTED_TYPE(TSortOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationOptions
    : public TSortOperationOptionsBase
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationOptions, 0x91e3968d);
};

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationOptions
    : public TSimpleOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyOperationOptions, 0xf3893dc8);
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TVanillaOperationOptions
    : public TOperationOptions
{
public:
    //! Maximum number of tasks allowed.
    int MaxTaskCount;

    //! Maximum total number of jobs.
    int MaxTotalJobCount;

    TVanillaOperationOptions()
    {
        RegisterParameter("max_task_count", MaxTaskCount)
            .Default(100);
        RegisterParameter("max_total_job_count", MaxTotalJobCount)
            .Default(100 * 1000);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationOptions, 0x93998ffa);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConfig
    : public NChunkClient::TChunkTeleporterConfig
{
public:
    //! Number of chunk lists to be allocated when an operation starts.
    int ChunkListPreallocationCount;

    //! Maximum number of chunk lists to request via a single request.
    int MaxChunkListAllocationCount;

    //! Better keep the number of spare chunk lists above this threshold.
    int ChunkListWatermarkCount;

    //! Each time the number of spare chunk lists drops below #ChunkListWatermarkCount or
    //! the controller requests more chunk lists than we currently have,
    //! another batch is allocated. Each time we allocate #ChunkListAllocationMultiplier times
    //! more chunk lists than previously.
    double ChunkListAllocationMultiplier;

    //! Desired number of chunks to release in one batch.
    int DesiredChunkListsPerRelease;

    //! If |true|, snapshots are periodically constructed and uploaded into the system.
    bool EnableSnapshotBuilding;

    //! Interval between consequent snapshots.
    TDuration SnapshotPeriod;

    //! Timeout for snapshot construction.
    TDuration SnapshotTimeout;

    //! Timeout to wait for controller suspension before constructing a snapshot.
    TDuration OperationControllerSuspendTimeout;

    //! Number of parallel operation snapshot builders.
    int ParallelSnapshotBuilderCount;

    //! Configuration for uploading snapshots to Cypress.
    NApi::TFileWriterConfigPtr SnapshotWriter;

    //! If |true|, snapshots are loaded during revival.
    bool EnableSnapshotLoading;

    //! Configuration for downloading snapshots from Cypress.
    NApi::TFileReaderConfigPtr SnapshotReader;

    TDuration TransactionsRefreshPeriod;
    TDuration OperationsUpdatePeriod;
    TDuration ChunkUnstagePeriod;

    bool EnableUnrecognizedAlert;

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    NEventLog::TEventLogConfigPtr EventLog;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration SchedulerHandshakeRpcTimeout;

    //! Controller agent-to-scheduler handshake failure backoff.
    TDuration SchedulerHandshakeFailureBackoff;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration SchedulerHeartbeatRpcTimeout;

    //! Controller agent-to-scheduler heartbeat failure backoff.
    TDuration SchedulerHeartbeatFailureBackoff;

    // Controller agent-to-scheduler heartbeat period.
    TDuration SchedulerHeartbeatPeriod;

    //! Period for requesting exec nodes from scheduler.
    TDuration ExecNodesUpdatePeriod;

    //! Period for requesting config from scheduler.
    TDuration ConfigUpdatePeriod;

    //! Period for pushing any operation info from agent to scheduler.
    TDuration OperationsPushPeriod;

    //! Period for pushing operation alerts from agent to scheduler.
    TDuration OperationAlertsPushPeriod;

    //! Period for pushing suspicious jobs from agent to scheduler.
    TDuration SuspiciousJobsPushPeriod;

    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Period of controller static orchid part update.
    TDuration ControllerStaticOrchidUpdatePeriod;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    TSuspiciousJobsOptionsPtr SuspiciousJobs;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    TNullable<TDuration> OperationTimeLimit;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    //! Timeout on operation initialization.
    //! Prevents hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration OperationTransactionTimeout;

    TDuration OperationLogProgressBackoff;

    TDuration OperationControllerFailTimeout;

    TDuration AvailableExecNodesCheckPeriod;
    TDuration BannedExecNodesCheckPeriod;

    TDuration OperationProgressAnalysisPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration TaskUpdatePeriod;

    //! Max available exec node resources are updated not more often then this period.
    TDuration MaxAvailableExecNodeResourcesUpdatePeriod;

    //! Maximum number of job nodes per operation.
    int MaxJobNodesPerOperation;

    //! Maximum number of job specs in archive per operation.
    int MaxArchivedJobSpecCountPerOperation;

    //! Maximum number of chunks per single fetch.
    int MaxChunksPerFetch;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of ranges on the input table.
    int MaxRangesOnTable;

    //! Maximum number of files per user job.
    int MaxUserFileCount;
    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxUserFileSize;
    //! Maximum data weight of table file allowed to be passed to jobs.
    i64 MaxUserFileTableDataWeight;
    //! Maximum chunk count of file allowed to be passed to jobs.
    i64 MaxUserFileChunkCount;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    // TODO(ignat): rename to SafeExecNodeCount.
    int SafeOnlineNodeCount;

    //! Don't check resource demand for sanity if scheduler is online
    //! less than this timeout.
    TDuration SafeSchedulerOnlineTime;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration ControllerExecNodeInfoUpdatePeriod;

    //! Maximum number of foreign chunks to locate per request.
    int MaxChunksPerLocateRequest;

    //! Enables using tmpfs if tmpfs_path is specified in user spec.
    bool EnableTmpfs;

    //! Enables dynamic change of job sizes.
    bool EnablePartitionMapJobSizeAdjustment;

    bool EnableMapJobSizeAdjustment;

    //! Enables splitting of long jobs.
    bool EnableJobSplitting;

    //! Acl used for intermediate tables and stderrs additional to acls specified by user.
    NYTree::IListNodePtr AdditionalIntermediateDataAcl;

    double UserJobMemoryDigestPrecision;
    double UserJobMemoryReserveQuantile;
    double JobProxyMemoryReserveQuantile;
    double ResourceOverdraftFactor;

    //! If user job iops threshold is exceeded, iops throttling is enabled via cgroups.
    TNullable<int> IopsThreshold;
    TNullable<int> IopsThrottlerLimit;

    //! Patch for all operation options.
    NYT::NYTree::INodePtr OperationOptions;

    //! Specific operation options.
    TMapOperationOptionsPtr MapOperationOptions;
    TReduceOperationOptionsPtr ReduceOperationOptions;
    TReduceOperationOptionsPtr JoinReduceOperationOptions;
    TEraseOperationOptionsPtr EraseOperationOptions;
    TOrderedMergeOperationOptionsPtr OrderedMergeOperationOptions;
    TUnorderedMergeOperationOptionsPtr UnorderedMergeOperationOptions;
    TSortedMergeOperationOptionsPtr SortedMergeOperationOptions;
    TMapReduceOperationOptionsPtr MapReduceOperationOptions;
    TSortOperationOptionsPtr SortOperationOptions;
    TRemoteCopyOperationOptionsPtr RemoteCopyOperationOptions;
    TVanillaOperationOptionsPtr VanillaOperationOptions;

    //! Default environment variables set for every job.
    THashMap<TString, TString> Environment;

    //! If |true|, jobs are revived from snapshot.
    bool EnableJobRevival;

    //! If |false|, all locality timeouts are considered 0.
    bool EnableLocality;

    //! Allow failing a controller by passing testing option `controller_failure`
    //! in operation spec. Used only for testing purposes.
    bool EnableControllerFailureSpecOption;

    NChunkClient::TFetcherConfigPtr Fetcher;

    TNullable<NYPath::TYPath> UdfRegistryPath;

    //! Discriminates between "heavy" and "light" job specs. For those with slice count
    //! not exceeding this threshold no throttling is done.
    int HeavyJobSpecSliceCountThreshold;

    //! We use the same config for input chunk scraper and intermediate chunk scraper.
    NControllerAgent::TIntermediateChunkScraperConfigPtr ChunkScraper;

    //! Total number of data slices in operation, summed up over all jobs.
    i64 MaxTotalSliceCount;

    TOperationAlertsConfigPtr OperationAlerts;

    //! Chunk size in per-controller row buffers.
    i64 ControllerRowBufferChunkSize;

    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    //! Period between consequent job metrics pushes from agent to scheduler.
    TDuration JobMetricsReportPeriod;

    // Cypress path to a special layer containing YT-specific data required to
    // run jobs with custom rootfs, e.g. statically linked job-satellite.
    // Is applied on top of user layers if they are used.
    TNullable<TString> SystemLayerPath;

    // Running jobs cached YSON string update period.
    TDuration CachedRunningJobsUpdatePeriod;

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleJobStatisticsLogBackoff;

    //! Controls the rate at which jobs are scheduled in termes of slices per second.
    NConcurrency::TThroughputThrottlerConfigPtr JobSpecSliceThrottler;

    // Period of tagged memory statistics section update.
    TDuration TaggedMemoryStatisticsUpdatePeriod;

    TDuration StaticOrchidCacheUpdatePeriod;

    TDuration AlertsUpdatePeriod;

    TControllerAgentConfig();

private:
    template <class TOptions>
    void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConfig)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentBootstrapConfig
    : public TServerConfig
{
public:
    //! Node-to-master connection.
    NApi::TNativeConnectionConfigPtr ClusterConnection;

    //! Node directory synchronization.
    NNodeTrackerClient::TNodeDirectorySynchronizerConfigPtr NodeDirectorySynchronizer;

    NControllerAgent::TControllerAgentConfigPtr ControllerAgent;

    //! Known scheduler addresses.
    NNodeTrackerClient::TNetworkAddressList Addresses;

    TControllerAgentBootstrapConfig()
    {
        RegisterParameter("cluster_connection", ClusterConnection);
        RegisterParameter("node_directory_synchronizer", NodeDirectorySynchronizer)
            .DefaultNew();
        RegisterParameter("controller_agent", ControllerAgent)
            .DefaultNew();
        RegisterParameter("addresses", Addresses)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
