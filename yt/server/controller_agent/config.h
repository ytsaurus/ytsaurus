#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/event_log/config.h>

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

// TODO(babenko): split further
class TTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    // Testing options that enables random master disconnections.
    bool EnableRandomMasterDisconnection;
    TDuration RandomMasterDisconnectionMaxBackoff;

    // Testing option that enables sleeping during master disconnect.
    TNullable<TDuration> MasterDisconnectDelay;

    // Testing option that enables snapshot build/load cycle after operation materialization.
    bool EnableSnapshotCycleAfterMaterialization;

    // Testing option that enables sleeping between intermediate and final states of operation.
    TNullable<TDuration> FinishOperationTransitionDelay;

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

    TJobSplitterConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

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

    //! Maximum number of output tables times job count an operation can have.
    int MaxOutputTablesTimesJobsCount;

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
    TJobSplitterConfigPtr JobSplitter;

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
    TJobSplitterConfigPtr JobSplitter;

    TReduceOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceOperationOptions
    : public TReduceOperationOptions
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TJoinReduceOperationOptions, 0xdd9303bc);
};

DEFINE_REFCOUNTED_TYPE(TJoinReduceOperationOptions)

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

    //! Maximum number of chunk trees to attach per request.
    int MaxChildrenPerAttachRequest;

    //! Limits the rate (measured in chunks) of location requests issued by all active chunk scrapers.
    NConcurrency::TThroughputThrottlerConfigPtr ChunkLocationThrottler;

    NEventLog::TEventLogConfigPtr EventLog;

    // Controller agent-to-scheduler heartbeat period.
    TDuration ControllerAgentHeartbeatPeriod;

    //! Controller agent-to-scheduler heartbeat timeout.
    TDuration ControllerAgentHeartbeatRpcTimeout;

    //! Controller agent-to-scheduler heartbeat failure backoff.
    TDuration ControllerAgentHeartbeatFailureBackoff;

    //! Period between requesting exec nodes from scheduler.
    TDuration ExecNodesRequestPeriod;

    //! Number of threads for running controllers invokers.
    int ControllerThreadCount;

    //! Limit on the number of concurrent core dumps that can be written because
    //! of failed safe assertions inside controllers.
    int MaxConcurrentSafeCoreDumps;

    //! Timeout to store cached value of exec nodes information
    //! for scheduling tag filter without access.
    TDuration SchedulingTagFilterExpireTimeout;

    //! Duration of no activity by job to be considered as suspicious.
    TDuration SuspiciousInactivityTimeout;

    //! Cpu usage delta that is considered insignificant when checking if job is suspicious.
    i64 SuspiciousCpuUsageThreshold;

    //! Time fraction spent in idle state enough for job to be considered suspicious.
    double SuspiciousInputPipeIdleTimeFraction;

    //! Suspicious jobs per operation recalculation period.
    TDuration SuspiciousJobsUpdatePeriod;

    //! Maximum allowed running time of operation. Null value is interpreted as infinity.
    TNullable<TDuration> OperationTimeLimit;

    TDuration OperationTimeLimitCheckPeriod;

    TDuration ResourceDemandSanityCheckPeriod;

    //! Timeout on operation initialization.
    //! Prevents hanging of remote copy when remote cluster is unavailable.
    TDuration OperationInitializationTimeout;

    TDuration OperationTransactionTimeout;

    TDuration OperationLogProgressBackoff;

    TDuration OperationLogFairSharePeriod;

    TDuration OperationControllerFailTimeout;

    TDuration AvailableExecNodesCheckPeriod;

    TDuration OperationProgressAnalysisPeriod;

    TDuration OperationBuildProgressPeriod;

    TDuration TaskUpdatePeriod;

    //! Max available exec node resources are updated not more often then this period.
    TDuration MaxAvailableExecNodeResourcesUpdatePeriod;

    //! Maximum number of job nodes per operation.
    int MaxJobNodesPerOperation;

    //! Maximum number of chunks per single fetch.
    int MaxChunksPerFetch;

    //! Maximum size of file allowed to be passed to jobs.
    i64 MaxFileSize;

    //! Maximum number of input tables an operation can have.
    int MaxInputTableCount;

    //! Maximum number of ranges on the input table.
    int MaxRangesOnTable;

    //! Maximum number of files per user job.
    int MaxUserFileCount;

    //! Don't check resource demand for sanity if the number of online
    //! nodes is less than this bound.
    // TODO(ignat): rename to SafeExecNodeCount.
    int SafeOnlineNodeCount;

    //! Don't check resource demand for sanity if scheduler is online
    //! less than this timeout.
    TDuration SafeSchedulerOnlineTime;

    //! Time between two consecutive calls in operation controller to get exec nodes information from scheduler.
    TDuration ControllerUpdateExecNodesInformationDelay;

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
    TJoinReduceOperationOptionsPtr JoinReduceOperationOptions;
    TEraseOperationOptionsPtr EraseOperationOptions;
    TOrderedMergeOperationOptionsPtr OrderedMergeOperationOptions;
    TUnorderedMergeOperationOptionsPtr UnorderedMergeOperationOptions;
    TSortedMergeOperationOptionsPtr SortedMergeOperationOptions;
    TMapReduceOperationOptionsPtr MapReduceOperationOptions;
    TSortOperationOptionsPtr SortOperationOptions;
    TRemoteCopyOperationOptionsPtr RemoteCopyOperationOptions;

    //! Default environment variables set for every job.
    yhash<TString, TString> Environment;

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

    //! Some special options for testing purposes.
    TTestingOptionsPtr TestingOptions;

    NCompression::ECodec JobSpecCodec;

    //! Backoff to report job metrics from operation to scheduler.
    TDuration JobMetricsDeltaReportBackoff;

    // Cypress path to a special layer containing YT-specific data required to
    // run jobs with custom rootfs, e.g. statically linked job-satellite.
    // Is applied on top of user layers if they are used.
    TNullable<TString> SystemLayerPath;

    //! Backoff between schedule job statistics logging.
    TDuration ScheduleJobStatisticsLogBackoff;

    //! Controls the rate at which jobs are scheduled in termes of slices per second.
    NConcurrency::TThroughputThrottlerConfigPtr JobSpecSliceThrottler;

    TControllerAgentConfig();

private:
    template <class TOptions>
    void UpdateOptions(TOptions* options, NYT::NYTree::INodePtr patch);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
