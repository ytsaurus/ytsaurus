#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/backoff_strategy_api.h>
#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/library/vector_hdrf/public.h>
#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Ratio of MaxWeight and MinWeight shouldn't lose precision.
const double MinSchedulableWeight = sqrt(std::numeric_limits<double>::epsilon());
const double MaxSchedulableWeight = 1.0 / MinSchedulableWeight;

////////////////////////////////////////////////////////////////////////////////

class TPoolName
{
public:
    TPoolName() = default;
    TPoolName(TString pool, std::optional<TString> parent);

    static const char Delimiter;

    TString ToString() const;
    static TPoolName FromString(const TString& value);

    const TString& GetPool() const;
    const std::optional<TString>& GetParentPool() const;
    const TString& GetSpecifiedPoolName() const;

private:
    TString Pool;
    std::optional<TString> ParentPool;
};

void Deserialize(TPoolName& value, NYTree::INodePtr node);
void Deserialize(TPoolName& value, NYson::TYsonPullParserCursor* cursor);
void Serialize(const TPoolName& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesConfig
    : public NYTree::TYsonStruct
    , public NVectorHdrf::TJobResourcesConfig
{
public:
    TJobResourcesConfigPtr Clone();

    TJobResourcesConfigPtr operator-();

    REGISTER_YSON_STRUCT(TJobResourcesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobResourcesConfig)

////////////////////////////////////////////////////////////////////////////////

class TCommonPreemptionConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableAggressiveStarvation;

    REGISTER_YSON_STRUCT(TCommonPreemptionConfig);

    static void Register(TRegistrar registrar);
};

class TPoolPreemptionConfig
    : public TCommonPreemptionConfig
{
public:
    // The following settings override scheduler configuration is specified.
    std::optional<TDuration> FairShareStarvationTimeout;
    std::optional<double> FairShareStarvationTolerance;

    std::optional<bool> AllowAggressivePreemption;

    // NB(eshcherbin): Intended for testing purposes only.
    // Should not be used in real clusters, unless something extraordinary happens.
    bool AllowNormalPreemption;

    REGISTER_YSON_STRUCT(TPoolPreemptionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolPreemptionConfig)

////////////////////////////////////////////////////////////////////////////////

class TOffloadingPoolSettingsConfig
    : public NYTree::TYsonStruct
{
public:
    TString Pool;

    std::optional<double> Weight;

    bool Tentative;

    TJobResourcesConfigPtr ResourceLimits;

    REGISTER_YSON_STRUCT(TOffloadingPoolSettingsConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TOffloadingPoolSettingsConfig)
DEFINE_REFCOUNTED_TYPE(TOffloadingPoolSettingsConfig)

using TOffloadingSettings = THashMap<TString, TOffloadingPoolSettingsConfigPtr>;

static const inline TOffloadingSettings EmptyOffloadingSettings = {};

////////////////////////////////////////////////////////////////////////////////

class TSchedulableConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<double> Weight;

    // Specifies resource limits in terms of a share of all cluster resources.
    std::optional<double> MaxShareRatio;

    // Specifies resource limits in absolute values.
    TJobResourcesConfigPtr ResourceLimits;

    // Specifies guaranteed resources in absolute values.
    TJobResourcesConfigPtr StrongGuaranteeResources;

    TBooleanFormula SchedulingTagFilter;

    REGISTER_YSON_STRUCT(TSchedulableConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TExtendedSchedulableConfig
    : public TSchedulableConfig
{
public:
    std::optional<TString> Pool;

    REGISTER_YSON_STRUCT(TExtendedSchedulableConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExtendedSchedulableConfig)

////////////////////////////////////////////////////////////////////////////////

class TEphemeralSubpoolConfig
    : public NYTree::TYsonStruct
{
public:
    NVectorHdrf::ESchedulingMode Mode;

    std::optional<int> MaxRunningOperationCount;
    std::optional<int> MaxOperationCount;

    TJobResourcesConfigPtr ResourceLimits;

    REGISTER_YSON_STRUCT(TEphemeralSubpoolConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEphemeralSubpoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TPoolIntegralGuaranteesConfig
    : public NYTree::TYsonStruct
{
public:
    NVectorHdrf::EIntegralGuaranteeType GuaranteeType;

    TJobResourcesConfigPtr ResourceFlow;

    TJobResourcesConfigPtr BurstGuaranteeResources;

    std::optional<double> RelaxedShareMultiplierLimit;

    bool CanAcceptFreeVolume;
    std::optional<bool> ShouldDistributeFreeVolumeAmongChildren;

    REGISTER_YSON_STRUCT(TPoolIntegralGuaranteesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolIntegralGuaranteesConfig)

////////////////////////////////////////////////////////////////////////////////

// This config contains options allowed in pool preset configs.
class TPoolPresetConfig
    : public TPoolPreemptionConfig
{
public:
    bool AllowRegularJobsOnSsdNodes;

    REGISTER_YSON_STRUCT(TPoolPresetConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolPresetConfig)

////////////////////////////////////////////////////////////////////////////////

class TPoolConfig
    : public TSchedulableConfig
    , public TPoolPresetConfig
{
public:
    NVectorHdrf::ESchedulingMode Mode;

    std::optional<int> MaxRunningOperationCount;
    std::optional<int> MaxOperationCount;

    std::vector<EFifoSortParameter> FifoSortParameters;
    std::optional<EFifoPoolSchedulingOrder> FifoPoolSchedulingOrder;

    bool ForbidImmediateOperations;

    bool CreateEphemeralSubpools;

    std::optional<TEphemeralSubpoolConfigPtr> EphemeralSubpoolConfig;

    bool InferChildrenWeightsFromHistoricUsage;
    THistoricUsageConfigPtr HistoricUsageConfig;

    THashSet<TString> AllowedProfilingTags;

    std::optional<bool> EnableByUserProfiling;

    NObjectClient::TAbcConfigPtr Abc;

    TPoolIntegralGuaranteesConfigPtr IntegralGuarantees;

    bool EnableDetailedLogs;

    std::optional<TString> ConfigPreset;

    // Overrides the same option in tree config.
    std::optional<bool> EnableFairShareTruncationInFifoPool;

    THashMap<TString, TString> MeteringTags;

    TOffloadingSettings OffloadingSettings;

    TJobResourcesConfigPtr NonPreemptibleResourceUsageThreshold;

    std::optional<bool> UsePoolSatisfactionForScheduling;

    std::optional<bool> AllowIdleCpuPolicy;

    bool ComputePromisedGuaranteeFairShare;

    std::optional<bool> EnablePrioritySchedulingSegmentModuleAssignment;

    void Validate(const TString& poolName);

    REGISTER_YSON_STRUCT(TPoolConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TTentativeTreeEligibilityConfig
    : public NYTree::TYsonStruct
{
public:
    // The number of jobs of a task that have to finish before we allow any more
    // jobs to start in a tentative tree. After this many jobs finish, we start
    // making decisions on that task being eligible for the tree (or not).
    int SampleJobCount;

    // Maximum ratio between average job duration in a tentative tree to that in
    // other (non-tentative) trees. Exceeding this ratio will render a task
    // ineligible for the tentative tree.
    double MaxTentativeJobDurationRatio;

    // If either average job duration in the tentative tree or average job
    // duration other trees is shorter that this, they're not compared (i.e. the
    // #MaxTentativeJobDurationRatio is not checked).
    TDuration MinJobDuration;

    bool IgnoreMissingPoolTrees;

    REGISTER_YSON_STRUCT(TTentativeTreeEligibilityConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTentativeTreeEligibilityConfig)

////////////////////////////////////////////////////////////////////////////////

class TSamplingConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! The probability for each particular row to remain in the output.
    std::optional<double> SamplingRate;

    //! An option regulating the total data slice count during the sampling job creation procedure.
    //! It should not be used normally and left only for manual setup in marginal cases.
    //! If not set, it is overridden with MaxTotalSliceCount from controller agent options.
    std::optional<i64> MaxTotalSliceCount;

    //! Size of IO block to consider when calculating the lower bound for sampling job size.
    i64 IOBlockSize;

    REGISTER_YSON_STRUCT(TSamplingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSamplingConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPackingMetricType,
    ((Angle)       (0))
    ((AngleLength) (1))
);

class TFairShareStrategyPackingConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool Enable;

    EPackingMetricType Metric;

    int MaxBetterPastSnapshots;
    double AbsoluteMetricValueTolerance;
    double RelativeMetricValueTolerance;
    int MinWindowSizeForSchedule;
    int MaxHeartbeatWindowSize;
    TDuration MaxHeartbeatAge;

    REGISTER_YSON_STRUCT(TFairShareStrategyPackingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyPackingConfig)

////////////////////////////////////////////////////////////////////////////////

class TStrategyOperationSpec
    : public TSchedulableConfig
    , public TCommonPreemptionConfig
    , public virtual NPhoenix::TDynamicTag
{
public:
    std::optional<TString> Pool;

    //! This options have higher priority than Pool and other options
    //! defined in this class.
    THashMap<TString, TExtendedSchedulableConfigPtr> SchedulingOptionsPerPoolTree;

    //! Pool trees to schedule operation in.
    //! Operation will be scheduled in default tree (if any) if this parameter
    //! is not specified.
    std::optional<THashSet<TString>> PoolTrees;

    // NB(eshcherbin): This limit is only checked once every fair share update. Finer throttling is achieved
    // via the "per node shard" limit in controller config.
    //! Limit on the number of concurrent calls to ScheduleJob of single controller.
    std::optional<int> MaxConcurrentControllerScheduleJobCalls;

    //! If set and several regular pool trees have been specified, then the scheduler will choose
    //! one of those trees based on some heuristic, and all jobs will be scheduled only in the chosen tree.
    //! This option can't be used simultaneously with TentativePoolTrees or UseDefaultTentativePoolTrees;
    bool ScheduleInSingleTree;

    bool ConsiderGuaranteesForSingleTree;

    //! Tentative pool trees to schedule operation in.
    //! Operation's job will be scheduled to these pool trees as long as they're
    //! not much slower than those in other (non-tentative) trees.
    //! If TentativePoolTrees is not empty, PoolTrees must not be empty, too.
    std::optional<THashSet<TString>> TentativePoolTrees;

    //! Probing pool tree to schedule probing jobs in.
    //! Scheduler will occasionally launch probing jobs in this tree.
    std::optional<TString> ProbingPoolTree;

    //! Enables using default tentative pool trees from scheduler config. It has effect only if TentativePoolTrees is not specified.
    bool UseDefaultTentativePoolTrees;

    // Config for tentative pool tree eligibility - the part of the scheduler that decides
    // whether a job should (or shouldn't) be launched in a pool tree marked as tentative.
    TTentativeTreeEligibilityConfigPtr TentativeTreeEligibility;

    int UpdatePreemptibleJobsListLoggingPeriod;

    std::optional<TString> CustomProfilingTag;

    std::optional<int> MaxUnpreemptibleRunningJobCount;

    bool TryAvoidDuplicatingJobs;

    int MaxSpeculativeJobCountPerTask;
    int MaxProbingJobCountPerTask;

    std::optional<double> ProbingRatio;

    EPreemptionMode PreemptionMode;

    std::optional<ESchedulingSegment> SchedulingSegment;
    std::optional<THashSet<TString>> SchedulingSegmentModules;

    bool EnableLimitingAncestorCheck;

    bool IsGang;

    TTestingOperationOptionsPtr TestingOperationOptions;

    bool EraseTreesWithPoolLimitViolations;

    bool ApplySpecifiedResourceLimitsToDemand;

    //! Allow to run operation's jobs with cpu_policy=idle.
    std::optional<bool> AllowIdleCpuPolicy;

    REGISTER_YSON_STRUCT(TStrategyOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TStrategyOperationSpec, 0x22fc73fa);
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TJobIOConfig
    : public NYTree::TYsonStruct
{
public:
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NTableClient::TTableWriterConfigPtr DynamicTableWriter;

    NFormats::TControlAttributesConfigPtr ControlAttributes;

    NApi::TFileWriterConfigPtr ErrorFileWriter;

    i64 BufferRowCount;

    int PipeIOPoolSize;

    NChunkClient::TBlockCacheConfigPtr BlockCache;

    class TTestingOptions
        : public NYTree::TYsonStruct
    {
    public:
        TDuration PipeDelay;

        REGISTER_YSON_STRUCT(TTestingOptions);

        static void Register(TRegistrar registrar);
    };

    TIntrusivePtr<TTestingOptions> Testing;

    REGISTER_YSON_STRUCT(TJobIOConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobIOConfig)
DEFINE_REFCOUNTED_TYPE(TJobIOConfig::TTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class TDelayConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration Duration;
    EDelayType Type;

    REGISTER_YSON_STRUCT(TDelayConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDelayConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDelayInsideOperationCommitStage,
    (Start)
    (Stage1)
    (Stage2)
    (Stage3)
    (Stage4)
    (Stage5)
    (Stage6)
    (Stage7)
);

DEFINE_ENUM(ECancelationStage,
    (ColumnarStatisticsFetch)
);

DEFINE_ENUM(EControllerFailureType,
    (None)
    (AssertionFailureInPrepare)
    (ExceptionThrownInOnJobCompleted)
);

DEFINE_ENUM(ETestingSpeculativeLaunchMode,
    (None)
    (Once)
    (Always)
);

class TTestingOperationOptions
    : public NYTree::TYsonStruct
{
public:
    //! The following delays are used inside the operation controller.

    TDelayConfigPtr ScheduleJobDelay;
    TDelayConfigPtr InsideScheduleJobDelay;

    std::optional<TDuration> DelayInsideOperationCommit;
    std::optional<EDelayInsideOperationCommitStage> DelayInsideOperationCommitStage;
    bool NoDelayOnSecondEntranceToCommit;

    std::optional<TDuration> DelayInsideInitialize;

    std::optional<TDuration> DelayInsidePrepare;

    std::optional<TDuration> DelayInsideRevive;

    std::optional<TDuration> DelayInsideSuspend;

    std::optional<TDuration> DelayInsideMaterialize;

    //! The following delays are used inside the scheduler.

    TDelayConfigPtr ScheduleJobDelayScheduler;

    TDelayConfigPtr DelayInsideMaterializeScheduler;

    std::optional<TDuration> DelayInsideAbort;

    std::optional<TDuration> DelayInsideRegisterJobsFromRevivedOperation;

    std::optional<TDuration> DelayInsideValidateRuntimeParameters;

    std::optional<TDuration> DelayBeforeStart;

    std::optional<i64> AllocationSize;

    std::optional<TDuration> AllocationReleaseDelay;

    //! Intentionally fails the operation controller. Used only for testing purposes.
    std::optional<EControllerFailureType> ControllerFailure;

    std::optional<ECancelationStage> CancelationStage;

    std::optional<TDuration> GetJobSpecDelay;

    std::optional<TDuration> BuildJobSpecProtoDelay;

    bool FailGetJobSpec;

    ETestingSpeculativeLaunchMode TestingSpeculativeLaunchMode;

    bool LogResidualCustomJobMetricsOnTermination;

    bool TestJobSpeculationTimeout;

    //! Crashes controller agent without safe core.
    //! Think twice before using!
    bool CrashControllerAgent;

    bool ThrowExceptionDuringOperationAbort;

    REGISTER_YSON_STRUCT(TTestingOperationOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingOperationOptions)

////////////////////////////////////////////////////////////////////////////////

// TODO(gritukan): Move some of the heuristics options from NControllerAgent::TJobSplitterConfig here.
class TJobSplitterConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableJobSplitting;

    bool EnableJobSpeculation;

    REGISTER_YSON_STRUCT(TJobSplitterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobSplitterConfig)

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeConfig
    : public NYTree::TYsonStruct
{
public:
    TJobIOConfigPtr JobIO;

    std::optional<i64> MaxIntermediateChunkCount;
    std::optional<i64> ChunkCountPerMergeJob;
    i64 ChunkSizeThreshold;
    EAutoMergeMode Mode;

    //! Whether intermediate chunks should be allocated in intermediate data account.
    bool UseIntermediateDataAccount;

    //! Whether shallow merge is enabled. Shallow merge optimizes merging output
    //! tables, as it doesn't require serializing and deserializing data to do it.
    bool EnableShallowMerge;

    //! Whether we allow unknown chunk meta extensions during shallow merge. Useful only
    //! when EnableShallowMerge is set to true.
    bool AllowUnknownExtensions;

    //! Maximum number of blocks allowed in the merged chunks. Useful only when EnableShallowMerge
    //! is set to true.
    std::optional<i64> MaxBlockCount;

    //! Minimum data weight per chunk for shallow merge job. Useful only when EnableShallowMerge
    //! is set to true.
    i64 ShallowMergeMinDataWeightPerChunk;

    REGISTER_YSON_STRUCT(TAutoMergeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeConfig)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsVolumeConfig
    : public NYTree::TYsonStruct
{
public:
    i64 Size;
    TString Path;

    REGISTER_YSON_STRUCT(TTmpfsVolumeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTmpfsVolumeConfig)

void ToProto(NControllerAgent::NProto::TTmpfsVolume* protoTmpfsVolume, const TTmpfsVolumeConfig& tmpfsVolumeConfig);
void FromProto(TTmpfsVolumeConfig* tmpfsVolumeConfig, const NControllerAgent::NProto::TTmpfsVolume& protoTmpfsVolume);

////////////////////////////////////////////////////////////////////////////////

class TDiskRequestConfig
    : public NYTree::TYsonStruct
{
public:
    i64 DiskSpace;
    std::optional<i64> InodeCount;
    std::optional<TString> MediumName;
    std::optional<int> MediumIndex;
    std::optional<TString> Account;

    REGISTER_YSON_STRUCT(TDiskRequestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskRequestConfig)

void ToProto(
    NProto::TDiskRequest* protoDiskRequest,
    const TDiskRequestConfig& diskRequestConfig);

////////////////////////////////////////////////////////////////////////////////

class TJobShell
    : public NYTree::TYsonStruct
{
public:
    TString Name;

    TString Subcontainer;

    std::vector<TString> Owners;

    REGISTER_YSON_STRUCT(TJobShell);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobShell)

////////////////////////////////////////////////////////////////////////////////

class TUserJobMonitoringConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    std::vector<TString> SensorNames;

    REGISTER_YSON_STRUCT(TUserJobMonitoringConfig);

    static void Register(TRegistrar registrar);

private:
    static const std::vector<TString>& GetDefaultSensorNames();
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProfilingBinary,
    ((JobProxy)         (0))
    ((UserJob)          (1))
);

DEFINE_ENUM(EProfilerType,
    ((Cpu)              (0))
    ((Memory)           (1))
    ((PeakMemory)       (2))
);

class TJobProfilerSpec
    : public NYTree::TYsonStruct
{
public:
    //! Binary to profile.
    EProfilingBinary Binary;

    //! Resource to profile.
    EProfilerType Type;

    //! Probability of profiler enabling.
    double ProfilingProbability;

    //! Number of the samples per second.
    int SamplingFrequency;

    //! If true, profile will be symbolized by llvm-symbolizer.
    bool RunExternalSymbolizer;

    REGISTER_YSON_STRUCT(TJobProfilerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProfilerSpec)

void ToProto(NControllerAgent::NProto::TJobProfilerSpec* protoJobProfilerSpec, const TJobProfilerSpec& jobProfilerSpec);
void FromProto(TJobProfilerSpec* jobProfilerSpec, const NControllerAgent::NProto::TJobProfilerSpec& protoJobProfilerSpec);

////////////////////////////////////////////////////////////////////////////////

class TColumnarStatisticsConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> Enabled;

    NTableClient::EColumnarStatisticsFetcherMode Mode;

    REGISTER_YSON_STRUCT(TColumnarStatisticsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TColumnarStatisticsConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobExperimentConfig
    : public NYTree::TYsonStruct
{
public:
    //! The base layer used in the treatment jobs of the experiment.
    std::optional<TString> BaseLayerPath;

    //! The network project used in the treatment jobs of the experiment.
    std::optional<TString> NetworkProject;

    //! Do not run any more treatment jobs if the `MaxFailedTreatmentJobs` of them failed.
    int MaxFailedTreatmentJobs;

    //! Use the treatment configuration for all subsequent jobs
    //! if the probing job completed successfully.
    bool SwitchOnExperimentSuccess;

    //! If true the alert is set even if some of non-probing jobs have failed too.
    //! By default the alert is set only when all failed jobs are probing.
    //! Thus, the level of false positives is reduced.
    bool AlertOnAnyTreatmentFailure;

    REGISTER_YSON_STRUCT(TJobExperimentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobExperimentConfig)

////////////////////////////////////////////////////////////////////////////////

class TOperationSpecBase
    : public TStrategyOperationSpec
{
public:
    //! Account holding intermediate data produces by the operation.
    TString IntermediateDataAccount;

    //! Codec used for compressing intermediate output during shuffle.
    NCompression::ECodec IntermediateCompressionCodec;

    //! Replication factor for intermediate data.
    int IntermediateDataReplicationFactor;

    //! Minimum replication factor for intermediate data.
    int IntermediateMinDataReplicationFactor;

    //! SyncOnClose option for intermediate data.
    bool IntermediateDataSyncOnClose;

    TString IntermediateDataMediumName;

    //! Limit for the data that will be written to the fast medium (SSD) in the default intermediate account.
    i64 FastIntermediateMediumLimit;

    //! Account for job nodes and operation files (stderrs and input contexts of failed jobs).
    TString DebugArtifactsAccount;

    //! What to do during initialization if some chunks are unavailable.
    EUnavailableChunkAction UnavailableChunkStrategy;

    //! What to do during operation progress when some chunks get unavailable.
    EUnavailableChunkAction UnavailableChunkTactics;

    i64 MaxDataWeightPerJob;
    i64 MaxPrimaryDataWeightPerJob;

    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;

    //! Maximum number of saved stderr per job type.
    int MaxStderrCount;

    //! Maximum number of saved coredumps info per job type.
    int MaxCoreInfoCount;

    std::optional<i64> JobProxyMemoryOvercommitLimit;

    TDuration JobProxyRefCountedTrackerLogPeriod;

    //! An arbitrary user-provided string that is, however, logged by the scheduler.
    std::optional<TString> Title;

    //! Limit on operation execution time.
    std::optional<TDuration> TimeLimit;

    //! Timeout to gracefully fail jobs after timeout limit exceeded.
    TDuration TimeLimitJobFailTimeout;

    //! Users that can change operation parameters, e.g abort or suspend it.
    std::vector<TString> Owners;

    //! ACL for operation.
    //! It can consist of "allow"-only ACE-s with "read", "manage" and "administer" permissions.
    NSecurityClient::TSerializableAccessControlList Acl;

    //! Add the "read" and "manage" rights for the authenticated_user to |Acl|.
    bool AddAuthenticatedUserToAcl;

    //! A storage keeping YSON map that is hidden under ACL in Cypress. It will be exported
    //! to all user jobs via environment variables.
    NYTree::IMapNodePtr SecureVault;

    //! This flag enables secure vault variables in job shell.
    bool EnableSecureVaultVariablesInJobShell;

    //! Suspend operation in case of jobs failed due to account limit exceeded.
    bool SuspendOperationIfAccountLimitExceeded;

    //! Suspend operation right after the materialization phase.
    bool SuspendOperationAfterMaterialization;

    //! If total input data weight of operation is less, we disable locality timeouts.
    //! Also disables partitioned data balancing for small operations.
    i64 MinLocalityInputDataWeight;

    //! Various auto-merge knobs.
    TAutoMergeConfigPtr AutoMerge;

    TLogDigestConfigPtr JobProxyMemoryDigest;
    std::optional<double> JobProxyResourceOverdraftMemoryMultiplier;

    //! If set to true, any aborted/failed job will result in operation fail.
    bool FailOnJobRestart;

    // TODO(gritukan): Deprecate.
    bool EnableJobSplitting;

    //! If set to true, erasure chunks are forcefully sliced into data parts,
    //! and only then sliced by row indices. This should deal with locality issues,
    //! but leads to an 12x memory consumption in controller at worst case scenario.
    bool SliceErasureChunksByParts;

    //! Option controlling the presence of a legacy live preview.
    //! If set to std::nullopt, live preview is enabled depending on
    //! presence of user in LegacyLivePreviewUserBlacklist.
    std::optional<bool> EnableLegacyLivePreview;

    //! These fields are not used in scheduler but specified in order
    //! to not appear in unrecognized spec.
    NYTree::IMapNodePtr StartedBy;

    // TODO(gritukan@): Drop it in favor of `Annotations["description"]'.
    NYTree::IMapNodePtr Description;

    NYTree::IMapNodePtr Annotations;

    //! If true, enables the columnar statistics machinery to estimate job sizes.
    //! Note that turning this on may significantly affect workload partitioning for existing operations.
    bool UseColumnarStatistics;

    //! If true, node is banned each time a job is failed there.
    bool BanNodesWithFailedJobs;

    // If true, job failed at a banned node is considered aborted.
    bool IgnoreJobFailuresAtBannedNodes;

    // If true, operations fails if all available nodes get banned.
    bool FailOnAllNodesBanned;

    TSamplingConfigPtr Sampling;

    //! If set, operation will be accessible through the scheduler API calls under this name
    //! (it should start with an asterisk).
    std::optional<TString> Alias;

    //! If true, then omits columns that are inaccessible due to columnar ACL restriction instead of
    //! failing the operation.
    bool OmitInaccessibleColumns;

    //! These tags are propagated to all operation outputs (unless overridden).
    std::vector<NSecurityClient::TSecurityTag> AdditionalSecurityTags;

    //! Timeout of waiting job start on the host.
    std::optional<TDuration> WaitingJobTimeout;

    //! Force running speculative job after this timeout. Has lower priority than `JobSpeculationTimeout`
    //! from TUserJobSpec.
    std::optional<TDuration> JobSpeculationTimeout;

    //! Should match the atomicity of output dynamic tables.
    NTransactionClient::EAtomicity Atomicity;

    //! If explicitly set, overrides the default behaviour or locking output dynamic tables depending
    //! on their atomicity.
    std::optional<bool> LockOutputDynamicTables;

    TJobCpuMonitorConfigPtr JobCpuMonitor;

    //! If explicitly false, then data from dynamic stores of dynamic tables should not be read.
    std::optional<bool> EnableDynamicStoreRead;

    //! Describes suitable controller agent tag for operation.
    TString ControllerAgentTag;

    //! Description of possible shells for operation jobs.
    std::vector<TJobShellPtr> JobShells;

    TJobSplitterConfigPtr JobSplitter;

    //! Explicitly specified names of experiments.
    std::optional<std::vector<TString>> ExperimentOverrides;

    //! Enable trace log level for operation controller.
    bool EnableTraceLogging;

    //! Columnar statistics config for input tables.
    TColumnarStatisticsConfigPtr InputTableColumnarStatistics;

    //! Columnar statistics config for user files.
    TColumnarStatisticsConfigPtr UserFileColumnarStatistics;

    //! Enable job proxy tracing for all jobs.
    bool ForceJobProxyTracing;

    //! If true, operation is suspended when job failed.
    bool SuspendOnJobFailure;

    NYTree::IMapNodePtr JobTestingOptions;

    //! Enable prefetching throttler.
    //! This option is not expected to be set by users manually.
    bool EnablePrefetchingJobThrottler;

    NChunkClient::EChunkAvailabilityPolicy ChunkAvailabilityPolicy;

    //! Delay for performing sanity checks for operations (useful in tests).
    TDuration SanityCheckDelay;

    //! List of the enabled profilers.
    std::vector<TJobProfilerSpecPtr> Profilers;

    //! Default base layer used if no other layers are requested.
    std::optional<TString> DefaultBaseLayerPath;

    //! The setup for the experimental jobs.
    TJobExperimentConfigPtr JobExperiment;

    bool AdjustDynamicTableDataSlices;

    bool EnableChunkListsPreallocation;

    REGISTER_YSON_STRUCT(TOperationSpecBase);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOperationSpecBase, 0xf0494353);
};

DEFINE_REFCOUNTED_TYPE(TOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TTaskOutputStreamConfig
    : public NYTree::TYsonStruct
{
public:
    NTableClient::TTableSchemaPtr Schema;

    REGISTER_YSON_STRUCT(TTaskOutputStreamConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTaskOutputStreamConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobSpec
    : public NYTree::TYsonStruct
{
public:
    TString Command;

    TString TaskTitle;

    std::vector<NYPath::TRichYPath> FilePaths;
    std::vector<NYPath::TRichYPath> LayerPaths;

    std::optional<NFormats::TFormat> Format;
    std::optional<NFormats::TFormat> InputFormat;
    std::optional<NFormats::TFormat> OutputFormat;

    std::vector<TTaskOutputStreamConfigPtr> OutputStreams;

    std::optional<bool> EnableInputTableIndex;

    THashMap<TString, TString> Environment;

    double CpuLimit;
    int GpuLimit;
    int PortCount;
    std::optional<TDuration> JobTimeLimit;
    TDuration PrepareTimeLimit;
    i64 MemoryLimit;
    //! If set, overrides both of the next two values.
    std::optional<double> MemoryReserveFactor;
    double UserJobMemoryDigestDefaultValue;
    double UserJobMemoryDigestLowerBound;
    std::optional<double> UserJobResourceOverdraftMemoryMultiplier;

    TLogDigestConfigPtr JobProxyMemoryDigest;
    std::optional<double> JobProxyResourceOverdraftMemoryMultiplier;

    bool IgnoreMemoryReserveFactorLessThanOne;

    bool IncludeMemoryMappedFiles;

    bool UseYamrDescriptors;
    bool CheckInputFullyConsumed;

    i64 MaxStderrSize;

    i64 CustomStatisticsCountLimit;

    // COMPAT(ignat)
    std::optional<i64> TmpfsSize;
    std::optional<TString> TmpfsPath;

    std::vector<TTmpfsVolumeConfigPtr> TmpfsVolumes;

    // COMPAT(ignat)
    std::optional<i64> DiskSpaceLimit;
    std::optional<i64> InodeLimit;

    TDiskRequestConfigPtr DiskRequest;

    bool CopyFiles;

    //! Flag showing that user code is guaranteed to be deterministic.
    bool Deterministic;

    //! This flag forces creation of memory cgroup for user job and getting memory usage statistics from this cgroup.
    //! Makes sense only with Porto environment.
    bool UsePortoMemoryTracking;

    //! This flag currently makes sense only for Porto environment.
    //! It forces setting container CPU limit on slot container equal to CpuLimit provided in task spec
    //! and overrides setting in operation options.
    bool SetContainerCpuLimit;

    //! Forcefully run job with proper ulimit -c in order to enable core dump collection.
    //! This option should not be used outside tests.
    bool ForceCoreDump;

    std::optional<TString> InterruptionSignal;
    bool SignalRootProcessOnly;
    std::optional<int> RestartExitCode;

    bool EnableSetupCommands;
    bool EnableGpuLayers;

    std::optional<TString> CudaToolkitVersion;

    //! Name of layer with GPU check.
    std::optional<TString> GpuCheckLayerName;

    //! Path to the file with GPU check binary inside layer.
    std::optional<TString> GpuCheckBinaryPath;

    //! Command line arguments for the GPU check binary.
    std::optional<std::vector<TString>> GpuCheckBinaryArgs;

    //! Force running speculative job after this timeout. Has higher priority than `JobSpeculationTimeout`
    //! from TOperationBaseSpec.
    std::optional<TDuration> JobSpeculationTimeout;

    //! Name of the network project to use in job.
    std::optional<TString> NetworkProject;

    //! Configures |enable_porto| setting for user job containers.
    //! If not given, then the global default from controller agent's config is used.
    std::optional<EEnablePorto> EnablePorto;

    //! If true, then a job is considered failed once is produces a core dump.
    bool FailJobOnCoreDump;

    //! If true, RootFS in user job is writable.
    bool MakeRootFSWritable;

    //! If true, job proxy looks through all the user job allocations
    //! via reading /proc/$PID/smaps and checks, whether they belong to
    //! file in tmpfs. This allows not to account memory for mapped files
    //! from tmpfs twice.
    bool UseSMapsMemoryTracker;

    //! Describes user job monitoring settings.
    TUserJobMonitoringConfigPtr Monitoring;

    std::optional<TString> SystemLayerPath;

    //! The docker image to use in the operation.
    std::optional<TString> DockerImage;

    //! If set, overrides |Profilers| from operation spec.
    std::optional<std::vector<TJobProfilerSpecPtr>> Profilers;

    bool EnableRpcProxyInJobProxy;
    int RpcProxyWorkerThreadPoolSize;

    void InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig);

    REGISTER_YSON_STRUCT(TUserJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TMandatoryUserJobSpec
    : public TUserJobSpec
{
public:
    REGISTER_YSON_STRUCT(TMandatoryUserJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMandatoryUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TOptionalUserJobSpec
    : public TUserJobSpec
{
public:
    bool IsNontrivial() const;

    REGISTER_YSON_STRUCT(TOptionalUserJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOptionalUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TVanillaTaskSpec
    : public TMandatoryUserJobSpec
{
public:
    //! Number of jobs that will be run in this task. This field is mandatory.
    int JobCount;

    TJobIOConfigPtr JobIO;

    std::vector<NYPath::TRichYPath> OutputTablePaths;

    bool RestartCompletedJobs;

    REGISTER_YSON_STRUCT(TVanillaTaskSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaTaskSpec)

////////////////////////////////////////////////////////////////////////////////

class TInputlyQueryableSpec
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<TString> InputQuery;
    std::optional<NTableClient::TTableSchema> InputSchema;

    REGISTER_YSON_STRUCT(TInputlyQueryableSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInputlyQueryableSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationWithUserJobSpec
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<NYPath::TRichYPath> StderrTablePath;
    NTableClient::TBlobTableWriterConfigPtr StderrTableWriter;

    std::optional<NYPath::TRichYPath> CoreTablePath;
    NTableClient::TBlobTableWriterConfigPtr CoreTableWriter;

    bool EnableCudaGpuCoreDump;

    REGISTER_YSON_STRUCT(TOperationWithUserJobSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationWithUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationSpecBase
    : public TOperationSpecBase
{
public:
    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    std::optional<i64> DataWeightPerJob;

    std::optional<int> JobCount;
    std::optional<int> MaxJobCount;

    std::optional<int> MaxDataSlicesPerJob;

    bool ForceJobSizeAdjuster;

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    REGISTER_YSON_STRUCT(TSimpleOperationSpecBase);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationSpecBase, 0x7819ae12);
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TOperationWithInputSpec
    : public virtual NYTree::TYsonStruct
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    REGISTER_YSON_STRUCT(TOperationWithInputSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOperationWithInputSpec, 0xd4e68bb7);
};

DEFINE_REFCOUNTED_TYPE(TOperationWithInputSpec)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedOperationSpecBase
    : public virtual TSimpleOperationSpecBase
    , public TInputlyQueryableSpec
    , public virtual TOperationWithInputSpec
{
public:
    REGISTER_YSON_STRUCT(TUnorderedOperationSpecBase);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedOperationSpecBase, 0x79aafe77);
};

DEFINE_REFCOUNTED_TYPE(TUnorderedOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationSpec
    : public TUnorderedOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    TMandatoryUserJobSpecPtr Mapper;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    bool Ordered;

    REGISTER_YSON_STRUCT(TMapOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapOperationSpec, 0x4aa00f9d);
};


DEFINE_REFCOUNTED_TYPE(TMapOperationSpec)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

class TMergeOperationSpec
    : public virtual TSimpleOperationSpecBase
    , public virtual TOperationWithInputSpec
{
public:
    NYPath::TRichYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    bool ForceTransform;

    ESchemaInferenceMode SchemaInferenceMode;

    REGISTER_YSON_STRUCT(TMergeOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMergeOperationSpec, 0x646bd8cb);
};

DEFINE_REFCOUNTED_TYPE(TMergeOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationSpec
    : public TUnorderedOperationSpecBase
    , public TMergeOperationSpec
{
public:
    REGISTER_YSON_STRUCT(TUnorderedMergeOperationSpec);

    static void Register(TRegistrar /*registrar*/)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationSpec, 0x969d7fbc);
};

DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeOperationSpec
    : public TMergeOperationSpec
    , public TInputlyQueryableSpec
{
public:
    REGISTER_YSON_STRUCT(TOrderedMergeOperationSpec);

    static void Register(TRegistrar)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationSpec, 0xff44f136);
};

////////////////////////////////////////////////////////////////////////////////

class TSortedOperationSpec
    : public virtual NYTree::TYsonStruct
{
public:
    bool UseNewSortedPool;
    NTableClient::TSortColumns MergeBy;

    REGISTER_YSON_STRUCT(TSortedOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedOperationSpec, 0xaa7471bf);
};

DEFINE_REFCOUNTED_TYPE(TSortedOperationSpec)

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationSpec)

class TSortedMergeOperationSpec
    : public TMergeOperationSpec
    , public TSortedOperationSpec
{
public:
    REGISTER_YSON_STRUCT(TSortedMergeOperationSpec);

    static void Register(TRegistrar)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeOperationSpec, 0x213a54d6);
};

DEFINE_REFCOUNTED_TYPE(TSortedMergeOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TEraseOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    NYPath::TRichYPath TablePath;
    bool CombineChunks;
    ESchemaInferenceMode SchemaInferenceMode;

    REGISTER_YSON_STRUCT(TEraseOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseOperationSpec, 0xbaec2ff5);
};


DEFINE_REFCOUNTED_TYPE(TEraseOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpec
    : public TSimpleOperationSpecBase
    , public TOperationWithUserJobSpec
    , public TSortedOperationSpec
{
public:
    TMandatoryUserJobSpecPtr Reducer;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;

    NTableClient::TSortColumns ReduceBy;
    NTableClient::TSortColumns SortBy;
    NTableClient::TSortColumns JoinBy;

    std::optional<bool> EnableKeyGuarantee;

    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;

    bool ValidateKeyColumnTypes;

    bool ConsiderOnlyPrimarySize;

    bool SliceForeignChunks;

    std::optional<i64> ForeignTableLookupKeysThreshold;

    REGISTER_YSON_STRUCT(TReduceOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceOperationSpec, 0x7353c0af);
};


DEFINE_REFCOUNTED_TYPE(TReduceOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationSpecBase
    : public TOperationSpecBase
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    //! Amount of (uncompressed) data to be distributed to one partition.
    //! It used only to determine partition count.
    std::optional<i64> PartitionDataWeight;
    std::optional<int> PartitionCount;

    //! Maximum number of child partitions of a partition.
    std::optional<int> MaxPartitionFactor;

    //! Amount of (uncompressed) data to be given to a single partition job.
    //! It used only to determine partition job count.
    std::optional<i64> DataWeightPerPartitionJob;
    std::optional<int> PartitionJobCount;

    //! Data size per shuffle job.
    i64 DataWeightPerShuffleJob;

    i64 DataWeightPerIntermediatePartitionJob;

    //! Limit number of chunk slices per shuffle job.
    i64 MaxChunkSlicePerShuffleJob;

    i64 MaxChunkSlicePerIntermediatePartitionJob;

    //! The expected ratio of data size after partitioning to data size before partitioning.
    //! For sort operations, this is always 1.0.
    double MapSelectivityFactor;

    double ShuffleStartThreshold;
    double MergeStartThreshold;

    TDuration SimpleSortLocalityTimeout;
    TDuration SimpleMergeLocalityTimeout;

    TDuration PartitionLocalityTimeout;
    TDuration SortLocalityTimeout;
    TDuration MergeLocalityTimeout;

    TJobIOConfigPtr PartitionJobIO;
    // Also works for ReduceCombiner if present.
    TJobIOConfigPtr SortJobIO;
    TJobIOConfigPtr MergeJobIO;

    int ShuffleNetworkLimit;

    //! Hard limit on the number of data slices in shuffle pool.
    int MaxShuffleDataSliceCount;

    //! Hard limit on the number of shuffle jobs.
    int MaxShuffleJobCount;

    //! Hard limit on the total number of data slices in all merge pool.
    int MaxMergeDataSliceCount;

    NTableClient::TSortColumns SortBy;

    //! If |true| then the scheduler attempts to distribute partition jobs evenly
    //! (w.r.t. the uncompressed input data size) across the cluster to balance IO
    //! load during the subsequent shuffle stage.
    bool EnablePartitionedDataBalancing;

    //! If |true| then unavailable intermediate chunks are regenerated by restarted jobs.
    //! Otherwise operation waits for them to become available again (or fails, according to
    //! unavailable chunk tactics).
    bool EnableIntermediateOutputRecalculation;

    std::optional<i64> DataWeightPerSortedJob;

    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;

    bool UseNewPartitionsHeuristic;

    double PartitionSizeFactor;

    bool UseNewSortedPool;

    REGISTER_YSON_STRUCT(TSortOperationSpecBase);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationSpecBase, 0xdd19ecde);
};


DEFINE_REFCOUNTED_TYPE(TSortOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationSpec
    : public TSortOperationSpecBase
{
public:
    NYPath::TRichYPath OutputTablePath;

    // Desired number of samples per partition.
    int SamplesPerPartition;

    ESchemaInferenceMode SchemaInferenceMode;

    //! Hard limit on the size of allowed input data weight.
    i64 MaxInputDataWeight;

    REGISTER_YSON_STRUCT(TSortOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortOperationSpec, 0xa6709f80);
};


DEFINE_REFCOUNTED_TYPE(TSortOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationSpec
    : public TSortOperationSpecBase
    , public TInputlyQueryableSpec
    , public TOperationWithUserJobSpec
{
public:
    std::vector<NYPath::TRichYPath> OutputTablePaths;

    std::vector<TString> ReduceBy;

    TOptionalUserJobSpecPtr Mapper;
    TOptionalUserJobSpecPtr ReduceCombiner;
    TMandatoryUserJobSpecPtr Reducer;

    bool ForceReduceCombiners;

    // First `MapperOutputTableCount` tables will be constructed from
    // mapper's output to file handlers #4, #7, ...
    int MapperOutputTableCount;

    // Turn map phase into ordered map.
    bool Ordered;

    // Enable table indices and schematization in case of trivial mapper.
    // TODO(levysotsky): Remove this option after successful deploy to production.
    // It is necessary only for emergency switching off.
    bool EnableTableIndexIfHasTrivialMapper;

public:
    bool HasNontrivialMapper() const;
    bool HasNontrivialReduceCombiner() const;
    bool HasSchemafulIntermediateStreams() const;

    REGISTER_YSON_STRUCT(TMapReduceOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapReduceOperationSpec, 0x99837bbc);
};

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    std::optional<TString> ClusterName;
    std::optional<TString> NetworkName;
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks;
    // TODO(max42): do we still need this?
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    int MaxChunkCountPerJob;
    bool CopyAttributes;
    std::optional<std::vector<TString>> AttributeKeys;

    // Specifies how many chunks to read/write concurrently.
    int Concurrency;

    // Specifies buffer size for blocks of one chunk.
    // At least one block will be read so this buffer size can be violated
    // if block is bigger than this value.
    i64 BlockBufferSize;

    ESchemaInferenceMode SchemaInferenceMode;

    //! For testing purposes only.
    TDuration DelayInCopyChunk;

    //! If a part of erasure chunk was not read within this timeout, repair starts.
    TDuration ErasureChunkRepairDelay;

    //! If true, erasure chunks could be repaired during copy if some of the parts
    //! are missing at the source cluster.
    bool RepairErasureChunks;

    //! If true, jobs will never open a channel to masters and will always use
    //! remote master caches.
    bool UseRemoteMasterCaches;

    REGISTER_YSON_STRUCT(TRemoteCopyOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyOperationSpec, 0x3c0ce9c0);
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TVanillaOperationSpec
    : public TOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    //! Map consisting of pairs <task_name, task_spec>.
    THashMap<TString, TVanillaTaskSpecPtr> Tasks;

    REGISTER_YSON_STRUCT(TVanillaOperationSpec);

    static void Register(TRegistrar registrar);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationSpec, 0x001004fe);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationFairShareTreeRuntimeParameters
    : public NYTree::TYsonStruct
{
public:
    std::optional<double> Weight;
    TPoolName Pool;
    TJobResourcesConfigPtr ResourceLimits;

    // Can only be enabled by an administrator.
    bool EnableDetailedLogs;

    // Cannot be specified by user.
    bool Tentative;
    bool Probing;
    bool Offloading;

    REGISTER_YSON_STRUCT(TOperationFairShareTreeRuntimeParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationFairShareTreeRuntimeParameters)

////////////////////////////////////////////////////////////////////////////////

class TOperationJobShellRuntimeParameters
    : public NYTree::TYsonStruct
{
public:
    std::vector<TString> Owners;

    REGISTER_YSON_STRUCT(TOperationJobShellRuntimeParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationJobShellRuntimeParameters)

////////////////////////////////////////////////////////////////////////////////

class TOperationRuntimeParameters
    : public TRefCounted
{
public:
    // Keep the stuff below synchronized with Serialize/Deserialize functions.
    // Heavy parameters may be serialized separately, check SerializeHeavyRuntimeParameters function.

    // COMPAT(levysotsky): We need to support both |Owners| and |Acl|
    // to be able to revive old operations.
    std::vector<TString> Owners;
    NSecurityClient::TSerializableAccessControlList Acl;
    TJobShellOptionsMap OptionsPerJobShell;
    THashMap<TString, TOperationFairShareTreeRuntimeParametersPtr> SchedulingOptionsPerPoolTree;
    NYTree::IMapNodePtr Annotations;
    TString ControllerAgentTag;

    // Erased trees of operation, should be used only for information purposes.
    std::vector<TString> ErasedTrees;
};

void SerializeHeavyRuntimeParameters(NYTree::TFluentMap fluent, const TOperationRuntimeParameters& parameters);
void Serialize(const TOperationRuntimeParameters& parameters, NYson::IYsonConsumer* consumer, bool serializeHeavy = true);
void Serialize(const TOperationRuntimeParametersPtr& parameters, NYson::IYsonConsumer* consumer, bool serializeHeavy = true);
void Deserialize(TOperationRuntimeParameters& parameters, NYTree::INodePtr node);
void Deserialize(TOperationRuntimeParameters& parameters, NYson::TYsonPullParserCursor* cursor);

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeParameters)

////////////////////////////////////////////////////////////////////////////////

class TOperationFairShareTreeRuntimeParametersUpdate
    : public NYTree::TYsonStruct
{
public:
    std::optional<double> Weight;
    std::optional<TString> Pool;
    TJobResourcesConfigPtr ResourceLimits;
    // Can only be set by an administrator.
    std::optional<bool> EnableDetailedLogs;

    REGISTER_YSON_STRUCT(TOperationFairShareTreeRuntimeParametersUpdate);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationFairShareTreeRuntimeParametersUpdate)

////////////////////////////////////////////////////////////////////////////////

class TOperationRuntimeParametersUpdate
    : public NYTree::TYsonStruct
{
public:
    std::optional<double> Weight;
    std::optional<TString> Pool;
    std::optional<NSecurityClient::TSerializableAccessControlList> Acl;
    THashMap<TString, TOperationFairShareTreeRuntimeParametersUpdatePtr> SchedulingOptionsPerPoolTree;
    TJobShellOptionsUpdeteMap OptionsPerJobShell;
    std::optional<NYTree::IMapNodePtr> Annotations;
    std::optional<TString> ControllerAgentTag;


    bool ContainsPool() const;

    NYTree::EPermissionSet GetRequiredPermissions() const;

    REGISTER_YSON_STRUCT(TOperationRuntimeParametersUpdate);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeParametersUpdate)

//! Return new fair share tree runtime parameters applying |update| to |origin|.
//! |origin| can be |nullptr|, in this case an attempt
//! to create a new parameters object from |update| will be taken.
//! |origin| object is not changed.
TOperationFairShareTreeRuntimeParametersPtr UpdateFairShareTreeRuntimeParameters(
    const TOperationFairShareTreeRuntimeParametersPtr& origin,
    const TOperationFairShareTreeRuntimeParametersUpdatePtr& update);

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to schedulers.
    TDuration RpcTimeout;
    //! Timeout for acknowledgements for all RPC requests to schedulers.
    TDuration RpcAcknowledgementTimeout;

    REGISTER_YSON_STRUCT(TSchedulerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobCpuMonitorConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableCpuReclaim;

    TDuration StartDelay;
    TDuration CheckPeriod;

    double SmoothingFactor;

    double RelativeUpperBound;
    double RelativeLowerBound;

    double IncreaseCoefficient;
    double DecreaseCoefficient;

    int VoteWindowSize;
    int VoteDecisionThreshold;

    double MinCpuLimit;

    REGISTER_YSON_STRUCT(TJobCpuMonitorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobCpuMonitorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
