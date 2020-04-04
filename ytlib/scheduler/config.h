#pragma once

#include "public.h"
#include "helpers.h"
#include "job_resources.h"

#include <yt/ytlib/api/native/config.h>

#include <yt/client/formats/format.h>
#include <yt/client/formats/config.h>

#include <yt/client/table_client/schema.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/security_client/public.h>
#include <yt/client/security_client/acl.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/transaction_client/public.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/permission.h>
#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/misc/arithmetic_formula.h>

#include <yt/core/misc/phoenix.h>

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

private:
    TString Pool;
    std::optional<TString> ParentPool;
};

void Deserialize(TPoolName& value, NYTree::INodePtr node);
void Serialize(const TPoolName& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TSupportsSchedulingTagsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TBooleanFormula SchedulingTagFilter;

    TSupportsSchedulingTagsConfig();
};

DEFINE_REFCOUNTED_TYPE(TSupportsSchedulingTagsConfig)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> UserSlots;
    std::optional<double> Cpu;
    std::optional<int> Network;
    std::optional<i64> Memory;
    std::optional<int> Gpu;

    template <class T>
    void ForEachResource(T processResource)
    {
        // NB(renadeen): must be in sync with the lines below.
        YT_VERIFY(GetParameterCount() == 5);

        processResource(&TResourceLimitsConfig::UserSlots, "user_slots");
        processResource(&TResourceLimitsConfig::Cpu, "cpu");
        processResource(&TResourceLimitsConfig::Network, "network");
        processResource(&TResourceLimitsConfig::Memory, "memory");
        processResource(&TResourceLimitsConfig::Gpu, "gpu");
    }

    TResourceLimitsConfig();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSchedulableConfig
    : public TSupportsSchedulingTagsConfig
{
public:
    std::optional<double> Weight;

    // Specifies resource limits in terms of a share of all cluster resources.
    std::optional<double> MaxShareRatio;
    // Specifies resource limits in absolute values.
    TResourceLimitsConfigPtr ResourceLimits;

    // Specifies guaranteed resources in absolute values.
    TResourceLimitsConfigPtr MinShareResources;

    // The following settings override scheduler configuration.
    std::optional<TDuration> MinSharePreemptionTimeout;
    std::optional<TDuration> FairSharePreemptionTimeout;
    std::optional<double> FairShareStarvationTolerance;

    std::optional<TDuration> MinSharePreemptionTimeoutLimit;
    std::optional<TDuration> FairSharePreemptionTimeoutLimit;
    std::optional<double> FairShareStarvationToleranceLimit;

    std::optional<bool> AllowAggressiveStarvationPreemption;

    TSchedulableConfig();
};

////////////////////////////////////////////////////////////////////////////////

class TExtendedSchedulableConfig
    : public TSchedulableConfig
{
public:
    std::optional<TString> Pool;

    TExtendedSchedulableConfig();
};

DEFINE_REFCOUNTED_TYPE(TExtendedSchedulableConfig)

////////////////////////////////////////////////////////////////////////////////

class TEphemeralSubpoolConfig
    : public NYTree::TYsonSerializable
{
public:
    ESchedulingMode Mode;

    std::optional<int> MaxRunningOperationCount;
    std::optional<int> MaxOperationCount;

    TEphemeralSubpoolConfig();
};

DEFINE_REFCOUNTED_TYPE(TEphemeralSubpoolConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHistoricUsageAggregationMode,
    ((None)                     (0))
    ((ExponentialMovingAverage) (1))
);

class THistoricUsageConfig
    : public NYTree::TYsonSerializable
{
public:
    EHistoricUsageAggregationMode AggregationMode;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha;

    THistoricUsageConfig();
};

DEFINE_REFCOUNTED_TYPE(THistoricUsageConfig)

////////////////////////////////////////////////////////////////////////////////

class TPoolConfig
    : public TSchedulableConfig
{
public:
    ESchedulingMode Mode;

    std::optional<int> MaxRunningOperationCount;
    std::optional<int> MaxOperationCount;

    std::vector<EFifoSortParameter> FifoSortParameters;

    bool EnableAggressiveStarvation;

    bool ForbidImmediateOperations;

    bool CreateEphemeralSubpools;

    TEphemeralSubpoolConfigPtr EphemeralSubpoolConfig;

    bool InferChildrenWeightsFromHistoricUsage;
    THistoricUsageConfigPtr HistoricUsageConfig;

    THashSet<TString> AllowedProfilingTags;

    std::optional<bool> EnableByUserProfiling;

    TPoolConfig();

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TPoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TTentativeTreeEligibilityConfig
    : public virtual NYTree::TYsonSerializable
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

    TTentativeTreeEligibilityConfig();
};

DEFINE_REFCOUNTED_TYPE(TTentativeTreeEligibilityConfig)

////////////////////////////////////////////////////////////////////////////////

class TSamplingConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! The probability for each particular row to remain in the output.
    std::optional<double> SamplingRate;

    //! An option regulating the total data slice count during the sampling job creation procedure.
    //! It should not be used normally and left only for manual setup in marginal cases.
    //! If not set, it is overriden with MaxTotalSliceCount from controller agent options.
    std::optional<i64> MaxTotalSliceCount;

    //! Size of IO block to consider when calculating the lower bound for sampling job size.
    i64 IOBlockSize;

    TSamplingConfig();
};

DEFINE_REFCOUNTED_TYPE(TSamplingConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPackingMetricType,
    ((Angle)       (0))
    ((AngleLength) (1))
);

class TFairShareStrategyPackingConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    bool Enable;

    EPackingMetricType Metric;

    int MaxBetterPastSnapshots;
    double AbsoluteMetricValueTolerance;
    double RelativeMetricValueTolerance;
    int MinWindowSizeForSchedule;
    int MaxHearbeatWindowSize;
    TDuration MaxHeartbeatAge;

    TFairShareStrategyPackingConfig();
};

DEFINE_REFCOUNTED_TYPE(TFairShareStrategyPackingConfig)

////////////////////////////////////////////////////////////////////////////////

class TStrategyOperationSpec
    : public TSchedulableConfig
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
    //! This option can't be used simultaneously with TentativePoolTrees or UseDefaulTentativePoolTrees;
    bool ScheduleInSingleTree;

    //! Tentative pool trees to schedule operation in.
    //! Operation's job will be scheduled to these pool trees as long as they're
    //! not much slower than those in other (non-tentative) trees.
    //! If TentativePoolTrees is not empty, PoolTrees must not be empty, too.
    std::optional<THashSet<TString>> TentativePoolTrees;

    //! Enables using default tentative pool trees from scheduler config. It has effect only if TentativePoolTrees is not specified.
    bool UseDefaultTentativePoolTrees;

    // Config for tentative pool tree eligibility - the part of the scheduler that decides
    // whether a job should (or shouldn't) be launched in a pool tree marked as tentative.
    TTentativeTreeEligibilityConfigPtr TentativeTreeEligibility;

    int UpdatePreemptableJobsListLoggingPeriod;

    std::optional<TString> CustomProfilingTag;

    std::optional<int> MaxUnpreemptableRunningJobCount;

    int MaxSpeculativeJobCountPerTask;

    EPreemptionMode PreemptionMode;

    TStrategyOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TStrategyOperationSpec, 0x22fc73fa);
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationSpec);

////////////////////////////////////////////////////////////////////////////////

class TJobIOConfig
    : public NYTree::TYsonSerializable
{
public:
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NTableClient::TTableWriterConfigPtr DynamicTableWriter;

    NFormats::TControlAttributesConfigPtr ControlAttributes;

    NApi::TFileWriterConfigPtr ErrorFileWriter;

    i64 BufferRowCount;

    int PipeIOPoolSize;

    class TTestingOptions
        : public TYsonSerializable
    {
    public:
        TDuration PipeDelay;

        TTestingOptions()
        {
            RegisterParameter("pipe_delay", PipeDelay)
                .Default();
        }
    };

    TIntrusivePtr<TTestingOptions> Testing;

    TJobIOConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobIOConfig)
DEFINE_REFCOUNTED_TYPE(TJobIOConfig::TTestingOptions)

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
)

class TTestingOperationOptions
    : public NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> SchedulingDelay;
    ESchedulingDelayType SchedulingDelayType;

    //! The following delays are used inside the operation controller.

    std::optional<TDuration> DelayInsideOperationCommit;
    std::optional<EDelayInsideOperationCommitStage> DelayInsideOperationCommitStage;
    bool NoDelayOnSecondEntranceToCommit;

    std::optional<TDuration> DelayInsideInitialize;

    std::optional<TDuration> DelayInsidePrepare;

    std::optional<TDuration> DelayInsideRevive;

    std::optional<TDuration> DelayInsideSuspend;

    std::optional<TDuration> DelayInsideMaterialize;

    //! The following delays are used inside the scheduler.

    std::optional<TDuration> DelayAfterMaterialize;

    std::optional<TDuration> DelayInsideAbort;

    std::optional<i64> AllocationSize;

    //! Intentionally fails the operation controller. Used only for testing purposes.
    std::optional<EControllerFailureType> ControllerFailure;

    std::optional<ECancelationStage> CancelationStage;

    std::optional<TDuration> GetJobSpecDelay;

    bool FailGetJobSpec;

    bool RegisterSpeculativeJobOnJobScheduled;
    bool RegisterSpeculativeJobOnJobScheduledOnce;

    bool LogResidualCustomJobMetricsOnTermination;

    TTestingOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TTestingOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeConfig
    : public NYTree::TYsonSerializable
{
public:
    TJobIOConfigPtr JobIO;

    std::optional<i64> MaxIntermediateChunkCount;
    std::optional<i64> ChunkCountPerMergeJob;
    i64 ChunkSizeThreshold;
    EAutoMergeMode Mode;

    TAutoMergeConfig();
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeConfig)

////////////////////////////////////////////////////////////////////////////////

class TTmpfsVolumeConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 Size;
    TString Path;

    TTmpfsVolumeConfig();
};

DEFINE_REFCOUNTED_TYPE(TTmpfsVolumeConfig)

void ToProto(NScheduler::NProto::TTmpfsVolume* protoTmpfsVolume, const TTmpfsVolumeConfig& tmpfsVolumeConfig);
void FromProto(TTmpfsVolumeConfig* tmpfsVolumeConfig, const NScheduler::NProto::TTmpfsVolume& protoTmpfsVolume);

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

    TString IntermediateDataMediumName;

    //! Account for job nodes and operation files (stderrs and input contexts of failed jobs).
    TString JobNodeAccount;

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

    TTestingOperationOptionsPtr TestingOperationOptions;

    //! Users that can change operation parameters, e.g abort or suspend it.
    std::vector<TString> Owners;

    //! ACL for operation.
    //! It can consist of "allow"-only ACE-s with "read" and "manage" permissions.
    NYTree::INodePtr AclNode;
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

    //! Generic map to turn on/off different experimental options.
    NYTree::IMapNodePtr NightlyOptions;

    //! If total input data weight of operation is less, we disable locality timeouts.
    //! Also disables partitioned data balancing for small operations.
    i64 MinLocalityInputDataWeight;

    //! Various auto-merge knobs.
    TAutoMergeConfigPtr AutoMerge;

    // TODO(max42): make this field per-task.
    TLogDigestConfigPtr JobProxyMemoryDigest;

    //! If set to true, any aborted/failed job will result in operation fail.
    bool FailOnJobRestart;

    bool EnableJobSplitting;

    //! If set to true, erasure chunks are forcefully sliced into data parts,
    //! and only then sliced by row indices. This should deal with locality issues,
    //! but leads to an 12x memory consumption in controller at worst case scenario.
    bool SliceErasureChunksByParts;

    //! Controls operation storage mode. UNUSED.
    bool EnableCompatibleStorageMode;

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

    //! Should match the atomicity of output dynamic tables. If present, output dynamic tables are not locked.
    NTransactionClient::EAtomicity Atomicity;

    TJobCpuMonitorConfigPtr JobCpuMonitor;

    TOperationSpecBase();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOperationSpecBase, 0xf0494353);
};


DEFINE_REFCOUNTED_TYPE(TOperationSpecBase);

////////////////////////////////////////////////////////////////////////////////

class TUserJobSpec
    : public NYTree::TYsonSerializable
{
public:
    TString Command;

    TString TaskTitle;

    std::vector<NYPath::TRichYPath> FilePaths;
    std::vector<NYPath::TRichYPath> LayerPaths;

    std::optional<NFormats::TFormat> Format;
    std::optional<NFormats::TFormat> InputFormat;
    std::optional<NFormats::TFormat> OutputFormat;

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

    bool IncludeMemoryMappedFiles;

    bool UseYamrDescriptors;
    bool CheckInputFullyConsumed;

    i64 MaxStderrSize;

    bool EnableProfiling;
    i64 MaxProfileSize;

    i64 CustomStatisticsCountLimit;

    std::optional<i64> TmpfsSize;
    std::optional<TString> TmpfsPath;

    std::vector<TTmpfsVolumeConfigPtr> TmpfsVolumes;

    std::optional<i64> DiskSpaceLimit;
    std::optional<i64> InodeLimit;

    bool CopyFiles;

    //! Flag showing that user code is guaranteed to be deterministic.
    bool Deterministic;

    //! This flag forces creation of memory cgroup for user job and getting memory usage statistics from this cgroup.
    //! Makes sense only with porto environment.
    bool UsePortoMemoryTracking;

    //! This flag currently makes sense only for porto environment. It forces restriction on cpu limit with the
    //! container means. This option should normally be useful only for experiments and benchmarks.
    bool SetContainerCpuLimit;

    //! Forcefully run job with proper ulimit -c in order to enable core dump collection.
    //! This option should not be used outside tests.
    bool ForceCoreDump;

    std::optional<TString> InterruptionSignal;

    bool EnableSetupCommands;
    bool EnableGpuLayers;

    std::optional<TString> CudaToolkitVersion;

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

    TUserJobSpec();

    void InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TMandatoryUserJobSpec
    : public TUserJobSpec
{
public:
    TMandatoryUserJobSpec();
};

DEFINE_REFCOUNTED_TYPE(TMandatoryUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TOptionalUserJobSpec
    : public TUserJobSpec
{
public:
    bool IsNontrivial() const;

    TOptionalUserJobSpec();
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

    TVanillaTaskSpec();
};

DEFINE_REFCOUNTED_TYPE(TVanillaTaskSpec)

////////////////////////////////////////////////////////////////////////////////

class TInputlyQueryableSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TString> InputQuery;
    std::optional<NTableClient::TTableSchema> InputSchema;

    TInputlyQueryableSpec();
};

DEFINE_REFCOUNTED_TYPE(TInputlyQueryableSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationWithUserJobSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<NYPath::TRichYPath> StderrTablePath;
    NTableClient::TBlobTableWriterConfigPtr StderrTableWriter;

    std::optional<NYPath::TRichYPath> CoreTablePath;
    NTableClient::TBlobTableWriterConfigPtr CoreTableWriter;

    TOperationWithUserJobSpec();
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

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    TSimpleOperationSpecBase();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleOperationSpecBase, 0x7819ae12);
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationSpecBase);

////////////////////////////////////////////////////////////////////////////////

class TUnorderedOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TInputlyQueryableSpec
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    TUnorderedOperationSpecBase();

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

    TMapOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapOperationSpec, 0x4aa00f9d);
};


DEFINE_REFCOUNTED_TYPE(TMapOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeOperationSpec
    : public TUnorderedOperationSpecBase
{
public:
    NYPath::TRichYPath OutputTablePath;
    bool CombineChunks;
    bool ForceTransform;
    ESchemaInferenceMode SchemaInferenceMode;

    TUnorderedMergeOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeOperationSpec, 0x969d7fbc);
};


DEFINE_REFCOUNTED_TYPE(TUnorderedMergeOperationSpec)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMergeMode,
    (Sorted)
    (Ordered)
    (Unordered)
);

class TMergeOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    EMergeMode Mode;
    bool CombineChunks;
    bool ForceTransform;
    NTableClient::TKeyColumns MergeBy;

    ESchemaInferenceMode SchemaInferenceMode;

    TMergeOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMergeOperationSpec, 0x646bd8cb);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TMergeOperationSpec)

class TOrderedMergeOperationSpec
    : public TMergeOperationSpec
    , public TInputlyQueryableSpec
{
private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeOperationSpec, 0xff44f136);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationSpec)

class TSortedMergeOperationSpec
    : public TMergeOperationSpec
{
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

    TEraseOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseOperationSpec, 0xbaec2ff5);
};


DEFINE_REFCOUNTED_TYPE(TEraseOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    TMandatoryUserJobSpecPtr Reducer;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    NTableClient::TKeyColumns JoinBy;

    bool ConsiderOnlyPrimarySize;

    TReduceOperationSpecBase();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceOperationSpecBase, 0x7353c0af);
};


DEFINE_REFCOUNTED_TYPE(TReduceOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TNewReduceOperationSpec
    : public TReduceOperationSpecBase
{
public:
    NTableClient::TKeyColumns ReduceBy;
    NTableClient::TKeyColumns SortBy;

    std::optional<bool> EnableKeyGuarantee;

    std::vector<NTableClient::TOwningKey> PivotKeys;

    bool ValidateKeyColumnTypes;

    TNewReduceOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TNewReduceOperationSpec, 0xbbc5bdcd);
};


DEFINE_REFCOUNTED_TYPE(TNewReduceOperationSpec)

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

    //! Amount of (uncompressed) data to be given to a single partition job.
    //! It used only to determine partition job count.
    std::optional<i64> DataWeightPerPartitionJob;
    std::optional<int> PartitionJobCount;

    //! Data size per shuffle job.
    i64 DataWeightPerShuffleJob;

    //! Limit number of chunk slices per shuffle job.
    i64 MaxChunkSlicePerShuffleJob;

    //! The expected ratio of data size after partitioning to data size before partitioning.
    //! For sort operations, this is always 1.0.
    double MapSelectivityFactor;

    double ShuffleStartThreshold;
    double MergeStartThreshold;

    TDuration SimpleSortLocalityTimeout;
    TDuration SimpleMergeLocalityTimeout;

    TDuration PartitionLocalityTimeout;
    TDuration SortLocalityTimeout;
    TDuration SortAssignmentTimeout;
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

    std::vector<TString> SortBy;

    //! If |true| then the scheduler attempts to distribute partition jobs evenly
    //! (w.r.t. the uncompressed input data size) across the cluster to balance IO
    //! load during the subsequent shuffle stage.
    bool EnablePartitionedDataBalancing;

    //! If |true| then unavailable intermediate chunks are regenerated by restarted jobs.
    //! Otherwise operation waits for them to become available again (or fails, according to
    //! unavailable chunk tactics).
    bool EnableIntermediateOutputRecalculation;

    std::optional<i64> DataWeightPerSortedJob;

    std::vector<NTableClient::TOwningKey> PivotKeys;

    TSortOperationSpecBase();

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

    TSortOperationSpec();

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

    TMapReduceOperationSpec();

    bool HasNontrivialMapper() const;
    bool HasNontrivialReduceCombiner() const;

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
    std::optional<NApi::NNative::TConnectionConfigPtr> ClusterConnection;
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

    TRemoteCopyOperationSpec();

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

    TVanillaOperationSpec();

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaOperationSpec, 0x001004fe);
};

DEFINE_REFCOUNTED_TYPE(TVanillaOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationFairShareTreeRuntimeParameters
    : public NYTree::TYsonSerializable
{
public:
    std::optional<double> Weight;
    TPoolName Pool;
    TResourceLimitsConfigPtr ResourceLimits;

    // Can only be enabled by an administrator.
    bool EnableDetailedLogs;

    // Cannot be specified by user.
    bool Tentative;

    TOperationFairShareTreeRuntimeParameters();
};

DEFINE_REFCOUNTED_TYPE(TOperationFairShareTreeRuntimeParameters)

class TOperationRuntimeParameters
    : public NYTree::TYsonSerializable
{
public:
    // COMPAT(levysotsky): We need to support both |Owners| and |Acl|
    // to be able to revive old operations.
    std::vector<TString> Owners;
    NSecurityClient::TSerializableAccessControlList Acl;
    THashMap<TString, TOperationFairShareTreeRuntimeParametersPtr> SchedulingOptionsPerPoolTree;
    NYTree::IMapNodePtr Annotations;

    TOperationRuntimeParameters();
};

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeParameters)

////////////////////////////////////////////////////////////////////////////////

class TOperationFairShareTreeRuntimeParametersUpdate
    : public NYTree::TYsonSerializable
{
public:
    std::optional<double> Weight;
    std::optional<TPoolName> Pool;
    TResourceLimitsConfigPtr ResourceLimits;
    // Can only be set by an administrator.
    std::optional<bool> EnableDetailedLogs;

    TOperationFairShareTreeRuntimeParametersUpdate();
};

DEFINE_REFCOUNTED_TYPE(TOperationFairShareTreeRuntimeParametersUpdate)

class TOperationRuntimeParametersUpdate
    : public NYTree::TYsonSerializable
{
public:
    std::optional<double> Weight;
    std::optional<TString> Pool;
    std::optional<NSecurityClient::TSerializableAccessControlList> Acl;
    THashMap<TString, TOperationFairShareTreeRuntimeParametersUpdatePtr> SchedulingOptionsPerPoolTree;
    std::optional<NYTree::IMapNodePtr> Annotations;

    TOperationRuntimeParametersUpdate();

    bool ContainsPool() const;

    NYTree::EPermissionSet GetRequiredPermissions() const;
};

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeParametersUpdate)

//! Return new fair share tree runtime parameters applying |update| to |origin|.
//! |origin| can be |nullptr|, in this case an attempt
//! to create a new parameters object from |update| will be taken.
//! |origin| object is not changed.
TOperationFairShareTreeRuntimeParametersPtr UpdateFairShareTreeRuntimeParameters(
    const TOperationFairShareTreeRuntimeParametersPtr& origin,
    const TOperationFairShareTreeRuntimeParametersUpdatePtr& update);

//! Return new runtime parameters applying |update| to |origin|.
//! |origin| object is not changed.
//! NOTE: |origin| can not be |nullptr|.
TOperationRuntimeParametersPtr UpdateRuntimeParameters(
    const TOperationRuntimeParametersPtr& origin,
    const TOperationRuntimeParametersUpdatePtr& update);

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to schedulers.
    TDuration RpcTimeout;
    //! Timeout for acknowledgements for all RPC requests to schedulers.
    TDuration RpcAcknowledgementTimeout;

    TSchedulerConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobCpuMonitorConfig
    : public NYTree::TYsonSerializable
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

    TJobCpuMonitorConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobCpuMonitorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
