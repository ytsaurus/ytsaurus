#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/formats/config.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/misc/dnf.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// Ratio of MaxWeight and MinWeight shouldn't lose precision.
const double MinSchedulableWeight = sqrt(std::numeric_limits<double>::epsilon());
const double MaxSchedulableWeight = 1.0 / MinSchedulableWeight;

////////////////////////////////////////////////////////////////////////////////

class TJobIOConfig
    : public NYTree::TYsonSerializable
{
public:
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;

    NFormats::TControlAttributesConfigPtr ControlAttributes;

    NApi::TFileWriterConfigPtr ErrorFileWriter;

    i64 BufferRowCount;

    int PipeIOPoolSize;

    TJobIOConfig();
};

DEFINE_REFCOUNTED_TYPE(TJobIOConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControllerFailureType,
    (None)
    (AssertionFailureInPrepare)
    (ExceptionThrownInOnJobCompleted)
)

class TTestingOperationOptions
    : public NYTree::TYsonSerializable
{
public:
    TDuration SchedulingDelay;
    ESchedulingDelayType SchedulingDelayType;

    //! Intentionally fails the operation controller. Used only for testing purposes.
    EControllerFailureType ControllerFailure;

    TTestingOperationOptions();
};

DEFINE_REFCOUNTED_TYPE(TTestingOperationOptions)

////////////////////////////////////////////////////////////////////////////////

class TSupportsSchedulingTagsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TNullable<TString> SchedulingTag;
    TDnfFormula SchedulingTagFilter;

    TSupportsSchedulingTagsConfig();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TSupportsSchedulingTagsConfig)

////////////////////////////////////////////////////////////////////////////////

class TOperationSpecBase
    : public TSupportsSchedulingTagsConfig
{
public:
    //! Account holding intermediate data produces by the operation.
    TString IntermediateDataAccount;

    //! Codec used for compressing intermediate output during shuffle.
    NCompression::ECodec IntermediateCompressionCodec;

    //! Replication factor for intermediate data.
    int IntermediateDataReplicationFactor;

    TString IntermediateDataMediumName;

    //! Acl used for intermediate tables and stderrs.
    NYTree::IListNodePtr IntermediateDataAcl;

    //! Account for job nodes and operation files (stderrs and input contexts of failed jobs).
    TString JobNodeAccount;

    //! What to do during initialization if some chunks are unavailable.
    EUnavailableChunkAction UnavailableChunkStrategy;

    //! What to do during operation progress when some chunks get unavailable.
    EUnavailableChunkAction UnavailableChunkTactics;

    i64 MaxDataSizePerJob;

    //! Once this limit is reached the operation fails.
    int MaxFailedJobCount;

    //! Maximum number of saved stderr per job type.
    int MaxStderrCount;

    TNullable<i64> JobProxyMemoryOvercommitLimit;

    TDuration JobProxyRefCountedTrackerLogPeriod;

    bool EnableSortVerification;

    TNullable<TString> Title;

    //! Limit on operation execution time.
    TNullable<TDuration> TimeLimit;

    bool CheckMultichunkFiles;

    TTestingOperationOptionsPtr TestingOperationOptions;

    //! Users that can change operation parameters, e.g abort or suspend it.
    std::vector<TString> Owners;

    //! A storage keeping YSON map that is hidden under ACL in Cypress. It will be exported
    //! to all user jobs via environment variables.
    NYTree::IMapNodePtr SecureVault;

    //! If candidate exec nodes are not found for more than timeout time then operation will be failed.
    TDuration AvailableNodesMissingTimeout;

    TOperationSpecBase();
};

DEFINE_REFCOUNTED_TYPE(TOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TUserJobSpec
    : public NYTree::TYsonSerializable
{
public:
    TString Command;

    std::vector<NYPath::TRichYPath> FilePaths;

    TNullable<NFormats::TFormat> Format;
    TNullable<NFormats::TFormat> InputFormat;
    TNullable<NFormats::TFormat> OutputFormat;

    TNullable<bool> EnableInputTableIndex;

    yhash<TString, TString> Environment;

    double CpuLimit;
    TNullable<TDuration> JobTimeLimit;
    i64 MemoryLimit;
    double MemoryReserveFactor;

    bool IncludeMemoryMappedFiles;

    bool UseYamrDescriptors;
    bool CheckInputFullyConsumed;

    i64 MaxStderrSize;

    i64 CustomStatisticsCountLimit;

    TNullable<i64> TmpfsSize;
    TNullable<TString> TmpfsPath;

    bool CopyFiles;

    TUserJobSpec();

    void InitEnableInputTableIndex(int inputTableCount, TJobIOConfigPtr jobIOConfig);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

class TInputlyQueryableSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    TNullable<TString> InputQuery;
    TNullable<NTableClient::TTableSchema> InputSchema;

    TInputlyQueryableSpec();
};

DEFINE_REFCOUNTED_TYPE(TInputlyQueryableSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationWithUserJobSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    TNullable<NYPath::TRichYPath> StderrTablePath;
    NTableClient::TBlobTableWriterConfigPtr StderrTableWriterConfig;

    TNullable<NYPath::TRichYPath> CoreTablePath;
    NTableClient::TBlobTableWriterConfigPtr CoreTableWriterConfig;

    bool EnableJobSplitting;

    TOperationWithUserJobSpec();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TOperationWithUserJobSpec)

////////////////////////////////////////////////////////////////////////////////

// COMPAT(max42): remove this when YT-6547 is closed and legacy controllers are finally deprecated.
class TOperationWithLegacyControllerSpec
    : public virtual NYTree::TYsonSerializable
{
public:
    bool UseLegacyController;

    TOperationWithLegacyControllerSpec();
};

DEFINE_REFCOUNTED_TYPE(TOperationWithLegacyControllerSpec)

////////////////////////////////////////////////////////////////////////////////

class TSimpleOperationSpecBase
    : public TOperationSpecBase
{
public:
    //! During sorted merge the scheduler tries to ensure that large connected
    //! groups of chunks are partitioned into tasks of this or smaller size.
    //! This number, however, is merely an estimate, i.e. some tasks may still
    //! be larger.
    TNullable<i64> DataSizePerJob;

    TNullable<int> JobCount;
    TNullable<int> MaxJobCount;

    TDuration LocalityTimeout;
    TJobIOConfigPtr JobIO;

    // Operations inherited from this class produce the only kind
    // of jobs. This option corresponds to jobs of this kind.
    TLogDigestConfigPtr JobProxyMemoryDigest;

    TSimpleOperationSpecBase();
};

DEFINE_REFCOUNTED_TYPE(TSimpleOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TUnorderedOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TInputlyQueryableSpec
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    TUnorderedOperationSpecBase();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TUnorderedOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TMapOperationSpec
    : public TUnorderedOperationSpecBase
    , public TOperationWithUserJobSpec
{
public:
    TUserJobSpecPtr Mapper;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    bool Ordered;

    TMapOperationSpec();

    virtual void OnLoaded() override;
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

    virtual void OnLoaded() override;
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

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TMergeOperationSpec)

class TOrderedMergeOperationSpec
    : public TMergeOperationSpec
    , public TInputlyQueryableSpec
{ };

DEFINE_REFCOUNTED_TYPE(TOrderedMergeOperationSpec)

class TSortedMergeOperationSpec
    : public TMergeOperationSpec
    , public TOperationWithLegacyControllerSpec
{ };

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

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TEraseOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpecBase
    : public TSimpleOperationSpecBase
    , public TOperationWithUserJobSpec
    , public TOperationWithLegacyControllerSpec
{
public:
    TUserJobSpecPtr Reducer;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    std::vector<NYPath::TRichYPath> OutputTablePaths;
    NTableClient::TKeyColumns JoinBy;

    TReduceOperationSpecBase();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationSpecBase)

////////////////////////////////////////////////////////////////////////////////

class TReduceOperationSpec
    : public TReduceOperationSpecBase
{
public:
    NTableClient::TKeyColumns ReduceBy;
    NTableClient::TKeyColumns SortBy;

    std::vector<NTableClient::TOwningKey> PivotKeys;

    TReduceOperationSpec();
};

DEFINE_REFCOUNTED_TYPE(TReduceOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceOperationSpec
    : public TReduceOperationSpecBase
{
public:
    TJoinReduceOperationSpec();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TJoinReduceOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TSortOperationSpecBase
    : public TOperationSpecBase
    , public TOperationWithLegacyControllerSpec
{
public:
    std::vector<NYPath::TRichYPath> InputTablePaths;

    //! Amount of (uncompressed) data to be distributed to one partition.
    //! It used only to determine partition count.
    TNullable<i64> PartitionDataSize;
    TNullable<int> PartitionCount;

    //! Amount of (uncompressed) data to be given to a single partition job.
    //! It used only to determine partition job count.
    TNullable<i64> DataSizePerPartitionJob;
    TNullable<int> PartitionJobCount;

    //! Data size per shuffle job.
    i64 DataSizePerShuffleJob;

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

    std::vector<TString> SortBy;

    //! If |true| then the scheduler attempts to distribute partition jobs evenly
    //! (w.r.t. the uncompressed input data size) across the cluster to balance IO
    //! load during the subsequent shuffle stage.
    bool EnablePartitionedDataBalancing;

    //! When #EnablePartitionedDataBalancing is |true| the scheduler tries to maintain the following
    //! invariant regarding (uncompressed) |DataSize(i)| assigned to each node |i|:
    //! |max_i DataSize(i) <= avg_i DataSize(i) + DataSizePerJob * PartitionedDataBalancingTolerance|
    double PartitionedDataBalancingTolerance;

    // For all kinds of sort jobs: simple_sort, intermediate_sort, final_sort.
    TLogDigestConfigPtr SortJobProxyMemoryDigest;
    // For partition and partition_map jobs.
    TLogDigestConfigPtr PartitionJobProxyMemoryDigest;

    TNullable<i64> DataSizePerSortedJob;

    TSortOperationSpecBase();

    virtual void OnLoaded() override;
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

    // For sorted_merge and unordered_merge jobs.
    TLogDigestConfigPtr MergeJobProxyMemoryDigest;

    ESchemaInferenceMode SchemaInferenceMode;

    TSortOperationSpec();

    virtual void OnLoaded() override;
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

    TUserJobSpecPtr Mapper;
    TUserJobSpecPtr ReduceCombiner;
    TUserJobSpecPtr Reducer;

    // For sorted_reduce jobs.
    TLogDigestConfigPtr SortedReduceJobProxyMemoryDigest;
    // For partition_reduce jobs.
    TLogDigestConfigPtr PartitionReduceJobProxyMemoryDigest;
    // For reduce_combiner jobs.
    TLogDigestConfigPtr ReduceCombinerJobProxyMemoryDigest;

    bool ForceReduceCombiners;

    TMapReduceOperationSpec();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TMapReduceOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyOperationSpec
    : public TSimpleOperationSpecBase
{
public:
    TNullable<TString> ClusterName;
    TNullable<TString> NetworkName;
    TNullable<NApi::TNativeConnectionConfigPtr> ClusterConnection;
    std::vector<NYPath::TRichYPath> InputTablePaths;
    NYPath::TRichYPath OutputTablePath;
    int MaxChunkCountPerJob;
    bool CopyAttributes;
    TNullable<std::vector<TString>> AttributeKeys;

    ESchemaInferenceMode SchemaInferenceMode;

    TRemoteCopyOperationSpec();

    virtual void OnLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TRemoteCopyOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TResourceLimitsConfig
    : public NYTree::TYsonSerializable
{
public:
    TNullable<int> UserSlots;
    TNullable<int> Cpu;
    TNullable<int> Network;
    TNullable<i64> Memory;

    TResourceLimitsConfig();
};

class TSchedulableConfig
    : public TSupportsSchedulingTagsConfig
{
public:
    double Weight;

    // Specifies resource limits in terms of a share of all cluster resources.
    double MaxShareRatio;
    // Specifies resource limits in absolute values.
    TResourceLimitsConfigPtr ResourceLimits;

    // Specifies guaranteed resources in terms of a share of all cluster resources.
    double MinShareRatio;
    // Specifies guaranteed resources in absolute values.
    TResourceLimitsConfigPtr MinShareResources;

    // The following settings override scheduler configuration.
    TNullable<TDuration> MinSharePreemptionTimeout;
    TNullable<TDuration> FairSharePreemptionTimeout;
    TNullable<double> FairShareStarvationTolerance;

    TNullable<TDuration> MinSharePreemptionTimeoutLimit;
    TNullable<TDuration> FairSharePreemptionTimeoutLimit;
    TNullable<double> FairShareStarvationToleranceLimit;

    TNullable<bool> AllowAggressiveStarvationPreemption;

    TSchedulableConfig();
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsConfig)

class TPoolConfig
    : public TSchedulableConfig
{
public:
    ESchedulingMode Mode;

    TNullable<int> MaxRunningOperationCount;
    TNullable<int> MaxOperationCount;

    std::vector<EFifoSortParameter> FifoSortParameters;

    bool EnableAggressiveStarvation;

    bool ForbidImmediateOperations;

    TPoolConfig();

    void Validate();
};

DEFINE_REFCOUNTED_TYPE(TPoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TStrategyOperationSpec
    : public TSchedulableConfig
{
public:
    TNullable<TString> Pool;

    TStrategyOperationSpec();
};

DEFINE_REFCOUNTED_TYPE(TStrategyOperationSpec)

////////////////////////////////////////////////////////////////////////////////

class TOperationRuntimeParams
    : public NYTree::TYsonSerializable
{
public:
    double Weight;

    TOperationRuntimeParams();
};

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeParams)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to schedulers.
    TDuration RpcTimeout;

    TSchedulerConnectionConfig();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
