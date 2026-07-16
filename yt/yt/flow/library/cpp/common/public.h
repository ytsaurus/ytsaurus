#pragma once

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>
#include <yt/yt/flow/library/cpp/misc/identifier.h>

#include <yt/yt/flow/library/cpp/misc/versioned_value.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TJobId, TGuid);
YT_DEFINE_STRONG_TYPEDEF(TIncarnationId, TGuid);
YT_DEFINE_STRONG_TYPEDEF(TKey, TCompactUnversionedOwningRow);
struct TKeyRange;
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TMessageId);
YT_DEFINE_STRONG_TYPEDEF(TPayload, TCompactUnversionedOwningRow);
YT_DEFINE_STRONG_TYPEDEF(TPartitionId, TGuid);
YT_DEFINE_STRONG_TYPEDEF(TWorkerGroupId, std::string);
YT_DEFINE_STRONG_TYPEDEF(TPartitionData, std::string);
YT_DEFINE_STRONG_TYPEDEF(TWatermarkAlignmentGroup, std::string);

YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TStreamId);
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TResourceId);
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TComputationId);
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TSinkId);
YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TThrottlerId);

using TLeaseId = NTransactionClient::TTransactionId;
constexpr TLeaseId NullLeaseId = NTransactionClient::NullTransactionId;

YT_DEFINE_STRONG_TYPEDEF(TStreamSpecId, i64);

YT_DEFINE_STRONG_TYPEDEF(TWatermarkPercentile, double);
static constexpr auto PreciseWatermarkPercentile = TWatermarkPercentile(100);
static constexpr auto IgnoreInflightWatermarkPercentile = TWatermarkPercentile(0);

//! Sequence number for internal use in persisted state data structure. Should be numeric, at least comparable and incrementable.
YT_DEFINE_STRONG_TYPEDEF(TSequenceId, i64);
//! Type of a name of persisted state table, like it is stored in database.
using TPersistedStateName = std::string;

//! Target is represented as an arbitrary string.
YT_DEFINE_STRONG_TYPEDEF(TFlowCoreTarget, std::string);

using TVersionedFlowCoreTarget = TVersionedValue<TFlowCoreTarget>;
DECLARE_REFCOUNTED_TYPE(TVersionedFlowCoreTarget);

// Essentially a unix timestamp (in seconds).
YT_DEFINE_STRONG_TYPEDEF(TSystemTimestamp, ui64);
YT_DEFINE_STRONG_TYPEDEF(TUniqueSeqNo, ui64);

static constexpr auto InfinitySystemTimestamp = TSystemTimestamp(0xffffffffffffffff);
static constexpr auto ZeroSystemTimestamp = TSystemTimestamp(0);

DECLARE_REFCOUNTED_STRUCT(ITimeProvider);

struct TMessageMeta;
struct TMessage;

DECLARE_REFCOUNTED_STRUCT(TInputMessage);
using TInputMessageConstPtr = TIntrusivePtr<const TInputMessage>;

DECLARE_REFCOUNTED_STRUCT(TOutputMessage);
using TOutputMessageConstPtr = TIntrusivePtr<const TOutputMessage>;

using TTimerMeta = TMessageMeta;
struct TTimer;

DECLARE_REFCOUNTED_STRUCT(TInputTimer);
using TInputTimerConstPtr = TIntrusivePtr<const TInputTimer>;

DECLARE_REFCOUNTED_STRUCT(IInputContext);

DECLARE_REFCOUNTED_STRUCT(TInputVisit);
using TInputVisitConstPtr = TIntrusivePtr<const TInputVisit>;

DECLARE_REFCOUNTED_STRUCT(IOutputCollector);

DECLARE_REFCOUNTED_CLASS(TMessageBatcher);

DEFINE_ENUM(EPartitionState,
    // Fully workable partition, can receive input and distribute output messages.
    ((Executing)       (1))
    // Partition processing is interrupting. It cannot receive input messages, but can distribute output.
    ((Interrupting)    (2))
    // Partition processing has been successfully completed. Cleaning states and other partition data.
    ((Completing)      (3))
    // Partition processing has been successfully completed and fully cleaned. So, it will never start again.
    ((Completed)       (4))
    // Partition processing has been interrupted and will never start again. For example, in case of repartitioning.
    // This state is not persisted, such partitions are removed instantly now.
    ((Interrupted)     (5))
);

DEFINE_ENUM(EBufferManagementStrategy,
    ((Uniform)        (0))
    ((FairShare)      (1))
);

DEFINE_ENUM(EStreamState,
    ((Active)         (0))
    ((Drained)        (1))
    ((Completed)      (2))
);

DEFINE_ENUM(EInflightMerge,
    ((None)  (0))
    ((Sum)   (1))
    ((Max)   (2))
);

DEFINE_ENUM(ETimeType,
    ((EventTime)      (0))
    ((SystemTime)     (1))
    ((CurrentTime)    (2))
);

DEFINE_ENUM(ETimestampFormat,
    ((Seconds)      (0))
    ((MilliSeconds) (1))
    ((Iso8601)      (2))
);

DEFINE_ENUM(EJobFinishReason,
    ((Unknown)               (0))
    ((Failed)                (1))
    ((Rebalanced)            (2))
    ((Stopped)               (3))
    ((LostWorker)            (4))
    ((ExpiredLease)          (5))
    ((PartitionUpdated)      (6))
);

DEFINE_ENUM(EDistributionOrdering,
    // Output messages (created from input messages with the same key) are distributed in their creation order.
    // Ordering survives repartitioning (race between finishing and executing partitions is not possible).
    ((Strict)       (1))
    // Ordering does not survive repartitioning.
    ((Relaxed)      (2))
);

DEFINE_ENUM(EPreloadedResourceState,
    ((Preloading)     (0))
    ((Preloaded)      (1))
);

using TVersionedPipelineState = TVersionedValue<EPipelineState>;
DECLARE_REFCOUNTED_TYPE(TVersionedPipelineState);

DECLARE_REFCOUNTED_STRUCT(TInflightMetrics);
DECLARE_REFCOUNTED_STRUCT(TStreamTraverseData);
DECLARE_REFCOUNTED_STRUCT(TInflightStreamTraverseData);
DECLARE_REFCOUNTED_STRUCT(TNodeTraverseData);
DECLARE_REFCOUNTED_STRUCT(TFromPartitionTraverseData);
DECLARE_REFCOUNTED_STRUCT(TToPartitionTraverseData);
DECLARE_REFCOUNTED_STRUCT(TPipelineTraverseData);

DECLARE_REFCOUNTED_STRUCT(TWatermarks);
DECLARE_REFCOUNTED_STRUCT(TWatermarkState);
using TVersionedWatermarkState = TVersionedValue<TWatermarkStatePtr>;
DECLARE_REFCOUNTED_TYPE(TVersionedWatermarkState);

DECLARE_REFCOUNTED_STRUCT(TPartition);
DECLARE_REFCOUNTED_STRUCT(TJob);
DECLARE_REFCOUNTED_STRUCT(TEpoch);
DECLARE_REFCOUNTED_STRUCT(TAggregatedNodeInputStreamMetrics);
DECLARE_REFCOUNTED_STRUCT(TNodePerformanceMetrics);
DECLARE_REFCOUNTED_STRUCT(TAggregatedNodePerformanceMetrics);
DECLARE_REFCOUNTED_STRUCT(TNodeInputMetrics);
DECLARE_REFCOUNTED_STRUCT(TAggregatedNodeInputMetrics);
DECLARE_REFCOUNTED_STRUCT(TJobStatus);
DECLARE_REFCOUNTED_STRUCT(TMessageDistributorStatus);
DECLARE_REFCOUNTED_STRUCT(TWorkerResourceStatus);
DECLARE_REFCOUNTED_STRUCT(TWorkerStatus);
DECLARE_REFCOUNTED_STRUCT(TWorkerSpec);
DECLARE_REFCOUNTED_STRUCT(TPartitionJobStatus);

DECLARE_REFCOUNTED_STRUCT(TFlowLayout);

DECLARE_REFCOUNTED_STRUCT(TNodeInfo);
DECLARE_REFCOUNTED_STRUCT(TWorker);

DECLARE_REFCOUNTED_STRUCT(TStreamSpecStorageState);
using TVersionedStreamSpecStorageState = TVersionedValue<TStreamSpecStorageStatePtr>;
DECLARE_REFCOUNTED_TYPE(TVersionedStreamSpecStorageState);

DECLARE_REFCOUNTED_STRUCT(TStreamSpec);
DECLARE_REFCOUNTED_STRUCT(TResourceDescription);
DECLARE_REFCOUNTED_STRUCT(TResourceSpec);
DECLARE_REFCOUNTED_STRUCT(TEventTimestampAssignerSpec);
DECLARE_REFCOUNTED_STRUCT(TIdlePartitionsSpec);
DECLARE_REFCOUNTED_STRUCT(TUnavailablePartitionGroupsSpec);
DECLARE_REFCOUNTED_STRUCT(TLateDataPartitionsSpec);
DECLARE_REFCOUNTED_STRUCT(TWatermarkGeneratorSpec);
DECLARE_REFCOUNTED_STRUCT(TWatermarkAlignmentSpec);
DECLARE_REFCOUNTED_STRUCT(TWatermarkPercentileSpec);
DECLARE_REFCOUNTED_STRUCT(TWatermarkStrategySpec);
DECLARE_REFCOUNTED_STRUCT(TSourceSpec);
DECLARE_REFCOUNTED_STRUCT(TSinkSpec);
DECLARE_REFCOUNTED_STRUCT(TTimerSpec);
DECLARE_REFCOUNTED_STRUCT(TKeyVisitorStreamSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicKeyVisitorStreamSpec);
DECLARE_REFCOUNTED_STRUCT(TInputOrderingSpec);

DECLARE_REFCOUNTED_STRUCT(IExternalStateManager);
DECLARE_REFCOUNTED_STRUCT(IExternalStateJoiner);
DECLARE_REFCOUNTED_STRUCT(TExternalStateManagerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicExternalStateManagerContext);
DECLARE_REFCOUNTED_STRUCT(TExternalStateJoinerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicExternalStateJoinerContext);
DECLARE_REFCOUNTED_STRUCT(TExternalStateManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicExternalStateManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TExternalStateJoinerSpec);
DECLARE_REFCOUNTED_STRUCT(TStateJoinSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicExternalStateJoinerSpec);
DECLARE_REFCOUNTED_STRUCT(TStateJoinerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicStateJoinerSpec);

DECLARE_REFCOUNTED_STRUCT(TComputationSpec);
DECLARE_REFCOUNTED_STRUCT(TPipelineSpec);

DECLARE_REFCOUNTED_STRUCT(TExtendedComputationSpec);
DECLARE_REFCOUNTED_STRUCT(TExtendedPipelineSpec);

DECLARE_REFCOUNTED_STRUCT(TDynamicSourceSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicSinkSpec)
DECLARE_REFCOUNTED_STRUCT(TDynamicStateFormatSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicTableRequestSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicStateSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicStateManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicInputStoreSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicTimerStoreSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicOutputStoreSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicRetryableClientSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicRetryableRequestSpec);
DECLARE_REFCOUNTED_STRUCT(TMessageBatcherSettings);
DECLARE_REFCOUNTED_STRUCT(TDynamicComputationSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicPartitionTracerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicResourceSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicThrottlerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobBalancerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobManagerGroupSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicBufferStateManagerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicMessageDistributorSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicJobTrackerSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicControllerConnectorSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicStateCacheSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicPipelineSpec);

using TVersionedPipelineSpec = TVersionedValue<TPipelineSpecPtr>;
DECLARE_REFCOUNTED_TYPE(TVersionedPipelineSpec);
using TVersionedExtendedPipelineSpec = TVersionedValue<TExtendedPipelineSpecPtr>;
DECLARE_REFCOUNTED_TYPE(TVersionedExtendedPipelineSpec);
using TVersionedDynamicPipelineSpec = TVersionedValue<TDynamicPipelineSpecPtr>;
DECLARE_REFCOUNTED_TYPE(TVersionedDynamicPipelineSpec);

using TStreamsTraverse = THashMap<TStreamId, TStreamTraverseDataPtr>;
using TVersionedStreamsTraverse = TVersionedValue<TStreamsTraverse>;
DECLARE_REFCOUNTED_TYPE(TVersionedStreamsTraverse);

DECLARE_REFCOUNTED_STRUCT(TExecutionSpec);
DECLARE_REFCOUNTED_STRUCT(TExecutionSpecVersions);

DECLARE_REFCOUNTED_STRUCT(TJobManagerState);

DECLARE_REFCOUNTED_STRUCT(TFlowFeedback);
DECLARE_REFCOUNTED_STRUCT(TPartitionEphemeralState);
DECLARE_REFCOUNTED_STRUCT(TStreamTraverseDataMetrics);
DECLARE_REFCOUNTED_STRUCT(TMessageTransferingInfo);
DECLARE_REFCOUNTED_STRUCT(TFlowEphemeralState);
DECLARE_REFCOUNTED_STRUCT(TFlowState);
DECLARE_REFCOUNTED_STRUCT(TFlowView);
DECLARE_REFCOUNTED_CLASS(TFlowViewKeeper);
DECLARE_REFCOUNTED_STRUCT(TPipelineImportantVersions);

struct IFactory;

DECLARE_REFCOUNTED_STRUCT(IStateHolder);
DECLARE_REFCOUNTED_STRUCT(IMutableStateProvider);
DECLARE_REFCOUNTED_STRUCT(IMutableStateKeyProvider);
DECLARE_REFCOUNTED_STRUCT(IJoinedStateKeyProvider);
DECLARE_REFCOUNTED_STRUCT(IInitContext);

DECLARE_REFCOUNTED_CLASS(TStreamLimitUsageState);
DECLARE_REFCOUNTED_STRUCT(TComputationContext);
DECLARE_REFCOUNTED_STRUCT(IComputationRunContext);
DECLARE_REFCOUNTED_STRUCT(TComputationOrchidState);
DECLARE_REFCOUNTED_STRUCT(TComputationStatus);
DECLARE_REFCOUNTED_STRUCT(TDynamicComputationContext);
DECLARE_REFCOUNTED_STRUCT(IComputation);

DECLARE_REFCOUNTED_STRUCT(IProcessFunctionBase);
DECLARE_REFCOUNTED_STRUCT(IProcessFunction);
DECLARE_REFCOUNTED_STRUCT(IBatchProcessFunction);
DECLARE_REFCOUNTED_STRUCT(IKeyedBatchProcessFunction);
struct ISyncProcessFunction;
DECLARE_REFCOUNTED_STRUCT(IRuntimeContext);
DECLARE_REFCOUNTED_STRUCT(IRuntimeInitContext);

DECLARE_REFCOUNTED_STRUCT(TSourceContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicSourceContext);
DECLARE_REFCOUNTED_STRUCT(ISource);
DECLARE_REFCOUNTED_STRUCT(TExtendedSourcePartitionStatus);
DECLARE_REFCOUNTED_STRUCT(TSourceControllerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicSourceControllerContext);
DECLARE_REFCOUNTED_STRUCT(ISourceController);
DECLARE_REFCOUNTED_STRUCT(TSinkContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicSinkContext);
DECLARE_REFCOUNTED_STRUCT(ISink);
DECLARE_REFCOUNTED_STRUCT(TSinkControllerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicSinkControllerContext);
DECLARE_REFCOUNTED_STRUCT(ISinkController);

DECLARE_REFCOUNTED_STRUCT(TProcessPartitionTraverseDataResult);
DECLARE_REFCOUNTED_STRUCT(TComputationControllerCommonContext);
DECLARE_REFCOUNTED_STRUCT(TComputationControllerContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicComputationControllerContext);
DECLARE_REFCOUNTED_STRUCT(IComputationController);

DECLARE_REFCOUNTED_STRUCT(TResourceContext);
DECLARE_REFCOUNTED_STRUCT(TDynamicResourceContext);
DECLARE_REFCOUNTED_STRUCT(IResource);
DECLARE_REFCOUNTED_STRUCT(TResourceManagerContext);
DECLARE_REFCOUNTED_STRUCT(IResourceManager);

DECLARE_REFCOUNTED_CLASS(TInflightTracker);
DECLARE_REFCOUNTED_CLASS(TMultiInflightTracker);

DECLARE_REFCOUNTED_CLASS(TPersistedStateTransactionBase);
using TPersistedStateTransactionPtr = TPersistedStateTransactionBasePtr;

struct TPayloadMigrationOptions;
using TPayloadMigrationFunction = std::function<TPayload(const TPayload&, const TPayloadMigrationOptions&)>;

DECLARE_REFCOUNTED_STRUCT(IPayloadConverter);
DECLARE_REFCOUNTED_STRUCT(IPayloadConverterCache);

DECLARE_REFCOUNTED_STRUCT(TQueueControllerState);
DECLARE_REFCOUNTED_STRUCT(TQueueCommonSpec);
DECLARE_REFCOUNTED_CLASS(TQueueControllerCommonImpl);

DECLARE_REFCOUNTED_CLASS(TStreamSpecs);
DECLARE_REFCOUNTED_CLASS(TStreamSpecStorage);
DECLARE_REFCOUNTED_CLASS(TComputationStreamSpecStorage);

DECLARE_REFCOUNTED_STRUCT(IJobDirectory);
DECLARE_REFCOUNTED_CLASS(TJobDirectorySnapshot);

DECLARE_REFCOUNTED_STRUCT(TYsonMessageMeta);
DECLARE_REFCOUNTED_STRUCT(TYsonMessage);

template <class T>
concept CYsonMessage = std::derived_from<T, TYsonMessage>;

DECLARE_REFCOUNTED_CLASS(TStateCache);
DECLARE_REFCOUNTED_CLASS(TJobStateCache);
DECLARE_REFCOUNTED_CLASS(TJobNamedStateCache);

DECLARE_REFCOUNTED_STRUCT(TDynamicExpiringJobNamedStateCacheSpec);
DECLARE_REFCOUNTED_CLASS(TExpiringJobNamedStateCache);

DECLARE_REFCOUNTED_STRUCT(IExternalPerformanceMetricsReporter);

DECLARE_REFCOUNTED_STRUCT(ICommonYTConnector);

DECLARE_REFCOUNTED_STRUCT(IPipelineAuthenticator);
DECLARE_REFCOUNTED_STRUCT(TAuthenticatorConfig);

////////////////////////////////////////////////////////////////////////////////

constinit const auto YoungWorkingJobThreshold = TDuration::Minutes(5);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

#define YT_FLOW_DEFINE_PARAMETERS_IMPL(type, TNewParameters, ...)                                               \
    using T##type##Parameters = TNewParameters;                                                                 \
    using T##type##ParametersPtr = ::NYT::TIntrusivePtr<T##type##Parameters>;                                   \
    T##type##ParametersPtr Get##type##Parameters() const                                                        \
    {                                                                                                           \
        return DynamicPointerCast<T##type##Parameters>(__VA_OPT__(__VA_ARGS__ ::) Get##type##ParametersBase()); \
    }

// Declare parameters in the most base class.

#define YT_FLOW_REGISTER_PARAMETERS(TNewParameters)            \
    YT_FLOW_DEFINE_PARAMETERS_IMPL(, TNewParameters)           \
    virtual TParametersPtr GetParametersBase() const = 0;      \
    virtual void DoubleParametersRegistrationProtector() final \
    { }

#define YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TNewDynamicParameters)      \
    YT_FLOW_DEFINE_PARAMETERS_IMPL(Dynamic, TNewDynamicParameters)      \
    virtual TDynamicParametersPtr GetDynamicParametersBase() const = 0; \
    virtual void DoubleDynamicParametersRegistrationProtector() final   \
    { }

#define YT_FLOW_REGISTER_DYNAMIC_PARTITION_SPEC(TNewDynamicPartitionSpec)                \
    using TDynamicPartitionSpec = TNewDynamicPartitionSpec;                              \
    using TDynamicPartitionSpecPtr = ::NYT::TIntrusivePtr<TNewDynamicPartitionSpec>;     \
    TDynamicPartitionSpecPtr GetDynamicPartitionSpec() const                             \
    {                                                                                    \
        return DynamicPointerCast<TDynamicPartitionSpec>(GetDynamicPartitionSpecBase()); \
    }                                                                                    \
    virtual TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const = 0;            \
    virtual void DoubleDynamicPartitionSpecRegistrationProtector() final                 \
    { }


// Override parameters in derived class.
// Arguments: TNewParameters [, TEntityBaseClass].
// __VA_ARGS__ is either empty or TEntityBaseClass.
// TEntityBaseClass must be explicitly provided if TEntityBaseClass::TParameters cannot be used implicitly
// (for example, if TEntityBaseClass is a template).

#define YT_FLOW_EXTEND_PARAMETERS(TNewParameters, ...)                                                  \
    static_assert(::std::is_base_of_v<__VA_OPT__(typename __VA_ARGS__::) TParameters, TNewParameters>); \
    YT_FLOW_DEFINE_PARAMETERS_IMPL(, TNewParameters __VA_OPT__(, __VA_ARGS__))

#define YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TNewDynamicParameters, ...)                                                  \
    static_assert(::std::is_base_of_v<__VA_OPT__(typename __VA_ARGS__ ::) TDynamicParameters, TNewDynamicParameters>); \
    YT_FLOW_DEFINE_PARAMETERS_IMPL(Dynamic, TNewDynamicParameters __VA_OPT__(, __VA_ARGS__))

#define YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TNewDynamicPartitionSpec)                  \
    static_assert(::std::is_base_of_v<TDynamicPartitionSpec, TNewDynamicPartitionSpec>); \
    using TDynamicPartitionSpec = TNewDynamicPartitionSpec;                              \
    using TDynamicPartitionSpecPtr = ::NYT::TIntrusivePtr<TNewDynamicPartitionSpec>;     \
    TDynamicPartitionSpecPtr GetDynamicPartitionSpec() const                             \
    {                                                                                    \
        return DynamicPointerCast<TDynamicPartitionSpec>(GetDynamicPartitionSpecBase()); \
    }

////////////////////////////////////////////////////////////////////////////////
