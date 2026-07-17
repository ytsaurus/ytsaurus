#pragma once

#include "public.h"

#include "key.h"
#include "persisted_state_control.h"
#include "spec.h"
#include "stream_spec_storage_state.h"
#include "timestamp_statistics.h"
#include "traverse.h"

#include <yt/yt/flow/library/cpp/misc/indexed_yson_string.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/ema_counter.h>
#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/ref.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Persisted state serializer.
//! Supports two key types:
//!   - std::string: encoded/decoded as-is.
//!   - Strong-guid key (e.g. TJobId): encoded via ToString(key.Underlying()), decoded via TKey{TGuid::FromString(...)}.
//! The value is always a shared ptr to a yson struct, serialized via YSON.
template <class TKey, class TValue>
class TCommonSerializer
{
public:
    std::string Encode(const std::optional<TKey>& value) const
    {
        if (!value.has_value()) {
            return "";
        }
        if constexpr (std::is_same_v<TKey, std::string>) {
            return value.value();
        } else {
            return ToString(value.value().Underlying());
        }
    }

    std::string Encode(const std::optional<TValue>& value) const
    {
        return value.has_value() ? NYson::ConvertToYsonString(value.value()).ToString() : "";
    }

    std::optional<TKey> Decode(const std::string& serial, std::type_identity<TKey>) const
    {
        if (serial.empty()) {
            return {std::nullopt};
        }
        if constexpr (std::is_same_v<TKey, std::string>) {
            return {serial};
        } else {
            return {TKey{TGuid::FromString(serial)}};
        }
    }

    std::optional<TValue> Decode(const std::string& serial, std::type_identity<TValue>) const
    {
        if (serial.empty()) {
            return {std::nullopt};
        }
        return {ConvertTo<TValue>(NYson::TYsonStringBuf(serial))};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartition
    : public NYTree::TYsonStruct
{
    // const
    TPartitionId PartitionId;
    TComputationId ComputationId;

    std::optional<TKey> LowerKey;
    std::optional<TKey> UpperKey;
    std::optional<TKey> SourceKey;

    // dynamic
    std::optional<TJobId> CurrentJobId;
    EPartitionState State{};
    i64 StateEpoch{};
    TInstant StateTimestamp;

    bool ContainsKey(const TKey& key) const;
    //! Check the State for states that requires execution (now Executing, Completing, Interrupting).
    bool IsWorking() const;

    REGISTER_YSON_STRUCT(TPartition);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartition);

////////////////////////////////////////////////////////////////////////////////

struct TJob
    : public NYTree::TYsonStruct
{
    TJobId JobId;
    std::string WorkerAddress;
    TIncarnationId WorkerIncarnationId;
    TPartitionId PartitionId;
    TLeaseId LeaseId;

    REGISTER_YSON_STRUCT(TJob);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJob);

////////////////////////////////////////////////////////////////////////////////

struct TNodePerformanceMetrics
    : public NYTree::TYsonStruct
{
    std::optional<double> CpuUsageCurrent;
    std::optional<double> CpuUsage30s;
    std::optional<double> CpuUsage10m;

    i64 MemoryUsageCurrent{};
    i64 MemoryUsage30s{};
    i64 MemoryUsage10m{};

    REGISTER_YSON_STRUCT(TNodePerformanceMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodePerformanceMetrics);

void NodePerformanceMetricsAdd(const TNodePerformanceMetricsPtr& result, const TNodePerformanceMetricsPtr& metric);

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedNodePerformanceMetrics
    : public NYTree::TYsonStruct
{
    TNodePerformanceMetricsPtr Total;
    TNodePerformanceMetricsPtr Avg;
    TNodePerformanceMetricsPtr Max;

    REGISTER_YSON_STRUCT(TAggregatedNodePerformanceMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAggregatedNodePerformanceMetrics);

TAggregatedNodePerformanceMetricsPtr AggregateNodePerformanceMetrics(const std::vector<TNodePerformanceMetricsPtr>& metrics);

////////////////////////////////////////////////////////////////////////////////

// Vector of pairs of key frequency and key itself.
using THeavyHitters = std::vector<std::pair<double, TKey>>;

struct TNodeInputStreamMetrics
    : public NYTree::TYsonStructLite
{
    double MessagesPerSecond{};
    double BytesPerSecond{};
    THeavyHitters HeavyHitters;
    std::vector<TKey> Pivots;

    REGISTER_YSON_STRUCT_LITE(TNodeInputStreamMetrics);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedNodeInputStreamMetrics
    : public NYTree::TYsonStruct
{
    TNodeInputStreamMetrics Total; // Unite heavy hitters.
    TNodeInputStreamMetrics Avg;   // Ignores heavy hitters.
    TNodeInputStreamMetrics Max;   // Ignores heavy hitters.

    REGISTER_YSON_STRUCT(TAggregatedNodeInputStreamMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAggregatedNodeInputStreamMetrics);

TAggregatedNodeInputStreamMetricsPtr AggregateNodeInputStreamMetrics(const std::vector<TNodeInputStreamMetrics>& metrics);

////////////////////////////////////////////////////////////////////////////////

struct TNodeInputMetrics
    : public NYTree::TYsonStruct
{
    TNodeInputStreamMetrics Global;

    THashMap<TStreamId, TNodeInputStreamMetrics> Streams;

    REGISTER_YSON_STRUCT(TNodeInputMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeInputMetrics);

////////////////////////////////////////////////////////////////////////////////

struct TAggregatedNodeInputMetrics
    : public NYTree::TYsonStruct
{
    TAggregatedNodeInputStreamMetricsPtr Global;

    THashMap<TStreamId, TAggregatedNodeInputStreamMetricsPtr> Streams;

    REGISTER_YSON_STRUCT(TAggregatedNodeInputMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAggregatedNodeInputMetrics);

TAggregatedNodeInputMetricsPtr AggregateNodeInputMetrics(const std::vector<TNodeInputMetricsPtr>& metrics);
THashMap<TComputationId, TAggregatedNodeInputMetricsPtr> AggregateInputMetricsByComputation(const TFlowViewPtr& flowView);

////////////////////////////////////////////////////////////////////////////////

struct TJobEntityLimitStatus
    : public NYTree::TYsonStructLite
{
    i64 Limit{};
    i64 Used{};
    std::optional<i64> Pending;

    REGISTER_YSON_STRUCT_LITE(TJobEntityLimitStatus);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TJobStatus
    : public NYTree::TYsonStruct
{
    TJobId JobId;

    bool IsFinished{};
    TError Error;
    // Component -> error.
    THashMap<std::string, TError> RetryableErrors;
    TInstant StartTime;
    std::optional<TInstant> FinishTime;
    TInstant UpdateTime;
    std::optional<TInstant> InitedTime;

    i64 Epoch{};

    TFromPartitionTraverseDataPtr FromPartitionTraverseData;

    TNodePerformanceMetricsPtr PerformanceMetrics;
    TNodeInputMetricsPtr InputMetrics;

    NYTree::IMapNodePtr PartitionStatus;

    THashMap<std::string, double> EpochPartTimes;
    // Limits of input buffer. Source buffers may be added.
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> InputLimits;
    // Limits of output buffer and output store. Sink buffers may be added.
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> OutputLimits;

    REGISTER_YSON_STRUCT(TJobStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobStatus);

////////////////////////////////////////////////////////////////////////////////

struct TMessageDistributorStatus
    : public NYTree::TYsonStruct
{
    THashMap<TStreamId, TTimestampStatistics> StreamTimestampStatistics;

    REGISTER_YSON_STRUCT(TMessageDistributorStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMessageDistributorStatus);

////////////////////////////////////////////////////////////////////////////////

struct TWorkerResourceStatus
    : public NYTree::TYsonStruct
{
    std::optional<double> QueueSize30s;
    std::optional<double> QueueSize10m;
    std::optional<double> QueueGrowthRate30s;
    std::optional<double> QueueGrowthRate10m;
    std::optional<double> QueuePushRate30s;
    std::optional<double> QueuePushRate10m;
    std::optional<double> QueueFetchRate30s;
    std::optional<double> QueueFetchRate10m;

    REGISTER_YSON_STRUCT(TWorkerResourceStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkerResourceStatus);

////////////////////////////////////////////////////////////////////////////////

struct TWorkerStatus
    : public NYTree::TYsonStruct
{
    TError PreviousCrashError;
    THashMap<std::string, TError> Errors;
    TMessageDistributorStatusPtr MessageDistributorStatus;
    THashMap<TResourceId, TWorkerResourceStatusPtr> ResourceStatuses;
    THashMap<TResourceId, EPreloadedResourceState> PreloadedResourceStates;

    REGISTER_YSON_STRUCT(TWorkerStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkerStatus);

////////////////////////////////////////////////////////////////////////////////

class TFlowStateMutationNotifier
    : public TRefCounted
{
public:
    virtual void OnCreatePartition(const TPartitionPtr& newPartition);
    virtual void OnUpdatePartition(const TPartitionPtr& oldPartition, const TPartitionPtr& newPartition);
    virtual void OnRemovePartition(const TPartitionPtr& oldPartition);
    virtual void OnCreateJob(const TJobPtr& newJob);
    virtual void OnUpdateJob(const TJobPtr& oldJob, const TJobPtr& newJob);
    virtual void OnRemoveJob(const TJobPtr& oldJob, EJobFinishReason reason);
};

using TFlowStateMutationNotifierPtr = TIntrusivePtr<TFlowStateMutationNotifier>;

////////////////////////////////////////////////////////////////////////////////

struct TWorkerSpec
    : public NYTree::TYsonStruct
{
    THashSet<TResourceId> PreloadResources;

    REGISTER_YSON_STRUCT(TWorkerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TFlowLayout
    : public NYTree::TYsonStruct
{
public:
    //! General content access.
    TTransactionalPersistedState<TJobId, TJobPtr> Jobs;
    TTransactionalPersistedState<TPartitionId, TPartitionPtr> Partitions;

    //! Per-worker specs (keyed by worker RPC address). Filled by the controller.
    TTransactionalPersistedState<std::string, TWorkerSpecPtr> WorkerSpecs;

    DEFINE_BYVAL_RO_PROPERTY(ssize_t, Updated);

private:
    TPersistedStateControlPtr<std::string> PersistedStateControl_;
    TPersistedStatePtr<TPartitionId, TPartitionPtr> CommittedPartitions_;
    TPersistedStatePtr<TJobId, TJobPtr> CommittedJobs_;
    TPersistedStatePtr<std::string, TWorkerSpecPtr> CommittedWorkerSpecs_;
    TPersistedStateTransactionPtr Txn_;
    //! Transient: a clone captures the source layout's live write transaction here so that
    //! CreateSnapshot(/*committed*/ false) can seed the snapshot with its uncommitted writes.
    TPersistedStateTransactionPtr SourceWriteTransaction_;
    TFlowStateMutationNotifierPtr MutationNotifier_ = nullptr;

public:
    TVersion GetVersion() const;

    void AttachToControl(TPersistedStateControlPtr<std::string> control);
    void SetAsSlave();
    TFlowLayoutPtr Clone() const;
    //! Serializes the layout's "partitions"/"jobs" maps in addition to the struct's own yson output.
    void SerializeAsYson(NYson::IYsonConsumer* consumer);
    void Apply(const std::vector<TPersistedStateStorageRow<std::string>>& rows);
    std::vector<TPersistedStateStorageRow<std::string>> Follow(TSequenceId from) const;

    void StartMutation(TFlowStateMutationNotifierPtr notifier = nullptr);
    void CreateSnapshot(bool committed = true);
    void CommitMutation(TPersistedStateCommitContext* context = nullptr);
    void CreatePartition(TPartitionPtr partition);
    void UpdatePartition(TPartitionPtr partition);
    void UpdatePartition(TPartitionId id, EPartitionState state, i64 stateEpoch, TInstant stateTimestamp);
    void RemovePartition(const TPartitionId& id);
    void CreateJob(TJobPtr job);
    void UpdateJob(TJobPtr job);
    void UpdateJob(TJobId id, TLeaseId leaseId);
    void RemoveJob(const TJobId& id, EJobFinishReason jobFinishReason);
    void Commit();

    REGISTER_YSON_STRUCT(TFlowLayout);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowLayout);

////////////////////////////////////////////////////////////////////////////////

struct TWorker
    : public TNodeInfo
{
    std::string LegacyAddress; // TODO: Remove after 2026-08-01.
    //! Worker groups provided by worker.
    std::vector<TWorkerGroupId> Groups;
    //! A map of capability->value pairs that the worker is able to provide (provided by worker).
    //! Note that zero value and absence of the key are cases with different meaning.
    //! A requirement with some key and zero value meet the first case while does not meet the second.
    THashMap<std::string, ssize_t> Capabilities;
    //! Instant of handshake with the worker.
    TInstant RegisterTime;

    REGISTER_YSON_STRUCT(TWorker);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorker);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarks
    : public NYTree::TYsonStruct
{
    TSystemTimestamp SystemWatermark;
    TSystemTimestamp EventWatermark;

    REGISTER_YSON_STRUCT(TWatermarks);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarks);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkState
    : public NYTree::TYsonStruct
{
    TSystemTimestamp CurrentTimestamp;
    THashMap<TStreamId, TWatermarksPtr> Streams;
    THashMap<TWatermarkAlignmentGroup, TWatermarksPtr> AlignmentGroups;

    TSystemTimestamp GetCurrentTimestamp() const;
    TSystemTimestamp GetEventWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetSystemWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetWatermark(const TStreamId& streamId, ETimeType timeType) const;

    TSystemTimestamp GetAlignmentEventWatermark(const TWatermarkAlignmentGroup& groupId) const;
    TSystemTimestamp GetAlignmentSystemWatermark(const TWatermarkAlignmentGroup& groupId) const;

    REGISTER_YSON_STRUCT(TWatermarkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkState);
DEFINE_REFCOUNTED_TYPE(TVersionedWatermarkState);

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TVersionedPipelineState);
DEFINE_REFCOUNTED_TYPE(TVersionedStreamsTraverse);
DEFINE_REFCOUNTED_TYPE(TVersionedFlowCoreTarget);

struct TExecutionSpec
    : public NYTree::TYsonStruct
{
    TVersionedPipelineStatePtr PipelineState;

    TVersionedPipelineSpecPtr PipelineSpec; // It is not ground of truth, it is just replica.
    TVersionedExtendedPipelineSpecPtr ExtendedPipelineSpec;

    TVersionedDynamicPipelineSpecPtr DynamicPipelineSpec; // It is not ground of truth, it is just replica.

    TFlowLayoutPtr Layout;

    //! Flow core binary target explicitly set by user.
    TVersionedFlowCoreTargetPtr FlowCoreTarget;

    i64 GetEpoch() const;

    TVersionedStreamSpecStorageStatePtr StreamSpecStorageState;

    TVersionedStreamsTraversePtr InputStreamsTraverse;
    TVersionedWatermarkStatePtr WatermarkState;

    void AttachToControl(TPersistedStateControlPtr<std::string> control);
    void SetAsSlave();
    TExecutionSpecPtr Clone() const;
    void SerializeAsYson(NYson::IYsonConsumer* consumer) const;

    void StartMutation(TFlowStateMutationNotifierPtr notifier = nullptr);
    void CreateSnapshot(bool committed = true);
    void CommitMutation(TPersistedStateCommitContext* context = nullptr);

    REGISTER_YSON_STRUCT(TExecutionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutionSpec);

bool CheckFlowCoreTarget(const TFlowCoreTarget& flowCoreTarget, const std::string& actualFlowCoreVersion);
bool CheckFlowCoreTarget(const TFlowViewPtr& flowView, const std::string& actualFlowCoreVersion);

////////////////////////////////////////////////////////////////////////////////

struct TExecutionSpecVersions
    : public NYTree::TYsonStruct
{
    i64 GetEpoch() const;

    TVersion PipelineStateVersion;

    TVersion PipelineSpecVersion;
    TVersion ExtendedPipelineSpecVersion;
    TVersion DynamicPipelineSpecVersion;

    TVersion LayoutVersion;

    TVersion StreamSpecStorageStateVersion;

    TVersion InputStreamsTraverseVersion;
    TVersion WatermarkStateVersion;

    REGISTER_YSON_STRUCT(TExecutionSpecVersions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutionSpecVersions);

TExecutionSpecVersionsPtr BuildExecutionSpecVersions(const TExecutionSpecPtr& executionSpec);

std::pair<TExecutionSpecPtr, std::vector<TPersistedStateStorageRow<std::string>>> PrepareExecutionSpecUpdate(const TExecutionSpecPtr& current, const TExecutionSpecVersionsPtr& versions);

TExecutionSpecPtr ApplyExecutionSpecUpdate(const TExecutionSpecPtr& current, const TExecutionSpecPtr& update, const std::vector<TPersistedStateStorageRow<std::string>>& stateUpdate);

////////////////////////////////////////////////////////////////////////////////

struct TJobManagerState
    : public NYTree::TYsonStruct
{
    THashMap<TComputationId, THashMap<std::string, NYson::TYsonString>> Computations;

    REGISTER_YSON_STRUCT(TJobManagerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobManagerState);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionJobStatusBase
{
    TFromPartitionTraverseDataPtr LastTraverseData;
    NYTree::IMapNodePtr LastPartitionStatus;
    std::optional<TJobId> CurrentJobId;
    TInstant CurrentJobStatusUpdateTime; // max(statusInstanceCreateTime, actualStatusReceivingTime).
    TJobStatusPtr CurrentJobStatus;      // Can be nullptr.
    TInstant LastRetryableErrorInstant;
};

struct TPartitionJobStatus
    : public NYTree::TYsonStruct
    , public TPartitionJobStatusBase
{
    REGISTER_YSON_STRUCT(TPartitionJobStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionJobStatus);

////////////////////////////////////////////////////////////////////////////////

struct TFlowFeedback
    : public NYTree::TYsonStruct
{
    THashMap<TPartitionId, TPartitionJobStatusPtr> PartitionJobStatuses;
    THashMap<std::string, TWorkerStatusPtr, THash<TStringBuf>, TEqualTo<TStringBuf>> WorkerStatuses;
    TInstant UpdateTime;

    //! Returns the current job status for the given partition, or nullptr if the
    //! partition has no status snapshot or its CurrentJobStatus is not set.
    const TJobStatusPtr& GetCurrentJobStatus(const TPartitionId& partitionId) const;

    REGISTER_YSON_STRUCT(TFlowFeedback);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowFeedback);

////////////////////////////////////////////////////////////////////////////////

//! The dynamic per-partition spec delivered to the job with every heartbeat:
//! generic job-control fields owned by the controller's job manager plus opaque
//! computation-type-specific parameters owned by the computation controller
//! (parsed on the job via YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC).
struct TDynamicPartitionSpec
    : public NYTree::TYsonStruct
{
    //! Finish the job after its current epoch (graceful rebalance): set by the
    //! job manager, cleared by it when the job is recreated on the target worker.
    bool FinishAfterCurrentEpoch = false;

    //! Computation-type-specific parameters. Null until the computation
    //! controller sets them; the spec is not delivered to the worker (and the
    //! job is not started) until then — in particular, a source partition's job
    //! never starts without its ActiveSource.
    NYTree::IMapNodePtr ComputationPartitionSpec;

    REGISTER_YSON_STRUCT(TDynamicPartitionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicPartitionSpec);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionEphemeralState
    : public NYTree::TYsonStruct
{
    TInstant LastOkTime;
    EJobFinishReason PreviousJobFinishReason{};

    TInstant PreviousJobFailInstant;
    TError PreviousJobFailError;

    TInstant PreviousRebalancingInstant;

    TDynamicPartitionSpecPtr DynamicPartitionSpec;

    //! If set, the partition is being gracefully migrated to this worker address.
    //! The current job will finish after its current epoch, then a new job will be
    //! created on this worker. Stored on the controller side only.
    std::optional<std::string> PendingGracefulRebalanceWorkerAddress;

    REGISTER_YSON_STRUCT(TPartitionEphemeralState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPartitionEphemeralState);

////////////////////////////////////////////////////////////////////////////////

struct TStreamTraverseDataMetrics
    : public NYTree::TYsonStruct
{
    double SystemWatermarkMinMaxDifference{};
    double EventWatermarkMinMaxDifference{};

    REGISTER_YSON_STRUCT(TStreamTraverseDataMetrics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStreamTraverseDataMetrics);

////////////////////////////////////////////////////////////////////////////////

struct TStreamSpeedStatistics
    : public NYTree::TYsonStructLite
{
    double ProcessedMessagesPerSecond{};
    double ProcessedBytesPerSecond{};

    REGISTER_YSON_STRUCT_LITE(TStreamSpeedStatistics);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPipelineSpeedStatistics
    : public NYTree::TYsonStructLite
{
    THashMap<TStreamId, TStreamSpeedStatistics> StreamSpeed1d; // EMA speeds with half-decay period of 1 day.
    TSystemTimestamp LastUpdated;                              // Timestamp of the last EMA update. Zero means never updated.

    REGISTER_YSON_STRUCT_LITE(TPipelineSpeedStatistics);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TMessageTransferingInfo
    : public virtual NYTree::TYsonStruct
{
    THashMap<TStreamId, TTimestampStatistics> StreamTimestampStatistics;

    TPipelineSpeedStatistics SpeedStatistics;

    REGISTER_YSON_STRUCT(TMessageTransferingInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMessageTransferingInfo);

////////////////////////////////////////////////////////////////////////////////

struct TFlowEphemeralState
    : public NYTree::TYsonStruct
{
    THashMap<TPartitionId, TPartitionEphemeralStatePtr> Partitions;
    THashMap<TComputationId, THashMap<TStreamId, TStreamTraverseDataMetricsPtr>> StreamTraverseDataMetrics;
    THashMap<TWorkerGroupId, TSequenceId> MaxAppliedBalancerSequenceIds;
    THashMap<TIncarnationId, THashSet<TJobId>> WorkerIncarnationsJobs;
    TMessageTransferingInfoPtr MessageTransferingInfo;
    NYPath::TRichYPath PipelinePath;
    THashSet<TComputationId> TraverseUncoveredComputations;

    //! Workers excluded from scheduling due to FlowCoreTarget mismatch, grouped by their FlowCoreVersion.
    struct TFlowCoreTargetMismatchedVersionGroup
    {
        std::string ExampleAddress;
        ui64 Count = 0;
    };

    THashMap<std::string, TFlowCoreTargetMismatchedVersionGroup> FlowCoreTargetMismatchedWorkers;

    const TPartitionEphemeralStatePtr& GetPartitionState(const TPartitionId& partitionId);

    REGISTER_YSON_STRUCT(TFlowEphemeralState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowEphemeralState);

////////////////////////////////////////////////////////////////////////////////

struct TFlowState
    : public NYTree::TYsonStruct
{
    TSystemTimestamp CurrentTimestamp;
    // To be synced with workers.
    TExecutionSpecPtr ExecutionSpec;

    // materialized ExecutionSpec->GetEpoch()
    // for debug reasons
    i64 Epoch{};

    THashMap<std::string, TWorkerPtr> Workers;

    // Aggregated TraverseData
    TPipelineTraverseDataPtr TraverseData;

    TJobManagerStatePtr JobManagerState;

    // Used to warm up buffer demand on pipeline restart. Stored here to survive controller restarts.
    TPipelineSpeedStatistics SpeedStatistics;

    void AttachToControl(TPersistedStateControlPtr<std::string> control);
    void SetAsSlave();
    TFlowStatePtr Clone() const;
    void SerializeAsYson(NYson::IYsonConsumer* consumer) const;

    void StartMutation(TFlowStateMutationNotifierPtr notifier = nullptr);
    void CreateSnapshot(bool committed = true);
    void CommitMutation(TPersistedStateCommitContext* context = nullptr);

    REGISTER_YSON_STRUCT(TFlowState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowState);

////////////////////////////////////////////////////////////////////////////////

struct TFlowViewBase
{
    TFlowFeedbackPtr Feedback;             // Not persisted, filled from heartbeats.
    TFlowEphemeralStatePtr EphemeralState; // Not persisted, filled from DoIteration.
    TFlowStatePtr State;                   // Persisted, filled from DoIteration.
    TVersionedPipelineSpecPtr CurrentSpec;
    TVersionedDynamicPipelineSpecPtr CurrentDynamicSpec;

    bool IsSynced() const
    {
        return CurrentSpec->GetVersion() == State->ExecutionSpec->PipelineSpec->GetVersion() &&
            CurrentDynamicSpec->GetVersion() == State->ExecutionSpec->DynamicPipelineSpec->GetVersion();
    }

    void EnsureIsSynced() const
    {
        THROW_ERROR_EXCEPTION_IF(!IsSynced(), "FlowView is not synced");
    }
};

struct TFlowView
    : public NYTree::TYsonStruct
    , public TFlowViewBase
{
    void SerializeAsYson(NYson::IYsonConsumer* consumer) const;
    NYson::TYsonString SerializeAsYsonString() const;

    //! Deserializes a flow view from the serialized node and rebuilds its queryable layout
    //! (Partitions/Jobs) in one call, so consumers (e.g. describe) can resolve a partition or job by id.
    //!
    //! CAVEAT: the layout is restored from the node, not from real persisted state. The resulting flow
    //! view has no persisted-state backing and MUST NOT be used to save or persist back to YT.
    static TFlowViewPtr LoadFromNode(const NYTree::INodePtr& node, TPersistedStateControlPtr<std::string> control);

    TFlowViewPtr CopyPtr() const;

    REGISTER_YSON_STRUCT(TFlowView);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFlowView);

////////////////////////////////////////////////////////////////////////////////

//! A pre-compressed full flow view plus the codec used, so callers can decompress.
struct TCompressedFlowView
{
    TSharedRef Data;
    NCompression::ECodec Codec{};
};

class TFlowViewKeeper
    : public TRefCounted
{
public:
    TFlowViewKeeper();
    ~TFlowViewKeeper() override;

    void Init(TFlowStatePtr state, TFlowEphemeralStatePtr ephemeralState, TVersionedPipelineSpecPtr spec, TVersionedDynamicPipelineSpecPtr dynamicSpec);
    void Reset();

    // Returns true if the |feedback| was installed; false if dropped because the current static spec
    // version no longer matches |expectedSpecVersion| (the version captured at snapshot time, against
    // which the |feedback| was computed).
    bool SetFeedback(TFlowFeedbackPtr feedback, TVersion expectedSpecVersion);
    void SetStates(TFlowStatePtr state, TFlowEphemeralStatePtr ephemeralState);
    void SetSpecs(
        std::optional<TVersionedPipelineSpecPtr> spec,
        std::optional<TVersionedDynamicPipelineSpecPtr> dynamicSpec);
    void SetFlowCoreTarget(TVersionedFlowCoreTargetPtr flowCoreTarget);

    TFlowViewPtr GetFlowView() const;
    NYson::TYsonString GetYsonString(bool cache) const;
    //! Returns the subtree of the flow view at |path| (served from the by-path index for the cached view).
    NYson::TYsonString GetYsonStringByPath(const NYPath::TYPath& path, bool cache) const;
    //! Returns the cached compressed full flow view (rebuilt by RebuildNodeCache) and its codec.
    TCompressedFlowView GetCompressedYsonString() const;
    //! Compresses arbitrary flow view YSON (e.g. a sub-path result) with the current dynamic-spec codec.
    TCompressedFlowView CompressYson(const NYson::TYsonString& yson) const;
    //! |invoker| is a pool invoker used to build the by-path index in parallel with the compression.
    void RebuildNodeCache(const IInvokerPtr& invoker);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TFlowViewPtr FlowView_ = nullptr;
    NYson::TYsonString CachedYsonString_;
    TIndexedYsonStringPtr CachedYsonIndex_;
    TSharedRef CachedCompressedFlowView_;
    NCompression::ECodec CachedCompressionCodec_{};

    void EnsureInit(TGuard<NThreading::TSpinLock>& guard) const;
};

DEFINE_REFCOUNTED_TYPE(TFlowViewKeeper);

////////////////////////////////////////////////////////////////////////////////

struct TPipelineImportantVersions
    : public NYTree::TYsonStruct
{
    TVersion PipelineSpecVersion;
    TVersion PipelineStateVersion;
    TVersion FlowCoreTargetVersion;

    //! Throws if any version in |this| (the expected) differs from |actual|.
    void EnsureEqual(const TPipelineImportantVersions& actual) const;

    REGISTER_YSON_STRUCT(TPipelineImportantVersions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipelineImportantVersions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
