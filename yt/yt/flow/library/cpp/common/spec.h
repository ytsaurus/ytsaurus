#pragma once

#include "public.h"
#include "yt_path_option.h"

#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/rpc/public.h>

#include <yt/yt/flow/lib/client/public.h>
#include <yt/yt/flow/lib/serializer/state.h>

#include <yt/yt/core/ytree/size.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TStreamId MakeGlobalStreamId(const TComputationId& computationId, const TStreamId& localStreamId, const TComputationSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

struct TStreamSpec
    : public NYTree::TYsonStruct
{
    std::optional<std::string> ClassName;
    NTableClient::TTableSchemaPtr Schema;
    std::string MigrationFunction;

    REGISTER_YSON_STRUCT(TStreamSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStreamSpec);

bool operator==(const TStreamSpec& lhs, const TStreamSpec& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TResourceDescription
    : public NYTree::TYsonStruct
{
    //! Alias is used to distinguish different resources for computations
    //! with the same class. This way computation implementation can refer its
    //! required resources by predefined name.
    std::optional<TResourceId> Alias;
    bool Worker{};
    bool Controller{};

    REGISTER_YSON_STRUCT(TResourceDescription);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceDescription);

////////////////////////////////////////////////////////////////////////////////

struct TResourceSpec
    : public NYTree::TYsonStruct
{
    std::string ResourceClassName;
    NYTree::IMapNodePtr Parameters;
    THashMap<TResourceId, TResourceDescriptionPtr> Dependencies;
    THashMap<std::string, ssize_t> RequiredCapabilities;
    bool PreloadRequired{};
    //! When true, the resource is deployed on every unit regardless of which jobs are present,
    //! and is never deallocated. Mutually exclusive with preload_required. Intended for node-wide singletons,
    //! e.g. a resource exposing /reload and /ping for REX.
    bool AlwaysOn{};

    REGISTER_YSON_STRUCT(TResourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceSpec);

////////////////////////////////////////////////////////////////////////////////

struct TEventTimestampAssignerSpec
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Column;
    ETimestampFormat Format{};
    bool LimitBySystemTimestamp{};

    REGISTER_YSON_STRUCT(TEventTimestampAssignerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventTimestampAssignerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TIdlePartitionsSpec
    : public NYTree::TYsonStruct
{
    TDuration Duration;
    double MaxRatio{};

    REGISTER_YSON_STRUCT(TIdlePartitionsSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIdlePartitionsSpec);

////////////////////////////////////////////////////////////////////////////////

struct TUnavailablePartitionGroupsSpec
    : public NYTree::TYsonStruct
{
    // Advance watermark when number of unavailable availability groups is <= MaxGroups.
    int MaxGroups{};

    // If one availability group is fully unavailable but some partitions of group have messages,
    // ignore these partitions and advance watermark.
    // TODO: add some threshold here.
    bool IgnoreNotIdlePartitions{};

    REGISTER_YSON_STRUCT(TUnavailablePartitionGroupsSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUnavailablePartitionGroupsSpec);

////////////////////////////////////////////////////////////////////////////////

struct TLateDataPartitionsSpec
    : public virtual NYTree::TYsonStruct
{
    TWatermarkPercentile Value;
    TDuration Delay;

    REGISTER_YSON_STRUCT(TLateDataPartitionsSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLateDataPartitionsSpec);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkGeneratorSpec
    : public NYTree::TYsonStruct
{
    bool UseSourceWatermark{};
    TDuration OutOfOrdernessBound;
    TIdlePartitionsSpecPtr IdlePartitions;
    TUnavailablePartitionGroupsSpecPtr UnavailablePartitionGroups;
    TLateDataPartitionsSpecPtr LateDataPartitions;

    REGISTER_YSON_STRUCT(TWatermarkGeneratorSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkGeneratorSpec);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkAlignmentSpec
    : public NYTree::TYsonStruct
{
    TWatermarkAlignmentGroup GroupName;
    TDuration DriftBound;
    std::optional<THashMap<TStreamId, TDuration>> ReadDelays;

    REGISTER_YSON_STRUCT(TWatermarkAlignmentSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkAlignmentSpec);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkPercentileSpec
    : public NYTree::TYsonStruct
{
    TWatermarkPercentile Value;
    TDuration Delay;

    REGISTER_YSON_STRUCT(TWatermarkPercentileSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkPercentileSpec);

////////////////////////////////////////////////////////////////////////////////

struct TWatermarkStrategySpec
    : public NYTree::TYsonStruct
{
    // Would applied to all OutputStreams.
    // Would not override already set EventTimestamps.
    TEventTimestampAssignerSpecPtr EventTimestampAssigner;
    TWatermarkGeneratorSpecPtr WatermarkGenerator;
    TWatermarkAlignmentSpecPtr WatermarkAlignment;
    TWatermarkPercentileSpecPtr WatermarkPercentile;

    REGISTER_YSON_STRUCT(TWatermarkStrategySpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatermarkStrategySpec);

////////////////////////////////////////////////////////////////////////////////

struct TSourceSpec
    : public NYTree::TYsonStruct
{
    std::string SourceClassName;

    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TSourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSourceSpec);

////////////////////////////////////////////////////////////////////////////////

struct TSinkSpec
    : public NYTree::TYsonStruct
{
    std::string SinkClassName;

    THashSet<TStreamId> InputStreamIds;

    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TSinkSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSinkSpec);

////////////////////////////////////////////////////////////////////////////////

struct TTimerSpec
    : public NYTree::TYsonStruct
{
    ETimeType TimeType{};
    // By default - timer would use all InputStreams of Computation.
    // But it could be overriden by specifically setting streams or "streams_with_delays"
    std::optional<THashSet<TStreamId>> Streams;
    std::optional<THashMap<TStreamId, TDuration>> StreamsWithDelays;
    bool DeduplicateEqualTimestamps{};

    REGISTER_YSON_STRUCT(TTimerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TKeyVisitorStreamSpec
    : public NYTree::TYsonStruct
{
    // Internal state names to visit. If both Names and ExternalNames are unset, the visitor
    // sweeps all internal names and all external state managers; otherwise only the listed ones.
    std::optional<THashSet<std::string>> Names;
    // External-state-manager and joiner names to visit. A joiner is swept only when explicitly
    // listed here; the unset scan-all mode never sweeps joiners.
    // A visitor-driven joiner may be listed in at most one key-visitor stream.
    std::optional<THashSet<std::string>> ExternalNames;

    // Static — changing it invalidates persisted coverage intervals.
    int BucketCount{};

    REGISTER_YSON_STRUCT(TKeyVisitorStreamSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyVisitorStreamSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicKeyVisitorStreamSpec
    : public NYTree::TYsonStruct
{
    //! Best-effort target wall-clock duration of one full sweep over the
    //! partition's hash range — not a hard SLA. The throttler is shaped to
    //! cover the range in `Period`; under throttler pressure (slow KeyStates
    //! reads, congested backend) the achieved period grows and the sweep
    //! drifts behind schedule. The drift is observable on the visit-stream:
    //! every emitted visit carries `EventTimestamp = now − scheduleLag`, so
    //! the watermark gap on the downstream consumer equals the staleness
    //! of the visit relative to "when it was supposed to be visited".
    TDuration Period;

    //! Maximum number of visits buffered between the background fill task
    //! and the consumer, in rows.
    NYTree::TSize BufferRowLimit;

    NYTree::TSize MaxScanRowsPerIteration;

    //! Schedule-lag threshold above which the visitor switches into catch-up
    //! mode: each next read scans a slice wider than the throttler granted
    //! by `CatchupSpeedupMultiplier`, so the accumulated lag drains back to
    //! zero. Useful after restarts when the controller boot-up time piles
    //! into the visit-stream's EventTimestamp.
    TDuration CatchupLagThreshold;

    //! Throughput multiplier applied on top of the base throttler rate while
    //! in catch-up mode. Must be strictly greater than 1.0.
    double CatchupSpeedupMultiplier{};

    REGISTER_YSON_STRUCT(TDynamicKeyVisitorStreamSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicKeyVisitorStreamSpec);

////////////////////////////////////////////////////////////////////////////////

struct THeavyHittersSpec
    : public virtual NYTree::TYsonStructLite
{
    TDuration Window;
    double Threshold{};
    i64 Limit{};

    REGISTER_YSON_STRUCT_LITE(THeavyHittersSpec);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPivotFinderSpec
    : public virtual NYTree::TYsonStructLite
{
    ssize_t PartCount{};
    ssize_t WindowSize{};

    REGISTER_YSON_STRUCT_LITE(TPivotFinderSpec);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TInputOrderingSpec
    : public NYTree::TYsonStruct
{
    ETimeType TimeType{};
    THashMap<TStreamId, TDuration> StreamDelays;

    REGISTER_YSON_STRUCT(TInputOrderingSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInputOrderingSpec);

////////////////////////////////////////////////////////////////////////////////

//! Spec entry for an external state manager bound to a name inside a computation.
//! ``ExternalStateManagerClassName`` is ``TypeName<TManager>()`` looked up in
//! ``TRegistry``; ``Parameters`` is the manager-specific YSON parameters.
struct TExternalStateManagerSpec
    : public NYTree::TYsonStruct
{
    std::string ExternalStateManagerClassName;
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TExternalStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExternalStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicExternalStateManagerSpec
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TDynamicExternalStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicExternalStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

//! Describes how the joiner is joined with the computation's streams.
struct TStateJoinSpec
    : public NYTree::TYsonStruct
{
    //! When set, the joiner's lookup key is re-extracted from each input message under this
    //! schema (instead of using the computation's group-by key as-is).
    NTableClient::TTableSchemaPtr KeySchemaOverride;
    //! Streams whose messages and timers provide keys for this joiner. ``nullopt`` means
    //! "every input/timer/source stream of the computation".
    std::optional<THashSet<TStreamId>> KeyProviderStreams;

    REGISTER_YSON_STRUCT(TStateJoinSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStateJoinSpec);

////////////////////////////////////////////////////////////////////////////////

//! Read-only counterpart of #TExternalStateManagerSpec.
struct TExternalStateJoinerSpec
    : public NYTree::TYsonStruct
{
    std::string ExternalStateJoinerClassName;
    //! When set, the framework looks up this static resource (must implement
    //! #IYTClientProvider) and uses its #Get() as the joiner's YT client. The joiner's
    //! ``GetClient(cluster)`` ignores the ``cluster`` argument when this is set.
    std::optional<TResourceId> ClientProviderResourceId;
    //! When set, the framework looks up this static resource (must implement
    //! #IYTClientProvider) and uses ``GetClient(cluster)`` against it. Mutually exclusive with
    //! ``ClientProviderResourceId`` (the latter takes precedence).
    std::optional<TResourceId> ClientFactoryResourceId;
    TStateJoinSpecPtr JoinOn;
    //! When ``true`` (default), framework auto-preloads keys before each #DoProcess().
    //! When ``false``, the computation must call #PreloadKeyStates() itself.
    bool AutoPreload{};
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TExternalStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExternalStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicExternalStateJoinerSpec
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TDynamicExternalStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicExternalStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

//! Read-only join over the internal per-key state of another computation. Unlike
//! ``TExternalStateJoinerSpec`` (which reads an arbitrary YT table through a user-registered
//! class), this reads the foreign computation's framework-managed state table directly, so it
//! carries no ``class_name``/``parameters`` — only the target and the join key.
struct TStateJoinerSpec
    : public NYTree::TYsonStruct
{
    //! Computation whose internal state is read.
    TComputationId ComputationId;
    //! State-client name within the target computation — the prefix it passed to ``InitClient``
    //! (must start with ``/``).
    std::string StateName;
    TStateJoinSpecPtr JoinOn;
    //! When ``true`` (default), framework auto-preloads keys before each ``DoProcess``.
    //! When ``false``, the computation must call ``PreloadKeyStates`` itself.
    bool AutoPreload{};

    REGISTER_YSON_STRUCT(TStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStateJoinerSpec
    : public NYTree::TYsonStruct
{
    //! TTL cache of loaded states. Disabled by default (``ttl = 0``): each epoch reloads at
    //! ``SyncLastCommittedTimestamp``.
    TDynamicExpiringJobNamedStateCacheSpecPtr Cache;

    REGISTER_YSON_STRUCT(TDynamicStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TComputationSpec
    : public NYTree::TYsonStruct
{
    static constexpr bool ExperimentalEnableNonUintKeyDefault = false;

    std::string ComputationClassName;

    //! For the generic process-function adapters (computation_class_name selects the adapter):
    //! the registered name of the process function to host and its static parameters block.
    //! Ignored by computations that are not process-function adapters.
    std::optional<std::string> ProcessingFunction;
    NYTree::IMapNodePtr ProcessingFunctionParameters;

    NTableClient::TTableSchemaPtr GroupBySchema;
    std::optional<bool> ExperimentalEnableNonUintKey;
    THashSet<TStreamId> InputStreamIds;
    THashSet<TStreamId> OutputStreamIds;

    THashMap<TStreamId, THashSet<TStreamId>> StreamsDependency;

    TWatermarkStrategySpecPtr WatermarkStrategy;

    THashMap<TResourceId, TResourceDescriptionPtr> RequiredResourceIds;

    NYTree::IMapNodePtr Parameters;

    THashMap<TStreamId, TTimerSpecPtr> TimerStreams;
    THashMap<TStreamId, TKeyVisitorStreamSpecPtr> KeyVisitorStreams;
    THashMap<TStreamId, TSourceSpecPtr> SourceStreams;
    THashMap<TSinkId, TSinkSpecPtr> Sinks;

    //! External state managers/joiners bound to user-facing client names. Look up the
    //! ``ClassName`` of each entry in ``TRegistry`` to obtain the implementation.
    THashMap<std::string, TExternalStateManagerSpecPtr> ExternalStateManagers;
    THashMap<std::string, TExternalStateJoinerSpecPtr> ExternalStateJoiners;

    //! Read-only joiners over other computations' internal state, bound to client names.
    THashMap<std::string, TStateJoinerSpecPtr> StateJoiners;

    THeavyHittersSpec HeavyHitters;
    TPivotFinderSpec PivotFinder;

    TInputOrderingSpecPtr InputOrdering;

    EDistributionOrdering DistributionOrdering{};

    TWorkerGroupId WorkerGroup;

    bool AllowTimerSelfDependency{};
    std::optional<bool> UseCompactInputMessages;

    REGISTER_YSON_STRUCT(TComputationSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComputationSpec);

////////////////////////////////////////////////////////////////////////////////

THashSet<TStreamId> ComputeAllowedInputStreams(
    const THashSet<TStreamId>& allowedOutputStreams,
    const TComputationSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

//! Resolves the effective ``use_compact_input_messages`` decision for a computation.
//! An explicit spec value wins; otherwise compact deduplication is enabled whenever
//! ``experimental_enable_non_uint_key`` is not set, i.e. partitioning is exclusively by
//! the required uint64 hash column.
bool ResolveUseCompactInputMessages(const TComputationSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

struct TPipelineSpec
    : public NYTree::TYsonStruct
{
    bool ValidateBinaryVersion{};
    std::string BinaryVersion;
    bool ValidateBinaryChecksum{};
    std::string BinaryChecksum;

    THashMap<TComputationId, TComputationSpecPtr> Computations;
    THashMap<TResourceId, TResourceSpecPtr> Resources;
    THashMap<TStreamId, TStreamSpecPtr> Streams;

    REGISTER_YSON_STRUCT(TPipelineSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipelineSpec);

////////////////////////////////////////////////////////////////////////////////

struct TExtendedComputationSpec
    : public NYTree::TYsonStruct
{
    THashSet<TStreamId> AllStreamIds;
    THashMap<TStreamId, THashSet<TComputationId>> Subscribers;

    REGISTER_YSON_STRUCT(TExtendedComputationSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExtendedComputationSpec);

////////////////////////////////////////////////////////////////////////////////

struct TExtendedPipelineSpec
    : public NYTree::TYsonStruct
{
    THashMap<TComputationId, TExtendedComputationSpecPtr> Computations;

    REGISTER_YSON_STRUCT(TExtendedPipelineSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExtendedPipelineSpec);

////////////////////////////////////////////////////////////////////////////////

TExtendedPipelineSpecPtr BuildExtendedPipelineSpec(const TPipelineSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSourceSpec
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr Parameters;
    bool Draining{};

    REGISTER_YSON_STRUCT(TDynamicSourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSourceSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSinkSpec
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TDynamicSinkSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSinkSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStateFormatSpec
    : public NYsonSerializer::TFormat
{
    bool Compress{};
    double RecodeProbability{};

    REGISTER_YSON_STRUCT(TDynamicStateFormatSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStateFormatSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableRequestSpec
    : public NYTree::TYsonStruct
{
    NYTree::TSize SelectMinLimit;
    i64 SelectLimitMultiplier{};
    NYTree::TSize SelectMaxLimit;

    REGISTER_YSON_STRUCT(TDynamicTableRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableRequestSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStateSpec
    : public NYTree::TYsonStruct
{
    TDynamicStateFormatSpecPtr Format;
    TDynamicTableRequestSpecPtr TableRequest;

    REGISTER_YSON_STRUCT(TDynamicStateSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStateSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStateManagerSpec
    : public TDynamicStateSpec
{
    THashMap<std::string, TDynamicStateFormatSpecPtr> FormatOverrides;

    REGISTER_YSON_STRUCT(TDynamicStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicInputStoreSpec
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TDynamicInputStoreSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicInputStoreSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTimerStoreSpec
    : public NYTree::TYsonStruct
{
    TDynamicTableRequestSpecPtr TableRequest;

    REGISTER_YSON_STRUCT(TDynamicTimerStoreSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTimerStoreSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicOutputStoreSpec
    : public NYTree::TYsonStruct
{
    TDynamicTableRequestSpecPtr TableRequest;
    NCompression::ECodec CompressionCodec{};
    NYTree::TSize MaxChunkMessageCount;

    REGISTER_YSON_STRUCT(TDynamicOutputStoreSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicOutputStoreSpec);

////////////////////////////////////////////////////////////////////////////////

struct TMessageBatcherSettings
    : public NYTree::TYsonStruct
{
    // Maximum time to collect input batch.
    TDuration BatchDuration;

    // Batching parameters for transformations
    // TODO(mikari): right now this parameters applied independently to timers, inputs and sources.
    NYTree::TSize MaxRowsPerBatch;
    NYTree::TSize MaxBytesPerBatch;

    REGISTER_YSON_STRUCT(TMessageBatcherSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMessageBatcherSettings);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicRetryableRequestSpec
    : public TDynamicRetryableClientSpec
{
    TDuration LeaseCheckPeriod;

    REGISTER_YSON_STRUCT(TDynamicRetryableRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicRetryableRequestSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicComputationSpec
    : public TMessageBatcherSettings
{
    bool Draining{};

    // TODO(mikari): revise parameters

    // Would sleep for EmptyBatchBackoff if batch is empty.
    TDuration EmptyBatchBackoff;

    // Every real transaction checks lease,
    // and real transactions occur at least once every LeaseCheckPeriod (with applied jitter).
    TDuration LeaseCheckPeriod;

    TDynamicRetryableRequestSpecPtr RetryableRequest;

    TDynamicPartitionTracerSpecPtr Tracer;

    NYTree::IMapNodePtr Parameters;

    //! Dynamic counterpart of ``TComputationSpec::ProcessingFunctionParameters``: the
    //! reconfigurable parameters block of the hosted process function.
    NYTree::IMapNodePtr ProcessingFunctionParameters;

    THashMap<TStreamId, TDynamicSourceSpecPtr> SourceStreams;

    THashMap<TStreamId, TDynamicKeyVisitorStreamSpecPtr> KeyVisitorStreams;

    THashMap<TSinkId, TDynamicSinkSpecPtr> Sinks;

    //! Dynamic counterpart of ``TComputationSpec::ExternalStateManagers``/``ExternalStateJoiners``.
    //! Keys must match the static side; missing entries default to empty dynamic parameters.
    THashMap<std::string, TDynamicExternalStateManagerSpecPtr> ExternalStateManagers;
    THashMap<std::string, TDynamicExternalStateJoinerSpecPtr> ExternalStateJoiners;
    THashMap<std::string, TDynamicStateJoinerSpecPtr> StateJoiners;

    TDynamicStateManagerSpecPtr StateManager;
    TDynamicInputStoreSpecPtr InputStore;
    TDynamicTimerStoreSpecPtr TimerStore;
    TDynamicOutputStoreSpecPtr OutputStore;

    // Limits for TimerStore.
    // In case of too much data in memory - some computations may fail, some may suspend processing new data.
    NYTree::TSize TimerStoreCountLimit;
    NYTree::TSize TimerStoreByteSizeLimit;

    // Limits for OutputStore.
    // In case of too much data in memory - computation may suspend any processing.
    NYTree::TSize OutputStoreCountLimit;
    NYTree::TSize OutputStoreByteSizeLimit;

    //! Optional throttlers for the input batch.
    //! Before each Process step the computation draws its batch row count
    //! and byte size from the respective throttler. Null means "no limit in
    //! this dimension". Ids must be listed in TDynamicPipelineSpec::Throttlers.
    std::optional<TThrottlerId> InputRowsThrottlerId;
    std::optional<TThrottlerId> InputBytesThrottlerId;

    //! YTQL predicate over message meta and payload; a message is skipped when it holds.
    std::optional<std::string> SkipIfExpression;

    REGISTER_YSON_STRUCT(TDynamicComputationSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicComputationSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicPartitionTracerSpec
    : public NYTree::TYsonStruct
{
    int EpochsInTraceContext{};
    double TraceProbability{};
    THashMap<TPartitionId, double> TraceProbabilityPartitionOverride;

    TDuration WallTimeHalfDecayPeriod;

    REGISTER_YSON_STRUCT(TDynamicPartitionTracerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicPartitionTracerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicResourceSpec
    : public NYTree::TYsonStruct
{
    NYTree::IMapNodePtr Parameters;

    REGISTER_YSON_STRUCT(TDynamicResourceSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicResourceSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicThrottlerSpec
    : public NYTree::TYsonStruct
{
    //! Quota emitted globally per `Period`. Null means unlimited.
    std::optional<double> Limit;

    //! Quota bucket refill window on the server.
    TDuration Period;

    //! How often a client fetches a fresh batch of quota from the server.
    TDuration RequestPeriod;

    //! Retrying channel for tolerating transient RPC errors and controller failover.
    NRpc::TRetryingChannelConfigPtr RetryingChannel;

    //! Per-RPC timeout for RequestQuota calls.
    TDuration RpcTimeout;

    //! Server-side token bucket config.
    NConcurrency::TThroughputThrottlerConfigPtr BuildThroughputConfig() const;

    //! Client-side prefetching config.
    NConcurrency::TPrefetchingThrottlerConfigPtr BuildPrefetchingConfig() const;

    REGISTER_YSON_STRUCT(TDynamicThrottlerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicThrottlerSpec);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobBalancerType,
    (Greedy)
    (CpuAware)
    (ResourceQueue)
);

struct TDynamicJobBalancerSpec
    : public NYTree::TYsonStruct
{
    EJobBalancerType BalancerType{};
    // Deprecated. Use BalancerType = "CpuAware" instead.
    std::optional<bool> UseCpuAwareBalancer;
    TDuration RebalanceDelayAfterPipelineSync;
    double RebalanceTargetDeviation{};
    double RebalanceHotModeCoef{};
    TDuration RebalanceActionMinTime;
    TDuration RebalanceActionMaxTime;
    TDuration RebalanceSyncPeriod;
    double RebalanceCountExceedAllowed{};
    double RebalanceMinCpuSpread{};
    double RebalanceMinCpuRatio{};
    // Test-only: when set to true, the even-load gate is bypassed and rebalancing always runs.
    std::optional<bool> DisableEvenLoadGate;
    bool AsyncBalancing{};
    bool GracefulMove{};
    TDuration ZeroQueueLatency;
    TDuration PlanningHorizon;

    REGISTER_YSON_STRUCT(TDynamicJobBalancerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobBalancerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicJobManagerSpec
    : public TDynamicJobBalancerSpec
{
    ui64 MinimumWorkerCount{};
    TDuration LostJobTimeout;
    TDuration FaultyAddressWindow;
    ui64 FaultyAddressAttempts{};
    THashMap<TWorkerGroupId, TDynamicJobBalancerSpecPtr> WorkerGroupOverride;

    REGISTER_YSON_STRUCT(TDynamicJobManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobManagerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicBufferStateManagerSpec
    : public NYTree::TYsonStruct
{
    struct TOneSideBufferSpec
        : public virtual NYTree::TYsonStruct
    {
        NYTree::TSize FairSharePool;
        NYTree::TSize JobGuarantee;
        NYTree::TSize JobLimit;
        TDuration MaxDuration;
        THashMap<TComputationId, THashMap<TStreamId, NYTree::TSize>> JobOverrides;

        REGISTER_YSON_STRUCT(TOneSideBufferSpec);

        static void Register(TRegistrar registrar);
    };

    using TOneSideBufferSpecPtr = TIntrusivePtr<TOneSideBufferSpec>;

    TDuration ManagePeriod;
    TDuration DemandWindow;

    TOneSideBufferSpecPtr InputBuffer;
    TOneSideBufferSpecPtr OutputBuffer;

    REGISTER_YSON_STRUCT(TDynamicBufferStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicBufferStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicMessageDistributorSpec
    : public NYTree::TYsonStruct
{
    NYTree::TSize SendQueueMaxRowsPerBatch;
    NYTree::TSize SendQueueMaxBytesPerBatch;
    TDuration SendQueueBatchDuration;

    TDuration PushMessagesTimeout;
    NCompression::ECodec CompressionCodec{};

    int ThreadCount{};

    TDuration HungTaskThreshold;

    i64 MaxProcessedBatchSize{};

    REGISTER_YSON_STRUCT(TDynamicMessageDistributorSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicMessageDistributorSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicJobTrackerSpec
    : public NYTree::TYsonStruct
{
    static constexpr int DefaultJobThreads = 30;

    int JobControlThreads{};
    std::optional<int> JobThreads;

    TDynamicBufferStateManagerSpecPtr BufferStateManager;

    TLoadThroughputThrottlerSpecPtr LoadThroughputThrottler;

    TDynamicStateCacheSpecPtr StateCache;

    REGISTER_YSON_STRUCT(TDynamicJobTrackerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicJobTrackerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicControllerConnectorSpec
    : public NYTree::TYsonStruct
{
    //! After ControllerWaitTimeout without alive connection to Controller,
    //! Worker should stop still alive jobs.
    TDuration ControllerWaitTimeout;
    TDuration ControllerDiscoverPeriod;
    TDuration ControllerHeartbeatPeriod;
    TDuration ControllerHeartbeatRpcTimeout;
    TDuration ControllerHeartbeatFailureBackoff;

    TDuration ControllerHandshakeRpcTimeout;
    TDuration ControllerHandshakeFailureBackoff;

    TDuration OrchidUpdatePeriod;

    REGISTER_YSON_STRUCT(TDynamicControllerConnectorSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicControllerConnectorSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStateCacheSpec
    : public virtual NYTree::TYsonStruct
{
    NYTree::TSize CompressedCacheWeight;
    NYTree::TSize UncompressedCacheWeight;

    REGISTER_YSON_STRUCT(TDynamicStateCacheSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStateCacheSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicPipelineSpec
    : public NYTree::TYsonStruct
{
    EPipelineState TargetState{};

    THashMap<TComputationId, TDynamicComputationSpecPtr> Computations;
    THashMap<TResourceId, TDynamicResourceSpecPtr> Resources;
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> Throttlers;

    TDynamicJobManagerSpecPtr JobManager;
    TDynamicMessageDistributorSpecPtr MessageDistributor;
    TDynamicJobTrackerSpecPtr JobTracker;
    TDynamicControllerConnectorSpecPtr ControllerConnector;

    TSingletonsDynamicConfigPtr Singletons;

    //! Experimental: enable the describe-pipeline mermaid diagnostic graph.
    bool EnableMermaidGraphDescribe = false;

    //! Codec for the compressed flow view served by the get-flow-view-v2 command.
    NCompression::ECodec FlowViewCacheCodec{};

    REGISTER_YSON_STRUCT(TDynamicPipelineSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicPipelineSpec);

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TVersionedPipelineSpec);
DEFINE_REFCOUNTED_TYPE(TVersionedExtendedPipelineSpec);
DEFINE_REFCOUNTED_TYPE(TVersionedDynamicPipelineSpec);

////////////////////////////////////////////////////////////////////////////////

THashMap<TStreamId, std::vector<TStreamId>> BuildStreamGraph(const TPipelineSpecPtr& spec, bool addReadDelayEdges = false);
void ValidateIsDirectedAcyclicGraph(const THashMap<TStreamId, std::vector<TStreamId>>& streamGraph);
void ValidatePipelineSpec(const TPipelineSpecPtr& spec);
void ValidateDynamicPipelineSpec(const TDynamicPipelineSpecPtr& spec);

//! Returns the deduplicated, stably sorted list of YT-path ownership claims declared by every
//! entity in the pipeline. Same collection that ValidatePipelineSpec uses for conflict detection.
std::vector<TYTPathClaim> CollectPipelineYTPaths(const TPipelineSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
