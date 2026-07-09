#pragma once

#include "payload_converter.h"
#include "public.h"

#include "describe_traits.h"
#include "distributing_tracker.h"
#include "external_metrics_reporter.h"
#include "flow_view.h"
#include "spec_validation.h"
#include "stream_inflight_limits.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/query/engine_api/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TComputationContextBase
{
    TComputationSpecPtr ComputationSpec;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    TPartitionPtr Partition;
    TJobPtr Job;
    IInvokerPtr SerializedInvoker;
    IInvokerPtr PoolInvoker;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
    NObjectClient::TCellTag ClockClusterTag;
    THashMap<TResourceId, IResourcePtr> StaticResources;
    NQueryClient::IColumnEvaluatorCachePtr EvaluatorCache;
    IPayloadConverterCachePtr ConverterCache;
    TLoadThroughputThrottlerPtr LoadThroughputThrottler;
    TStreamLimitUsageStateMap OutputStreamLimitUsageStates;
    TComputationStreamSpecStoragePtr StreamSpecStorage;

    TJobStateCachePtr JobStateCache;
    IExternalPerformanceMetricsReporterPtr ExternalMetricsReporter;
    NYT::NHttp::IClientPtr HttpClient;
    NYT::NHttp::IClientPtr HttpsClient;
    NYT::NConcurrency::IPollerPtr Poller;

    //! Raw channel provider to the controller's distributed-throttler service.
    //! TComputationBase uses it to build its IDistributedThrottlerFactory.
    std::function<NRpc::IChannelPtr()> DistributedThrottlerControllerChannelProvider;

    NApi::IClientPtr GetClient() const;

    IResourcePtr GetStaticResource(const char resourceId[]);
    IResourcePtr GetStaticResource(const TResourceId& resourceId);
};

struct TComputationContext
    : public TRefCounted
    , public TComputationContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TComputationContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicComputationContextBase
{
    i64 SpecGeneration;
    TDynamicComputationSpecPtr DynamicComputationSpec;
    NYTree::IMapNodePtr DynamicPartitionSpec;
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> Throttlers;
};

struct TDynamicComputationContext
    : public TRefCounted
    , public TDynamicComputationContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TDynamicComputationContext);

////////////////////////////////////////////////////////////////////////////////

struct IComputationRunContext
    : public TRefCounted
{
    // Throws exception if PartitionState == Interrupting.
    virtual TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>& allowedStreams = {}) = 0;

    // Immediately mark messages as persisted.
    virtual void MarkPersisted(std::span<const TMessageId> messageIds) = 0;

    void MarkPersisted(const std::vector<TInputMessageConstPtr>& messages);
    void MarkPersisted(TMessageId messageId);

    // Registers callbacks in tracker for each subscriber (sinks and downstream computations).
    // Distribution starts only after Commit() call.
    // Batch variant: registers all messages at once, taking locks once for the whole batch.
    // |trackers| must have the same size as |messages|.
    virtual void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> messages,
        std::span<TDistributingTracker> trackers) = 0;

    virtual void Commit() = 0;

    //! Smallest stabilized event timestamp across pending input messages;
    virtual TSystemTimestamp GetInputStabilizedEventTimestamp() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IComputationRunContext);

////////////////////////////////////////////////////////////////////////////////

//! Base class for computation orchid state.
struct TComputationOrchidState
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TComputationOrchidState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComputationOrchidState);

////////////////////////////////////////////////////////////////////////////////

struct TComputationStatus
    : public NYTree::TYsonStruct
{
    TNodeTraverseDataPtr NodeTraverse;
    NYTree::IMapNodePtr PartitionStatus;
    THashMap<std::string, double> EpochPartTimes;
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> InputLimits;
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> OutputLimits;

    REGISTER_YSON_STRUCT(TComputationStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComputationStatus);

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(EWatchComputationReconfigure,
    ((Never)                    (0x00))
    ((SpecGeneration)           (0x01))
    ((DynamicComputationSpec)   (0x02))
    ((DynamicPartitionSpec)     (0x04))
    ((AnySpec)                  (0x06))
    ((Any)                      (0xFF))
);

////////////////////////////////////////////////////////////////////////////////

struct IComputation
    : public TRefCounted
{
private:
    struct TParametersBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TParametersBase);

        static void Register(TRegistrar registrar);
    };

    struct TDynamicParametersBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TDynamicParametersBase);

        static void Register(TRegistrar registrar);
    };

    struct TDynamicPartitionSpecBase
        : public virtual NYTree::TYsonStruct
    {
        REGISTER_YSON_STRUCT(TDynamicPartitionSpecBase);

        static void Register(TRegistrar registrar);
    };

public:
    using EWatchReconfigure = EWatchComputationReconfigure;

    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in computation registration for future parsing.
    // They may be shadowed by macroses YT_FLOW_EXTEND_[DYNAMIC_]PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    // Provide TDynamicPartitionSpec[Ptr] alias.
    // They may be shadowed by macroses YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC in derived types.
    YT_FLOW_REGISTER_DYNAMIC_PARTITION_SPEC(TDynamicPartitionSpecBase);

    // Default describe traits; connectors may shadow it with their own `using TDescribeTraits = ...;`.
    using TDescribeTraits = TDescribeTraitsBase;

    using TValidator = TNoopSpecValidator;

    //! Runs the computation logic.
    /*!
     *  Data is split based on partitioning key.
     *
     *  Constructor and Run() must be called in TComputationContext::SerializedInvoker.
     *  And the computation is expected to use this invoker to carry out its actions.
     *  Though TComputationContext::PoolInvoker also can be used with caution for heavy actions.
     *
     *  If computation is finite (for example, it is finite injector or "interrupting" transformer) - it should return sucessfully.
     *  In case of any error - exception should be thrown.
     */
    virtual void Run(const IComputationRunContextPtr& context) = 0;

    //! All methods below must be thread-safe.

    //! Fire-and-forget reconfiguration. The new spec will be applied at the start of the next run iteration.
    virtual void Reconfigure(TDynamicComputationContextPtr dynamicContextPtr) = 0;
    virtual void SubscribeOnReconfigure(
        NYT::TCallback<void()> callback,
        EWatchReconfigure watchReconfigureMask = EWatchReconfigure::DynamicComputationSpec) = 0;

    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;

    virtual void SetInputTraverse(
        THashMap<TStreamId, TStreamTraverseDataPtr> inputStreams) = 0;

    virtual TComputationStatusPtr GetStatus() = 0;

    virtual TComputationOrchidStatePtr GetOrchidState() = 0;
};

DEFINE_REFCOUNTED_TYPE(IComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
