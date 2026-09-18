#pragma once

#include "public.h"

#include "state.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TComputationControllerContextBase
{
    TComputationSpecPtr ComputationSpec;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    TComputationId ComputationId;
    ITimeProviderPtr TimeProvider;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
    NLogging::TLogger Logger;
    // Public controller logger with extra tags.
    NLogging::TLogger PublicLogger;
    IInvokerPtr Invoker;
    THashMap<TResourceId, IResourcePtr> StaticResources;

    NApi::IClientPtr GetClient() const;

    IResourcePtr GetStaticResource(const char resourceId[]);
    IResourcePtr GetStaticResource(const TResourceId& resourceId);
};

struct TComputationControllerContext
    : public TRefCounted
    , public TComputationControllerContextBase
{
    IVersionProviderPtr VersionProvider;
};

DEFINE_REFCOUNTED_TYPE(TComputationControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicComputationControllerContextBase
{
    TDynamicComputationSpecPtr DynamicComputationSpec;
};

struct TDynamicComputationControllerContext
    : public TRefCounted
    , public TDynamicComputationControllerContextBase
{
};

DEFINE_REFCOUNTED_TYPE(TDynamicComputationControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct TProcessPartitionTraverseDataResult
    : public TRefCounted
{
    //! Traverse data accepted by the controller after merging current and future partitions and
    //! advancing the previously accepted data. Ready to become the computation's current traverse.
    TNodeTraverseDataPtr AcceptedTraverseData;

    THashMap<TStreamId, TStreamTraverseDataMetricsPtr> StreamMetrics;
};

DEFINE_REFCOUNTED_TYPE(TProcessPartitionTraverseDataResult);

////////////////////////////////////////////////////////////////////////////////

struct IComputationController
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicComputationControllerContext>
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

public:
    struct TPartitioningStatus
    {
        //! All current source keys; a null value means that job feedback is not available yet.
        THashMap<TKey, TExtendedSourcePartitionStatusPtr> SourcePartitions;
    };

    struct TPartitioningDescription
    {
        struct TRange
        {
            //! Dynamic policy used to calculate the desired count and key ranges.
            TPartitioningSpecPtr PartitioningSpec;
            //! Widest last-known channel count among currently configured sinks, or null when none is available.
            std::optional<i64> MaxSinkChannelCount;
            //! Version of the accumulated last-known per-sink channel counts; zero means none were observed.
            TVersion SinkTopologyVersion;
        };

        struct TSource
        {
            //! Current source keys and their worker specs; null means enumeration is unavailable.
            std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ExpectedKeys;
            //! Keys whose availability group was suppressed by the last accepted traverse.
            THashSet<TKey> UnavailableKeys;
        };

        std::variant<TRange, TSource> Value;
    };

    struct TPartitioningTopology
    {
        struct TRange
        { };

        struct TSource
        {
            //! Expected source keys; null means enumeration is unavailable.
            std::optional<THashSet<TKey>> ExpectedKeys;
        };

        std::variant<TRange, TSource> Value;
    };

    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in computation controller registration for future parsing. They may be shadowed by macroses
    // YT_FLOW_EXTEND_PARAMETERS and YT_FLOW_EXTEND_DYNAMIC_PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    virtual void Init(IInitContextPtr initContext) = 0;
    virtual void Sync() = 0;
    virtual void Commit() = 0;

    virtual TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TNodeTraverseDataPtr& currentTraverseData,
        const TFlowViewPtr& flowView) = 0;

    //! Describes the desired partition keyspace without processing worker status.
    virtual TPartitioningTopology DescribePartitioningTopology() = 0;

    //! Describes the desired partitioning without mutating the execution layout.
    virtual TPartitioningDescription DescribePartitioning(
        const TPartitioningStatus& status) = 0;

    virtual double ComputePartitionWeight(const TPartitionId& partitionId, const TFlowViewPtr& flowView) = 0;

    // Update already committed watermark state.
    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;
};

DEFINE_REFCOUNTED_TYPE(IComputationController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
