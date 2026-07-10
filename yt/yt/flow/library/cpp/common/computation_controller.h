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

//! A common context shared between all computation controllers.
struct TComputationControllerCommonContext
    : public TRefCounted
{
    //! Latest instant when the controller decided to recreate all partitions (perhaps to change their count).
    //! Currently is modified only by universal controller for a computation with input streams (not a source computation).
    TInstant LastRepartitioningInstant = TInstant::Zero();
};

DEFINE_REFCOUNTED_TYPE(TComputationControllerCommonContext);

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
    TComputationControllerCommonContextPtr CommonContext;

    TComputationControllerContext(TComputationControllerCommonContextPtr commonContext);
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
    // Aggregated traverse data. Includes future partitions, partitions without traverse data
    // and ignored partition replacements (when partition is ignored as idle/unavailable it still holds watermark).
    // Contains null if controller cannot handle incomplete partition traverse data.
    TNodeTraverseDataPtr MergedTraverseData;

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
    // Provide TParameter[Ptr] and TDynamicParameter[Ptr] aliases. They are types of specs `Parameters` fields.
    // These types are used in computation controller registration for future parsing. They may be shadowed by macroses
    // YT_FLOW_EXTEND_PARAMETERS and YT_FLOW_EXTEND_DYNAMIC_PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(TDynamicParametersBase);

    virtual void Init(IInitContextPtr initContext) = 0;
    virtual void Sync() = 0;
    virtual void Commit() = 0;

    virtual bool IsFullCoverage(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView) = 0;

    virtual void DoPartitioning(
        const std::vector<TPartitionId>& computationPartitions,
        const TFlowViewPtr& flowView) = 0;

    virtual TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
        const TFlowViewPtr& flowView) = 0;

    virtual double ComputePartitionWeight(const TPartitionId& partitionId, const TFlowViewPtr& flowView) = 0;

    // Update already committed watermark state.
    virtual void UpdateWatermarkState(TWatermarkStatePtr watermarkState) = 0;
};

DEFINE_REFCOUNTED_TYPE(IComputationController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
