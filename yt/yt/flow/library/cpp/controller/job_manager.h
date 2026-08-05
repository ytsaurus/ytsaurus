#pragma once

#include "public.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/cache.h>

namespace NYT::NFlow::NController {

// TODO(mikari): revise naming

////////////////////////////////////////////////////////////////////////////////

struct TJobManagerContext
    : public TRefCounted
{
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr Invoker;
    IInvokerPtr MainCycleInvoker;
    ITimeProviderPtr TimeProvider;
    IVersionProviderPtr VersionProvider;
    IStatusProfilerPtr StatusProfiler;
};

DEFINE_REFCOUNTED_TYPE(TJobManagerContext);

////////////////////////////////////////////////////////////////////////////////

struct IJobManager
    : public TRefCounted
{
    virtual void CheckCompletedPartitions(const TFlowViewPtr& flowView) = 0;
    virtual void RemoveFailedJobs(const TFlowViewPtr& flowView) = 0;

    // modify |flowView| inplace
    virtual void AggregateTraverseData(const TFlowViewPtr& flowView) = 0;
    virtual void UpdateInputStreamsTraverse(const TFlowViewPtr& flowView) = 0;
    virtual void UpdateWatermarkState(const TFlowViewPtr& flowView) = 0;
    //! Feeds worker statuses to the resource controllers, publishes the target revisions they
    //! build and reflects their views into the flow state.
    virtual void UpdateResourceControllers(const TFlowViewPtr& flowView) = 0;

    virtual bool CheckPipelineStopped(const TFlowViewPtr& flowView) = 0;
    virtual bool CheckPipelineCompleted(const TFlowViewPtr& flowView) = 0;

    //! Drops pending graceful migrations whose target worker is no longer registered.
    //! This is the only place an unhonorable migration is cancelled, so it must run on every
    //! scheduling iteration, before jobs are created.
    virtual void CancelInvalidGracefulRebalances(const TFlowViewPtr& flowView) = 0;

    virtual void RemoveLostJobs(const TFlowViewPtr& flowView) = 0;
    // Split and merge partitions.
    virtual void DoPartitioning(const TFlowViewPtr& flowView) = 0;
    // Do job distributions between hosts.
    virtual void DistributeJobs(const TFlowViewPtr& flowView) = 0;

    // just stop everything
    virtual void StopAllJobs(const TFlowViewPtr& flowView) = 0;

    virtual TJobManagerStatePtr GetState() = 0;
    virtual void Commit(const TFlowViewPtr& flowView) = 0;

    virtual void Reconfigure(TDynamicPipelineSpecPtr dynamicSpec) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobManager);

////////////////////////////////////////////////////////////////////////////////

IJobManagerPtr CreateJobManager(TJobManagerContextPtr context, TPipelineSpecPtr spec, TDynamicPipelineSpecPtr dynamicSpec, TJobManagerStatePtr initialState, IPipelineAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
