#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TResourceManagerContext
    : public TRefCounted
{
    IPipelineAuthenticatorPtr PipelineAuthenticator;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
    IInvokerPtr Invoker;
};

DEFINE_REFCOUNTED_TYPE(TResourceManagerContext);

////////////////////////////////////////////////////////////////////////////////

struct IResourceManager
    : public TRefCounted
{
    //! Returns the resource object. For resources with PreloadRequired=true, throws if the
    //! resource is not yet in the Preloaded state. Non-preloadable resources are returned
    //! unconditionally regardless of whether Load has been called.
    virtual IResourcePtr Get(TResourceId resourceId) = 0;

    //! Triggers loading of the resource and all its dependencies; returns a future that
    //! resolves when loading is complete. Idempotent: repeated calls return the same future.
    //! Throws if the resource requires preloading but its preload has not been scheduled via
    //! UpdatePreloadedResources.
    virtual TFuture<void> Load(TResourceId resourceId) = 0;

    //! Loads the given resources and returns a single AllSucceeded future over them.
    //! Besides the passed ids it also drives the always-on resources (which live outside
    //! requiredResourceIds), so it must be called even with an empty requiredResourceIds.
    virtual TFuture<void> LoadRequiredResources(const THashSet<TResourceId>& requiredResourceIds) = 0;

    //! Report queue activity for a resource to the resource manager.
    //! Must be called by the user on every push/fetch so that the resource-aware
    //! job balancer has up-to-date queue size and throughput statistics.
    //! \param morePushedToQueue   Number of items pushed to the resource queue since the last call.
    //! \param moreFetchedFromQueue Number of items fetched from the resource queue since the last call.
    virtual void FeedStatus(TResourceId resourceId, i64 morePushedToQueue, i64 moreFetchedFromQueue) = 0;


    // The following methods are for internal use by the job tracker / controller only.
    // User code (jobs, computations) should not call them directly.

    //! Applies updated dynamic specs to the matching resources.
    //! Only resources whose dynamic spec has actually changed are reconfigured.
    virtual void Reconfigure(const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicSpecs) = 0;

    //! Returns a snapshot of queue size and throughput statistics for every resource
    //! that has received at least one FeedStatus call.
    virtual THashMap<TResourceId, TWorkerResourceStatusPtr> CollectResourceStatuses() = 0;

    //! Sets the resources that should be eagerly preloaded on this worker.
    //! Resources present in the new set but not yet loaded are loaded immediately;
    //! resources absent from the new set are dropped and recreated on next Get/Load.
    virtual void UpdatePreloadedResources(const THashSet<TResourceId>& resourceIds) = 0;

    //! Returns the current preload state (Preloading / Preloaded) for each resource
    //! that is in the active preload set.
    virtual THashMap<TResourceId, EPreloadedResourceState> GetPreloadedStates() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceManager);

////////////////////////////////////////////////////////////////////////////////

IResourceManagerPtr CreateResourceManager(
    TResourceManagerContextPtr managerContext,
    const THashMap<TResourceId, TResourceSpecPtr>& resources,
    const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
