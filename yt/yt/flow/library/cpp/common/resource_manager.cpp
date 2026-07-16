#include "resource_manager.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/misc/ema.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/ema_counter.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

using namespace NThreading;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TFailedResource
    : public IResource
{
public:
    explicit TFailedResource(const IStatusProfilerPtr& statusProfiler, const TError& error)
        : Error_(error)
        , ErrorState_(statusProfiler->ErrorState("/failed_resource"))
    {
        ErrorState_->SetError(error);
    }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        THROW_ERROR_EXCEPTION(Error_);
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    { }

    TParametersPtr GetParametersBase() const override
    {
        THROW_ERROR_EXCEPTION(Error_);
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        THROW_ERROR_EXCEPTION(Error_);
    }

private:
    const TError Error_;
    const IStatusErrorStatePtr ErrorState_;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceStatus
{
public:
    explicit TResourceStatus()
    { }

    void Update(i64 morePushedToQueue, i64 moreFetchedFromQueue)
    {
        QueuePushedTotal_ += morePushedToQueue;
        QueueFetchedTotal_ += moreFetchedFromQueue;
        QueuePushCount_.Update(QueuePushedTotal_);
        QueueFetchCount_.Update(QueueFetchedTotal_);
        QueueSize_.Set(QueuePushedTotal_ - QueueFetchedTotal_);
    }

    TWorkerResourceStatusPtr Collect() const
    {
        auto status = New<TWorkerResourceStatus>();
        status->QueueSize30s = QueueSize_.Average()[0];
        status->QueueSize10m = QueueSize_.Average()[1];
        status->QueueGrowthRate30s = QueueSize_.GrowthRate()[0];
        status->QueueGrowthRate10m = QueueSize_.GrowthRate()[1];
        status->QueuePushRate30s = QueuePushCount_.GetRate(0);
        status->QueuePushRate10m = QueuePushCount_.GetRate(1);
        status->QueueFetchRate30s = QueueFetchCount_.GetRate(0);
        status->QueueFetchRate10m = QueueFetchCount_.GetRate(1);
        return status;
    }

private:
    static constexpr int TimeWindowsCount = 2;
    static constexpr std::array<TDuration, TimeWindowsCount> TimeWindowDurations = {TDuration::Seconds(30), TDuration::Minutes(10)};
    i64 QueuePushedTotal_ = 0;
    i64 QueueFetchedTotal_ = 0;
    TEmaCounter<double, TimeWindowsCount> QueuePushCount_{{TimeWindowDurations.begin(), TimeWindowDurations.end()}};
    TEmaCounter<double, TimeWindowsCount> QueueFetchCount_{{TimeWindowDurations.begin(), TimeWindowDurations.end()}};
    TMultiWindowEma<double, TimeWindowsCount, true> QueueSize_{{TimeWindowDurations}};
};

////////////////////////////////////////////////////////////////////////////////

struct TResourcePreloadState
    : public TRefCounted
{
    EPreloadedResourceState State = EPreloadedResourceState::Preloading;
    // Set to true when the resource is removed from the preload set while still loading.
    // The fiber checks this flag and skips the state update if cancelled.
    bool Cancelled = false;
};

using TResourcePreloadStatePtr = TIntrusivePtr<TResourcePreloadState>;

////////////////////////////////////////////////////////////////////////////////

class TResourceManager
    : public IResourceManager
{
public:
    TResourceManager(
        TResourceManagerContextPtr managerContext,
        const THashMap<TResourceId, TResourceSpecPtr>& resources,
        const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs)
        : ManagerContext_(std::move(managerContext))
        , Invoker_(ManagerContext_->Invoker)
        , Logger(ManagerContext_->Logger)
    {
        ResourceSpecs_ = resources;
        DynamicResourceSpecs_ = dynamicResourceSpecs;

        // Create resources in order of their resourceId. It's only needed by tests now but it's a good practice anyway.
        std::map<TResourceId, TResourceSpecPtr> resourceSpecs(ResourceSpecs_.begin(), ResourceSpecs_.end());
        for (const auto& [resourceId, resourceSpec] : resourceSpecs) {
            EmplaceOrCrash(Resources_, resourceId, CreateResource(resourceId));
        }

        // Always-on resources are loaded eagerly and kept for the manager's whole lifetime,
        // independent of jobs and of the balancer: a regular Load is never undone (only preloaded
        // resources are dropped, via UpdatePreloadedResources). Only resources that some computation
        // requires on this unit are loaded here.
        {
            auto guard = Guard(Lock_);
            std::vector<TFuture<void>> alwaysOnFutures;

            // For each resource referenced by some computation, whether this unit requires it: a
            // resource may be needed only on the worker or only on the controller.
            THashMap<TResourceId, bool> requiredOnThisUnit;
            for (const auto& [computationId, computationSpec] : ManagerContext_->Computations) {
                for (const auto& [resourceId, resourceDescription] : computationSpec->RequiredResourceIds) {
                    requiredOnThisUnit[resourceId] |= ManagerContext_->IsController
                        ? resourceDescription->Controller
                        : resourceDescription->Worker;
                }
            }

            for (const auto& [resourceId, resourceSpec] : resourceSpecs) {
                if (!resourceSpec->AlwaysOn) {
                    continue;
                }
                // Load the resource only where some computation requires it. A resource that no
                // computation requires on this unit (including one referenced by none) is skipped.
                if (!GetOrDefault(requiredOnThisUnit, resourceId)) {
                    YT_TLOG_INFO("Skipping always-on resource out of unit scope")
                        .With("ResourceId", resourceId)
                        .With("IsController", ManagerContext_->IsController);
                    continue;
                }
                YT_TLOG_INFO("Loading always-on resource")
                    .With("ResourceId", resourceId);
                alwaysOnFutures.push_back(LoadGuarded(resourceId, guard, /*isPreload*/ false));
            }
            // Collect the load futures so callers can await readiness via LoadRequiredResources().
            // AllSucceeded over an empty vector resolves immediately.
            AlwaysOnLoadedFuture_ = AllSucceeded(std::move(alwaysOnFutures));
        }
    }

    IResourcePtr Get(TResourceId resourceId) override
    {
        auto guard = Guard(Lock_);

        return GetGuarded(resourceId, guard);
    }

    TFuture<void> Load(TResourceId resourceId) override
    {
        auto guard = Guard(Lock_);

        return LoadGuarded(resourceId, guard, /*isPreload*/ false);
    }

    TFuture<void> LoadRequiredResources(const THashSet<TResourceId>& requiredResourceIds) override
    {
        auto guard = Guard(Lock_);

        std::vector<TFuture<void>> futures;
        futures.reserve(requiredResourceIds.size() + 1);
        for (const auto& resourceId : requiredResourceIds) {
            // Always-on resources are already loaded eagerly and awaited via AlwaysOnLoadedFuture_,
            // so skip them here to avoid requesting the same load twice.
            auto specIt = ResourceSpecs_.find(resourceId);
            if (specIt != ResourceSpecs_.end() && specIt->second->AlwaysOn) {
                continue;
            }
            futures.push_back(LoadGuarded(resourceId, guard, /*isPreload*/ false));
        }
        // Besides the passed ids, this also awaits the always-on resources (loaded eagerly in the
        // ctor), so it must be called even with an empty requiredResourceIds to ensure they are ready.
        futures.push_back(AlwaysOnLoadedFuture_);
        return AllSucceeded(std::move(futures));
    }

    void Reconfigure(const THashMap<TResourceId, TDynamicResourceSpecPtr>& newDynamicSpecs) override
    {
        std::vector<std::pair<IResourcePtr, TDynamicResourceSpecPtr>> toReconfigure;
        {
            auto guard = Guard(Lock_);

            for (const auto& [resourceId, newDynamicSpec] : newDynamicSpecs) {
                auto resourceIt = Resources_.find(resourceId);
                if (resourceIt == Resources_.end()) {
                    continue;
                }

                auto oldDynamicSpec = GetOrDefault(DynamicResourceSpecs_, resourceId);

                // Check if dynamic spec has changed.
                bool specChanged = !oldDynamicSpec ||
                    !AreNodesEqual(oldDynamicSpec->Parameters, newDynamicSpec->Parameters);

                if (specChanged) {
                    DynamicResourceSpecs_[resourceId] = newDynamicSpec;
                    YT_TLOG_INFO("Reconfiguring resource")
                        .With("ResourceId", resourceId);
                    toReconfigure.emplace_back(resourceIt->second, newDynamicSpec);
                }
            }
        }

        for (const auto& [resource, newDynamicSpec] : toReconfigure) {
            auto dynamicContext = New<TDynamicResourceContext>();
            dynamicContext->DynamicResourceSpec = newDynamicSpec;
            resource->Reconfigure(dynamicContext);
        }
    }

    void FeedStatus(TResourceId resourceId, i64 morePushedToQueue, i64 moreFetchedFromQueue) override
    {
        auto guard = Guard(Lock_);

        ResourceStatuses_[resourceId].Update(morePushedToQueue, moreFetchedFromQueue);
    }

    THashMap<TResourceId, TWorkerResourceStatusPtr> CollectResourceStatuses() override
    {
        auto guard = Guard(Lock_);

        THashMap<TResourceId, TWorkerResourceStatusPtr> result;

        for (const auto& [resourceId, resourceStatus] : ResourceStatuses_) {
            EmplaceOrCrash(result, resourceId, resourceStatus.Collect());
        }

        return result;
    }

    void UpdatePreloadedResources(const THashSet<TResourceId>& resourceIds) override
    {
        auto guard = Guard(Lock_);

        // Handle removed resources: those currently in PreloadStatus_ but not in the new set.
        // Collect first to avoid iterator invalidation.
        std::vector<TResourceId> toRemove;
        for (const auto& [resourceId, preloadState] : PreloadStatus_) {
            if (!resourceIds.contains(resourceId)) {
                toRemove.push_back(resourceId);
            }
        }

        for (const auto& resourceId : toRemove) {
            auto& preloadState = GetOrCrash(PreloadStatus_, resourceId);
            if (preloadState->State == EPreloadedResourceState::Preloading) {
                // Mark as cancelled so the subscribe callback does nothing on completion.
                preloadState->Cancelled = true;
            }
            // Erase the initialization future so the resource can be re-loaded fresh next time.
            ResourcesInitializationFutures_.erase(resourceId);
            PreloadStatus_.erase(resourceId);
            // Recreate the resource object so it starts from a clean state on next load.
            // Now its the only way to undo loading of the resource.
            Resources_[resourceId] = CreateResource(resourceId);
        }

        // Handle added resources: those in the new set but not yet in PreloadStatus_.
        for (const auto& resourceId : resourceIds) {
            if (PreloadStatus_.contains(resourceId)) {
                continue;
            }

            auto preloadState = New<TResourcePreloadState>();
            EmplaceOrCrash(PreloadStatus_, resourceId, preloadState);

            YT_TLOG_INFO("Starting preload for resource")
                .With("ResourceId", resourceId);

            // LoadGuarded is called while already holding Lock_.
            // isPreload=true bypasses the preload-required check since we are the ones initiating it.
            auto loadFuture = LoadGuarded(resourceId, guard, /*isPreload*/ true);
            loadFuture.Subscribe(
                BIND([weakThis = MakeWeak(this), resourceId, weakPreloadState = MakeWeak(preloadState), Logger = Logger] (const TError& error) {
                    auto strongThis = weakThis.Lock();
                    if (!strongThis) {
                        return;
                    }

                    auto strongPreloadState = weakPreloadState.Lock();
                    if (!strongPreloadState) {
                        return;
                    }

                    auto guard = Guard(strongThis->Lock_);

                    // If the resource was removed from the preload set while loading, do nothing.
                    if (strongPreloadState->Cancelled) {
                        return;
                    }

                    if (error.IsOK()) {
                        YT_TLOG_INFO("Resource preloaded successfully")
                            .With("ResourceId", resourceId);
                        strongPreloadState->State = EPreloadedResourceState::Preloaded;
                    } else {
                        YT_TLOG_ERROR("Resource preload failed")
                            .With("ResourceId", resourceId)
                            .With(error);
                        // Drop status in order to enable retry.
                        strongThis->PreloadStatus_.erase(resourceId);
                    }
                })
                    .Via(Invoker_));
        }
    }

    THashMap<TResourceId, EPreloadedResourceState> GetPreloadedStates() const override
    {
        auto guard = Guard(Lock_);

        THashMap<TResourceId, EPreloadedResourceState> result;
        for (const auto& [resourceId, preloadState] : PreloadStatus_) {
            result[resourceId] = preloadState->State;
        }
        return result;
    }

private:
    const TResourceManagerContextPtr ManagerContext_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    THashMap<TResourceId, IResourcePtr> Resources_;
    THashMap<TResourceId, TResourceSpecPtr> ResourceSpecs_;
    THashMap<TResourceId, TDynamicResourceSpecPtr> DynamicResourceSpecs_;
    THashMap<TResourceId, TResourceStatus> ResourceStatuses_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TResourceId, TFuture<void>> ResourcesInitializationFutures_;
    THashMap<TResourceId, TResourcePreloadStatePtr> PreloadStatus_;

    // AllSucceeded over all always-on resource loads; always set once in the constructor.
    TFuture<void> AlwaysOnLoadedFuture_;

    // Returns the resource for the given resourceId. Must be called with Lock_ held.
    IResourcePtr GetGuarded(TResourceId resourceId, const TGuard<TSpinLock>&)
    {
        auto resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);
        if (resourceSpec->PreloadRequired) {
            auto it = PreloadStatus_.find(resourceId);
            if (it == PreloadStatus_.end() || it->second->State != EPreloadedResourceState::Preloaded) {
                THROW_ERROR_EXCEPTION("Resource %v is used via Get while it's not preloaded", resourceId);
            }
        }
        return GetOrCrash(Resources_, resourceId);
    }

    // Loads the resource with the given resourceId. Must be called with Lock_ held.
    // isPreload=true skips the preload-required check (used by UpdatePreloadedResources
    // to initiate loading of a preload-required resource).
    TFuture<void> LoadGuarded(TResourceId resourceId, const TGuard<TSpinLock>& guard, bool isPreload = false)
    {
        if (auto it = ResourcesInitializationFutures_.find(resourceId); it != ResourcesInitializationFutures_.end()) {
            return it->second;
        }

        const auto& resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);
        const auto& resource = GetOrCrash(Resources_, resourceId);

        if (!isPreload && resourceSpec->PreloadRequired) {
            if (!PreloadStatus_.contains(resourceId)) {
                THROW_ERROR_EXCEPTION("Resource %v is used via Load while its preload is not scheduled", resourceId);
            }
        }

        auto promise = NewPromise<void>();
        auto future = promise.ToFuture().ToUncancelable();
        EmplaceOrCrash(ResourcesInitializationFutures_, resourceId, future);

        std::vector<TFuture<void>> dependencyFutures;
        THashMap<TResourceId, IResourcePtr> readyDependencies;
        {
            for (const auto& [depResourceId, resourceDescription] : resourceSpec->Dependencies) {
                dependencyFutures.push_back(LoadGuarded(depResourceId, guard));
                const auto& aliasResourceId = resourceDescription->Alias ? *resourceDescription->Alias : depResourceId;
                readyDependencies[aliasResourceId] = GetGuarded(depResourceId, guard);
            }
        }

        auto result = AllSucceeded(dependencyFutures)
            .Apply(
                BIND([weakThis = MakeWeak(this), resourceId, resource, readyDependencies = std::move(readyDependencies)] {
                    auto strongThis = weakThis.Lock();
                    THROW_ERROR_EXCEPTION_UNLESS(strongThis, "Resource manager is dead");

                    const auto& Logger = strongThis->Logger;

                    YT_TLOG_INFO("Loading resource")
                        .With("ResourceId", resourceId);
                    return resource->Load(readyDependencies);
                })
                    .AsyncVia(Invoker_))
            .ToUncancelable();

        promise.SetFrom(result);

        return future;
    }

    // Creates the resource object for the given resourceId.
    IResourcePtr CreateResource(TResourceId resourceId)
    {
        const auto& resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);

        auto context = New<TResourceContext>();
        context->ResourceId = resourceId;
        context->ResourceSpec = resourceSpec;
        context->ResourceManager = MakeWeak(this);
        context->PipelineAuthenticator = ManagerContext_->PipelineAuthenticator;
        context->Invoker = Invoker_;
        context->Logger = Logger.WithTag("Resource: %v", resourceId.Underlying());
        context->Profiler = ManagerContext_->Profiler.WithTag("resource", resourceId.Underlying()).WithPrefix("/resource");
        context->StatusProfiler = ManagerContext_->StatusProfiler->WithPrefix(Format("/resources/%v", resourceId));

        try {
            auto dynamicResourceContext = New<TDynamicResourceContext>();
            dynamicResourceContext->DynamicResourceSpec = GetOrDefault(DynamicResourceSpecs_, resourceId, New<TDynamicResourceSpec>());
            return TRegistry::Get()->CreateResource(context, dynamicResourceContext);
        } catch (const std::exception& ex) {
            return New<TFailedResource>(context->StatusProfiler, TError(ex));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IResourceManagerPtr CreateResourceManager(
    TResourceManagerContextPtr managerContext,
    const THashMap<TResourceId, TResourceSpecPtr>& resources,
    const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs)
{
    return New<TResourceManager>(std::move(managerContext), resources, dynamicResourceSpecs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
