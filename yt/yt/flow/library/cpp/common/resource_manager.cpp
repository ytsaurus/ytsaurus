#include "resource_manager.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_queue_statistics.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/producer.h>

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
        CancelableContext_->Cancel(error);
    }

    ~TFailedResource() override
    {
        DestroyedPromise_.Set();
    }

    const TCancelableContextPtr& GetCancelableContext() const override
    {
        return CancelableContext_;
    }

    TResourceStatus GetStatus() const override
    {
        return {.Error = CancelableContext_->GetCancelationError()};
    }

    TFuture<void> GetDestroyedFuture() const override
    {
        return DestroyedPromise_.ToFuture().ToUncancelable();
    }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        return MakeFuture<void>(Error_);
    }

    TResourceRevisionState GetRevisionState() const override
    {
        return {};
    }

    TParametersPtr GetParametersBase() const override
    {
        THROW_ERROR_EXCEPTION(Error_);
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        THROW_ERROR_EXCEPTION(Error_);
    }

private:
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();
    const TError Error_;
    const IStatusErrorStatePtr ErrorState_;
    const TPromise<void> DestroyedPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

struct TResourcePreloadState
    : public TRefCounted
{
    EPreloadedResourceState State = EPreloadedResourceState::Preloading;
};

using TResourcePreloadStatePtr = TIntrusivePtr<TResourcePreloadState>;

////////////////////////////////////////////////////////////////////////////////

class TResourcePreloadSensors
    : public NProfiling::ISensorProducer
{
public:
    explicit TResourcePreloadSensors(TFuture<void> loadFuture)
        : LoadFuture_(std::move(loadFuture))
    { }

    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        const auto result = LoadFuture_.TryGet();
        const bool preloading = !result;
        const bool preloaded = result && result->IsOK();
        writer->AddGauge("/preloading", preloading);
        writer->AddGauge("/preloaded", preloaded);
        writer->AddGauge("/preload_state", preloading + 2 * preloaded);
    }

private:
    const TFuture<void> LoadFuture_;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceManager
    : public IResourceManager
{
public:
    TResourceManager(
        TResourceManagerContextPtr managerContext,
        const THashMap<TResourceId, TResourceSpecPtr>& resources,
        const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs,
        const THashMap<TResourceId, TResourceRevisionPtr>& targetRevisions,
        const THashMap<TResourceId, TResourceInstanceState>& predecessorInstanceStates,
        bool externallyAssigned = false)
        : ManagerContext_(std::move(managerContext))
        , Invoker_(ManagerContext_->Invoker)
        , Logger(ManagerContext_->Logger)
        , ExternallyAssigned_(externallyAssigned)
    {
        ResourceSpecs_ = resources;
        DynamicResourceSpecs_ = dynamicResourceSpecs;
        TargetRevisions_ = targetRevisions;

        ResourceInstanceStates_ = predecessorInstanceStates;

        // Create resources in order of their resourceId. It's only needed by tests now but it's a good practice anyway.
        std::map<TResourceId, TResourceSpecPtr> resourceSpecs(ResourceSpecs_.begin(), ResourceSpecs_.end());
        std::vector<TClosure> subscriptions;
        {
            auto guard = Guard(Lock_);
            for (const auto& [resourceId, resourceSpec] : resourceSpecs) {
                EmplaceOrCrash(Resources_, resourceId, CreateResource(resourceId));
            }
            for (const auto& [resourceId, _] : Resources_) {
                subscriptions.push_back(BindResourceDependencies(resourceId));
            }
        }
        for (const auto& subscribe : subscriptions) {
            subscribe();
        }

        // Always-on resources are loaded eagerly and kept for the manager's whole lifetime,
        // independent of jobs and of the balancer: a regular Load is never undone (only preloaded
        // resources are dropped, via UpdatePreloadedResources). Only resources that some computation
        // requires on this unit are loaded here.
        {
            auto guard = Guard(Lock_);

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
                AlwaysOnResourceIds_.push_back(resourceId);
                Y_UNUSED(LoadGuarded(resourceId, guard, /*isPreload*/ false));
            }
        }
    }

    ~TResourceManager() override
    {
        for (const auto& [resourceId, resource] : Resources_) {
            resource->Stop(TError(NYT::EErrorCode::Canceled, "Resource instance stopped"));
        }
        for (const auto& [resourceId, future] : ResourcesInitializationFutures_) {
            future.Cancel(TError("Resource manager destroyed")
                    .With("resource_id", resourceId));
        }
    }

    IResourcePtr Get(TResourceInstanceId instanceId) override
    {
        IResourcePtr resource;
        {
            auto guard = Guard(Lock_);
            resource = GetGuarded(GetResourceIdOrThrow(instanceId), guard);
        }
        const auto status = resource->GetStatus();
        status.Error.ThrowOnError();
        return resource;
    }

    TFuture<void> Load(const THashSet<TResourceInstanceId>& instanceIds) override
    {
        try {
            std::vector<std::pair<IResourcePtr, TFuture<void>>> loads;
            {
                auto guard = Guard(Lock_);
                for (auto instanceId : instanceIds) {
                    auto resourceId = GetResourceIdOrThrow(instanceId);
                    auto futureLoaded = LoadGuarded(resourceId, guard);
                    loads.emplace_back(GetOrCrash(Resources_, resourceId), std::move(futureLoaded));
                }
            }
            std::vector<TFuture<void>> futures;
            for (const auto& [resource, futureLoaded] : loads) {
                futures.push_back(ValidateLoadResult(resource, futureLoaded));
            }
            return AllSucceeded(std::move(futures));
        } catch (const std::exception& ex) {
            return MakeFuture<void>(TError(ex));
        }
    }

    void FeedStatus(TResourceInstanceId instanceId, i64 morePushedToQueue, i64 moreFetchedFromQueue) override
    {
        auto guard = Guard(Lock_);
        if (auto it = InstanceResourceIds_.find(instanceId); it != InstanceResourceIds_.end()) {
            auto& resourceStatus = ResourceQueueStatistics_[it->second];
            resourceStatus.Update(morePushedToQueue, moreFetchedFromQueue);
            UpdateQueueSizeGaugeGuarded(it->second, resourceStatus.GetQueueSize(), guard);
        }
    }

    THashMap<TResourceInstanceId, TWorkerResourceStatusPtr> CollectResourceInstanceStatuses() override
    {
        THashMap<TResourceInstanceId, TWorkerResourceStatusPtr> result;
        std::vector<std::pair<IResourcePtr, TWorkerResourceStatusPtr>> resources;
        {
            auto guard = Guard(Lock_);
            auto now = TInstant::Now();
            for (const auto& [instanceId, resourceId] : InstanceResourceIds_) {
                auto status = ResourceQueueStatistics_[resourceId].Collect(now);
                status->ResourceId = resourceId;
                status->ResourceInstanceId = instanceId;
                if (auto future = ResourcesInitializationFutures_.find(resourceId); future != ResourcesInitializationFutures_.end()) {
                    if (auto error = future->second.TryGet()) {
                        status->LoadState = error->IsOK() ? EResourceLoadState::Loaded : EResourceLoadState::Failed;
                        status->LoadError = *error;
                    } else {
                        status->LoadState = EResourceLoadState::Loading;
                    }
                }
                result.emplace(instanceId, status);
                resources.emplace_back(GetOrCrash(Resources_, resourceId), std::move(status));
            }
        }
        for (const auto& [resource, status] : resources) {
            auto resourceStatus = resource->GetStatus();
            FillRevisionStatus(status, resourceStatus.RevisionState);
            if (!resourceStatus.Error.IsOK()) {
                status->LoadState = EResourceLoadState::Failed;
                status->LoadError = resourceStatus.Error;
            }
        }
        return result;
    }

    void ReconfigureInstances(
        const THashMap<TResourceId, TResourceSpecPtr>& resourceSpecs,
        const THashMap<TResourceInstanceId, TResourceInstancePtr>& instances,
        const THashMap<TResourceInstanceId, TResourceInstanceDynamicSpecPtr>& dynamicSpecs) override
    {
        YT_VERIFY(ExternallyAssigned_);
        THashMap<TResourceId, TResourceInstanceId> assignedIds;
        for (const auto& [instanceId, instance] : instances) {
            EmplaceOrCrash(assignedIds, instance->ResourceId, instanceId);
            THROW_ERROR_EXCEPTION_UNLESS(dynamicSpecs.contains(instanceId),
                "Missing dynamic spec for resource instance %v",
                instanceId);
        }
        std::vector<TResourceId> created;
        std::vector<TClosure> subscriptions;
        std::vector<IResourcePtr> released;
        std::vector<std::pair<TResourceInstanceId, TFuture<void>>> canceledLoads;
        std::vector<std::pair<IResourcePtr, TDynamicResourceContextPtr>> reconfigured;
        THashSet<TResourceInstanceId> eager;
        {
            auto guard = Guard(Lock_);
            for (auto it = PredecessorDestructionFutures_.begin(); it != PredecessorDestructionFutures_.end();) {
                if (it->second.IsSet()) {
                    PredecessorDestructionFutures_.erase(it++);
                } else {
                    ++it;
                }
            }
            for (auto it = Resources_.begin(); it != Resources_.end();) {
                const auto& resourceId = it->first;
                auto instanceId = GetOrCrash(ResourceInstanceStates_, resourceId).InstanceId;
                auto assigned = assignedIds.find(resourceId);
                if (assigned != assignedIds.end() && assigned->second == instanceId) {
                    ++it;
                    continue;
                }
                InstanceResourceIds_.erase(instanceId);
                if (auto load = ResourcesInitializationFutures_.find(resourceId); load != ResourcesInitializationFutures_.end()) {
                    canceledLoads.emplace_back(instanceId, std::move(load->second));
                    ResourcesInitializationFutures_.erase(load);
                }
                ResourceQueueStatistics_.erase(resourceId);
                ResourceMetrics_.erase(resourceId);
                ResourcePreloadSensors_.erase(resourceId);
                DynamicResourceSpecs_.erase(resourceId);
                TargetRevisions_.erase(resourceId);
                auto destroyed = it->second->GetDestroyedFuture();
                auto& predecessorsDestroyed = PredecessorDestructionFutures_[resourceId];
                // An unloaded intermediate instance must not hide an older live predecessor.
                predecessorsDestroyed = predecessorsDestroyed
                    ? AllSucceeded(std::vector{predecessorsDestroyed, destroyed}).ToUncancelable()
                    : destroyed;
                released.push_back(std::move(it->second));
                Resources_.erase(it++);
            }
            ResourceSpecs_ = resourceSpecs;
            AssignedPreloadResources_.clear();
            AlwaysOnResourceIds_.clear();
            for (const auto& [resourceId, instanceId] : assignedIds) {
                const auto& dynamicSpec = GetOrCrash(dynamicSpecs, instanceId);
                auto it = Resources_.find(resourceId);
                if (it == Resources_.end()) {
                    DynamicResourceSpecs_[resourceId] = dynamicSpec->DynamicResourceSpec;
                    if (dynamicSpec->TargetRevision) {
                        TargetRevisions_[resourceId] = dynamicSpec->TargetRevision;
                    }
                    Resources_.emplace(resourceId, CreateResource(resourceId, instanceId));
                    created.push_back(resourceId);
                } else {
                    if (auto context = UpdateDynamicContext(resourceId, dynamicSpec->DynamicResourceSpec, dynamicSpec->TargetRevision)) {
                        reconfigured.emplace_back(it->second, std::move(context));
                    }
                }
                if (GetOrCrash(resourceSpecs, resourceId)->AlwaysOn) {
                    AlwaysOnResourceIds_.push_back(resourceId);
                }
                if (dynamicSpec->Preload) {
                    AssignedPreloadResources_.insert(resourceId);
                }
                if (GetOrCrash(resourceSpecs, resourceId)->AlwaysOn || dynamicSpec->Preload) {
                    eager.insert(instanceId);
                }
            }
            for (const auto& resourceId : created) {
                subscriptions.push_back(BindResourceDependencies(resourceId));
            }
        }
        for (const auto& subscribe : subscriptions) {
            subscribe();
        }
        if (!released.empty()) {
            // Cancellation can release dependencies, and resource destructors may switch fibers.
            GetFinalizerInvoker()->Invoke(BIND([
                canceledLoads = std::move(canceledLoads),
                released = std::move(released)
            ] () mutable {
                for (const auto& resource : released) {
                    resource->Stop(TError(NYT::EErrorCode::Canceled, "Resource instance stopped"));
                }
                for (const auto& [instanceId, future] : canceledLoads) {
                    future.Cancel(TError("Resource instance is no longer assigned")
                            .With("resource_instance_id", instanceId));
                }
                canceledLoads.clear();
                released.clear();
            }));
        }
        for (const auto& [resource, context] : reconfigured) {
            ReconfigureResource(resource, context);
        }
        YT_UNUSED_FUTURE(Load(eager));
    }

    IResourcePtr Get(TResourceId resourceId) override
    {
        auto guard = Guard(Lock_);

        return GetGuarded(resourceId, guard);
    }

    TFuture<void> Load(TResourceId resourceId) override
    {
        IResourcePtr resource;
        TFuture<void> futureLoaded;
        {
            auto guard = Guard(Lock_);
            futureLoaded = LoadGuarded(resourceId, guard, /*isPreload*/ false);
            resource = GetOrDefault(Resources_, resourceId);
        }
        return ValidateLoadResult(resource, futureLoaded);
    }

    TFuture<void> LoadRequiredResources(const THashSet<TResourceId>& requiredResourceIds) override
    {
        auto resourceIds = requiredResourceIds;
        {
            auto guard = Guard(Lock_);
            resourceIds.insert(AlwaysOnResourceIds_.begin(), AlwaysOnResourceIds_.end());
        }
        std::vector<TFuture<void>> futures;
        for (const auto& resourceId : resourceIds) {
            futures.push_back(Load(resourceId));
        }
        return AllSucceeded(std::move(futures));
    }

    void Reconfigure(
        const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicSpecs,
        const THashMap<TResourceId, TResourceRevisionPtr>& targetRevisions) override
    {
        std::vector<std::pair<IResourcePtr, TDynamicResourceContextPtr>> toReconfigure;
        {
            auto guard = Guard(Lock_);

            for (const auto& [resourceId, resource] : Resources_) {
                if (auto context = UpdateDynamicContext(resourceId, GetOrDefault(dynamicSpecs, resourceId), GetOrDefault(targetRevisions, resourceId))) {
                    toReconfigure.emplace_back(resource, std::move(context));
                }
            }
        }

        for (const auto& [resource, dynamicContext] : toReconfigure) {
            ReconfigureResource(resource, dynamicContext);
        }
    }

    void FeedStatus(TResourceId resourceId, i64 morePushedToQueue, i64 moreFetchedFromQueue) override
    {
        auto guard = Guard(Lock_);

        auto& resourceStatus = ResourceQueueStatistics_[resourceId];
        resourceStatus.Update(morePushedToQueue, moreFetchedFromQueue);
        UpdateQueueSizeGaugeGuarded(resourceId, resourceStatus.GetQueueSize(), guard);
    }

    THashMap<TResourceId, TWorkerResourceStatusPtr> CollectResourceStatuses() override
    {
        THashMap<TResourceId, TWorkerResourceStatusPtr> result;
        THashMap<TResourceId, IResourcePtr> scheduledResources;
        {
            auto guard = Guard(Lock_);

            // One instant for the whole snapshot keeps the resources consistent with each other.
            const auto now = TInstant::Now();
            for (auto& [resourceId, resourceStatus] : ResourceQueueStatistics_) {
                EmplaceOrCrash(result, resourceId, resourceStatus.Collect(now));
            }
            // Report resources whose load was scheduled, including a pending initial load.
            for (const auto& [resourceId, future] : ResourcesInitializationFutures_) {
                Y_UNUSED(future);
                scheduledResources.emplace(resourceId, GetOrCrash(Resources_, resourceId));
            }
        }

        // Status queries may invoke resource-specific code.
        for (const auto& [resourceId, resource] : scheduledResources) {
            auto resourceStatus = resource->GetStatus();
            const auto& revisionState = resourceStatus.RevisionState;
            if (resourceStatus.Error.IsOK() &&
                !revisionState.AppliedRevisionId &&
                !revisionState.TargetRevisionId &&
                !revisionState.ActiveFileSnapshotId &&
                !revisionState.PreparingFileSnapshot)
            {
                continue;
            }
            auto& status = result[resourceId];
            if (!status) {
                status = New<TWorkerResourceStatus>();
            }
            FillRevisionStatus(status, revisionState);
            if (!resourceStatus.Error.IsOK()) {
                status->LoadState = EResourceLoadState::Failed;
                status->LoadError = resourceStatus.Error;
            }
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
            PreloadStatus_.erase(resourceId);
            ResetResourceGuarded(resourceId, guard);
            if (auto it = ResourceQueueStatistics_.find(resourceId); it != ResourceQueueStatistics_.end()) {
                UpdateQueueSizeGaugeGuarded(resourceId, it->second.GetQueueSize(), guard);
            }
        }

        // Handle added resources: those in the new set but not yet in PreloadStatus_.
        for (const auto& resourceId : resourceIds) {
            if (PreloadStatus_.contains(resourceId)) {
                continue;
            }

            YT_TLOG_INFO("Starting preload for resource")
                .With("ResourceId", resourceId);
            StartPreloadGuarded(resourceId, guard);
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

    THashMap<TResourceId, TResourceInstanceState> GetResourceInstanceStates() const override
    {
        auto guard = Guard(Lock_);

        return ResourceInstanceStates_;
    }

private:
    struct TResourceMetrics
    {
        NProfiling::TGauge QueueSize;

        TResourceMetrics(const NProfiling::TProfiler& profiler, const TResourceId& resourceId)
            : QueueSize(profiler
                    .WithTag("resource", resourceId.Underlying())
                    .WithPrefix("/resource")
                    .Gauge("/queue_size"))
        { }
    };

    const TResourceManagerContextPtr ManagerContext_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    const bool ExternallyAssigned_;

    THashMap<TResourceId, IResourcePtr> Resources_;
    THashMap<TResourceId, TResourceSpecPtr> ResourceSpecs_;
    THashMap<TResourceId, TDynamicResourceSpecPtr> DynamicResourceSpecs_;
    THashMap<TResourceId, TResourceRevisionPtr> TargetRevisions_;
    THashMap<TResourceId, TResourceQueueStatistics> ResourceQueueStatistics_;
    THashMap<TResourceId, TResourceMetrics> ResourceMetrics_;
    THashMap<TResourceInstanceId, TResourceId> InstanceResourceIds_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    // Protected by Lock_.
    THashMap<TResourceId, TResourceInstanceState> ResourceInstanceStates_;
    THashMap<TResourceId, TFuture<void>> ResourcesInitializationFutures_;
    THashMap<TResourceId, TFuture<void>> PredecessorDestructionFutures_;
    THashMap<TResourceId, TResourcePreloadStatePtr> PreloadStatus_;
    THashSet<TResourceId> AssignedPreloadResources_;
    THashMap<TResourceId, NProfiling::ISensorProducerPtr> ResourcePreloadSensors_;

    // Always-on resources required on this unit.
    std::vector<TResourceId> AlwaysOnResourceIds_;

    void ReconfigureResource(
        const IResourcePtr& resource,
        const TDynamicResourceContextPtr& context)
    {
        try {
            resource->Reconfigure(context);
        } catch (const std::exception& ex) {
            resource->Stop(TError(ex));
        }
    }

    TResourceId GetResourceIdOrThrow(TResourceInstanceId instanceId) const
    {
        auto it = InstanceResourceIds_.find(instanceId);
        THROW_ERROR_EXCEPTION_UNLESS(it != InstanceResourceIds_.end(), "Resource instance %v is not assigned", instanceId);
        return it->second;
    }

    static void FillRevisionStatus(const TWorkerResourceStatusPtr& status, const TResourceRevisionState& revision)
    {
        status->AppliedRevisionId = revision.AppliedRevisionId;
        status->TargetRevisionId = revision.TargetRevisionId;
        if (revision.ResourceInstanceId) {
            status->ResourceInstanceId = revision.ResourceInstanceId;
        }
        status->ResourceIncarnationGeneration = revision.ResourceIncarnationGeneration;
        status->ActiveFileSnapshotId = revision.ActiveFileSnapshotId;
        status->PreparingFileSnapshot = revision.PreparingFileSnapshot;
        status->LiveAccessorCounts = revision.LiveAccessorCounts;
    }

    TDynamicResourceContextPtr UpdateDynamicContext(
        const TResourceId& resourceId,
        const TDynamicResourceSpecPtr& dynamicSpec,
        const TResourceRevisionPtr& targetRevision)
    {
        bool changed = false;
        if (dynamicSpec) {
            auto oldSpec = GetOrDefault(DynamicResourceSpecs_, resourceId);
            if (!oldSpec || !AreNodesEqual(NYTree::ConvertToNode(oldSpec), NYTree::ConvertToNode(dynamicSpec))) {
                DynamicResourceSpecs_[resourceId] = dynamicSpec;
                changed = true;
            }
        }
        auto oldTarget = GetOrDefault(TargetRevisions_, resourceId);
        if ((oldTarget ? std::optional(oldTarget->RevisionId) : std::nullopt) !=
            (targetRevision ? std::optional(targetRevision->RevisionId) : std::nullopt))
        {
            if (targetRevision) {
                TargetRevisions_[resourceId] = targetRevision;
            } else {
                TargetRevisions_.erase(resourceId);
            }
            changed = true;
        }
        return changed ? BuildDynamicContext(resourceId) : nullptr;
    }

    // Returns the resource for the given resourceId. Must be called with Lock_ held.
    IResourcePtr GetGuarded(TResourceId resourceId, const TGuard<TSpinLock>&)
    {
        auto resource = Resources_.find(resourceId);
        THROW_ERROR_EXCEPTION_UNLESS(resource != Resources_.end(), "Resource %Qv is not assigned", resourceId);
        auto resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);
        if (!ExternallyAssigned_ && resourceSpec->PreloadRequired) {
            auto it = PreloadStatus_.find(resourceId);
            if (it == PreloadStatus_.end() || it->second->State != EPreloadedResourceState::Preloaded) {
                THROW_ERROR_EXCEPTION("Resource %v is used via Get while it's not preloaded", resourceId);
            }
        }
        return resource->second;
    }

    // Loads the resource with the given resourceId. Must be called with Lock_ held.
    // isPreload=true skips the preload-required check (used by UpdatePreloadedResources
    // to initiate loading of a preload-required resource).
    TFuture<void> LoadGuarded(TResourceId resourceId, const TGuard<TSpinLock>& guard, bool isPreload = false)
    {
        if (OwnsResourceRecovery() && ResetResourceIfFailedGuarded(resourceId, guard)) {
            YT_TLOG_INFO("Reloading failed resource")
                .With("ResourceId", resourceId);
        }
        if (auto it = ResourcesInitializationFutures_.find(resourceId); it != ResourcesInitializationFutures_.end()) {
            return it->second.ToUncancelable();
        }

        auto resourceIt = Resources_.find(resourceId);
        if (resourceIt == Resources_.end()) {
            return MakeFuture<void>(TError("Resource %Qv is not assigned", resourceId));
        }
        const auto& resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);
        const auto& resource = resourceIt->second;
        auto instanceId = GetOrCrash(ResourceInstanceStates_, resourceId).InstanceId;

        if (!ExternallyAssigned_ && !isPreload && resourceSpec->PreloadRequired) {
            if (!PreloadStatus_.contains(resourceId)) {
                THROW_ERROR_EXCEPTION("Resource %v is used via Load while its preload is not scheduled", resourceId);
            }
        }

        auto promise = NewPromise<void>();
        auto future = promise.ToFuture();
        auto onLoaded = BIND([weakResource = MakeWeak(resource), promise] (const TError& error) {
            if (!error.IsOK()) {
                if (auto resource = weakResource.Lock()) {
                    resource->Stop(error);
                }
            }
            promise.TrySet(error);
        }).Via(GetFinalizerInvoker());
        EmplaceOrCrash(ResourcesInitializationFutures_, resourceId, future);
        if (AssignedPreloadResources_.contains(resourceId)) {
            auto sensors = New<TResourcePreloadSensors>(future);
            ManagerContext_->Profiler
                .WithTag("resource", resourceId.Underlying())
                .AddProducer("/resource", sensors);
            EmplaceOrCrash(ResourcePreloadSensors_, resourceId, std::move(sensors));
        }

        std::vector<TFuture<void>> dependencyFutures;
        if (auto it = PredecessorDestructionFutures_.find(resourceId); it != PredecessorDestructionFutures_.end()) {
            dependencyFutures.push_back(it->second);
        }
        THashMap<TResourceId, IResourcePtr> readyDependencies;
        try {
            for (const auto& [depResourceId, resourceDescription] : resourceSpec->Dependencies) {
                dependencyFutures.push_back(LoadGuarded(depResourceId, guard));
                const auto& aliasResourceId = resourceDescription->Alias ? *resourceDescription->Alias : depResourceId;
                readyDependencies[aliasResourceId] = GetGuarded(depResourceId, guard);
            }
        } catch (const std::exception& ex) {
            onLoaded(TError(ex));
            return future.ToUncancelable();
        }

        auto result = AllSucceeded(dependencyFutures)
            // Cancel this wait without affecting shared dependencies or the destruction barrier.
            .ToImmediatelyCancelable(false)
            .Apply(
                BIND([
                    weakThis = MakeWeak(this),
                    resourceId,
                    instanceId,
                    weakResource = MakeWeak(resource),
                    readyDependencies = std::move(readyDependencies)
                ] {
                    auto strongThis = weakThis.Lock();
                    THROW_ERROR_EXCEPTION_UNLESS(strongThis, "Resource manager is dead");

                    auto resource = weakResource.Lock();
                    THROW_ERROR_EXCEPTION_UNLESS(resource,
                        "Resource instance %v was released before loading",
                        instanceId);
                    if (strongThis->ExternallyAssigned_) {
                        auto guard = Guard(strongThis->Lock_);
                        strongThis->GetResourceIdOrThrow(instanceId);
                    }
                    const auto status = resource->GetStatus();
                    status.Error.ThrowOnError();
                    const auto Logger = strongThis->Logger;
                    strongThis.Reset();

                    YT_TLOG_INFO("Loading resource")
                        .With("ResourceId", resourceId);
                    return resource->Load(readyDependencies).Apply(BIND([resource] (const TError& error) {
                        // A resource may return a future that does not retain it.
                        Y_UNUSED(resource);
                        error.ThrowOnError();
                    }));
                })
                    .AsyncVia(Invoker_));

        result.Subscribe(onLoaded);
        // Cancellation can release dependencies with yielding destructors.
        promise.OnCanceled(BIND([promise, cancelable = result.AsCancelable()] (const TError& error) {
            cancelable.Cancel(error);
            promise.TrySet(error);
        }).Via(GetFinalizerInvoker()));
        GetFinalizerInvoker()->Invoke(BIND([context = resource->GetCancelableContext(), future] {
            context->PropagateTo(future);
        }));

        return future.ToUncancelable();
    }

    void StartPreloadGuarded(TResourceId resourceId, const TGuard<TSpinLock>& guard)
    {
        auto loadFuture = LoadGuarded(resourceId, guard, /*isPreload*/ true);
        auto preloadState = New<TResourcePreloadState>();
        PreloadStatus_[resourceId] = preloadState;
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

                // A retired preload must not update its replacement.
                if (GetOrDefault(strongThis->PreloadStatus_, resourceId) != strongPreloadState) {
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
                    strongThis->PreloadStatus_.erase(resourceId);
                    // Unless a new load has already replaced the failed one.
                    strongThis->ResetResourceIfFailedGuarded(resourceId, guard);
                }
            })
                .Via(Invoker_));
    }

    // Forgets the load and recreates the resource object. Must be called with Lock_ held.
    void ResetResourceGuarded(TResourceId resourceId, const TGuard<TSpinLock>& guard)
    {
        ResourcesInitializationFutures_.erase(resourceId);
        auto retired = std::exchange(Resources_[resourceId], CreateResource(resourceId));
        GetFinalizerInvoker()->Invoke(BindResourceDependencies(resourceId));
        GetFinalizerInvoker()->Invoke(BIND([retired = std::move(retired)] () mutable {
            retired->Stop(TError(NYT::EErrorCode::Canceled, "Resource instance stopped"));
            retired.Reset();
        }));
        if (PreloadStatus_.contains(resourceId)) {
            StartPreloadGuarded(resourceId, guard);
        }
    }

    // Updates the queue size gauge, dropping it for an unloaded resource with an empty queue. Must be called with Lock_ held.
    void UpdateQueueSizeGaugeGuarded(TResourceId resourceId, i64 queueSize, const TGuard<TSpinLock>&)
    {
        if (queueSize == 0 && !ResourcesInitializationFutures_.contains(resourceId)) {
            ResourceMetrics_.erase(resourceId);
            return;
        }
        auto& resourceMetrics = ResourceMetrics_.try_emplace(resourceId, ManagerContext_->Profiler, resourceId).first->second;
        resourceMetrics.QueueSize.Update(queueSize);
    }

    // Resets a failed load or a resource stopped after loading. Must be called with Lock_ held.
    bool ResetResourceIfFailedGuarded(TResourceId resourceId, const TGuard<TSpinLock>& guard)
    {
        auto it = ResourcesInitializationFutures_.find(resourceId);
        if (it == ResourcesInitializationFutures_.end()) {
            return false;
        }
        auto result = it->second.TryGet();
        if (!result || (result->IsOK() && !GetOrCrash(Resources_, resourceId)->GetCancelableContext()->IsCanceled())) {
            return false;
        }
        for (const auto& [dependencyId, _] : GetOrCrash(ResourceSpecs_, resourceId)->Dependencies) {
            ResetResourceIfFailedGuarded(dependencyId, guard);
        }
        ResetResourceGuarded(resourceId, guard);
        return true;
    }

    // Creates the resource object for the given resourceId.
    IResourcePtr CreateResource(TResourceId resourceId, TResourceInstanceId assignedId = {})
    {
        const auto& resourceSpec = GetOrCrash(ResourceSpecs_, resourceId);

        ui64 incarnationGeneration = 0;
        if (auto it = ResourceInstanceStates_.find(resourceId); it != ResourceInstanceStates_.end()) {
            incarnationGeneration = it->second.IncarnationGeneration + 1;
            InstanceResourceIds_.erase(it->second.InstanceId);
        }
        auto resourceInstanceId = assignedId ? assignedId : TResourceInstanceId(TGuid::Create());
        InstanceResourceIds_.emplace(resourceInstanceId, resourceId);
        ResourceInstanceStates_[resourceId] = {
            .InstanceId = resourceInstanceId,
            .IncarnationGeneration = incarnationGeneration,
        };

        auto context = New<TResourceContext>();
        context->ResourceId = resourceId;
        context->ResourceInstanceId = resourceInstanceId;
        context->ResourceIncarnationGeneration = incarnationGeneration;
        context->ResourceSpec = resourceSpec;
        context->ResourceManager = MakeWeak(this);
        context->PipelineAuthenticator = ManagerContext_->PipelineAuthenticator;
        context->ClientsCache = ManagerContext_->ClientsCache;
        context->PipelinePath = ManagerContext_->PipelinePath;
        context->FileStorage = ManagerContext_->FileStorage;
        context->Invoker = Invoker_;
        context->Logger = Logger.WithTag("Resource", resourceId);
        context->Profiler = ManagerContext_->Profiler.WithTag("resource", resourceId.Underlying()).WithPrefix("/resource");
        context->StatusProfiler = ManagerContext_->StatusProfiler->WithPrefix(
            ExternallyAssigned_ ? Format("/resources/%v", resourceInstanceId) : Format("/resources/%v", resourceId));

        IResourcePtr resource;
        try {
            resource = TRegistry::Get()->CreateResource(context, BuildDynamicContext(resourceId));
        } catch (const std::exception& ex) {
            resource = New<TFailedResource>(context->StatusProfiler, TError(ex));
        }
        return resource;
    }

    TClosure BindResourceDependencies(TResourceId resourceId)
    {
        auto weakResource = MakeWeak(GetOrCrash(Resources_, resourceId));
        std::vector<IResourcePtr> dependencies;
        for (const auto& [dependencyId, _] : GetOrCrash(ResourceSpecs_, resourceId)->Dependencies) {
            if (auto dependency = GetOrDefault(Resources_, dependencyId)) {
                dependencies.push_back(std::move(dependency));
            }
        }
        // Late subscribers can run immediately; never invoke user callbacks under Lock_.
        return BIND([weakResource, dependencies = std::move(dependencies)] {
            auto resource = weakResource.Lock();
            if (!resource) {
                return;
            }
            for (const auto& dependency : dependencies) {
                dependency->GetCancelableContext()->PropagateTo(resource->GetCancelableContext());
            }
        });
    }

    bool OwnsResourceRecovery() const
    {
        // The controller replaces externally assigned instances. Controller-local managers
        // are recreated with their JobManager or on the next state-access request.
        // Other local managers recreate failed resources on the next load.
        return !ExternallyAssigned_ && !ManagerContext_->IsController;
    }

    TDynamicResourceContextPtr BuildDynamicContext(const TResourceId& resourceId)
    {
        auto dynamicContext = New<TDynamicResourceContext>();
        dynamicContext->DynamicResourceSpec = GetOrDefault(DynamicResourceSpecs_, resourceId, New<TDynamicResourceSpec>());
        dynamicContext->TargetRevision = GetOrDefault(TargetRevisions_, resourceId);
        return dynamicContext;
    }

    static TFuture<void> ValidateLoadResult(const IResourcePtr& resource, const TFuture<void>& futureLoaded)
    {
        return futureLoaded.Apply(BIND([resource] (const TError& error) {
            error.ThrowOnError();
            const auto status = resource->GetStatus();
            status.Error.ThrowOnError();
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

IResourceManagerPtr CreateResourceManager(TResourceManagerContextPtr managerContext)
{
    return New<TResourceManager>(
        std::move(managerContext),
        THashMap<TResourceId, TResourceSpecPtr>{},
        THashMap<TResourceId, TDynamicResourceSpecPtr>{},
        THashMap<TResourceId, TResourceRevisionPtr>{},
        THashMap<TResourceId, TResourceInstanceState>{},
        /*externallyAssigned*/ true);
}

IResourceManagerPtr CreateResourceManager(
    TResourceManagerContextPtr managerContext,
    const THashMap<TResourceId, TResourceSpecPtr>& resources,
    const THashMap<TResourceId, TDynamicResourceSpecPtr>& dynamicResourceSpecs,
    const THashMap<TResourceId, TResourceRevisionPtr>& targetRevisions,
    const THashMap<TResourceId, TResourceInstanceState>& predecessorInstanceStates)
{
    return New<TResourceManager>(
        std::move(managerContext),
        resources,
        dynamicResourceSpecs,
        targetRevisions,
        predecessorInstanceStates);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
