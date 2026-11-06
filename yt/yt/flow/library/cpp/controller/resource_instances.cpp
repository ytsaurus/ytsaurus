#include "resource_instances.h"

#include "job_balancer_common.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

namespace {

using TResourceInstanceIndex = THashMap<std::string, THashMap<TIncarnationId, THashMap<TResourceId, TResourceInstanceId>>>;

TResourceInstanceIndex BuildResourceInstanceIndex(const TFlowViewPtr& flowView)
{
    TResourceInstanceIndex result;
    for (const auto& [instanceId, instance] : flowView->State->ExecutionSpec->Layout->ResourceInstances) {
        EmplaceOrCrash(result[instance->WorkerAddress][instance->WorkerIncarnationId], instance->ResourceId, instanceId);
    }
    return result;
}

std::vector<TResourceInstanceId> GetDependencyInstances(
    const TPipelineSpecPtr& spec,
    const TResourceInstanceIndex& index,
    const TResourceInstancePtr& instance)
{
    const auto& ids = GetOrCrash(GetOrCrash(index, instance->WorkerAddress), instance->WorkerIncarnationId);
    std::vector<TResourceInstanceId> result;
    for (const auto& [resourceId, _] : GetOrCrash(spec->Resources, instance->ResourceId)->Dependencies) {
        if (auto it = ids.find(resourceId); it != ids.end()) {
            result.push_back(it->second);
        }
    }
    return result;
}

THashSet<TResourceInstanceId> GetJobResourceInstances(const TFlowViewPtr& flowView)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    THashMap<std::string, THashMap<TIncarnationId, THashSet<TResourceId>>> requiredResources;
    for (const auto& [_, job] : layout->Jobs) {
        const auto& computation = GetOrCrash(spec->Computations, GetOrCrash(layout->Partitions, job->PartitionId)->ComputationId);
        auto& required = requiredResources[job->WorkerAddress][job->WorkerIncarnationId];
        for (const auto& [resourceId, description] : computation->RequiredResourceIds) {
            if (description->Worker) {
                required.insert(resourceId);
            }
        }
    }
    THashSet<TResourceInstanceId> result;
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        auto worker = requiredResources.find(instance->WorkerAddress);
        if (worker == requiredResources.end()) {
            continue;
        }
        auto incarnation = worker->second.find(instance->WorkerIncarnationId);
        if (incarnation != worker->second.end() && (incarnation->second.contains(instance->ResourceId) || GetOrCrash(spec->Resources, instance->ResourceId)->AlwaysOn)) {
            result.insert(instanceId);
        }
    }
    return result;
}

void RestartFailedResourceInstances(const TFlowViewPtr& flowView, const THashSet<TResourceInstanceId>& jobResourceInstances)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    std::vector<TResourceInstanceId> failed;
    for (const auto& [address, status] : flowView->Feedback->WorkerStatuses) {
        auto worker = GetOrDefault(flowView->State->Workers, address);
        if (!worker || status->WorkerIncarnationId != worker->IncarnationId) {
            continue;
        }
        for (const auto& [instanceId, instanceStatus] : status->ResourceInstanceStatuses) {
            auto instance = layout->ResourceInstances.FindPtr(instanceId);
            if (instance && (*instance)->WorkerAddress == address &&
                (*instance)->WorkerIncarnationId == worker->IncarnationId &&
                instanceStatus->LoadState == EResourceLoadState::Failed)
            {
                failed.push_back(instanceId);
            }
        }
    }
    if (failed.empty()) {
        return;
    }
    auto index = BuildResourceInstanceIndex(flowView);
    const auto& spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    THashMap<TResourceInstanceId, std::vector<TResourceInstanceId>> dependents;
    for (const auto& [instanceId, instance] : layout->ResourceInstances) {
        for (auto dependencyId : GetDependencyInstances(spec, index, instance)) {
            dependents[dependencyId].push_back(instanceId);
        }
    }
    for (auto failedId : failed) {
        if (!layout->ResourceInstances.contains(failedId)) {
            continue;
        }
        THashSet<TResourceInstanceId> replaced{failedId};
        std::vector<TResourceInstanceId> pending{failedId};
        bool hasJob = false;
        while (!pending.empty()) {
            auto instanceId = pending.back();
            pending.pop_back();
            hasJob |= jobResourceInstances.contains(instanceId);
            if (auto it = dependents.find(instanceId); it != dependents.end()) {
                for (auto dependentId : it->second) {
                    if (replaced.insert(dependentId).second) {
                        pending.push_back(dependentId);
                    }
                }
            }
        }
        if (!hasJob) {
            for (auto instanceId : replaced) {
                layout->ResourceInstances.erase(instanceId);
            }
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void EnsureResourceInstances(const TFlowViewPtr& flowView)
{
    auto state = flowView->State->ExecutionSpec->PipelineState->GetValue();
    if (state != EPipelineState::Working && state != EPipelineState::Draining) {
        return;
    }
    auto jobResourceInstances = GetJobResourceInstances(flowView);
    RestartFailedResourceInstances(flowView, jobResourceInstances);
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    auto index = BuildResourceInstanceIndex(flowView);
    THashMap<TResourceId, std::vector<TResourceId>> resourceDependents;
    for (const auto& [resourceId, resourceSpec] : spec->Resources) {
        for (const auto& [dependencyId, _] : resourceSpec->Dependencies) {
            resourceDependents[dependencyId].push_back(resourceId);
        }
    }
    THashSet<TResourceInstanceId> desired;
    for (const auto& [address, worker] : flowView->State->Workers) {
        TWorkerSpecPtr workerSpec;
        if (auto it = layout->WorkerSpecs.FindPtr(address);
            it && (!(*it)->WorkerIncarnationId || *(*it)->WorkerIncarnationId == worker->IncarnationId))
        {
            workerSpec = *it;
        }
        std::vector<TResourceId> pending;
        for (const auto& [resourceId, resourceSpec] : spec->Resources) {
            if (resourceSpec->PreloadRequired && (!workerSpec || !workerSpec->PreloadResources.contains(resourceId))) {
                pending.push_back(resourceId);
            }
        }
        THashSet<TResourceId> unavailable;
        while (!pending.empty()) {
            auto resourceId = pending.back();
            pending.pop_back();
            if (!unavailable.insert(resourceId).second) {
                continue;
            }
            if (auto it = resourceDependents.find(resourceId); it != resourceDependents.end()) {
                pending.insert(pending.end(), it->second.begin(), it->second.end());
            }
        }
        auto& ids = index[address][worker->IncarnationId];
        std::function<TResourceInstanceId(const TResourceId&)> ensure = [&] (const TResourceId& resourceId) -> TResourceInstanceId {
            if (auto it = ids.find(resourceId); it != ids.end()) {
                const auto instanceId = it->second;
                if (desired.insert(instanceId).second) {
                    for (const auto& [dependencyId, _] : GetOrCrash(spec->Resources, resourceId)->Dependencies) {
                        ensure(dependencyId);
                    }
                }
                return instanceId;
            }
            auto instance = New<TResourceInstance>();
            instance->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
            instance->ResourceId = resourceId;
            instance->WorkerAddress = address;
            instance->WorkerIncarnationId = worker->IncarnationId;
            ids.emplace(resourceId, instance->ResourceInstanceId);
            for (const auto& [dependencyId, _] : GetOrCrash(spec->Resources, resourceId)->Dependencies) {
                ensure(dependencyId);
            }
            layout->ResourceInstances.emplace(instance->ResourceInstanceId, instance);
            desired.insert(instance->ResourceInstanceId);
            return instance->ResourceInstanceId;
        };
        for (const auto& [_, computation] : spec->Computations) {
            if (!NBalancer::WorkerBelongsToGroup(worker, computation->WorkerGroup)) {
                continue;
            }
            for (const auto& [resourceId, description] : computation->RequiredResourceIds) {
                if (description->Worker && !GetOrCrash(spec->Resources, resourceId)->PreloadRequired && !unavailable.contains(resourceId)) {
                    ensure(resourceId);
                }
            }
        }
        if (workerSpec) {
            for (const auto& resourceId : workerSpec->PreloadResources) {
                if (!unavailable.contains(resourceId)) {
                    ensure(resourceId);
                }
            }
        }
    }
    std::function<void(TResourceInstanceId)> keep = [&] (TResourceInstanceId instanceId) {
        if (desired.insert(instanceId).second) {
            if (auto instance = layout->ResourceInstances.FindPtr(instanceId)) {
                for (auto dependencyId : GetDependencyInstances(spec, index, *instance)) {
                    keep(dependencyId);
                }
            }
        }
    };
    for (auto instanceId : jobResourceInstances) {
        keep(instanceId);
    }
    std::vector<TResourceInstanceId> removed;
    for (const auto& [instanceId, _] : layout->ResourceInstances) {
        if (!desired.contains(instanceId)) {
            removed.push_back(instanceId);
        }
    }
    for (auto instanceId : removed) {
        layout->ResourceInstances.erase(instanceId);
    }
}

void ProjectResourceInstanceStatuses(const TFlowViewPtr& flowView)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    for (auto& [address, status] : flowView->Feedback->WorkerStatuses) {
        status = NYTree::CloneYsonStruct(status);
        status->ResourceStatuses.clear();
        status->PreloadedResourceStates.clear();
        auto worker = GetOrDefault(flowView->State->Workers, address);
        if (!worker || status->WorkerIncarnationId != worker->IncarnationId) {
            continue;
        }
        for (const auto& [instanceId, instanceStatus] : status->ResourceInstanceStatuses) {
            auto instance = layout->ResourceInstances.FindPtr(instanceId);
            if (!instance || (*instance)->WorkerAddress != address ||
                (*instance)->WorkerIncarnationId != worker->IncarnationId ||
                (*instance)->ResourceId != instanceStatus->ResourceId)
            {
                continue;
            }
            if (instanceStatus->LoadState == EResourceLoadState::NotLoaded) {
                continue;
            }
            status->ResourceStatuses.emplace(instanceStatus->ResourceId, instanceStatus);
            if (auto workerSpec = layout->WorkerSpecs.FindPtr(address);
                workerSpec && (*workerSpec)->PreloadResources.contains(instanceStatus->ResourceId))
            {
                if (instanceStatus->LoadState == EResourceLoadState::Loaded) {
                    status->PreloadedResourceStates.emplace(instanceStatus->ResourceId, EPreloadedResourceState::Preloaded);
                } else if (instanceStatus->LoadState == EResourceLoadState::Loading) {
                    status->PreloadedResourceStates.emplace(instanceStatus->ResourceId, EPreloadedResourceState::Preloading);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
