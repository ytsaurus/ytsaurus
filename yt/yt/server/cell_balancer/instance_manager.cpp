#include "instance_manager.h"

#include "allocator_adapter.h"
#include "config.h"
#include "cypress_bindings.h"
#include "input_state.h"
#include "mutations.h"
#include "pod_id_helpers.h"
#include "spare_instances.h"

namespace NYT::NCellBalancer {

using namespace NBundleControllerClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

////////////////////////////////////////////////////////////////////////////////

bool IsAllocationFailed(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "FAILED";
}

bool IsAllocationCompleted(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "COMPLETED";
}

////////////////////////////////////////////////////////////////////////////////

void TInstanceManager::ManageInstances(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    ISpareInstanceAllocatorPtr spareInstances,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    ProcessExistingAllocations(bundleName, adapter, spareInstances, input, mutations);
    ProcessExistingDeallocations(bundleName, adapter, input, mutations);

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    YT_VERIFY(bundleInfo->EnableBundleController);

    auto zoneIt = input.Zones.find(bundleInfo->Zone);

    if (zoneIt == input.Zones.end()) {
        YT_LOG_WARNING("Cannot locate zone for bundle (BundleName: %v, Zone: %v)",
            bundleName,
            bundleInfo->Zone);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "can_not_find_zone_for_bundle",
            .BundleName = bundleName,
            .Description = Format("Cannot locate zone %v for bundle %v.", bundleInfo->Zone, bundleName)
        });
        return;
    }

    const auto& [zoneName, zoneInfo] = *zoneIt;
    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        auto disruptionIt = input.DatacenterDisrupted.find(std::pair(zoneName, dataCenterName));
        if (disruptionIt != input.DatacenterDisrupted.end() && adapter->IsDataCenterDisrupted(disruptionIt->second)) {
            YT_LOG_WARNING("Instance management skipped for bundle due to zone unhealthy state"
                " (BundleName: %v, Zone: %v, DataCenter: %v, InstanceType: %v)",
                bundleName,
                zoneName,
                dataCenterName,
                adapter->GetInstanceType());

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "zone_is_disrupted",
                .BundleName = bundleName,
                .DataCenter = dataCenterName,
                .Description = Format("Zone %Qv is disrupted. Disabling all %v allocations within %Qv.",
                    zoneName,
                    adapter->GetHumanReadableInstanceType(),
                    dataCenterName),
            });
            continue;
        }

        InitNewAllocations(
            bundleName,
            zoneName,
            dataCenterName,
            adapter,
            input,
            mutations);

        InitNewDeallocations(
            bundleName,
            zoneName,
            dataCenterName,
            adapter,
            input,
            mutations);
    }
}

bool TInstanceManager::IsResourceUsageExceeded(const TInstanceResourcesPtr& usage, const TResourceQuotaPtr& quota)
{
    if (!quota) {
        return false;
    }

    // In order to support clusters without network limits enabled we check network quotas only if they are explicitly set for bundle.
    if (quota->Network > 0 && usage->NetBytes.value_or(0) > quota->Network) {
        return true;
    }

    return usage->Vcpu > quota->Vcpu() || usage->Memory > quota->Memory;
}

int TInstanceManager::GetAllocationCountInDataCenter(
    const TIndexedEntries<TAllocationRequestState>& allocationsState,
    const std::string& dataCenterName)
{
    return std::count_if(allocationsState.begin(), allocationsState.end(), [&] (const auto& record) {
        return record.second->DataCenter.value_or(DefaultDataCenterName) == dataCenterName;
    });
}

void TInstanceManager::InitNewAllocations(
    const std::string& bundleName,
    const std::string& zoneName,
    const std::string& dataCenterName,
    IAllocatorAdapter* adapter,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);
    const auto& dataCenterInfo = GetOrCrash(zoneInfo->DataCenters, dataCenterName);
    auto& allocationsState = adapter->AllocationsState();

    YT_VERIFY(bundleInfo->EnableBundleController);

    if (!adapter->IsNewAllocationAllowed(bundleInfo, dataCenterName, input)) {
        YT_LOG_DEBUG("New allocation is not allowed (BundleName: %v, DataCenter: %v, InstanceType: %v)",
            bundleName,
            dataCenterName,
            adapter->GetInstanceType());
        return;
    }

    int aliveInstanceCount = std::ssize(adapter->GetAliveInstances(dataCenterName));
    int targetInstanceCount = adapter->GetTargetInstanceCount(bundleInfo, zoneInfo);
    int currentDataCenterAllocations = GetAllocationCountInDataCenter(allocationsState, dataCenterName);
    int instanceCountToAllocate = targetInstanceCount - aliveInstanceCount - currentDataCenterAllocations;

    YT_LOG_DEBUG("Scheduling allocations (BundleName: %v, DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, "
        "AliveInstanceCount: %v, PlannedAllocationCount: %v, ExistingAllocationCount: %v)",
        bundleName,
        dataCenterName,
        adapter->GetInstanceType(),
        targetInstanceCount,
        aliveInstanceCount,
        instanceCountToAllocate,
        currentDataCenterAllocations);

    if (instanceCountToAllocate > 0 && adapter->IsInstanceCountLimitReached(bundleInfo->Zone, dataCenterName, zoneInfo, input)) {
        YT_LOG_WARNING("Instance count limit reached (BundleName: %v, DataCenter: %v, InstanceType: %v)",
            bundleName,
            dataCenterName,
            adapter->GetInstanceType());

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "zone_instance_limit_reached",
            .BundleName = bundleName,
            .DataCenter = dataCenterName,
            .Description = Format("Cannot allocate new %v at zone %v for bundle %v.",
                adapter->GetInstanceType(), bundleInfo->Zone, bundleName)
        });
        return;
    }

    const auto& resourceUsage = GetOrCrash(input.BundleResourceTarget, bundleName);
    if (instanceCountToAllocate > 0 && IsResourceUsageExceeded(resourceUsage, bundleInfo->ResourceQuota)) {
        YT_LOG_WARNING("Bundle resource usage exceeded quota (Bundle: %v, ResourceQuota: {Vcpu: %v, Memory: %v, NetworkBytes: %v}, ResourceUsage: {Vcpu: %v, Memory: %v, NetworkBytes: %v})",
            bundleName,
            bundleInfo->ResourceQuota->Vcpu(),
            bundleInfo->ResourceQuota->Memory,
            bundleInfo->ResourceQuota->Network,
            resourceUsage->Vcpu,
            resourceUsage->Memory,
            resourceUsage->NetBytes);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "bundle_resource_quota_exceeded",
            .BundleName = bundleName,
            .Description = Format("Cannot allocate new %v instance for bundle %v. ResourceQuota: {Vcpu: %v, Memory: %v, NetworkBytes: %v}, ResourceUsage: {Vcpu: %v, Memory: %v, NetworkBytes: %v}",
                adapter->GetInstanceType(),
                bundleName,
                bundleInfo->ResourceQuota->Vcpu(),
                bundleInfo->ResourceQuota->Memory,
                bundleInfo->ResourceQuota->Network,
                resourceUsage->Vcpu,
                resourceUsage->Memory,
                resourceUsage->NetBytes)
        });
        return;
    }

    if (instanceCountToAllocate == 0) {
        auto outdatedInstanceCount = GetOutdatedInstanceCount(adapter, dataCenterName, input, bundleName, bundleInfo);
        instanceCountToAllocate = std::min(outdatedInstanceCount, input.Config->ReallocateInstanceBudget);
    }

    for (int i = 0; i < instanceCountToAllocate; ++i) {
        std::string allocationId = ToString(TGuid::Create());

        YT_LOG_INFO("Init allocation for bundle (BundleName: %v, InstanceType %v, AllocationId: %v)",
            bundleName,
            adapter->GetInstanceType(),
            allocationId);

        auto spec = New<TAllocationRequestSpec>();
        spec->YPCluster = dataCenterInfo->YPCluster;

        spec->NannyService = adapter->GetNannyService(dataCenterInfo);
        *spec->ResourceRequest = *adapter->GetResourceGuarantee(bundleInfo);
        spec->InstanceRole = adapter->GetInstanceRole();
        spec->HostTagFilter = adapter->GetHostTagFilter(bundleInfo, input);
        spec->PodIdTemplate = GetPodIdTemplate(
            bundleName,
            dataCenterName,
            zoneInfo,
            adapter,
            input,
            mutations);

        auto request = mutations->NewMutation<TAllocationRequest>();
        request->Spec = spec;
        mutations->NewAllocations[allocationId] = request;
        auto allocationState = New<TAllocationRequestState>();
        allocationState->CreationTime = TInstant::Now();
        allocationState->PodIdTemplate = spec->PodIdTemplate;
        allocationState->DataCenter = dataCenterName;
        allocationsState[allocationId] = allocationState;
    }
}

int TInstanceManager::GetOutdatedInstanceCount(
    IAllocatorAdapter* adapter,
    const std::string& dataCenterName,
    const TSchedulerInputState& input,
    const std::string& bundleName,
    const TBundleInfoPtr& bundleInfo)
{
    int count = 0;
    const auto& targetResource = adapter->GetResourceGuarantee(bundleInfo);

    for (const auto& instanceName : adapter->GetAliveInstances(dataCenterName)) {
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        const auto& instanceResource = instanceInfo->BundleControllerAnnotations->Resource;

        if (*instanceResource != *targetResource) {
            YT_LOG_WARNING("Instance resource is outdated "
                "(BundleName: %v, InstanceName: %v, InstanceResource: %v, TargetResource: %v)",
                bundleName,
                instanceName,
                ConvertToYsonString(instanceResource, EYsonFormat::Text),
                ConvertToYsonString(targetResource, EYsonFormat::Text));

            ++count;
            continue;
        }

        if (!instanceInfo->CmsMaintenanceRequests.empty()) {
            YT_LOG_WARNING("Instance is requested for maintenance "
                "(BundleName: %v, InstanceName: %v, CmsMaintenanceRequests: %v)",
                bundleName,
                instanceName,
                ConvertToYsonString(instanceInfo->CmsMaintenanceRequests, EYsonFormat::Text));

            ++count;
            continue;
        }
    }

    return count;
}

std::string TInstanceManager::GetPodIdTemplate(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const TZoneInfoPtr& zoneInfo,
    IAllocatorAdapter* adapter,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    std::vector<std::string> knownPodIds;
    for (const auto& instanceName : adapter->GetInstances(dataCenterName)) {
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        knownPodIds.push_back(GetPodIdForInstance(instanceInfo->CypressAnnotations, instanceName));
    }

    for (const auto& [allocationId, state] : adapter->AllocationsState()) {
        if (state->DataCenter.value_or(DefaultDataCenterName) != dataCenterName) {
            continue;
        }

        if (state->PodIdTemplate.empty()) {
            YT_LOG_WARNING("Empty PodIdTemplate found in allocation state "
                "(AllocationId: %v, InstanceType: %v, BundleName: %v)",
                allocationId,
                adapter->GetInstanceType(),
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                    .Id = "unexpected_pod_id",
                    .BundleName = bundleName,
                    .Description = Format("Allocation request %v for bundle %v has empty pod_id",
                        allocationId, bundleName),
                });
        }

        knownPodIds.push_back(state->PodIdTemplate);
    }

    for (const auto& [id, state] : adapter->DeallocationsState()) {
        auto deallocationRequest = GetOrDefault(input.DeallocationRequests, id, nullptr);
        if (deallocationRequest) {
            knownPodIds.push_back(deallocationRequest->Spec->PodId);
            continue;
        }

        auto instanceInfo = adapter->FindInstanceInfo(state->InstanceName, input);
        if (instanceInfo) {
            knownPodIds.push_back(GetPodIdForInstance(instanceInfo->CypressAnnotations, state->InstanceName));
            continue;
        }

        // Neither deallocation request nor instance info found, so it probably can be ignored here.
    }

    auto clusterShortName = input.Config->Cluster;
    if (zoneInfo->ShortName && !zoneInfo->ShortName->empty()) {
        clusterShortName = *zoneInfo->ShortName;
    }

    auto instanceIndex = FindNextInstanceId(
        knownPodIds,
        clusterShortName,
        adapter->GetInstanceType());

    auto bundleShortName = GetOrCrash(input.BundleToShortName, bundleName);

    return GetInstancePodIdTemplate(
        clusterShortName,
        bundleShortName,
        adapter->GetInstanceType(),
        instanceIndex);
}

void TInstanceManager::ProcessExistingAllocations(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    ISpareInstanceAllocatorPtr spareInstances,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& allocationsState = adapter->AllocationsState();

    TIndexedEntries<TAllocationRequestState> aliveAllocations;
    for (const auto& [allocationId, allocationState] : allocationsState) {
        auto it = input.AllocationRequests.find(allocationId);
        if (it == input.AllocationRequests.end()) {
            YT_LOG_WARNING("Cannot find allocation (AllocationId: %v, InstanceType: %v, BundleName: %v)",
                allocationId,
                adapter->GetInstanceType(),
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "can_not_find_allocation_request",
                .BundleName = bundleName,
                .Description = Format("Allocation request %v "
                    "found in bundle state, but is absent in hulk allocations.",
                    allocationId),
            });
            // It is better to keep this allocation, otherwise there is a chance to
            // create create unbounded amount of new instances.
            aliveAllocations[allocationId] = allocationState;
            continue;
        }

        const auto& allocationInfo = it->second;
        auto allocationAge = TInstant::Now() - allocationState->CreationTime;

        if (IsAllocationFailed(allocationInfo)) {
            YT_LOG_WARNING("Allocation Failed (AllocationId: %v, InstanceType: %v, BundleName: %v)",
                allocationId,
                adapter->GetInstanceType(),
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "instance_allocation_failed",
                .BundleName = bundleName,
                .DataCenter = allocationState->DataCenter,
                .Description = Format("Allocation request %v has failed.",
                    allocationId),
            });

            aliveAllocations[allocationId] = allocationState;
            continue;
        }

        auto instanceName = LocateAllocatedInstance(allocationInfo, input);

        if (!instanceName.empty() && adapter->EnsureAllocatedInstanceTagsSet(
                instanceName,
                bundleName,
                allocationState->DataCenter.value_or(DefaultDataCenterName),
                allocationInfo,
                input,
                mutations))
        {
            YT_LOG_INFO("Instance allocation completed (InstanceName: %v, AllocationId: %v, BundleName: %v)",
                instanceName,
                allocationId,
                bundleName);

            if (!input.Config->HasInstanceAllocatorService) {
                mutations->CompletedAllocations.insert(mutations->WrapMutation(allocationId));
            }
            continue;
        }

        if (allocationAge > input.Config->HulkRequestTimeout) {
            YT_LOG_WARNING("Allocation request is stuck (BundleName: %v, AllocationId: %v, AllocationAge: %v, Threshold: %v)",
                bundleName,
                allocationId,
                allocationAge,
                input.Config->HulkRequestTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_instance_allocation",
                .BundleName = bundleName,
                .DataCenter = allocationState->DataCenter,
                .Description = Format("Found stuck allocation %v with age %v which is more than threshold %v.",
                    allocationId,
                    allocationAge,
                    input.Config->HulkRequestTimeout),
            });
        }

        if (input.Config->HasInstanceAllocatorService) {
            YT_LOG_DEBUG("Tracking existing allocation (AllocationId: %v, Bundle: %v, InstanceName: %v)",
                allocationId,
                bundleName,
                instanceName);
        } else {
            CompleteExistingAllocationWithoutInstanceAllocatorService(
                allocationId,
                allocationInfo,
                bundleName,
                adapter,
                spareInstances,
                input,
                mutations);
        }

        aliveAllocations[allocationId] = allocationState;
    }

    allocationsState = std::move(aliveAllocations);
}

void TInstanceManager::CompleteExistingAllocationWithoutInstanceAllocatorService(
    const std::string& allocationId,
    const auto& allocationInfo,
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    ISpareInstanceAllocatorPtr spareInstances,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    YT_LOG_DEBUG("Instance allocator service is disabled, allocating instance from spare "
        "(AllocationId: %v, BundleName: %v, InstanceType: %v)",
        allocationId,
        bundleName,
        adapter->GetInstanceType());

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneName = bundleInfo->Zone;
    const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);
    // Available for 1-dc clusters only.
    YT_VERIFY(std::ssize(zoneInfo->DataCenters) == 1);
    const auto& dataCenterName = zoneInfo->DataCenters.begin()->first;
    auto spareBundleName = zoneInfo->SpareBundleName;

    if (!spareInstances->HasInstances(zoneName, dataCenterName)) {
        YT_LOG_WARNING("No spare instances available for bundle (AllocationId: %v, BundleName: %v, InstanceType: %v)",
            allocationId,
            bundleName,
            adapter->GetInstanceType());

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "no_spare_instances_available",
            .BundleName = bundleName,
            .DataCenter = dataCenterName,
            .Description = Format("No spare instances of type %v are available for allocation request %v",
                adapter->GetInstanceType(),
                allocationId),
        });

        return;
    }

    auto instanceName = spareInstances->Allocate(zoneName, dataCenterName, bundleName);

    YT_LOG_INFO("Allocating instance from spare (AllocationId: %v, BundleName: %v, InstanceType: %v, InstanceName: %v)",
        allocationId,
        bundleName,
        adapter->GetInstanceType(),
        instanceName);

    const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
    const auto& currentAnnotations = instanceInfo->BundleControllerAnnotations;
    YT_VERIFY(currentAnnotations->AllocatedForBundle == spareBundleName);
    auto newAnnotations = NYTree::CloneYsonStruct(currentAnnotations);
    newAnnotations->AllocatedForBundle = bundleName;
    adapter->SetInstanceAnnotations(instanceName, newAnnotations, mutations);

    auto newAllocationStatus = New<TAllocationRequestStatus>();
    newAllocationStatus->State = "COMPLETED";
    newAllocationStatus->NodeId = instanceName;
    newAllocationStatus->PodId = "";

    YT_VERIFY(newAllocationStatus->NodeId == instanceName);

    auto newAllocationInfo = NYTree::CloneYsonStruct(allocationInfo);
    newAllocationInfo->Status = newAllocationStatus;
    mutations->ChangedAllocations[allocationId] = newAllocationInfo;
}

bool TInstanceManager::ReturnToBundleBalancer(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    const std::string& deallocationId,
    const TDeallocationRequestStatePtr& deallocationState,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& instanceName = deallocationState->InstanceName;

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, BundleName: %v, Strategy: %v)",
        deallocationId,
        instanceName,
        bundleName,
        DeallocationStrategyReturnToBB);

    if (!adapter->EnsureDeallocatedInstanceTagsSet(bundleName, instanceName, DeallocationStrategyReturnToBB, input, mutations)) {
        return true;
    }

    mutations->ChangedDecommissionedFlag[instanceName] = mutations->WrapMutation(false);
    mutations->ChangedNodeUserTags[instanceName] = {};

    return false;
}

bool TInstanceManager::ReturnToSpareBundle(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    const std::string& deallocationId,
    const TDeallocationRequestStatePtr& deallocationState,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& instanceName = deallocationState->InstanceName;
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);
    auto spareBundleName = zoneInfo->SpareBundleName;
    const auto& bundleControllerAnnotations = adapter->GetInstanceInfo(instanceName, input)->BundleControllerAnnotations;

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, AllocatedFor: %v, BundleName: %v, Strategy: %v)",
        deallocationId,
        instanceName,
        bundleControllerAnnotations->AllocatedForBundle,
        bundleName,
        DeallocationStrategyReturnToSpareBundle);

    if (!adapter->EnsureDeallocatedInstanceTagsSet(bundleName, instanceName, DeallocationStrategyReturnToSpareBundle, input, mutations)) {
        return true;
    }

    if (bundleControllerAnnotations->AllocatedForBundle != spareBundleName) {
        YT_VERIFY(bundleControllerAnnotations->AllocatedForBundle.empty());

        auto newAnnotations = NYTree::CloneYsonStruct(bundleControllerAnnotations);
        newAnnotations->AllocatedForBundle = spareBundleName;
        adapter->SetInstanceAnnotations(instanceName, newAnnotations, mutations);

        adapter->SetDefaultSpareAttributes(instanceName, mutations);

        // Avoid race condition between initializing new deallocations and
        // marking node as node not from this bundle.
        return true;
    }

    return false;
}

bool TInstanceManager::ProcessHulkDeallocation(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    const std::string& deallocationId,
    const TDeallocationRequestStatePtr& deallocationState,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    // Validating node tags
    const auto& instanceName = deallocationState->InstanceName;
    const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
    const auto& bundleControllerAnnotations = instanceInfo->BundleControllerAnnotations;

    if (!deallocationState->HulkRequestCreated && bundleControllerAnnotations->YPCluster.empty()) {
        mutations->AlertsToFire.push_back(TAlert{
            .Id = "invalid_instance_tags",
            .BundleName = bundleName,
            .Description = Format("Instance %v cannot be deallocated with hulk. Please check allocation tags",
                instanceName),
        });
        return true;
    }

    if (!deallocationState->HulkRequestCreated) {
        CreateHulkDeallocationRequest(deallocationId, instanceName, adapter, input, mutations);
        deallocationState->HulkRequestCreated = true;
        return true;
    }

    auto it = input.DeallocationRequests.find(deallocationId);
    if (it == input.DeallocationRequests.end()) {
        YT_LOG_WARNING("Cannot find deallocation (DeallocationId: %v, BundleName: %v)",
            deallocationId,
            bundleName);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "can_not_find_deallocation_request",
            .BundleName = bundleName,
            .Description = Format("Deallocation request %v "
                "found in bundle state, but is absent in hulk deallocations.",
                deallocationId),
        });
        // Keep deallocation request and wait for an in duty person.
        return true;
    }

    if (IsAllocationFailed(it->second)) {
        YT_LOG_WARNING("Deallocation failed (DeallocationId: %v, BundleName: %v, InstanceName: %v, Strategy: %v)",
            deallocationId,
            bundleName,
            instanceName,
            DeallocationStrategyHulkRequest);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "instance_deallocation_failed",
            .BundleName = bundleName,
            .DataCenter = deallocationState->DataCenter,
            .Description = Format("Deallocation request %v has failed.",
                deallocationId),
        });

        return true;
    }

    if (IsAllocationCompleted(it->second) &&
        adapter->EnsureDeallocatedInstanceTagsSet(bundleName, instanceName, DeallocationStrategyHulkRequest, input, mutations))
    {
        YT_LOG_INFO("Instance deallocation completed (InstanceName: %v, DeallocationId: %v, BundleName: %v, Strategy: %v)",
            instanceName,
            deallocationId,
            bundleName,
            DeallocationStrategyHulkRequest);
        return false;
    }

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, BundleName: %v, Strategy: %v)",
        deallocationId,
        instanceName,
        bundleName,
        DeallocationStrategyHulkRequest);
    return true;
}

// Returns false if current deallocation should not be tracked any more.
bool TInstanceManager::ProcessDeallocation(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    const std::string& deallocationId,
    const TDeallocationRequestStatePtr& deallocationState,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto deallocationAge = TInstant::Now() - deallocationState->CreationTime;
    if (deallocationAge > input.Config->HulkRequestTimeout) {
        YT_LOG_WARNING("Deallocation request is stuck (BundleName: %v, DeallocationId: %v, DeallocationAge: %v, Threshold: %v)",
            bundleName,
            deallocationId,
            deallocationAge,
            input.Config->HulkRequestTimeout);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "stuck_instance_deallocation",
            .BundleName = bundleName,
            .DataCenter = deallocationState->DataCenter,
            .Description = Format("Found stuck deallocation %v with age %v which is more than threshold %v.",
                deallocationId,
                deallocationAge,
                input.Config->HulkRequestTimeout),
        });
    }

    const auto& instanceName = deallocationState->InstanceName;
    if (!adapter->IsInstanceReadyToBeDeallocated(
        instanceName,
        deallocationId,
        deallocationAge,
        bundleName,
        input,
        mutations))
    {
        return true;
    }

    if (deallocationState->Strategy == DeallocationStrategyHulkRequest) {
        return ProcessHulkDeallocation(bundleName, adapter, deallocationId, deallocationState, input, mutations);
    } else if (deallocationState->Strategy == DeallocationStrategyReturnToBB) {
        return ReturnToBundleBalancer(bundleName, adapter, deallocationId, deallocationState, input, mutations);
    } else if (deallocationState->Strategy == DeallocationStrategyReturnToSpareBundle) {
        return ReturnToSpareBundle(bundleName, adapter, deallocationId, deallocationState, input, mutations);
    }

    YT_LOG_WARNING("Unknown deallocation strategy (BundleName: %v, DeallocationId: %v, DeallocationStrategy: %v)",
        bundleName,
        deallocationId,
        deallocationState->Strategy);

    mutations->AlertsToFire.push_back(TAlert{
        .Id = "unknown_deallocation_strategy",
        .BundleName = bundleName,
        .Description = Format("Unknown deallocation strategy %Qv for deallocation %v.",
            deallocationState->Strategy,
            deallocationId),
    });

    return true;
}

void TInstanceManager::ProcessExistingDeallocations(
    const std::string& bundleName,
    IAllocatorAdapter* adapter,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& deallocations = adapter->DeallocationsState();
    TIndexedEntries<TDeallocationRequestState> aliveDeallocations;

    for (const auto& [deallocationId, deallocationState] : deallocations) {
        if (ProcessDeallocation(bundleName, adapter, deallocationId, deallocationState, input, mutations)) {
            aliveDeallocations[deallocationId] = deallocationState;
        }
    }

    deallocations.swap(aliveDeallocations);
}

void TInstanceManager::CreateHulkDeallocationRequest(
    const std::string& deallocationId,
    const std::string& instanceName,
    IAllocatorAdapter* adapter,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
    const auto& bundleControllerAnnotations = instanceInfo->BundleControllerAnnotations;
    const auto& cypressAnnotations = instanceInfo->CypressAnnotations;
    YT_VERIFY(bundleControllerAnnotations->DeallocationStrategy.empty() ||
        bundleControllerAnnotations->DeallocationStrategy == DeallocationStrategyHulkRequest);

    auto request = mutations->NewMutation<TDeallocationRequest>();
    auto& spec = request->Spec;
    spec->YPCluster = bundleControllerAnnotations->YPCluster;
    spec->PodId = GetPodIdForInstance(cypressAnnotations, instanceName);
    spec->InstanceRole = adapter->GetInstanceRole();
    mutations->NewDeallocations[deallocationId] = request;
}

void TInstanceManager::InitNewDeallocations(
    const std::string& bundleName,
    const std::string& zoneName,
    const std::string& dataCenterName,
    IAllocatorAdapter* adapter,
    const TSchedulerInputState& input,
    TSchedulerMutations* /*mutations*/)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    YT_VERIFY(bundleInfo->EnableBundleController);

    const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);

    if (!adapter->IsNewDeallocationAllowed(bundleInfo, dataCenterName, input)) {
        YT_LOG_DEBUG("New deallocation is not allowed (BundleName: %v, DataCenter: %v, InstanceType: %v)",
            bundleName,
            dataCenterName,
            adapter->GetInstanceType());
        return;
    }

    auto aliveInstances = adapter->GetAliveInstances(dataCenterName);
    auto targetInstanceCount = adapter->GetTargetInstanceCount(bundleInfo, zoneInfo);
    auto instanceCountToDeallocate = std::ssize(aliveInstances) - targetInstanceCount;
    auto& deallocationsState = adapter->DeallocationsState();

    YT_LOG_DEBUG("Scheduling deallocations (BundleName: %v, DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, AliveInstanceCount: %v, "
        "PlannedDeallocationCount: %v, ExistingDeallocationCount: %v)",
        bundleName,
        dataCenterName,
        adapter->GetInstanceType(),
        targetInstanceCount,
        std::ssize(aliveInstances),
        instanceCountToDeallocate,
        std::ssize(deallocationsState));

    if (instanceCountToDeallocate <= 0) {
        return;
    }

    const auto instancesToRemove = adapter->PickInstancesToDeallocate(
        instanceCountToDeallocate,
        dataCenterName,
        bundleInfo,
        input);

    for (const auto& instanceName : instancesToRemove) {
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);

        std::string deallocationId = ToString(TGuid::Create());
        auto deallocationState = New<TDeallocationRequestState>();
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->InstanceName = instanceName;
        deallocationState->DataCenter = dataCenterName;
        deallocationState->Strategy = instanceInfo->BundleControllerAnnotations->DeallocationStrategy;

        if (deallocationState->Strategy.empty()) {
            if (input.Config->HasInstanceAllocatorService) {
                deallocationState->Strategy = DeallocationStrategyHulkRequest;
            } else {
                deallocationState->Strategy = DeallocationStrategyReturnToSpareBundle;
            }
        }

        deallocationsState[deallocationId] = deallocationState;

        YT_LOG_INFO("Init instance deallocation (BundleName: %v, InstanceName: %v, InstanceType: %v, DeallocationId: %v, Strategy: %v)",
            bundleName,
            instanceName,
            adapter->GetInstanceType(),
            deallocationId,
            deallocationState->Strategy);
    }
}

std::string TInstanceManager::LocateAllocatedInstance(
    const TAllocationRequestPtr& requestInfo,
    const TSchedulerInputState& input) const
{
    if (!IsAllocationCompleted(requestInfo)) {
        return {};
    }

    if (!input.Config->HasInstanceAllocatorService && !requestInfo->Status->NodeId.empty()) {
        YT_LOG_DEBUG("Found allocated instance (InstanceName: %v)",
            requestInfo->Status->NodeId);
        return requestInfo->Status->NodeId;
    }

    const auto& podId = requestInfo->Status->PodId;
    auto it = input.PodIdToInstanceName.find(podId);
    if (it != input.PodIdToInstanceName.end()) {
        YT_LOG_DEBUG("Found allocated instance (PodId: %v, InstanceName: %v)",
            it->first,
            it->second);
        return it->second;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
