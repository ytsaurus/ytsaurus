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

bool IsAllocationFailed(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "FAILED";
}

bool IsAllocationCompleted(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "COMPLETED";
}

////////////////////////////////////////////////////////////////////////////////

TInstanceManager::TInstanceManager(
    std::string bundleName,
    const TSchedulerInputState& input,
    ISpareInstanceAllocatorPtr spareInstanceAllocator,
    IAllocatorAdapter* adapter,
    TSchedulerMutations* mutations)
    : BundleName_(std::move(bundleName))
    , Input_(input)
    , SpareInstanceAllocator_(std::move(spareInstanceAllocator))
    , Adapter_(adapter)
    , Mutations_(mutations)
    , Logger(BundleControllerLogger().WithTag("Bundle: %v", BundleName_))
{ }

void TInstanceManager::ManageInstances()
{
    ProcessExistingAllocations();
    ProcessExistingDeallocations();

    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    YT_VERIFY(bundleInfo->EnableBundleController);

    auto zoneIt = Input_.Zones.find(bundleInfo->Zone);

    if (zoneIt == Input_.Zones.end()) {
        YT_LOG_WARNING("Cannot locate zone for bundle (Zone: %v)",
            bundleInfo->Zone);

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "can_not_find_zone_for_bundle",
            .BundleName = BundleName_,
            .Description = Format("Cannot locate zone %v for bundle %v.", bundleInfo->Zone, BundleName_)
        });
        return;
    }

    const auto& [zoneName, zoneInfo] = *zoneIt;
    for (const auto& [dataCenterName, _] : zoneInfo->DataCenters) {
        auto disruptionIt = Input_.DataCenterDisruptionStatuses.find(std::pair(zoneName, dataCenterName));
        if (disruptionIt != Input_.DataCenterDisruptionStatuses.end() &&
            Adapter_->IsDataCenterDisrupted(disruptionIt->second))
        {
            YT_LOG_WARNING("Instance management skipped for bundle due to zone unhealthy state "
                "(Zone: %v, DataCenter: %v, InstanceType: %v)",
                zoneName,
                dataCenterName,
                Adapter_->GetInstanceType());

            Mutations_->AlertsToFire.push_back(TAlert{
                .Id = "zone_is_disrupted",
                .BundleName = BundleName_,
                .DataCenter = dataCenterName,
                .Description = Format("Zone %Qv is disrupted. Disabling all %v allocations within %Qv.",
                    zoneName,
                    Adapter_->GetHumanReadableInstanceType(),
                    dataCenterName),
            });
            continue;
        }

        InitNewAllocations(zoneName, dataCenterName);
        InitNewDeallocations(zoneName, dataCenterName);
    }
}

bool TInstanceManager::IsResourceUsageExceeded(const TInstanceResourcesPtr& usage, const TResourceQuotaPtr& quota)
{
    if (!quota) {
        return false;
    }

    // In order to support clusters without network limits enabled
    // we check network quotas only if they are explicitly set for bundle.
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

bool TInstanceManager::CheckInstanceCountLimitReached(
    const std::string& zoneName,
    const std::string& dataCenterName,
    const TZoneInfoPtr& zoneInfo)
{
    if (Adapter_->IsInstanceCountLimitReached(zoneName, dataCenterName, zoneInfo, Input_)) {
        YT_LOG_WARNING("Instance count limit reached (DataCenter: %v, InstanceType: %v)",
            dataCenterName,
            Adapter_->GetInstanceType());

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "zone_instance_limit_reached",
            .BundleName = BundleName_,
            .DataCenter = dataCenterName,
            .Description = Format("Cannot allocate new %v at zone %v for bundle %v.",
                Adapter_->GetInstanceType(), zoneName, BundleName_)
        });
        return true;
    }

    return false;
}

bool TInstanceManager::CheckResourceUsageExceeded(const TResourceQuotaPtr& resourceQuota)
{
    const auto& resourceUsage = GetOrCrash(Input_.BundleResourceTarget, BundleName_);
    if (IsResourceUsageExceeded(resourceUsage, resourceQuota)) {
        YT_LOG_WARNING("Bundle resource usage exceeded quota "
            "(ResourceQuota: {Vcpu: %v, Memory: %v, NetworkBytes: %v}, "
            "ResourceUsage: {Vcpu: %v, Memory: %v, NetworkBytes: %v})",
            resourceQuota->Vcpu(),
            resourceQuota->Memory,
            resourceQuota->Network,
            resourceUsage->Vcpu,
            resourceUsage->Memory,
            resourceUsage->NetBytes);

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "bundle_resource_quota_exceeded",
            .BundleName = BundleName_,
            .Description = Format("Cannot allocate new %v instance for bundle %v. "
                "ResourceQuota: {Vcpu: %v, Memory: %v, NetworkBytes: %v}, "
                "ResourceUsage: {Vcpu: %v, Memory: %v, NetworkBytes: %v}",
                Adapter_->GetInstanceType(),
                BundleName_,
                resourceQuota->Vcpu(),
                resourceQuota->Memory,
                resourceQuota->Network,
                resourceUsage->Vcpu,
                resourceUsage->Memory,
                resourceUsage->NetBytes)
        });
        return true;
    }

    return false;
}

TAllocationRequestStatePtr TInstanceManager::DoCreateAllocationRequest(
    const std::string& allocationId,
    const TBundleInfoPtr& bundleInfo,
    const std::string& dataCenterName,
    const TDataCenterInfoPtr& dataCenterInfo,
    const TZoneInfoPtr& zoneInfo)
{
    YT_LOG_INFO("Init allocation for bundle (InstanceType: %v, AllocationId: %v)",
        Adapter_->GetInstanceType(),
        allocationId);

    auto spec = New<TAllocationRequestSpec>();
    spec->YPCluster = dataCenterInfo->YPCluster;

    spec->NannyService = Adapter_->GetNannyService(dataCenterInfo);
    *spec->ResourceRequest = *Adapter_->GetResourceGuarantee(bundleInfo);
    spec->InstanceRole = Adapter_->GetInstanceRole();
    spec->HostTagFilter = Adapter_->GetHostTagFilter(bundleInfo, Input_);
    spec->PodIdTemplate = GetPodIdTemplate(
        BundleName_,
        dataCenterName,
        zoneInfo,
        Adapter_,
        Input_,
        Mutations_);

    auto request = Mutations_->NewMutation<TAllocationRequest>();
    request->Spec = spec;
    Mutations_->NewAllocations[allocationId] = request;
    auto allocationState = New<TAllocationRequestState>();
    allocationState->CreationTime = TInstant::Now();
    allocationState->PodIdTemplate = spec->PodIdTemplate;
    allocationState->DataCenter = dataCenterName;

    return allocationState;
}

void TInstanceManager::InitNewAllocations(
    const std::string& zoneName,
    const std::string& dataCenterName)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& zoneInfo = GetOrCrash(Input_.Zones, zoneName);
    const auto& dataCenterInfo = GetOrCrash(zoneInfo->DataCenters, dataCenterName);
    auto& allocationsState = Adapter_->AllocationsState();

    YT_VERIFY(bundleInfo->EnableBundleController);

    if (!Adapter_->IsNewAllocationAllowed(bundleInfo, dataCenterName, Input_)) {
        YT_LOG_DEBUG("New allocation is not allowed (DataCenter: %v, InstanceType: %v)",
            dataCenterName,
            Adapter_->GetInstanceType());
        return;
    }

    int aliveInstanceCount = std::ssize(Adapter_->GetAliveInstances(dataCenterName));
    int targetInstanceCount = Adapter_->GetTargetInstanceCount(bundleInfo, zoneInfo);
    int currentDataCenterAllocations = GetAllocationCountInDataCenter(allocationsState, dataCenterName);
    int instanceCountToAllocate = targetInstanceCount - aliveInstanceCount - currentDataCenterAllocations;
    int instanceCountToReallocate = GetInstanceCountToReallocate(dataCenterName);

    if (instanceCountToAllocate == 0 && instanceCountToReallocate == 0) {
        return;
    }

    YT_LOG_DEBUG("Scheduling allocations "
        "(DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, "
        "AliveInstanceCount: %v, PlannedAllocationCount: %v, ExistingAllocationCount: %v, "
        "InstanceCountToReallocate: %v)",
        dataCenterName,
        Adapter_->GetInstanceType(),
        targetInstanceCount,
        aliveInstanceCount,
        instanceCountToAllocate,
        currentDataCenterAllocations,
        instanceCountToReallocate);

    if (instanceCountToAllocate > 0) {
        if (CheckInstanceCountLimitReached(bundleInfo->Zone, dataCenterName, zoneInfo)) {
            return;
        }

        if (CheckResourceUsageExceeded(bundleInfo->ResourceQuota)) {
            return;
        }
    }

    if (instanceCountToAllocate == 0) {
        instanceCountToAllocate = std::min(
            instanceCountToReallocate,
            Input_.Config->ReallocateInstanceBudget);
    }

    for (int i = 0; i < instanceCountToAllocate; ++i) {
        auto allocationId = ToString(TGuid::Create());
        auto allocation = DoCreateAllocationRequest(
            allocationId,
            bundleInfo,
            dataCenterName,
            dataCenterInfo,
            zoneInfo);

        allocationsState[allocationId] = std::move(allocation);
    }
}

int TInstanceManager::GetInstanceCountToReallocate(const std::string& dataCenterName)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    const auto& targetResource = Adapter_->GetResourceGuarantee(bundleInfo);

    int count = 0;

    for (const auto& instanceName : Adapter_->GetAliveInstances(dataCenterName)) {
        const auto& instanceInfo = Adapter_->GetInstanceInfo(instanceName, Input_);
        const auto& instanceResource = instanceInfo->BundleControllerAnnotations->Resource;

        if (*instanceResource != *targetResource) {
            YT_LOG_WARNING("Instance resource is outdated "
                "(InstanceName: %v, InstanceResource: %v, TargetResource: %v)",
                instanceName,
                ConvertToYsonString(instanceResource, EYsonFormat::Text),
                ConvertToYsonString(targetResource, EYsonFormat::Text));

            ++count;
            continue;
        }

        if (!instanceInfo->CmsMaintenanceRequests.empty()) {
            YT_LOG_WARNING("Instance is requested for maintenance "
                "(InstanceName: %v, CmsMaintenanceRequests: %v)",
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
                "(AllocationId: %v, InstanceType: %v)",
                allocationId,
                adapter->GetInstanceType());

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

bool TInstanceManager::ProcessAllocation(
    const std::string& allocationId,
    const TAllocationRequestStatePtr& allocationState)
{
    auto it = Input_.AllocationRequests.find(allocationId);
    if (it == Input_.AllocationRequests.end()) {
        YT_LOG_WARNING("Cannot find allocation (AllocationId: %v, InstanceType: %v)",
            allocationId,
            Adapter_->GetInstanceType());

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "can_not_find_allocation_request",
            .BundleName = BundleName_,
            .Description = Format("Allocation request %v "
                "found in bundle state, but is absent in hulk allocations.",
                allocationId),
        });

        // It is better to keep this allocation, otherwise there is a chance to
        // create create unbounded amount of new instances.
        return true;
    }

    const auto& allocationInfo = it->second;

    if (IsAllocationFailed(allocationInfo)) {
        YT_LOG_WARNING("Allocation failed (AllocationId: %v, InstanceType: %v)",
            allocationId,
            Adapter_->GetInstanceType());

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "instance_allocation_failed",
            .BundleName = BundleName_,
            .DataCenter = allocationState->DataCenter,
            .Description = Format("Allocation request %v has failed.",
                allocationId),
        });

        return true;
    }

    auto instanceName = LocateAllocatedInstance(allocationInfo, Input_);

    if (!instanceName.empty() && Adapter_->EnsureAllocatedInstanceTagsSet(
        instanceName,
        BundleName_,
        allocationState->DataCenter.value_or(DefaultDataCenterName),
        allocationInfo,
        Input_,
        Mutations_))
    {
        YT_LOG_INFO("Instance allocation completed (InstanceName: %v, AllocationId: %v)",
            instanceName,
            allocationId);

        if (!Input_.Config->HasInstanceAllocatorService) {
            Mutations_->CompletedAllocations.insert(Mutations_->WrapMutation(allocationId));
        }
        return false;
    }

    auto allocationAge = TInstant::Now() - allocationState->CreationTime;
    if (allocationAge > Input_.Config->HulkRequestTimeout) {
        YT_LOG_WARNING("Allocation request is stuck (AllocationId: %v, AllocationAge: %v, Threshold: %v)",
            allocationId,
            allocationAge,
            Input_.Config->HulkRequestTimeout);

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "stuck_instance_allocation",
            .BundleName = BundleName_,
            .DataCenter = allocationState->DataCenter,
            .Description = Format("Found stuck allocation %v with age %v which is more than threshold %v.",
                allocationId,
                allocationAge,
                Input_.Config->HulkRequestTimeout),
        });
    }

    if (Input_.Config->HasInstanceAllocatorService) {
        YT_LOG_DEBUG("Tracking existing allocation (AllocationId: %v, InstanceName: %v)",
            allocationId,
            instanceName);
    } else {
        CompleteExistingAllocationWithoutInstanceAllocatorService(
            allocationId,
            allocationInfo,
            BundleName_,
            Adapter_,
            SpareInstanceAllocator_,
            Input_,
            Mutations_);
    }

    return true;
}

void TInstanceManager::ProcessExistingAllocations()
{
    auto& allocations = Adapter_->AllocationsState();
    TIndexedEntries<TAllocationRequestState> aliveAllocations;

    for (const auto& [allocationId, allocationState] : allocations) {
        if (ProcessAllocation(allocationId, allocationState)) {
            aliveAllocations[allocationId] = allocationState;
        }
    }

    allocations = std::move(aliveAllocations);
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
        "(AllocationId: %v, InstanceType: %v)",
        allocationId,
        adapter->GetInstanceType());

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& zoneName = bundleInfo->Zone;
    const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);
    // Available for 1-dc clusters only.
    YT_VERIFY(std::ssize(zoneInfo->DataCenters) == 1);
    const auto& dataCenterName = zoneInfo->DataCenters.begin()->first;
    auto spareBundleName = zoneInfo->SpareBundleName;

    if (!spareInstances->HasInstances(zoneName, dataCenterName)) {
        YT_LOG_WARNING("No spare instances available for bundle (AllocationId: %v, InstanceType: %v)",
            allocationId,
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

    YT_LOG_INFO("Allocating instance from spare (AllocationId: %v, InstanceType: %v, InstanceName: %v)",
        allocationId,
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

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, Strategy: %v)",
        deallocationId,
        instanceName,
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

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, AllocatedFor: %v, Strategy: %v)",
        deallocationId,
        instanceName,
        bundleControllerAnnotations->AllocatedForBundle,
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
        YT_LOG_WARNING("Cannot find deallocation (DeallocationId: %v)",
            deallocationId);

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
        YT_LOG_WARNING("Deallocation failed (DeallocationId: %v, InstanceName: %v, Strategy: %v)",
            deallocationId,
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
        YT_LOG_INFO("Instance deallocation completed "
            "(InstanceName: %v, DeallocationId: %v, Strategy: %v)",
            instanceName,
            deallocationId,
            DeallocationStrategyHulkRequest);
        return false;
    }

    YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, Strategy: %v)",
        deallocationId,
        instanceName,
        DeallocationStrategyHulkRequest);
    return true;
}

// Returns false if current deallocation should not be tracked any more.
bool TInstanceManager::ProcessDeallocation(
    const std::string& deallocationId,
    const TDeallocationRequestStatePtr& deallocationState)
{
    auto deallocationAge = TInstant::Now() - deallocationState->CreationTime;
    if (deallocationAge > Input_.Config->HulkRequestTimeout) {
        YT_LOG_WARNING("Deallocation request is stuck "
            "(DeallocationId: %v, DeallocationAge: %v, Threshold: %v)",
            deallocationId,
            deallocationAge,
            Input_.Config->HulkRequestTimeout);

        Mutations_->AlertsToFire.push_back(TAlert{
            .Id = "stuck_instance_deallocation",
            .BundleName = BundleName_,
            .DataCenter = deallocationState->DataCenter,
            .Description = Format("Found stuck deallocation %v with age %v which is more than threshold %v.",
                deallocationId,
                deallocationAge,
                Input_.Config->HulkRequestTimeout),
        });
    }

    const auto& instanceName = deallocationState->InstanceName;
    if (!Adapter_->IsInstanceReadyToBeDeallocated(
        instanceName,
        deallocationId,
        deallocationAge,
        BundleName_,
        Input_,
        Mutations_))
    {
        return true;
    }

    if (deallocationState->Strategy == DeallocationStrategyHulkRequest) {
        return ProcessHulkDeallocation(BundleName_, Adapter_, deallocationId, deallocationState, Input_, Mutations_);
    } else if (deallocationState->Strategy == DeallocationStrategyReturnToBB) {
        return ReturnToBundleBalancer(BundleName_, Adapter_, deallocationId, deallocationState, Input_, Mutations_);
    } else if (deallocationState->Strategy == DeallocationStrategyReturnToSpareBundle) {
        return ReturnToSpareBundle(BundleName_, Adapter_, deallocationId, deallocationState, Input_, Mutations_);
    }

    YT_LOG_WARNING("Unknown deallocation strategy (DeallocationId: %v, DeallocationStrategy: %v)",
        deallocationId,
        deallocationState->Strategy);

    Mutations_->AlertsToFire.push_back(TAlert{
        .Id = "unknown_deallocation_strategy",
        .BundleName = BundleName_,
        .Description = Format("Unknown deallocation strategy %Qv for deallocation %v.",
            deallocationState->Strategy,
            deallocationId),
    });

    return true;
}

void TInstanceManager::ProcessExistingDeallocations()
{
    auto& deallocations = Adapter_->DeallocationsState();
    TIndexedEntries<TDeallocationRequestState> aliveDeallocations;

    for (const auto& [deallocationId, deallocationState] : deallocations) {
        if (ProcessDeallocation(deallocationId, deallocationState)) {
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
    const std::string& zoneName,
    const std::string& dataCenterName)
{
    const auto& bundleInfo = GetOrCrash(Input_.Bundles, BundleName_);
    YT_VERIFY(bundleInfo->EnableBundleController);

    const auto& zoneInfo = GetOrCrash(Input_.Zones, zoneName);

    if (!Adapter_->IsNewDeallocationAllowed(bundleInfo, dataCenterName, Input_)) {
        YT_LOG_DEBUG("New deallocation is not allowed (DataCenter: %v, InstanceType: %v)",
            dataCenterName,
            Adapter_->GetInstanceType());
        return;
    }

    auto aliveInstances = Adapter_->GetAliveInstances(dataCenterName);
    auto targetInstanceCount = Adapter_->GetTargetInstanceCount(bundleInfo, zoneInfo);
    auto instanceCountToDeallocate = std::ssize(aliveInstances) - targetInstanceCount;
    auto& deallocationsState = Adapter_->DeallocationsState();

    YT_LOG_DEBUG("Scheduling deallocations "
        "(DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, AliveInstanceCount: %v, "
        "PlannedDeallocationCount: %v, ExistingDeallocationCount: %v)",
        dataCenterName,
        Adapter_->GetInstanceType(),
        targetInstanceCount,
        std::ssize(aliveInstances),
        instanceCountToDeallocate,
        std::ssize(deallocationsState));

    if (instanceCountToDeallocate <= 0) {
        return;
    }

    const auto instancesToRemove = Adapter_->PickInstancesToDeallocate(
        instanceCountToDeallocate,
        dataCenterName,
        bundleInfo,
        Input_);

    for (const auto& instanceName : instancesToRemove) {
        const auto& instanceInfo = Adapter_->GetInstanceInfo(instanceName, Input_);

        std::string deallocationId = ToString(TGuid::Create());
        auto deallocationState = New<TDeallocationRequestState>();
        deallocationState->CreationTime = TInstant::Now();
        deallocationState->InstanceName = instanceName;
        deallocationState->DataCenter = dataCenterName;
        deallocationState->Strategy = instanceInfo->BundleControllerAnnotations->DeallocationStrategy;

        if (deallocationState->Strategy.empty()) {
            if (Input_.Config->HasInstanceAllocatorService) {
                deallocationState->Strategy = DeallocationStrategyHulkRequest;
            } else {
                deallocationState->Strategy = DeallocationStrategyReturnToSpareBundle;
            }
        }

        deallocationsState[deallocationId] = deallocationState;

        YT_LOG_INFO("Init instance deallocation (InstanceName: %v, InstanceType: %v, DeallocationId: %v, Strategy: %v)",
            instanceName,
            Adapter_->GetInstanceType(),
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
        YT_LOG_DEBUG("Found allocated instance (InstanceName: %v)", requestInfo->Status->NodeId);
        return requestInfo->Status->NodeId;
    }

    const auto& podId = requestInfo->Status->PodId;
    auto it = input.PodIdToInstanceName.find(podId);
    if (it != input.PodIdToInstanceName.end()) {
        YT_LOG_DEBUG("Found allocated instance (PodId: %v, InstanceName: %v)", it->first, it->second);
        return it->second;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
