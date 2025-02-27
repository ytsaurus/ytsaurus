#include "bundle_scheduler.h"

#include "config.h"
#include "cypress_bindings.h"

#include <algorithm>
#include <library/cpp/yt/yson_string/public.h>

#include <util/string/subst.h>

#include <compare>
#include <cmath>
#include <vector>

namespace NYT::NCellBalancer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = BundleControllerLogger;
static const std::string DefaultDataCenterName = "default";

////////////////////////////////////////////////////////////////////////////////

bool IsAllocationFailed(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "FAILED";
}

bool IsAllocationCompleted(const auto& requestInfo)
{
    return requestInfo->Status && requestInfo->Status->State == "COMPLETED";
}

std::string GetPodIdForInstance(const std::string& name)
{
    // TODO(capone212): Get pod_id from node Cypress annotations.

    // For now we get PodId in a bit hacky way:
    // we expect PodId to be prefix of fqdn before the first dot.
    auto endPos = name.find(".");
    if (endPos == std::string::npos && name.starts_with("localhost")) {
        // For testing purposes.
        return name;
    }
    YT_VERIFY(endPos != std::string::npos);

    auto podId = name.substr(0, endPos);
    YT_VERIFY(!podId.empty());
    return podId;
}

////////////////////////////////////////////////////////////////////////////////

std::string GetInstancePodIdTemplate(
    const std::string& cluster,
    const std::string& bundleName,
    const std::string& instanceType,
    int index)
{
    return Format("<short-hostname>-%v-%03x-%v-%v", bundleName, index, instanceType, cluster);
}

std::optional<int> GetIndexFromPodId(
    const std::string& podId,
    const std::string& cluster,
    const std::string& instanceType)
{
    TStringBuf buffer = podId;
    auto suffix = Format("-%v-%v", instanceType, cluster);
    if (!buffer.ChopSuffix(suffix)) {
        return {};
    }

    constexpr char Delimiter = '-';
    auto indexString = buffer.RNextTok(Delimiter);

    int result = 0;
    if (TryIntFromString<16>(indexString, result)) {
        return result;
    }

    return {};
}

int FindNextInstanceId(
    const std::vector<std::string>& instanceNames,
    const std::string& cluster,
    const std::string& instanceType)
{
    std::vector<int> existingIds;
    existingIds.reserve(instanceNames.size());

    for (const auto& instanceName : instanceNames) {
        auto index = GetIndexFromPodId(instanceName, cluster, instanceType);
        if (index && *index > 0) {
            existingIds.push_back(*index);
        }
    }

    // Sort and make unique.
    std::sort(existingIds.begin(), existingIds.end());
    auto last = std::unique(existingIds.begin(), existingIds.end());
    existingIds.resize(std::distance(existingIds.begin(), last));

    if (existingIds.empty()) {
        return 1;
    }

    for (int index = 0; index < std::ssize(existingIds); ++index) {
        if (existingIds[index] != index + 1) {
            return index + 1;
        }
    }

    return existingIds.back() + 1;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TSpareInstances>
class TSpareInstanceAllocator {
    template <typename TValue>
    using TZoneToDataCenterToValue = THashMap<std::string, THashMap<std::string, TValue>>;

    template <typename TContainer>
    using TIteratorOf = typename TContainer::const_iterator;

    using TFreeInstances = const std::vector<std::string>;

public:
    explicit TSpareInstanceAllocator(const TZoneToDataCenterToValue<TSpareInstances>& spareInstances)
        : SpareInstances_(spareInstances)
    {
        FirstFreeInstance_.reserve(SpareInstances_.size());
        for (const auto& [zoneName, perZoneInstances] : SpareInstances_) {
            for (const auto& [dataCenterName, instances] : perZoneInstances) {
                FirstFreeInstance_[zoneName][dataCenterName] = instances.FreeInstances().begin();
            }
        }
    }

    std::string Allocate(const std::string& zoneName, const std::string& dataCenterName)
    {
        YT_VERIFY(HasInstances(zoneName, dataCenterName));
        return *GetNext(zoneName, dataCenterName);
    }

    bool HasInstances(const std::string& zoneName, const std::string& dataCenterName) const
    {
        if (!FirstFreeInstance_.contains(zoneName)) {
            return false;
        }
        auto& dcToIterator = GetOrCrash(FirstFreeInstance_, zoneName);
        if (!dcToIterator.contains(dataCenterName)) {
            return false;
        }
        auto it = GetOrCrash(dcToIterator, dataCenterName);
        return it != GetEnd(zoneName, dataCenterName);
    }

private:
    const TZoneToDataCenterToValue<TSpareInstances>& SpareInstances_;
    TZoneToDataCenterToValue<TIteratorOf<TFreeInstances>> FirstFreeInstance_;

    TIteratorOf<TFreeInstances> GetNext(const std::string& zoneName, const std::string& dataCenterName)
    {
        return FirstFreeInstance_[zoneName][dataCenterName]++;
    }

    TIteratorOf<TFreeInstances> GetEnd(const std::string& zoneName, const std::string& dataCenterName) const
    {
        return GetOrCrash(GetOrCrash(SpareInstances_, zoneName), dataCenterName).FreeInstances().end();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeAllocatorAdapter;
class TRpcProxyAllocatorAdapter;

template <typename TInstanceTypeAdapter>
class TInstanceManager
{
public:
    using TInstanceAllocator = TSpareInstanceAllocator<typename TInstanceTypeAdapter::TSpareInstanceInfo>;

    void ManageInstances(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        TInstanceAllocator& spareInstances,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        ProcessExistingAllocations(bundleName, adapter, spareInstances, input, mutations);
        ProcessExistingDeallocations(bundleName, adapter, input, mutations);

        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        YT_VERIFY(bundleInfo->EnableBundleController);

        auto zoneIt = input.Zones.find(bundleInfo->Zone);

        if (zoneIt == input.Zones.end()) {
            YT_LOG_WARNING("Cannot locate zone for bundle (Bundle: %v, Zone: %v)",
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
                YT_LOG_WARNING("Instance management skipped for bundle due zone unhealthy state"
                    " (BundleName: %v, InstanceType: %v)",
                    bundleName,
                    adapter->GetInstanceType());

                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "zone_is_disrupted",
                    .BundleName = bundleName,
                    .Description = Format("Zone %Qv is disrupted. Disabling all %v allocations within %Qv.",
                        zoneName,
                        adapter->GetInstanceType(),
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

private:
    static bool IsResourceUsageExceeded(const NBundleControllerClient::TInstanceResourcesPtr& usage, const TResourceQuotaPtr& quota)
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

    int GetAllocationCountInDataCenter(
        const TIndexedEntries<TAllocationRequestState>& allocationsState,
        const std::string& dataCenterName)
    {
        return std::count_if(allocationsState.begin(), allocationsState.end(), [&] (const auto& record) {
            return record.second->DataCenter.value_or(DefaultDataCenterName) == dataCenterName;
        });
    }

    void InitNewAllocations(
        const std::string& bundleName,
        const std::string& zoneName,
        const std::string& dataCenterName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);
        const auto& dataCenterInfo = GetOrCrash(zoneInfo->DataCenters, dataCenterName);
        auto& allocationsState = adapter->AllocationsState();

        YT_VERIFY(bundleInfo->EnableBundleController);

        if (!adapter->IsNewAllocationAllowed(bundleInfo, dataCenterName, input)) {
            return;
        }

        int aliveInstanceCount = std::ssize(adapter->GetAliveInstances(dataCenterName));
        int targetInstanceCount = adapter->GetTargetInstanceCount(bundleInfo, zoneInfo);
        int currentDataCenterAllocations = GetAllocationCountInDataCenter(allocationsState, dataCenterName);
        int instanceCountToAllocate = targetInstanceCount - aliveInstanceCount - currentDataCenterAllocations;

        YT_LOG_DEBUG("Scheduling allocations (BundleName: %v, DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, "
            "AliveInstanceCount: %v, RequestCount: %v, ExistingAllocations: %v)",
            bundleName,
            dataCenterName,
            adapter->GetInstanceType(),
            targetInstanceCount,
            aliveInstanceCount,
            instanceCountToAllocate,
            currentDataCenterAllocations);

        if (instanceCountToAllocate > 0 && adapter->IsInstanceCountLimitReached(bundleInfo->Zone, dataCenterName, zoneInfo, input)) {
            mutations->AlertsToFire.push_back(TAlert{
                .Id = "zone_instance_limit_reached",
                .BundleName = bundleName,
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
            auto outdatedInstanceCount = GetOutdatedInstanceCount(adapter, dataCenterName, input, bundleInfo);
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

            auto request = New<TAllocationRequest>();
            request->Spec = spec;
            mutations->NewAllocations[allocationId] = request;
            auto allocationState = New<TAllocationRequestState>();
            allocationState->CreationTime = TInstant::Now();
            allocationState->PodIdTemplate = spec->PodIdTemplate;
            allocationState->DataCenter = dataCenterName;
            allocationsState[allocationId] = allocationState;
        }
    }

    int GetOutdatedInstanceCount(
        TInstanceTypeAdapter* adapter,
        const std::string& dataCenterName,
        const TSchedulerInputState& input,
        const TBundleInfoPtr& bundleInfo)
    {
        int count = 0;
        const auto& targetResource = adapter->GetResourceGuarantee(bundleInfo);

        for (const auto& instanceName : adapter->GetAliveInstances(dataCenterName)) {
            const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
            const auto& instanceResource = instanceInfo->Annotations->Resource;

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

    std::string GetPodIdTemplate(
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        std::vector<std::string> knownPodIds;
        for (const auto& instanceName : adapter->GetInstances(dataCenterName)) {
            knownPodIds.push_back(GetPodIdForInstance(instanceName));
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

        for (const auto& [_, state] : adapter->DeallocationsState()) {
            knownPodIds.push_back(GetPodIdForInstance(state->InstanceName));
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

    void ProcessExistingAllocations(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        TInstanceAllocator& spareInstances,
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
                    mutations->CompletedAllocations.insert(allocationId);
                }
                continue;
            }

            YT_LOG_DEBUG_UNLESS(instanceName.empty(), "Setting allocated instance tags (AllocationId: %v, BundleName: %v, InstanceName: %v)",
                allocationId,
                bundleName,
                instanceName);

            if (allocationAge > input.Config->HulkRequestTimeout) {
                YT_LOG_WARNING("Allocation Request is stuck (AllocationId: %v, AllocationAge: %v, Threshold: %v)",
                    allocationId,
                    allocationAge,
                    input.Config->HulkRequestTimeout);

                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "stuck_instance_allocation",
                    .BundleName = bundleName,
                    .Description = Format("Found stuck allocation %v with age %v which is more than threshold %v.",
                        allocationId,
                        allocationAge,
                        input.Config->HulkRequestTimeout),
                });
            }

            if (input.Config->HasInstanceAllocatorService) {
                YT_LOG_DEBUG("Tracking existing allocation (AllocationId: %v, Bundle: %v,  InstanceName: %v)",
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

    void CompleteExistingAllocationWithoutInstanceAllocatorService(
        const std::string& allocationId,
        const auto& allocationInfo,
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        TInstanceAllocator& spareInstances,
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
        auto spareBundleName = GetSpareBundleName(zoneInfo);

        if (!spareInstances.HasInstances(zoneName, dataCenterName)) {
            YT_LOG_WARNING("No spare instances available for bundle (BundleName: %v, InstanceType: %v)",
                bundleName,
                adapter->GetInstanceType());

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "no_spare_instances_available",
                .BundleName = bundleName,
                .Description = Format("No spare instances of type %v are available for allocation request %v",
                    adapter->GetInstanceType(),
                    allocationId),
            });

            return;
        }

        auto instanceName = spareInstances.Allocate(zoneName, dataCenterName);

        YT_LOG_INFO("Allocating instance from spare (AllocationId: %v, BundleName: %v, InstanceType: %v, InstanceName: %v)",
            allocationId,
            bundleName,
            adapter->GetInstanceType(),
            instanceName);

        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        const auto& currentAnnotations = instanceInfo->Annotations;
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

    bool ReturnToBundleBalancer(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
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

        if (!adapter->EnsureDeallocatedInstanceTagsSet(instanceName, DeallocationStrategyReturnToBB, input, mutations)) {
            return true;
        }

        mutations->ChangedDecommissionedFlag[instanceName] = false;
        mutations->ChangedNodeUserTags[instanceName] = {};

        return false;
    }

    bool ReturnToSpareBundle(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& instanceName = deallocationState->InstanceName;
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);
        auto spareBundleName = GetSpareBundleName(zoneInfo);
        const auto& annotations = adapter->GetInstanceInfo(instanceName, input)->Annotations;

        YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v, AllocatedFor: %v, BundleName: %v, Strategy: %v)",
            deallocationId,
            instanceName,
            annotations->AllocatedForBundle,
            bundleName,
            DeallocationStrategyReturnToSpareBundle);

        if (!adapter->EnsureDeallocatedInstanceTagsSet(instanceName, DeallocationStrategyReturnToSpareBundle, input, mutations)) {
            return true;
        }

        // Avoid race condition between initializing new deallocations and
        // marking node as node not from this bundle.

        if (annotations->AllocatedForBundle != spareBundleName) {
            YT_VERIFY(annotations->AllocatedForBundle == bundleName);

            auto newAnnotations = NYTree::CloneYsonStruct(annotations);
            newAnnotations->AllocatedForBundle = spareBundleName;
            adapter->SetInstanceAnnotations(instanceName, newAnnotations, mutations);

            adapter->SetDefaultSpareAttributes(instanceName, mutations);

            return true;
        }

        return false;
    }

    bool ProcessHulkDeallocation(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        // Validating node tags
        const auto& instanceName = deallocationState->InstanceName;
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        const auto& instanceAnnotations = instanceInfo->Annotations;

        if (!deallocationState->HulkRequestCreated && instanceAnnotations->YPCluster.empty()) {
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
            YT_LOG_WARNING("Deallocation Failed (AllocationId: %v)",
                deallocationId);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "instance_deallocation_failed",
                .BundleName = bundleName,
                .Description = Format("Deallocation request %v has failed.",
                    deallocationId),
            });

            return true;
        }

        if (IsAllocationCompleted(it->second) &&
            adapter->EnsureDeallocatedInstanceTagsSet(instanceName, DeallocationStrategyHulkRequest, input, mutations))
        {
            YT_LOG_INFO("Instance deallocation completed (InstanceName: %v, DeallocationId: %v)",
                instanceName,
                deallocationId);
            return false;
        }

        YT_LOG_DEBUG("Tracking existing deallocation (DeallocationId: %v, InstanceName: %v)",
            deallocationId,
            instanceName);
        return true;
    }

    // Returns false if current deallocation should not be tracked any more.
    bool ProcessDeallocation(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
        const std::string& deallocationId,
        const TDeallocationRequestStatePtr& deallocationState,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        auto deallocationAge = TInstant::Now() - deallocationState->CreationTime;
        if (deallocationAge > input.Config->HulkRequestTimeout) {
            YT_LOG_WARNING("Deallocation Request is stuck (BundleName: %v, DeallocationId: %v, DeallocationAge: %v, Threshold: %v)",
                bundleName,
                deallocationId,
                deallocationAge,
                input.Config->HulkRequestTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_instance_deallocation",
                .BundleName = bundleName,
                .Description = Format("Found stuck deallocation %v with age %v which is more than threshold %v.",
                    deallocationId,
                    deallocationAge,
                    input.Config->HulkRequestTimeout),
            });
        }

        const auto& instanceName = deallocationState->InstanceName;
        if (!adapter->IsInstanceReadyToBeDeallocated(instanceName, deallocationId, input, mutations)) {
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

    void ProcessExistingDeallocations(
        const std::string& bundleName,
        TInstanceTypeAdapter* adapter,
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

    void CreateHulkDeallocationRequest(
        const std::string& deallocationId,
        const std::string& instanceName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        const auto& instanceInfo = adapter->GetInstanceInfo(instanceName, input);
        const auto& instanceAnnotations = instanceInfo->Annotations;
        YT_VERIFY(instanceAnnotations->DeallocationStrategy.empty() ||
            instanceAnnotations->DeallocationStrategy == DeallocationStrategyHulkRequest);

        auto request = New<TDeallocationRequest>();
        auto& spec = request->Spec;
        spec->YPCluster = instanceAnnotations->YPCluster;
        spec->PodId = GetPodIdForInstance(instanceName);
        spec->InstanceRole = adapter->GetInstanceRole();
        mutations->NewDeallocations[deallocationId] = request;
    }

    void InitNewDeallocations(
        const std::string& bundleName,
        const std::string& zoneName,
        const std::string& dataCenterName,
        TInstanceTypeAdapter* adapter,
        const TSchedulerInputState& input,
        TSchedulerMutations* /*mutations*/)
    {
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        YT_VERIFY(bundleInfo->EnableBundleController);

        const auto& zoneInfo = GetOrCrash(input.Zones, zoneName);

        if (!adapter->IsNewDeallocationAllowed(bundleInfo, dataCenterName, input)) {
            return;
        }

        auto aliveInstances = adapter->GetAliveInstances(dataCenterName);
        auto targetInstanceCount = adapter->GetTargetInstanceCount(bundleInfo, zoneInfo);
        auto instanceCountToDeallocate = std::ssize(aliveInstances) - targetInstanceCount;
        auto& deallocationsState = adapter->DeallocationsState();

        YT_LOG_DEBUG("Scheduling deallocations (BundleName: %v, DataCenter: %v, InstanceType: %v, TargetInstanceCount: %v, AliveInstances: %v, "
            "RequestCount: %v, ExistingDeallocations: %v)",
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
            deallocationState->Strategy = instanceInfo->Annotations->DeallocationStrategy;

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

    std::string LocateAllocatedInstance(
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
};

////////////////////////////////////////////////////////////////////////////////

template <typename TCollection>
TSchedulerInputState::TBundleToInstanceMapping MapBundlesToInstances(const TCollection& collection)
{
    TSchedulerInputState::TBundleToInstanceMapping result;

    for (const auto& [instanceName, instanceInfo] : collection) {
        auto dataCenter = instanceInfo->Annotations->DataCenter.value_or(DefaultDataCenterName);
        auto bundleName = instanceInfo->Annotations->AllocatedForBundle;

        if (!bundleName.empty()) {
            result[bundleName][dataCenter].push_back(instanceName);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TCollection>
TSchedulerInputState::TZoneToInstanceMap MapZonesToInstances(
    const TSchedulerInputState& input,
    const TCollection& collection)
{
    THashMap<std::string, std::string> nannyServiceToZone;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        for (const auto& [dataCenterName, dataCenterInfo] : zoneInfo->DataCenters) {
            if (!dataCenterInfo->TabletNodeNannyService.empty()) {
                nannyServiceToZone[dataCenterInfo->TabletNodeNannyService] = zoneName;
            }

            if (!dataCenterInfo->RpcProxyNannyService.empty()) {
                nannyServiceToZone[dataCenterInfo->RpcProxyNannyService] = zoneName;
            }
        }
    }

    TSchedulerInputState::TZoneToInstanceMap result;
    for (const auto& [instanceName, instanceInfo] : collection) {
        if (!instanceInfo->Annotations->Allocated) {
            continue;
        }
        auto it = nannyServiceToZone.find(instanceInfo->Annotations->NannyService);
        if (it == nannyServiceToZone.end()) {
            continue;
        }
        const auto& zoneName = it->second;
        const auto& dataCenterName = instanceInfo->Annotations->DataCenter.value_or(DefaultDataCenterName);
        result[zoneName].PerDataCenter[dataCenterName].push_back(instanceName);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, TDataCenterRackInfo> MapZonesToRacks(
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    THashMap<std::string, TDataCenterRackInfo> zoneToRacks;

    for (const auto& [zoneName, zoneNodes] : input.ZoneNodes) {
        auto zoneInfo = GetOrCrash(input.Zones, zoneName);
        auto spareBundleName = GetSpareBundleName(zoneInfo);

        for (const auto& [dataCenterName, dataCenterNodes] : zoneNodes.PerDataCenter) {
            auto& dataCenterRacks = zoneToRacks[zoneName][dataCenterName];

            for (const auto& tabletNode : dataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, tabletNode);
                if (nodeInfo->State != InstanceStateOnline) {
                    continue;
                }

                if (nodeInfo->Annotations->AllocatedForBundle == spareBundleName) {
                    ++dataCenterRacks.RackToSpareInstances[nodeInfo->Rack];
                } else {
                    ++dataCenterRacks.RackToBundleInstances[nodeInfo->Rack];
                }
            }
        }
    }

    for (auto& [_, zoneRacks] : zoneToRacks) {
        for (auto& [_, dataCenterRacks] : zoneRacks) {
            for (const auto& [rackName, bundleNodes] : dataCenterRacks.RackToBundleInstances) {
                int spareNodeCount = 0;

                const auto& spareRacks = dataCenterRacks.RackToSpareInstances;
                if (auto it = spareRacks.find(rackName); it != spareRacks.end()) {
                    spareNodeCount = it->second;
                }

                dataCenterRacks.RequiredSpareNodeCount = std::max(
                    dataCenterRacks.RequiredSpareNodeCount,
                    bundleNodes + spareNodeCount);
            }
        }
    }

    for (auto& [zone, zoneInfo] : input.Zones) {
        for (auto& [dataCenter, _] : zoneInfo->DataCenters) {
            auto zoneIt = zoneToRacks.find(zone);
            if (zoneIt == zoneToRacks.end()) {
                continue;
            }

            auto dataCenterIt = zoneIt->second.find(dataCenter);
            if (dataCenterIt == zoneIt->second.end()) {
                continue;
            }

            if (zoneInfo->RequiresMinusOneRackGuarantee && zoneInfo->SpareTargetConfig->TabletNodeCount < dataCenterIt->second.RequiredSpareNodeCount) {
                mutations->AlertsToFire.push_back(TAlert{
                    .Id = "minus_one_rack_guarantee_violation",
                    .Description = Format("Zone %v in data center %v has target spare nodes: %v "
                        ", where required count is at least %v.",
                        zone,
                        dataCenter,
                        zoneInfo->SpareTargetConfig->TabletNodeCount,
                        dataCenterIt->second.RequiredSpareNodeCount),
                });

                YT_LOG_WARNING("Zone spare nodes violate minus one rack guarantee (Zone: %v, DataCenter: %v, ZoneSpareNodes: %v, RequiredSpareNodes: %v)",
                    zone,
                    dataCenter,
                    zoneInfo->SpareTargetConfig->TabletNodeCount,
                    dataCenterIt->second.RequiredSpareNodeCount);
            }
        }
    }

    return zoneToRacks;
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::string> MapPodIdToInstanceName(const TSchedulerInputState& input)
{
    THashMap<std::string, std::string> result;

    for (const auto& [nodeName, _] : input.TabletNodes) {
        auto podId = GetPodIdForInstance(nodeName);
        result[podId] = nodeName;
    }

    for (const auto& [proxyName, _] : input.RpcProxies) {
        auto podId = GetPodIdForInstance(proxyName);
        result[podId] = proxyName;
    }

    return result;
}

std::string GenerateShortNameForBundle(
    const std::string& bundleName,
    const THashMap<std::string, std::string>& shortNameToBundle,
    int maxLength)
{
    auto shortName = bundleName;

    // pod id can not contain '_'
    SubstGlobal(shortName, '_', '-');
    if (std::ssize(shortName) <= maxLength && shortNameToBundle.count(shortName) == 0) {
        return shortName;
    }

    shortName.resize(maxLength - 1);

    for (int index = 1; index < 10; ++index) {
        auto proposed = Format("%v%v", shortName, index);

        if (shortNameToBundle.count(proposed) == 0) {
            return proposed;
        }
    }

    THROW_ERROR_EXCEPTION("Cannot generate short name for bundle")
        << TErrorAttribute("bundle_name", bundleName);
}

THashMap<std::string, std::string> MapBundlesToShortNames(const TSchedulerInputState& input)
{
    THashMap<std::string, std::string> bundleToShortName;
    THashMap<std::string, std::string> shortNameToBundle;

    // Instance pod-id looks like sas3-4993-venus212-001-rpc-hume.
    // Max pod id length is 35, so cluster short name and bundle short name
    // should be under 16 characters.
    constexpr int MaxBundlePlusClusterNamesLength = 16;
    constexpr int MinBundleShortName = 4;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (bundleInfo->ShortName) {
            bundleToShortName[bundleName] = *bundleInfo->ShortName;
            shortNameToBundle[*bundleInfo->ShortName] = bundleName;
        }
    }

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (auto it = bundleToShortName.find(bundleName); it != bundleToShortName.end()) {
            continue;
        }

        auto it = input.Zones.find(bundleInfo->Zone);
        if (it == input.Zones.end()) {
            continue;
        }

        auto clusterName = it->second->ShortName.value_or(input.Config->Cluster);
        int maxShortNameLength = MaxBundlePlusClusterNamesLength - clusterName.size();

        THROW_ERROR_EXCEPTION_UNLESS(
            maxShortNameLength >= MinBundleShortName,
            "Please set cluster short name, cluster name it too long");

        auto shortName = GenerateShortNameForBundle(bundleName, shortNameToBundle, maxShortNameLength);
        YT_VERIFY(std::ssize(shortName) <= maxShortNameLength);

        bundleToShortName[bundleName] = shortName;
        shortNameToBundle[shortName] = bundleName;
    }

    return bundleToShortName;
}

std::string GetInstanceSize(const NBundleControllerClient::TInstanceResourcesPtr& resource)
{
    auto cpuCores = resource->Vcpu / 1000;
    auto memoryGB = resource->Memory / 1_GB;

    return Format("%vCPUx%vGB", cpuCores, memoryGB);
}

void CalculateResourceUsage(TSchedulerInputState& input)
{
    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> aliveResources;
    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> allocatedResources;
    THashMap<std::string, NBundleControllerClient::TInstanceResourcesPtr> targetResources;

    auto calculateResources = [] (
        const auto& aliveNames,
        const auto& instancesInfo,
        NBundleControllerClient::TInstanceResourcesPtr& target,
        auto& countBySize)
    {
        for (const auto& instanceName : aliveNames) {
            const auto& instanceInfo = GetOrCrash(instancesInfo, instanceName);
            const auto& resource = instanceInfo->Annotations->Resource;
            target->Vcpu += resource->Vcpu;
            target->Memory += resource->Memory;
            ++countBySize[GetInstanceSize(resource)];
        }
    };

    input.AliveNodesBySize.clear();
    input.AllocatedProxiesBySize.clear();
    input.AllocatedProxiesBySize.clear();
    input.AliveProxiesBySize.clear();

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
            continue;
        }

        {
            auto aliveResourceUsage = New<NBundleControllerClient::TInstanceResources>();
            aliveResourceUsage->Clear();

            TBundleControllerStatePtr bundleState;
            if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
                bundleState = NYTree::CloneYsonStruct(it->second);
            }

            auto perDCaliveNodes = GetAliveNodes(
                bundleName,
                input.BundleNodes[bundleName],
                input,
                bundleState,
                EGracePeriodBehaviour::Wait);

            auto aliveNodes = FlattenAliveInstances(perDCaliveNodes);
            calculateResources(aliveNodes, input.TabletNodes, aliveResourceUsage, input.AliveNodesBySize[bundleName]);

            auto aliveProxies = FlattenAliveInstances(GetAliveProxies(input.BundleProxies[bundleName], input, EGracePeriodBehaviour::Wait));
            calculateResources(aliveProxies, input.RpcProxies, aliveResourceUsage, input.AliveProxiesBySize[bundleName]);

            aliveResources[bundleName] = aliveResourceUsage;
        }

        {
            auto allocated = New<NBundleControllerClient::TInstanceResources>();
            allocated->Clear();
            calculateResources(FlattenBundleInstances(input.BundleNodes[bundleName]), input.TabletNodes, allocated, input.AllocatedNodesBySize[bundleName]);
            calculateResources(FlattenBundleInstances(input.BundleProxies[bundleName]), input.RpcProxies, allocated, input.AllocatedProxiesBySize[bundleName]);

            allocatedResources[bundleName] = allocated;
        }

        {
            const auto& targetConfig = bundleInfo->TargetConfig;
            const auto& nodeGuarantee = targetConfig->TabletNodeResourceGuarantee;
            const auto& proxyGuarantee = targetConfig->RpcProxyResourceGuarantee;

            auto targetResource = New<NBundleControllerClient::TInstanceResources>();
            targetResource->Vcpu = nodeGuarantee->Vcpu * targetConfig->TabletNodeCount + proxyGuarantee->Vcpu * targetConfig->RpcProxyCount;
            targetResource->Memory = nodeGuarantee->Memory * targetConfig->TabletNodeCount + proxyGuarantee->Memory * targetConfig->RpcProxyCount;
            targetResource->NetBytes = nodeGuarantee->NetBytes.value_or(0) * targetConfig->TabletNodeCount + proxyGuarantee->NetBytes.value_or(0) * targetConfig->RpcProxyCount;

            targetResources[bundleName] = targetResource;
        }
    }

    input.BundleResourceAlive = aliveResources;
    input.BundleResourceAllocated = allocatedResources;
    input.BundleResourceTarget = targetResources;
}

THashMap<std::string, THashSet<std::string>> GetAliveNodes(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    const TBundleControllerStatePtr& bundleState,
    EGracePeriodBehaviour gracePeriodBehaviour)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    THashMap<std::string, THashSet<std::string>> aliveNodes;

    auto now = TInstant::Now();

    for (const auto& [dataCenterName, dataCenterNodes] : bundleNodes) {
        for (const auto& nodeName : dataCenterNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            if (!nodeInfo->Annotations->Allocated || nodeInfo->Banned) {
                continue;
            }

            bool internallyDecommissioned = bundleState &&
                (bundleState->BundleNodeAssignments.count(nodeName) != 0 ||
                bundleState->BundleNodeReleasements.count(nodeName) != 0);

            if (nodeInfo->DisableTabletCells) {
                YT_LOG_DEBUG("Tablet cells are disabled for the node (BundleName: %v, Node: %v)",
                    bundleName,
                    nodeName);
                continue;
            }

            if (!bundleInfo->NodeTagFilter.empty() && nodeInfo->Decommissioned && !internallyDecommissioned) {
                YT_LOG_DEBUG("Node is externally decommissioned (BundleName: %v, Node: %v)",
                    bundleName,
                    nodeName);
                continue;
            }

            if (nodeInfo->State != InstanceStateOnline) {
                if (gracePeriodBehaviour == EGracePeriodBehaviour::Immediately ||
                    now - nodeInfo->LastSeenTime > input.Config->OfflineInstanceGracePeriod)
                {
                    continue;
                }
            }

            aliveNodes[dataCenterName].insert(nodeName);
        }
    }

    return aliveNodes;
}

THashMap<std::string, THashSet<std::string>> GetAliveProxies(
    const TDataCenterToInstanceMap& bundleProxies,
    const TSchedulerInputState& input,
    EGracePeriodBehaviour gracePeriodBehaviour)
{
    THashMap<std::string, THashSet<std::string>> aliveProxies;
    auto now = TInstant::Now();

    for (const auto& [dataCenterName, dataCenterProxies] : bundleProxies) {
        for (const auto& proxyName : dataCenterProxies) {
            const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            if (!proxyInfo->Annotations->Allocated || proxyInfo->Banned) {
                continue;
            }

            if (!proxyInfo->Alive) {
                if (gracePeriodBehaviour == EGracePeriodBehaviour::Immediately ||
                    now - proxyInfo->ModificationTime > input.Config->OfflineInstanceGracePeriod)
                {
                    continue;
                }
            }

            aliveProxies[dataCenterName].insert(proxyName);
        }
    }

    return aliveProxies;
}

int GetUsedSlotCount(const TTabletNodeInfoPtr& nodeInfo)
{
    int usedSlotCount = 0;

    for (const auto& slotInfo : nodeInfo->TabletSlots) {
        if (slotInfo->State != TabletSlotStateEmpty) {
            ++usedSlotCount;
        }
    }

    return usedSlotCount;
}

struct TNodeRemoveOrder
{
    bool MaintenanceIsNotRequested = true;
    bool HasUpdatedResources = true;
    int UsedSlotCount = 0;
    std::string NodeName;

    auto AsTuple() const
    {
        return std::tie(MaintenanceIsNotRequested, HasUpdatedResources, UsedSlotCount, NodeName);
    }

    bool operator<(const TNodeRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

int GetTargetCellCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo)
{
    if (zoneInfo->DataCenters.empty()) {
        return 0;
    }

    const auto& targetConfig = bundleInfo->TargetConfig;
    int activeDataCenterCount = std::ssize(zoneInfo->DataCenters) - zoneInfo->RedundantDataCenterCount;
    int activeNodeCount = (targetConfig->TabletNodeCount * activeDataCenterCount) / std::ssize(zoneInfo->DataCenters);

    YT_VERIFY(activeNodeCount >= 0);
    return activeNodeCount * targetConfig->CpuLimits->WriteThreadPoolSize.value_or(DefaultWriteThreadPoolSize) / bundleInfo->Options->PeerCount;
}

bool EnsureNodeDecommissioned(
    const std::string& nodeName,
    const TTabletNodeInfoPtr& nodeInfo,
    TSchedulerMutations* mutations)
{
    if (!nodeInfo->Decommissioned) {
        mutations->ChangedDecommissionedFlag[nodeName] = true;
        return false;
    }
    // Wait tablet cells to migrate.
    return GetUsedSlotCount(nodeInfo) == 0;
}

struct TTabletCellRemoveOrder
{
    std::string Id;
    std::string HostNode;
    bool Disrupted = false;
    // No tablet host and non zero tablet nodes

    auto AsTuple() const
    {
        return std::tie(Disrupted, HostNode, Id);
    }

    bool operator<(const TTabletCellRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

std::string GetHostNodeForCell(const TTabletCellInfoPtr& cellInfo, const THashSet<std::string>& bundleNodes)
{
    std::string nodeName;

    for (const auto& peer : cellInfo->Peers) {
        if (bundleNodes.count(peer->Address) == 0) {
            continue;
        }

        if (nodeName.empty() || peer->State == PeerStateLeading) {
            nodeName = peer->Address;
        }
    }

    return nodeName;
}

std::vector<std::string> PickTabletCellsToRemove(
    int cellCountToRemove,
    const std::vector<std::string>& bundleCellIds)
{
    YT_VERIFY(std::ssize(bundleCellIds) >= cellCountToRemove);

    std::vector<std::string> result;
    result.reserve(bundleCellIds.size());

    for (auto& cell : bundleCellIds) {
        result.push_back(cell);
    }

    // add some determinism
    std::sort(result.begin(), result.end());
    result.resize(cellCountToRemove);
    return result;
}

void ProcessRemovingCells(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& /*bundleNodes*/,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    auto& state = mutations->ChangedStates[bundleName];
    std::vector<std::string> removeCompleted;

    for (const auto& [cellId, removingStateInfo] : state->RemovingCells) {
        auto it = input.TabletCells.find(cellId);
        if (it == input.TabletCells.end()) {
            YT_LOG_INFO("Tablet cell removal finished (BundleName: %v, TabletCellId: %v)",
                bundleName,
                cellId);
            removeCompleted.push_back(cellId);
            continue;
        }

        auto removingTime = TInstant::Now() - removingStateInfo->RemovedTime;

        if (removingTime > input.Config->CellRemovalTimeout) {
            YT_LOG_WARNING("Tablet cell removal is stuck (TabletCellId: %v, RemovingTime: %v, Threshold: %v)",
                cellId,
                removingTime,
                input.Config->CellRemovalTimeout);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "stuck_tablet_cell_removal",
                .BundleName = bundleName,
                .Description = Format("Found stuck tablet cell %v removal "
                    " with removing time %v, which is more than threshold %v.",
                    cellId,
                    removingTime,
                    input.Config->CellRemovalTimeout),
            });
        }

        YT_LOG_DEBUG("Tablet cell removal in progress"
            " (BundleName: %v, TabletCellId: %v)",
            bundleName,
            cellId);
    }

    for (const auto& cellId : removeCompleted) {
        state->RemovingCells.erase(cellId);
    }
}

void CreateRemoveTabletCells(
    const std::string& bundleName,
    const TDataCenterToInstanceMap& bundleNodes,
    const TSchedulerInputState& input,
    TSchedulerMutations* mutations)
{
    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    const auto& bundleState = mutations->ChangedStates[bundleName];

    if (!bundleInfo->EnableTabletCellManagement) {
        return;
    }

    const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

    auto aliveNodes = FlattenAliveInstances(GetAliveNodes(
        bundleName,
        bundleNodes,
        input,
        mutations->ChangedStates[bundleName],
        EGracePeriodBehaviour::Wait));

    if (std::ssize(aliveNodes) < bundleInfo->TargetConfig->TabletNodeCount ||
        !bundleState->NodeAllocations.empty() ||
        !bundleState->NodeDeallocations.empty())
    {
        // It is better not to mix node allocations with tablet cell management.
        return;
    }

    if (!bundleState->RemovingCells.empty()) {
        // Do not do anything with cells while tablet cell removal is in progress.
        return;
    }

    int targetCellCount = GetTargetCellCount(bundleInfo, zoneInfo);
    int cellCountDiff = targetCellCount - std::ssize(bundleInfo->TabletCellIds);

    YT_LOG_DEBUG("Managing tablet cells (BundleName: %v, TargetCellCount: %v, ExistingCount: %v)",
        bundleName,
        targetCellCount,
        std::ssize(bundleInfo->TabletCellIds));

    if (cellCountDiff < 0) {
        auto cellsToRemove = PickTabletCellsToRemove(std::abs(cellCountDiff), bundleInfo->TabletCellIds);

        YT_LOG_INFO("Removing tablet cells (BundleName: %v, CellIds: %v)",
            bundleName,
            cellsToRemove);

        mutations->CellsToRemove.insert(
            mutations->CellsToRemove.end(),
            cellsToRemove.begin(),
            cellsToRemove.end());

        for (auto& cellId : cellsToRemove) {
            auto removingCellState = New<TRemovingTabletCellState>();
            removingCellState->RemovedTime = TInstant::Now();
            bundleState->RemovingCells[cellId] = removingCellState;
        }
    } else if (cellCountDiff > 0) {
        YT_LOG_INFO("Creating tablet cells (BundleName: %v, CellCount: %v)",
            bundleName,
            cellCountDiff);

        mutations->CellsToCreate[bundleName] = cellCountDiff;
    }
}

struct TQuotaDiff
{
    i64 ChunkCount = 0;
    THashMap<std::string, i64> DiskSpacePerMedium;
    i64 NodeCount = 0;

    bool Empty() const
    {
        return ChunkCount == 0 &&
            NodeCount == 0 &&
            std::all_of(DiskSpacePerMedium.begin(), DiskSpacePerMedium.end(), [] (const auto& pair) {
                return pair.second == 0;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

using TQuotaChanges = THashMap<std::string, TQuotaDiff>;

void AddQuotaChanges(
    const std::string& bundleName,
    const TBundleInfoPtr& bundleInfo,
    const TSchedulerInputState& input,
    int cellCount,
    TQuotaChanges& changes)
{
    const auto& bundleOptions = bundleInfo->Options;

    if (bundleOptions->SnapshotAccount != bundleOptions->ChangelogAccount) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with different "
            "snapshot and changelog accounts (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto accountName = bundleOptions->SnapshotAccount;
    auto accountIt = input.SystemAccounts.find(accountName);
    if (accountIt == input.SystemAccounts.end()) {
        YT_LOG_DEBUG("Skip adjusting quota for bundle with custom account"
            " (BundleName: %v, SnapshotAccount: %v, ChangelogAccount: %v)",
            bundleName,
            bundleOptions->SnapshotAccount,
            bundleOptions->ChangelogAccount);
        return;
    }

    const auto& currentLimit = accountIt->second->ResourceLimits;
    const auto& config = input.Config;

    cellCount = std::max(cellCount, 1);
    auto multiplier = bundleInfo->SystemAccountQuotaMultiplier * cellCount;

    TQuotaDiff quotaDiff;

    quotaDiff.ChunkCount = std::max<i64>(config->ChunkCountPerCell * multiplier, config->MinChunkCount) - currentLimit->ChunkCount;
    quotaDiff.NodeCount = std::max<i64>(config->NodeCountPerCell * multiplier, config->MinNodeCount) - currentLimit->NodeCount;

    auto getSpace = [&] (const std::string& medium) -> i64 {
        auto it = currentLimit->DiskSpacePerMedium.find(medium);
        if (it == currentLimit->DiskSpacePerMedium.end()) {
            return 0;
        }
        return it->second;
    };

    i64 snapshotSpace = config->SnapshotDiskSpacePerCell * multiplier;
    i64 changelogSpace = config->JournalDiskSpacePerCell * multiplier;

    if (bundleOptions->ChangelogPrimaryMedium == bundleOptions->SnapshotPrimaryMedium) {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] =
            snapshotSpace + changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
    } else {
        quotaDiff.DiskSpacePerMedium[bundleOptions->ChangelogPrimaryMedium] =
            changelogSpace - getSpace(bundleOptions->ChangelogPrimaryMedium);
        quotaDiff.DiskSpacePerMedium[bundleOptions->SnapshotPrimaryMedium] =
            snapshotSpace - getSpace(bundleOptions->SnapshotPrimaryMedium);
    }

    if (!quotaDiff.Empty()) {
        changes[accountName] = quotaDiff;
    }
}

void ApplyQuotaChange(const TQuotaDiff& change, const TAccountResourcesPtr& limits)
{
    limits->ChunkCount += change.ChunkCount;
    limits->NodeCount += change.NodeCount;

    for (const auto& [medium, spaceDiff] : change.DiskSpacePerMedium) {
        limits->DiskSpacePerMedium[medium] += spaceDiff;
    }
}

bool IsLimitsLifted(const TQuotaDiff& change)
{
    for (const auto& [_, diff] : change.DiskSpacePerMedium) {
        if (diff < 0) {
            return false;
        }
    }

    if (change.ChunkCount < 0 || change.NodeCount < 0) {
        return false;
    }

    return true;
}

void ManageSystemAccountLimit(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TQuotaChanges quotaChanges;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableTabletCellManagement ||
            !bundleInfo->EnableSystemAccountManagement)
        {
            continue;
        }

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }
        const auto& zoneInfo = zoneIt->second;

        int cellCount = std::max<int>(GetTargetCellCount(bundleInfo, zoneInfo), std::ssize(bundleInfo->TabletCellIds));
        int cellPeerCount = cellCount * bundleInfo->Options->PeerCount;
        AddQuotaChanges(bundleName, bundleInfo, input, cellPeerCount, quotaChanges);
    }

    if (quotaChanges.empty()) {
        return;
    }

    auto rootQuota = CloneYsonStruct(input.RootSystemAccount->ResourceLimits);

    for (const auto& [accountName, quotaChange] : quotaChanges) {
        const auto& accountInfo = GetOrCrash(input.SystemAccounts, accountName);
        auto newQuota = CloneYsonStruct(accountInfo->ResourceLimits);
        ApplyQuotaChange(quotaChange, newQuota);
        ApplyQuotaChange(quotaChange, rootQuota);

        if (IsLimitsLifted(quotaChange)) {
            mutations->LiftedSystemAccountLimit[accountName] = newQuota;
        } else {
            mutations->LoweredSystemAccountLimit[accountName] = newQuota;
        }

        YT_LOG_INFO("Adjusting system account resource limits (Account: %v, NewResourceLimit: %v, OldResourceLimit: %v)",
            accountName,
            ConvertToYsonString(newQuota, EYsonFormat::Text),
            ConvertToYsonString(accountInfo->ResourceLimits, EYsonFormat::Text));
    }

    mutations->ChangedRootSystemAccountLimit = rootQuota;
    YT_LOG_INFO("Adjusting root system account resource limits (NewResourceLimit: %v, OldResourceLimit: %v)",
        ConvertToYsonString(rootQuota, EYsonFormat::Text),
        ConvertToYsonString(input.RootSystemAccount->ResourceLimits, EYsonFormat::Text));
}

////////////////////////////////////////////////////////////////////////////////

void ManageResourceLimits(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController ||
            !bundleInfo->EnableTabletCellManagement ||
            !bundleInfo->EnableResourceLimitsManagement)
        {
            continue;
        }

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& targetConfig = bundleInfo->TargetConfig;
        if (!targetConfig->MemoryLimits->TabletStatic) {
            continue;
        }

        auto availableTabletStatic = *targetConfig->MemoryLimits->TabletStatic * targetConfig->TabletNodeCount;

        if (availableTabletStatic != bundleInfo->ResourceLimits->TabletStaticMemory) {
            YT_LOG_INFO("Adjusting tablet static memory limit (BundleName: %v, NewValue: %v, OldValue: %v)",
                bundleName,
                availableTabletStatic,
                bundleInfo->ResourceLimits->TabletStaticMemory);

            mutations->ChangedTabletStaticMemory[bundleName] = availableTabletStatic;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string GetSpareBundleName(const TZoneInfoPtr& zoneInfo)
{
    return zoneInfo->SpareBundleName;
}

THashMap<TSchedulerInputState::TQualifiedDCName, TDataCenterDisruptedState> GetDataCenterDisruptedState(const TSchedulerInputState& input)
{
    using TQualifiedDCName = TSchedulerInputState::TQualifiedDCName;

    THashMap<TQualifiedDCName, int> zoneOfflineNodeCount;

    for (const auto& [zoneName, zoneNodes] : input.ZoneNodes) {
        for (const auto& [dataCenterName, dataCenterNodes] : zoneNodes.PerDataCenter) {
            for (const auto& nodeName : dataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
                if (nodeInfo->State == InstanceStateOnline) {
                    continue;
                }

                YT_LOG_DEBUG("Node is offline (NodeName: %v, NannyService: %v, Banned: %v, LastSeen: %v)",
                    nodeName,
                    nodeInfo->Annotations->NannyService,
                    nodeInfo->Banned,
                    nodeInfo->LastSeenTime);

                ++zoneOfflineNodeCount[std::pair(zoneName, dataCenterName)];
            }
        }
    }

    THashMap<TQualifiedDCName, int> zoneOfflineProxyCount;
    for (const auto& [zoneName, zoneProxies] : input.ZoneProxies) {
        for (const auto& [dataCenterName, dataCenterProxies] : zoneProxies.PerDataCenter) {
            for (const auto& proxyName : dataCenterProxies) {
                const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
                if (proxyInfo->Alive) {
                    continue;
                }

                YT_LOG_DEBUG("Proxy is offline (ProxyName: %v, NannyService: %v)",
                    proxyName,
                    proxyInfo->Annotations->NannyService);

                ++zoneOfflineProxyCount[std::pair(zoneName, dataCenterName)];
            }
        }
    }

    THashMap<TQualifiedDCName, TDataCenterDisruptedState> result;
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        for (const auto& [dataCenterName, dataCenterInfo] : zoneInfo->DataCenters) {
            auto& dataCenterDisrupted = result[std::pair(zoneName, dataCenterName)];

            dataCenterDisrupted.OfflineNodeCount = zoneOfflineNodeCount[std::pair(zoneName, dataCenterName)];
            dataCenterDisrupted.OfflineNodeThreshold = zoneInfo->SpareTargetConfig->TabletNodeCount * zoneInfo->DisruptedThresholdFactor / std::ssize(zoneInfo->DataCenters);

            YT_LOG_WARNING_IF(dataCenterDisrupted.IsNodesDisrupted(), "Zone data center is in disrupted state"
                " (ZoneName: %v, DataCenter: %v, NannyService: %v, DisruptedThreshold: %v, OfflineNodeCount: %v)",
                zoneName,
                dataCenterName,
                dataCenterInfo->TabletNodeNannyService,
                dataCenterDisrupted.OfflineNodeThreshold,
                dataCenterDisrupted.OfflineNodeCount);

            dataCenterDisrupted.OfflineProxyThreshold = zoneInfo->SpareTargetConfig->RpcProxyCount * zoneInfo->DisruptedThresholdFactor / std::ssize(zoneInfo->DataCenters);
            dataCenterDisrupted.OfflineProxyCount = zoneOfflineProxyCount[std::pair(zoneName, dataCenterName)];

            YT_LOG_WARNING_IF(dataCenterDisrupted.IsProxiesDisrupted(), "Zone data center is in disrupted state"
                " (ZoneName: %v, DataCenter: %v, NannyService: %v, DisruptedThreshold: %v, OfflineProxyCount: %v)",
                zoneName,
                dataCenterName,
                dataCenterInfo->RpcProxyNannyService,
                dataCenterDisrupted.OfflineProxyThreshold,
                dataCenterDisrupted.OfflineProxyCount);
        }
    }

    return result;
}

TInstanceAnnotationsPtr GetInstanceAnnotationsToSet(
    const std::string& bundleName,
    const std::string& dataCenterName,
    const TAllocationRequestPtr& allocationInfo,
    const TInstanceAnnotationsPtr& annotations)
{
    if (!annotations->AllocatedForBundle.empty() && annotations->Allocated) {
        return {};
    }
    auto result = NYTree::CloneYsonStruct(annotations);
    result->YPCluster = allocationInfo->Spec->YPCluster;
    result->NannyService = allocationInfo->Spec->NannyService;
    result->AllocatedForBundle = bundleName;
    result->DataCenter = dataCenterName;
    result->Allocated = true;
    result->DeallocationStrategy = DeallocationStrategyHulkRequest;
    ConvertToInstanceResources(*result->Resource, *allocationInfo->Spec->ResourceRequest);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

int GetTargetDataCenterInstanceCount(int targetCount, const TZoneInfoPtr& zone)
{
    if (zone->DataCenters.empty()) {
        return 0;
    }

    return targetCount / std::ssize(zone->DataCenters);
}

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeAllocatorAdapter
{
public:
    using TSpareInstanceInfo = TSpareNodesInfo;

    TTabletNodeAllocatorAdapter(
        TBundleControllerStatePtr state,
        const TDataCenterToInstanceMap& bundleNodes,
        const THashMap<std::string, THashSet<std::string>>& aliveBundleNodes)
        : State_(std::move(state))
        , BundleNodes_(bundleNodes)
        , AliveBundleNodes_(aliveBundleNodes)
    { }

    bool IsNewAllocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& dataCenterName,
        const TSchedulerInputState& input)
    {
        if (GetInProgressAssignmentCount(dataCenterName, input) > 0) {
            // Do not mix node tag operations with new node allocations.
            return false;
        }

        return true;
    }

    int GetInProgressAssignmentCount(const std::string& dataCenterName, const TSchedulerInputState& input)
    {
        auto dataCenterPredicate = [&] (const auto& pair) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, pair.first);
            return nodeInfo->Annotations->DataCenter == dataCenterName;
        };

        const auto& assignments = State_->BundleNodeAssignments;
        const auto& releasements = State_->BundleNodeReleasements;

        return std::count_if(assignments.begin(), assignments.end(), dataCenterPredicate) +
            std::count_if(releasements.begin(), releasements.end(), dataCenterPredicate);
    }

    bool IsDataCenterDisrupted(const TDataCenterDisruptedState& dcState)
    {
        return dcState.IsNodesDisrupted();
    }

    bool IsNewDeallocationAllowed(
        const TBundleInfoPtr& bundleInfo,
        const std::string& dataCenterName,
        const TSchedulerInputState& input)
    {
        bool deallocationsInDataCenter = std::any_of(
            State_->NodeDeallocations.begin(),
            State_->NodeDeallocations.end(),
            [&] (const auto& record) {
                return record.second->DataCenter.value_or(DefaultDataCenterName) == dataCenterName;
            });

        if (!State_->NodeAllocations.empty() ||
            deallocationsInDataCenter ||
            !State_->RemovingCells.empty() ||
            GetInProgressAssignmentCount(dataCenterName, input) > 0)
        {
            // It is better not to mix allocation and deallocation requests.
            return false;
        }

        const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

        if (bundleInfo->EnableTabletCellManagement) {
            if (GetTargetCellCount(bundleInfo, zoneInfo) != std::ssize(bundleInfo->TabletCellIds)) {
                // Wait for tablet cell management to complete.
                return false;
            }
        }

        if (bundleInfo->EnableNodeTagFilterManagement) {
            // Check that all alive instances have appropriate node_tag_filter and slots count
            auto expectedSlotCount = bundleInfo->TargetConfig->CpuLimits->WriteThreadPoolSize;

            std::vector<std::string> notReadyNodes;
            const auto aliveDataCenterNodes = GetAliveInstances(dataCenterName);

            for (const auto& nodeName : aliveDataCenterNodes) {
                const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
                if (nodeInfo->UserTags.count(bundleInfo->NodeTagFilter) == 0 ||
                    std::ssize(nodeInfo->TabletSlots) != expectedSlotCount)
                {
                    YT_LOG_DEBUG("Node is not ready (NodeName: %v, "
                        "ExpectedSlotCount: %v, NodeTagFilter: %v, SlotCount: %v, UserTags: %v)",
                        nodeName, expectedSlotCount, bundleInfo->NodeTagFilter, std::ssize(nodeInfo->TabletSlots), nodeInfo->UserTags);

                    notReadyNodes.push_back(nodeName);
                }
            }

            if (!notReadyNodes.empty() && std::ssize(notReadyNodes) != std::ssize(aliveDataCenterNodes)) {
                // Wait while all alive nodes have updated settings.

                YT_LOG_INFO("Skipping nodes deallocation because nodes are node ready (DataCenter: %v)",
                    dataCenterName);
                return false;
            }
        }

        return true;
    }

    bool IsInstanceCountLimitReached(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const
    {
        auto zoneIt = input.ZoneNodes.find(zoneName);
        if (zoneIt == input.ZoneNodes.end()) {
            // No allocated tablet nodes for this zone
            return false;
        }

        auto dataCenterIt = zoneIt->second.PerDataCenter.find(dataCenterName);
        if (dataCenterIt == zoneIt->second.PerDataCenter.end()) {
            // No allocated tablet nodes for this dc
            return false;
        }

        int currentDataCenterNodeCount = std::ssize(dataCenterIt->second);
        int datacenterMaxNodeCount = zoneInfo->MaxTabletNodeCount / std::ssize(zoneInfo->DataCenters);

        if (currentDataCenterNodeCount >= datacenterMaxNodeCount) {
            YT_LOG_WARNING("Max nodes count limit reached"
                " (Zone: %v, DataCenter: %v, CurrentDataCenterNodeCount: %v, ZoneMaxTabletNodeCount: %v, DatacenterMaxTabletNodeCount: %v)",
                zoneName,
                dataCenterName,
                currentDataCenterNodeCount,
                zoneInfo->MaxTabletNodeCount,
                datacenterMaxNodeCount);
            return true;
        }
        return false;
    }

    int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo) const
    {
        return GetTargetDataCenterInstanceCount(bundleInfo->TargetConfig->TabletNodeCount, zoneInfo);
    }

    int GetInstanceRole() const
    {
        return YTRoleTypeTabNode;
    }

    const NBundleControllerClient::TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
    }

    const std::optional<std::string> GetHostTagFilter(const TBundleInfoPtr& bundleInfo, const TSchedulerInputState& input) const
    {
        auto resources = bundleInfo->TargetConfig->TabletNodeResourceGuarantee;
        const auto& zoneInfo = GetOrCrash(input.Zones, bundleInfo->Zone);

        for (const auto& [name, instanceSize] : zoneInfo->TabletNodeSizes) {
            if (*instanceSize->ResourceGuarantee == *resources) {
                return instanceSize->HostTagFilter;
            }
        }

        return {};
    }

    const std::string& GetInstanceType()
    {
        static const std::string TabletNode = "tab";
        return TabletNode;
    }

    TIndexedEntries<TAllocationRequestState>& AllocationsState() const
    {
        return State_->NodeAllocations;
    }

    TIndexedEntries<TDeallocationRequestState>& DeallocationsState() const
    {
        return State_->NodeDeallocations;
    }

    const TTabletNodeInfoPtr& GetInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.TabletNodes, instanceName);
    }

    void SetInstanceAnnotations(const std::string& instanceName, TInstanceAnnotationsPtr annotations, TSchedulerMutations* mutations)
    {
        mutations->ChangeNodeAnnotations[instanceName] = std::move(annotations);
    }

    bool IsInstanceReadyToBeDeallocated(
        const std::string& instanceName,
        const std::string& deallocationId,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto nodeIt = input.TabletNodes.find(instanceName);
        if (nodeIt == input.TabletNodes.end()) {
            YT_LOG_ERROR("Cannot find node from deallocation request state (DeallocationId: %v, Node: %v)",
                deallocationId,
                instanceName);
            return false;
        }

        const auto& nodeInfo = nodeIt->second;
        if (input.Config->DecommissionReleasedNodes) {
            return EnsureNodeDecommissioned(instanceName, nodeInfo, mutations);
        }

        YT_LOG_INFO("Skipping node decommissioning due to configuration (DeallocationId: %v, Node: %v)",
            deallocationId,
            instanceName);

        return true;
    }

    std::string GetNannyService(const TDataCenterInfoPtr& dataCenterInfo) const
    {
        return dataCenterInfo->TabletNodeNannyService;
    }

    bool EnsureAllocatedInstanceTagsSet(
        const std::string& nodeName,
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
        if (nodeInfo->State != InstanceStateOnline) {
            return false;
        }

        if (nodeInfo->Decommissioned) {
            mutations->ChangedDecommissionedFlag[nodeName] = false;
            return false;
        }

        if (!nodeInfo->UserTags.empty()) {
            mutations->ChangedNodeUserTags[nodeName] = {};
            return false;
        }

        const auto& annotations = nodeInfo->Annotations;

        if (auto changed = GetInstanceAnnotationsToSet(bundleName, dataCenterName, allocationInfo, annotations)) {
            mutations->ChangeNodeAnnotations[nodeName] = changed;
            return false;
        }

        if (annotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, NodeName: %v)",
                annotations->AllocatedForBundle,
                bundleName,
                nodeName);

            mutations->AlertsToFire.push_back({
                .Id = "inconsistent_allocation_state",
                .BundleName = bundleName,
                .Description = Format("Inconsistent allocation state: Node annotation bundle name %v, actual bundle name %v.",
                    annotations->AllocatedForBundle,
                    bundleName)
            });

            return false;
        }

        if (GetAliveInstances(dataCenterName).count(nodeName) == 0) {
            return false;
        }

        return true;
    }

    bool EnsureDeallocatedInstanceTagsSet(
        const std::string& nodeName,
        const std::string& strategy,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        YT_VERIFY(!strategy.empty());

        const auto& instanceInfo = GetInstanceInfo(nodeName, input);
        const auto& annotations = instanceInfo->Annotations;
        if (strategy != DeallocationStrategyReturnToSpareBundle && (!annotations->AllocatedForBundle.empty() || annotations->Allocated)) {
            auto newAnnotations = New<TInstanceAnnotations>();
            newAnnotations->DeallocatedAt = TInstant::Now();
            newAnnotations->DeallocationStrategy = strategy;
            mutations->ChangeNodeAnnotations[nodeName] = newAnnotations;
            return false;
        }

        if (!instanceInfo->UserTags.empty()) {
            mutations->ChangedNodeUserTags[nodeName] = {};
            return false;
        }

        if (strategy == DeallocationStrategyReturnToBB) {
            if (!instanceInfo->EnableBundleBalancer || *instanceInfo->EnableBundleBalancer == false) {
                YT_LOG_DEBUG("Returning node to BundleBalancer (NodeName: %v)",
                    nodeName);

                mutations->ChangedEnableBundleBalancerFlag[nodeName] = true;
            }
        }
        return true;
    }

    void SetDefaultSpareAttributes(const std::string& nodeName, TSchedulerMutations* mutations) const
    {
        mutations->ChangedNodeUserTags[nodeName] = {};
    }

    const THashSet<std::string>& GetAliveInstances(const std::string& dataCenterName) const
    {
        const static THashSet<std::string> Dummy;

        auto it = AliveBundleNodes_.find(dataCenterName);
        if (it != AliveBundleNodes_.end()) {
            return it->second;
        }

        return Dummy;
    }

    const std::vector<std::string>& GetInstances(const std::string& dataCenterName) const
    {
        const static std::vector<std::string> Dummy;

        auto it = BundleNodes_.find(dataCenterName);
        if (it != BundleNodes_.end()) {
            return it->second;
        }

        return Dummy;
    }

    std::vector<std::string> PickInstancesToDeallocate(
        int nodeCountToRemove,
        const std::string& dataCenterName,
        const TBundleInfoPtr& bundleInfo,
        const TSchedulerInputState& input) const
    {
        const auto& aliveDataCenterNodes = GetAliveInstances(dataCenterName);

        std::vector<TNodeRemoveOrder> nodesOrder;

        nodesOrder.reserve(aliveDataCenterNodes.size());

        const auto& targetResource = GetResourceGuarantee(bundleInfo);

        for (auto nodeName : aliveDataCenterNodes) {
            const auto& nodeInfo = GetOrCrash(input.TabletNodes, nodeName);
            const auto& instanceResource = nodeInfo->Annotations->Resource;

            nodesOrder.push_back(TNodeRemoveOrder{
                .MaintenanceIsNotRequested = nodeInfo->CmsMaintenanceRequests.empty(),
                .HasUpdatedResources = (*targetResource == *instanceResource),
                .UsedSlotCount = GetUsedSlotCount(nodeInfo),
                .NodeName = nodeName,
            });
        }

        auto endIt = nodesOrder.end();
        if (std::ssize(nodesOrder) > nodeCountToRemove) {
            endIt = nodesOrder.begin() + nodeCountToRemove;
            std::nth_element(nodesOrder.begin(), endIt, nodesOrder.end());
        }

        std::vector<std::string> result;
        result.reserve(std::distance(nodesOrder.begin(), endIt));
        for (auto it = nodesOrder.begin(); it != endIt; ++it) {
            result.push_back(it->NodeName);
        }

        return result;
    }

private:
    TBundleControllerStatePtr State_;
    const TDataCenterToInstanceMap& BundleNodes_;
    const THashMap<std::string, THashSet<std::string>>& AliveBundleNodes_;
};

////////////////////////////////////////////////////////////////////////////////

struct TProxyRemoveOrder
{
    bool MaintenanceIsNotRequested = true;
    bool HasUpdatedResources = true;
    std::string ProxyName;

    auto AsTuple() const
    {
        return std::tie(MaintenanceIsNotRequested, HasUpdatedResources, ProxyName);
    }

    bool operator<(const TProxyRemoveOrder& other) const
    {
        return AsTuple() < other.AsTuple();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyAllocatorAdapter
{
public:
    using TSpareInstanceInfo = TSpareProxiesInfo;

    TRpcProxyAllocatorAdapter(
        TBundleControllerStatePtr state,
        const TDataCenterToInstanceMap& bundleProxies,
        const THashMap<std::string, THashSet<std::string>>& aliveProxies)
        : State_(std::move(state))
        , BundleProxies_(bundleProxies)
        , AliveProxies_(aliveProxies)
    { }

    bool IsNewAllocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& /*dataCenterName*/,
        const TSchedulerInputState& /*input*/)
    {
        return true;
    }

    bool IsNewDeallocationAllowed(
        const TBundleInfoPtr& /*bundleInfo*/,
        const std::string& dataCenterName,
        const TSchedulerInputState& /*input*/)
    {
        bool deallocationsInDataCenter = std::any_of(
            State_->ProxyDeallocations.begin(),
            State_->ProxyDeallocations.end(),
            [&] (const auto& record) {
                return record.second->DataCenter.value_or(DefaultDataCenterName) == dataCenterName;
            });

        if (!State_->ProxyAllocations.empty() || deallocationsInDataCenter) {
            // It is better not to mix allocation and deallocation requests.
            return false;
        }

        return true;
    }

    bool IsInstanceCountLimitReached(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const TZoneInfoPtr& zoneInfo,
        const TSchedulerInputState& input) const
    {
        auto zoneIt = input.ZoneProxies.find(zoneName);
        if (zoneIt == input.ZoneProxies.end()) {
            // No allocated rpc proxies for this zone
            return false;
        }

        auto dataCenterIt = zoneIt->second.PerDataCenter.find(dataCenterName);
        if (dataCenterIt == zoneIt->second.PerDataCenter.end()) {
            // No allocated rpc proxies for this dc
            return false;
        }

        int currentDataCenterProxyCount = std::ssize(dataCenterIt->second);
        int maxDataCenterProxyCount = zoneInfo->MaxRpcProxyCount / std::ssize(zoneInfo->DataCenters);

        if (currentDataCenterProxyCount >= maxDataCenterProxyCount) {
            YT_LOG_WARNING("Max Rpc proxies count limit reached"
                " (Zone: %v, DataCenter: %v, DataCenterRpcProxyCount: %v, ZoneMaxRpcProxyCount: %v, DataCenterMaxProxyCount: %v)",
                zoneName,
                dataCenterName,
                currentDataCenterProxyCount,
                zoneInfo->MaxRpcProxyCount,
                maxDataCenterProxyCount);
            return true;
        }
        return false;
    }

    bool IsDataCenterDisrupted(const TDataCenterDisruptedState& dcState)
    {
        return dcState.IsProxiesDisrupted();
    }

    int GetTargetInstanceCount(const TBundleInfoPtr& bundleInfo, const TZoneInfoPtr& zoneInfo) const
    {
        return GetTargetDataCenterInstanceCount(bundleInfo->TargetConfig->RpcProxyCount, zoneInfo);
    }

    int GetInstanceRole() const
    {
        return YTRoleTypeRpcProxy;
    }

    const NBundleControllerClient::TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
    {
        return bundleInfo->TargetConfig->RpcProxyResourceGuarantee;
    }

    const std::optional<std::string> GetHostTagFilter(const TBundleInfoPtr& /*bundleInfo*/, const TSchedulerInputState& /*input*/) const
    {
        return {};
    }

    const std::string& GetInstanceType()
    {
        static const std::string RpcProxy = "rpc";
        return RpcProxy;
    }

    TIndexedEntries<TAllocationRequestState>& AllocationsState() const
    {
        return State_->ProxyAllocations;
    }

    TIndexedEntries<TDeallocationRequestState>& DeallocationsState() const
    {
        return State_->ProxyDeallocations;
    }

    const TRpcProxyInfoPtr& GetInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.RpcProxies, instanceName);
    }

    void SetInstanceAnnotations(const std::string& instanceName, TInstanceAnnotationsPtr annotations, TSchedulerMutations* mutations)
    {
        mutations->ChangedProxyAnnotations[instanceName] = std::move(annotations);
    }

    bool IsInstanceReadyToBeDeallocated(
        const std::string& /*instanceName*/,
        const std::string& /*deallocationId*/,
        const TSchedulerInputState& /*input*/,
        TSchedulerMutations* /*mutations*/) const
    {
        return true;
    }

    std::string GetNannyService(const TDataCenterInfoPtr& dataCenterInfo) const
    {
        return dataCenterInfo->RpcProxyNannyService;
    }

    bool EnsureAllocatedInstanceTagsSet(
        const std::string& proxyName,
        const std::string& bundleName,
        const std::string& dataCenterName,
        const TAllocationRequestPtr& allocationInfo,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations) const
    {
        auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
        if (!proxyInfo->Alive) {
            return false;
        }

        if (proxyInfo->Role == TrashRole) {
            mutations->RemovedProxyRole.insert(proxyName);
            return false;
        }

        const auto& annotations = proxyInfo->Annotations;
        if (auto changed = GetInstanceAnnotationsToSet(bundleName, dataCenterName, allocationInfo, annotations)) {
            mutations->ChangedProxyAnnotations[proxyName] = changed;
            return false;
        }

        if (annotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, ProxyName: %v)",
                annotations->AllocatedForBundle,
                bundleName,
                proxyName);

            return false;
        }

        if (GetAliveInstances(dataCenterName).count(proxyName) == 0) {
            return false;
        }

        return true;
    }

    bool EnsureDeallocatedInstanceTagsSet(
        const std::string& proxyName,
        const std::string& strategy,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        YT_VERIFY(!strategy.empty());

        const auto& instanceInfo = GetInstanceInfo(proxyName, input);
        const auto& annotations = instanceInfo->Annotations;
        if (strategy != DeallocationStrategyReturnToSpareBundle && (!annotations->AllocatedForBundle.empty() || annotations->Allocated)) {
            auto newAnnotations = New<TInstanceAnnotations>();
            newAnnotations->DeallocatedAt = TInstant::Now();
            newAnnotations->DeallocationStrategy = strategy;
            mutations->ChangedProxyAnnotations[proxyName] = newAnnotations;
            return false;
        }

        if (strategy != DeallocationStrategyReturnToSpareBundle && (instanceInfo->Role != TrashRole)) {
            mutations->ChangedProxyRole[proxyName] = TrashRole;
            return false;
        }

        return true;
    }

    void SetDefaultSpareAttributes(const std::string& proxyName, TSchedulerMutations* mutations) const
    {
        mutations->ChangedProxyRole[proxyName] = DefaultRole;
    }

    const THashSet<std::string>& GetAliveInstances(const std::string& dataCenterName) const
    {
        const static THashSet<std::string> Dummy;

        auto it = AliveProxies_.find(dataCenterName);
        if (it != AliveProxies_.end()) {
            return it->second;
        }

        return Dummy;
    }

    const std::vector<std::string>& GetInstances(const std::string& dataCenterName) const
    {
        const static std::vector<std::string> Dummy;

        auto it = BundleProxies_.find(dataCenterName);
        if (it != BundleProxies_.end()) {
            return it->second;
        }

        return Dummy;
    }

    std::vector<std::string> PickInstancesToDeallocate(
        int proxyCountToRemove,
        const std::string& dataCenterName,
        const TBundleInfoPtr& bundleInfo,
        const TSchedulerInputState& input) const
    {
        const auto& aliveProxies = GetAliveInstances(dataCenterName);
        YT_VERIFY(std::ssize(aliveProxies) >= proxyCountToRemove);

        std::vector<TProxyRemoveOrder> proxyOrder;
        proxyOrder.reserve(aliveProxies.size());

        const auto& targetResource = GetResourceGuarantee(bundleInfo);

        for (const auto& proxyName : aliveProxies) {
            const auto& proxyInfo = GetOrCrash(input.RpcProxies, proxyName);
            const auto& instanceResource = proxyInfo->Annotations->Resource;

            proxyOrder.push_back(TProxyRemoveOrder{
                .MaintenanceIsNotRequested = proxyInfo->CmsMaintenanceRequests.empty(),
                .HasUpdatedResources = (*targetResource == *instanceResource),
                .ProxyName = proxyName,
            });
        }

        auto endIt = proxyOrder.end();
        if (std::ssize(proxyOrder) > proxyCountToRemove) {
            endIt = proxyOrder.begin() + proxyCountToRemove;
            std::nth_element(proxyOrder.begin(), endIt, proxyOrder.end());
        }

        std::vector<std::string> result;
        result.reserve(std::distance(proxyOrder.begin(), endIt));
        for (auto it = proxyOrder.begin(); it != endIt; ++it) {
            result.push_back(it->ProxyName);
        }

        return result;
    }

private:
    TBundleControllerStatePtr State_;
    const TDataCenterToInstanceMap& BundleProxies_;
    const THashMap<std::string, THashSet<std::string>>& AliveProxies_;
};

////////////////////////////////////////////////////////////////////////////////

bool IsOnline(const TTabletNodeInfoPtr& node)
{
    return node->State == InstanceStateOnline;
}

bool IsOnline(const TRpcProxyInfoPtr& proxy)
{
    return !!proxy->Alive;
}

template <typename TInstanceMap>
THashSet<std::string> ScanForObsoleteCypressNodes(const TSchedulerInputState& input, const TInstanceMap& instanceMap)
{
    THashSet<std::string> result;
    auto obsoleteThreshold = input.Config->RemoveInstanceCypressNodeAfter;
    auto now = TInstant::Now();

    for (const auto& [instanceName, instanceInfo] : instanceMap) {
        auto annotations = instanceInfo->Annotations;
        if (annotations->Allocated ||  !annotations->DeallocatedAt) {
            continue;
        }
        if (annotations->DeallocationStrategy != DeallocationStrategyHulkRequest) {
            continue;
        }

        if (now - *annotations->DeallocatedAt < obsoleteThreshold) {
            continue;
        }

        if (IsOnline(instanceInfo)) {
            YT_LOG_WARNING("Skipping obsolete Cypress node in online state (InstanceName: %v)", instanceName);
            continue;
        }

        result.insert(instanceName);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void InitializeVirtualSpareBundle(TSchedulerInputState& input)
{
    for (const auto& [zoneName, zoneInfo] : input.Zones) {
        auto spareVirtualBundle = GetSpareBundleName(zoneInfo);
        auto bundleInfo = New<TBundleInfo>();
        bundleInfo->EnableInstanceAllocation = input.Config->HasInstanceAllocatorService;
        bundleInfo->TargetConfig = zoneInfo->SpareTargetConfig;
        bundleInfo->EnableBundleController = true;
        bundleInfo->EnableTabletCellManagement = false;
        bundleInfo->EnableNodeTagFilterManagement = false;
        bundleInfo->EnableTabletNodeDynamicConfig = false;
        bundleInfo->EnableRpcProxyManagement = false;
        bundleInfo->EnableSystemAccountManagement = false;
        bundleInfo->EnableResourceLimitsManagement = false;
        bundleInfo->Zone = zoneName;
        input.Bundles[spareVirtualBundle] = bundleInfo;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageInstances(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TSpareInstanceAllocator<TSpareNodesInfo> spareNodesAllocator(input.ZoneToSpareNodes);
    TInstanceManager<TTabletNodeAllocatorAdapter> nodeAllocator;

    TSpareInstanceAllocator<TSpareProxiesInfo> spareProxiesAllocator(input.ZoneToSpareProxies);
    TInstanceManager<TRpcProxyAllocatorAdapter> proxyAllocator;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        const auto& zoneName = bundleInfo->Zone;

        if (auto zoneIt = input.Zones.find(zoneName); zoneIt == input.Zones.end()) {
            continue;
        }

        auto bundleState = New<TBundleControllerState>();
        if (auto it = input.BundleStates.find(bundleName); it != input.BundleStates.end()) {
            bundleState = NYTree::CloneYsonStruct(it->second);
        }
        mutations->ChangedStates[bundleName] = bundleState;

        if (!bundleInfo->EnableInstanceAllocation) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        auto aliveNodes = GetAliveNodes(
            bundleName,
            bundleNodes,
            input,
            bundleState,
            EGracePeriodBehaviour::Wait);
        TTabletNodeAllocatorAdapter nodeAdapter(bundleState, bundleNodes, aliveNodes);
        nodeAllocator.ManageInstances(bundleName, &nodeAdapter, spareNodesAllocator, input, mutations);

        const auto& bundleProxies = input.BundleProxies[bundleName];
        auto aliveProxies = GetAliveProxies(bundleProxies, input, EGracePeriodBehaviour::Wait);
        TRpcProxyAllocatorAdapter proxyAdapter(bundleState, bundleProxies, aliveProxies);
        proxyAllocator.ManageInstances(bundleName, &proxyAdapter, spareProxiesAllocator, input, mutations);
    }

    if (input.Config->HasInstanceAllocatorService) {
        mutations->NodesToCleanup = ScanForObsoleteCypressNodes(input, input.TabletNodes);
        mutations->ProxiesToCleanup = ScanForObsoleteCypressNodes(input, input.RpcProxies);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageCells(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (auto zoneIt = input.Zones.find(bundleInfo->Zone); zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& bundleNodes = input.BundleNodes[bundleName];
        CreateRemoveTabletCells(bundleName, bundleNodes, input, mutations);
        ProcessRemovingCells(bundleName, bundleNodes, input, mutations);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ManageBundlesDynamicConfig(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    TBundlesDynamicConfig freshConfig;

    for (const auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->EnableTabletNodeDynamicConfig) {
            continue;
        }

        if (bundleInfo->NodeTagFilter.empty()) {
            YT_LOG_WARNING("Bundle has empty node tag filter (BundleName: %v)", bundleName);
            continue;
        }

        auto bundleConfig = New<TBundleDynamicConfig>();
        bundleConfig->CpuLimits = NYTree::CloneYsonStruct(bundleInfo->TargetConfig->CpuLimits);
        bundleConfig->MemoryLimits = NYTree::CloneYsonStruct(bundleInfo->TargetConfig->MemoryLimits);
        bundleConfig->MediumThroughputLimits = NYTree::CloneYsonStructs(bundleInfo->TargetConfig->MediumThroughputLimits);
        freshConfig[bundleInfo->NodeTagFilter] = bundleConfig;
    }

    if (AreNodesEqual(ConvertTo<NYTree::IMapNodePtr>(freshConfig), ConvertTo<NYTree::IMapNodePtr>(input.DynamicConfig))) {
        return;
    }

    YT_LOG_INFO("Bundles dynamic config has changed (Config: %v)",
        ConvertToYsonString(freshConfig, EYsonFormat::Text));

    mutations->DynamicConfig = freshConfig;
}

////////////////////////////////////////////////////////////////////////////////

TIndexedEntries<TBundleControllerState> GetActuallyChangedStates(
    const TSchedulerInputState& input,
    const TSchedulerMutations& mutations)
{
    const auto inputStates = input.BundleStates;
    std::vector<std::string> unchangedBundleStates;

    for (auto [bundleName, possiblyChangedState] : mutations.ChangedStates) {
        auto it = inputStates.find(bundleName);
        if (it == inputStates.end()) {
            continue;
        }

        if (AreNodesEqual(ConvertTo<NYTree::INodePtr>(it->second), ConvertTo<NYTree::INodePtr>(possiblyChangedState))) {
            unchangedBundleStates.push_back(bundleName);
        } else {
            YT_LOG_DEBUG("Bundle state changed (Bundle: %v,  OldState: %v, NewSatate: %v)",
                bundleName,
                ConvertToYsonString(it->second, EYsonFormat::Text),
                ConvertToYsonString(possiblyChangedState, EYsonFormat::Text));
        }
    }

    auto result = mutations.ChangedStates;
    for (const auto& unchanged : unchangedBundleStates) {
        result.erase(unchanged);
    }

    return result;
}

void ManageBundleShortName(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, shortName] : input.BundleToShortName) {
        const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (bundleName == shortName || (bundleInfo->ShortName && *bundleInfo->ShortName == shortName)) {
            continue;
        }

        YT_LOG_INFO("Assigning short name for bundle (Bundle: %v, ShortName: %v)",
            bundleName,
            shortName);

        mutations->ChangedBundleShortName[bundleName] = shortName;
    }
}

void InitializeNodeTagFilters(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || bundleInfo->Zone.empty()) {
            continue;
        }

        if (!bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        if (bundleInfo->Areas.empty()) {
            YT_LOG_WARNING("Bundle does not have any tablet cell area (BundleName: %v)",
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "no_areas_found",
                .BundleName = bundleName,
                .Description = Format("Bundle does not have any tablet cell area"),
            });

            bundleInfo->EnableBundleController = false;

            continue;
        }

        if (bundleInfo->NodeTagFilter.empty()) {
            auto nodeTagFilter = Format("%v/%v", bundleInfo->Zone, bundleName);
            bundleInfo->NodeTagFilter = nodeTagFilter;
            mutations->ChangedNodeTagFilters[bundleName] = nodeTagFilter;

            YT_LOG_INFO("Initializing node tag filter for bundle (Bundle: %v, NodeTagFilter: %v)",
                bundleName,
                nodeTagFilter);
        }
    }
}

std::string GetDrillsNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName)
{
    return Format("%v/%v_drills_mode_on", bundleInfo->Zone, bundleName);
}

std::string GetNodeTagFilter(const TBundleInfoPtr& bundleInfo, const std::string& bundleName)
{
    return Format("%v/%v", bundleInfo->Zone, bundleName);
}

void ProcessEnableDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;
    if (!drillsMode->TurningOn) {
        return;
    }

    YT_LOG_DEBUG("Processing turning on drills mode (BundleName: %v)",
        bundleName);

    auto operationAge = TInstant::Now() - drillsMode->TurningOn->CreationTime;
    if (operationAge > input.Config->NodeAssignmentTimeout) {
        YT_LOG_WARNING("Turning on drills mode is stuck (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            input.Config->NodeAssignmentTimeout);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "stuck_drills_mode",
            .BundleName = bundleName,
            .Description = Format("Turning on drills mode is stuck for bundle %v. OperationAge: %v, Threshold: %v",
                bundleName,
                operationAge,
                input.Config->NodeAssignmentTimeout),
        });
    }

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    if (!bundleInfo->MuteTabletCellsCheck || !bundleInfo->MuteTabletCellSnapshotsCheck) {
        mutations->ChangedMuteTabletCellsCheck[bundleName] = true;
        mutations->ChangedMuteTabletCellSnapshotsCheck[bundleName] = true;

        YT_LOG_DEBUG("Disabling tablet cell checks for bundle (BundleName: %v)",
            bundleName);

        return;
    }

    auto drillsNodeTagFilter = GetDrillsNodeTagFilter(bundleInfo, bundleName);
    if (bundleInfo->NodeTagFilter != drillsNodeTagFilter) {
        YT_LOG_DEBUG("Setting drills node tag filter for bundle (BundleName: %v, NodeTagFilter: %v)",
            bundleName,
            drillsNodeTagFilter);

        mutations->ChangedNodeTagFilters[bundleName] = drillsNodeTagFilter;
        return;
    }

    YT_LOG_DEBUG("Finished turning on drills mode (BundleName: %v)",
        bundleName);

    drillsMode->TurningOn.Reset();
}

void ProcessDisableDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;

    if (!drillsMode->TurningOff) {
        return;
    }

    YT_LOG_DEBUG("Processing turning off drills mode (BundleName: %v)",
        bundleName);

    auto operationAge = TInstant::Now() - drillsMode->TurningOff->CreationTime;
    auto disableTimeout = input.Config->NodeAssignmentTimeout + input.Config->MuteTabletCellsCheckGracePeriod;

    if (operationAge > disableTimeout) {
        YT_LOG_WARNING("Turning off drills mode is stuck (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            disableTimeout);

        mutations->AlertsToFire.push_back(TAlert{
            .Id = "stuck_disable_drills_mode",
            .BundleName = bundleName,
            .Description = Format("Turning off drills mode is stuck for bundle %v. OperationAge: %v, Threshold: %v",
                bundleName,
                operationAge,
                disableTimeout),
        });
    }

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto nodeTagFilter = GetNodeTagFilter(bundleInfo, bundleName);
    if (bundleInfo->NodeTagFilter != nodeTagFilter) {
        mutations->ChangedNodeTagFilters[bundleName] = nodeTagFilter;

        YT_LOG_DEBUG("Setting node tag filter for bundle (BundleName: %v, NodeTagFilter: %v)",
            bundleName,
            nodeTagFilter);

        return;
    }

    if (operationAge < input.Config->MuteTabletCellsCheckGracePeriod) {
        YT_LOG_DEBUG("Waiting grace period before enabling tablet cell checks (BundleName: %v, OperationAge: %v, Threshold: %v)",
            bundleName,
            operationAge,
            input.Config->MuteTabletCellsCheckGracePeriod);

        return;
    }

    if (bundleInfo->MuteTabletCellsCheck || bundleInfo->MuteTabletCellSnapshotsCheck) {
        YT_LOG_DEBUG("Enabling tablet cell checks for bundle (BundleName: %v)",
            bundleName);

        mutations->ChangedMuteTabletCellsCheck[bundleName] = false;
        mutations->ChangedMuteTabletCellSnapshotsCheck[bundleName] = false;
        return;
    }

    YT_LOG_DEBUG("Finished turning off drills mode (BundleName: %v)",
        bundleName);

    drillsMode->TurningOff.Reset();
}

void ToggleDrillsMode(const std::string& bundleName, const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    auto& bundleState = GetOrCrash(mutations->ChangedStates, bundleName);
    const auto& drillsMode = bundleState->DrillsMode;

    YT_VERIFY(!drillsMode->TurningOn || !drillsMode->TurningOff);

    auto newState = New<TDrillsModeOperationState>();
    newState->CreationTime = TInstant::Now();

    const auto& bundleInfo = GetOrCrash(input.Bundles, bundleName);
    auto enableDrillsMode = bundleInfo->TargetConfig->EnableDrillsMode;
    auto drillsNodeTagFilter = GetDrillsNodeTagFilter(bundleInfo, bundleName);

    // Transition from TurningOffDrillsMode --> TurningOnDrillsMode.
    if (enableDrillsMode && drillsMode->TurningOff) {
        drillsMode->TurningOff.Reset();
        drillsMode->TurningOn = newState;
    }

    if (drillsMode->TurningOn || drillsMode->TurningOff) {
        return;
    }

    if (enableDrillsMode && bundleInfo->NodeTagFilter != drillsNodeTagFilter) {
        drillsMode->TurningOn = newState;
    } else if (!enableDrillsMode && bundleInfo->NodeTagFilter == drillsNodeTagFilter) {
        drillsMode->TurningOff = newState;
    }
}

void ManageDrillsMode(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || bundleInfo->Zone.empty()) {
            continue;
        }

        if (!bundleInfo->EnableNodeTagFilterManagement) {
            continue;
        }

        ProcessDisableDrillsMode(bundleName, input, mutations);
        ProcessEnableDrillsMode(bundleName, input, mutations);
        ToggleDrillsMode(bundleName, input, mutations);
    }
}

void TrimNetworkInfo(TSchedulerInputState* input)
{
    if (input->Config->EnableNetworkLimits) {
        return;
    }

    // Networking is disabled. We have to trim all networking info.

    for (auto& [_, bundleInfo] : input->Bundles) {
        if (!bundleInfo->EnableBundleController || !bundleInfo->TargetConfig) {
            continue;
        }

        const auto& targetConfig = bundleInfo->TargetConfig;
        targetConfig->TabletNodeResourceGuarantee->ResetNet();
        targetConfig->RpcProxyResourceGuarantee->ResetNet();
    }

    for (const auto& [_, nodeInfo] : input->TabletNodes) {
        if (nodeInfo->Annotations && nodeInfo->Annotations->Resource) {
            nodeInfo->Annotations->Resource->ResetNet();
        }
    }

    for (const auto& [_, proxyInfo] : input->RpcProxies) {
        if (proxyInfo->Annotations && proxyInfo->Annotations->Resource) {
            proxyInfo->Annotations->Resource->ResetNet();
        }
    }

    for (auto& [_, zoneInfo] : input->Zones) {
        if (!zoneInfo->SpareTargetConfig) {
            continue;
        }

        const auto& spareTargetConfig = zoneInfo->SpareTargetConfig;
        spareTargetConfig->TabletNodeResourceGuarantee->ResetNet();
        spareTargetConfig->RpcProxyResourceGuarantee->ResetNet();
    }
}

void InitDefaultDataCenter(TSchedulerInputState* input)
{
    for (const auto& [zoneName, zoneInfo] : input->Zones) {
        if (!zoneInfo->DataCenters.empty()) {
            continue;
        }
        auto dataCenter = New<TDataCenterInfo>();
        dataCenter->YPCluster = zoneInfo->DefaultYPCluster;
        dataCenter->TabletNodeNannyService = zoneInfo->DefaultTabletNodeNannyService;
        dataCenter->RpcProxyNannyService = zoneInfo->DefaultRpcProxyNannyService;
        zoneInfo->DataCenters[DefaultDataCenterName] = std::move(dataCenter);
    }
}

void InitializeBundleTargetConfig(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController || bundleInfo->TargetConfig) {
            continue;
        }
        auto targetConfig = New<TBundleConfig>();
        bundleInfo->TargetConfig = targetConfig;
        mutations->InitializedBundleTargetConfig[bundleName] = targetConfig;

        auto zoneIt = input.Zones.find(bundleInfo->Zone);
        if (zoneIt == input.Zones.end()) {
            continue;
        }

        const auto& zoneInfo = zoneIt->second;
        if (!zoneInfo->TabletNodeSizes.empty()) {
            auto& front = *zoneInfo->TabletNodeSizes.begin();
            targetConfig->TabletNodeResourceGuarantee = NYTree::CloneYsonStruct(front.second->ResourceGuarantee);
            targetConfig->TabletNodeResourceGuarantee->Type = front.first;
            targetConfig->CpuLimits = front.second->DefaultConfig->CpuLimits;
            targetConfig->MemoryLimits = front.second->DefaultConfig->MemoryLimits;
        }

        if (!zoneInfo->RpcProxySizes.empty()) {
            auto& front = *zoneInfo->RpcProxySizes.begin();
            targetConfig->RpcProxyResourceGuarantee = NYTree::CloneYsonStruct(front.second->ResourceGuarantee);
            targetConfig->RpcProxyResourceGuarantee->Type = front.first;
        }
    }

    for (const auto& [bundleName, targetConfig] : mutations->InitializedBundleTargetConfig) {
        YT_LOG_INFO("Initializing target config for bundle (Bundle: %v, TargetConfig: %v)",
            bundleName,
            ConvertToYsonString(targetConfig, EYsonFormat::Text));
    }
}

void MiscBundleChecks(const TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    for (auto& [bundleName, bundleInfo] : input.Bundles) {
        if (!bundleInfo->EnableBundleController) {
            continue;
        }

        if (bundleInfo->BundleHotfix) {
            YT_LOG_WARNING("Hotfix mode is enabled for bundle (BundleName: %v)",
                bundleName);

            mutations->AlertsToFire.push_back(TAlert{
                .Id = "hotfix_mode_is_enabled",
                .BundleName = bundleName,
                .Description = "Hotfix mode is enabled for the bundle",
            });
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ScheduleBundles(TSchedulerInputState& input, TSchedulerMutations* mutations)
{
    InitDefaultDataCenter(&input);

    input.ZoneNodes = MapZonesToInstances(input, input.TabletNodes);
    input.ZoneProxies = MapZonesToInstances(input, input.RpcProxies);
    input.BundleNodes = MapBundlesToInstances(input.TabletNodes);
    input.BundleProxies = MapBundlesToInstances(input.RpcProxies);
    input.PodIdToInstanceName = MapPodIdToInstanceName(input);

    input.ZoneToRacks = MapZonesToRacks(input, mutations);

    TrimNetworkInfo(&input);
    InitializeNodeTagFilters(input, mutations);
    InitializeBundleTargetConfig(input, mutations);

    InitializeVirtualSpareBundle(input);
    CalculateResourceUsage(input);
    input.DatacenterDisrupted = GetDataCenterDisruptedState(input);
    input.BundleToShortName = MapBundlesToShortNames(input);

    // TODO(grachevkirill): Remove condition later.
    if (!input.Config->HasInstanceAllocatorService) {
        InitializeZoneToSpareNodes(input, mutations);
        InitializeZoneToSpareProxies(input, mutations);
    }

    ManageBundlesDynamicConfig(input, mutations);
    ManageInstances(input, mutations);
    ManageCells(input, mutations);
    ManageSystemAccountLimit(input, mutations);
    ManageResourceLimits(input, mutations);

    // TODO(grachevkirill): Remove it later and rewrite node tag filter manager
    // and proxy roles manager.
    InitializeZoneToSpareNodes(input, mutations);
    InitializeZoneToSpareProxies(input, mutations);

    ManageNodeTagFilters(input, mutations);
    ManageRpcProxyRoles(input, mutations);
    ManageBundleShortName(input, mutations);
    ManageDrillsMode(input, mutations);
    MiscBundleChecks(input, mutations);

    mutations->ChangedStates = GetActuallyChangedStates(input, *mutations);
}

////////////////////////////////////////////////////////////////////////////////

TIndexedEntries<TBundleControllerState> MergeBundleStates(
    const TSchedulerInputState& schedulerState,
    const TSchedulerMutations& mutations)
{
    TIndexedEntries<TBundleControllerState> results = schedulerState.BundleStates;

    for (const auto& [bundleName, state] : mutations.ChangedStates) {
        results[bundleName] = NYTree::CloneYsonStruct(state);
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> FlattenAliveInstances(const THashMap<std::string, THashSet<std::string>>& instances)
{
    THashSet<std::string> result;

    for (const auto& [_, dataCenterInstances] : instances) {
        for (const auto& instance : dataCenterInstances) {
            result.insert(instance);
        }
    }

    return result;
}

std::vector<std::string> FlattenBundleInstances(const THashMap<std::string, std::vector<std::string>>& instances)
{
    std::vector<std::string> result;

    for (const auto& [_, dataCenterInstances] : instances) {
        for (const auto& instance : dataCenterInstances) {
            result.push_back(instance);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
