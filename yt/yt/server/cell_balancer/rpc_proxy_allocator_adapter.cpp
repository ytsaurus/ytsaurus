#include "allocator_adapter.h"

#include "cypress_bindings.h"
#include "helpers.h"
#include "input_state.h"
#include "mutations.h"

namespace NYT::NCellBalancer {

using namespace NBundleControllerClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = BundleControllerLogger;

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
    : public IAllocatorAdapter
{
public:
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

    const TInstanceResourcesPtr& GetResourceGuarantee(const TBundleInfoPtr& bundleInfo) const
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

    const std::string& GetHumanReadableInstanceType() const
    {
        static const std::string RpcProxy = "RPC proxy";
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

    TInstanceInfoBasePtr FindInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrDefault(input.RpcProxies, instanceName, nullptr);
    }

    TInstanceInfoBasePtr GetInstanceInfo(const std::string& instanceName, const TSchedulerInputState& input)
    {
        return GetOrCrash(input.RpcProxies, instanceName);
    }

    void SetInstanceAnnotations(const std::string& instanceName, TBundleControllerInstanceAnnotationsPtr bundleControllerAnnotations, TSchedulerMutations* mutations)
    {
        mutations->ChangedProxyAnnotations[instanceName] = mutations->WrapMutation(std::move(bundleControllerAnnotations));
    }

    bool IsInstanceReadyToBeDeallocated(
        const std::string& /*instanceName*/,
        const std::string& /*deallocationId*/,
        TDuration /*deallocationAge*/,
        const std::string& /*bundleName*/,
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
            YT_LOG_INFO("Removing proxy role (BundleName: %v, ProxyName: %v)",
                bundleName,
                proxyName);
            mutations->RemovedProxyRole.insert(mutations->WrapMutation(proxyName));
            return false;
        }

        const auto& bundleControllerAnnotations = proxyInfo->BundleControllerAnnotations;

        if (auto changed = GetBundleControllerInstanceAnnotationsToSet(bundleName, dataCenterName, allocationInfo, bundleControllerAnnotations)) {
            YT_LOG_DEBUG("Setting proxy annotations (BundleName: %v, NodeName: %v, Annotations: %v)",
                bundleName,
                proxyName,
                ConvertToYsonString(changed, EYsonFormat::Text));
            mutations->ChangedProxyAnnotations[proxyName] = mutations->WrapMutation(changed);
            return false;
        }

        if (bundleControllerAnnotations->AllocatedForBundle != bundleName) {
            YT_LOG_WARNING("Inconsistent allocation state (AnnotationsBundleName: %v, ActualBundleName: %v, ProxyName: %v)",
                bundleControllerAnnotations->AllocatedForBundle,
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
        const std::string& bundleName,
        const std::string& proxyName,
        const std::string& strategy,
        const TSchedulerInputState& input,
        TSchedulerMutations* mutations)
    {
        YT_VERIFY(!strategy.empty());

        auto instanceInfoBase = GetInstanceInfo(proxyName, input);
        const auto* instanceInfo = dynamic_cast<TRpcProxyInfo*>(instanceInfoBase.Get());
        const auto& bundleControllerAnnotations = instanceInfo->BundleControllerAnnotations;
        if (strategy != DeallocationStrategyReturnToSpareBundle && (!bundleControllerAnnotations->AllocatedForBundle.empty() || bundleControllerAnnotations->Allocated)) {
            auto newAnnotations = New<TBundleControllerInstanceAnnotations>();
            newAnnotations->DeallocatedAt = TInstant::Now();
            newAnnotations->DeallocationStrategy = strategy;
            mutations->ChangedProxyAnnotations[proxyName] = mutations->WrapMutation(newAnnotations);
            return false;
        }

        if (strategy == DeallocationStrategyReturnToSpareBundle && bundleControllerAnnotations->AllocatedForBundle == bundleName) {
            auto newAnnotations = NYTree::CloneYsonStruct(bundleControllerAnnotations);
            newAnnotations->AllocatedForBundle = "";
            mutations->ChangedProxyAnnotations[proxyName] = mutations->WrapMutation(newAnnotations);
            return false;
        }

        if (instanceInfo->Role != TrashRole) {
            mutations->ChangedProxyRole[proxyName] = mutations->WrapMutation(TrashRole);
            return false;
        }

        return true;
    }

    void SetDefaultSpareAttributes(const std::string& proxyName, TSchedulerMutations* mutations) const
    {
        mutations->ChangedProxyRole[proxyName] = mutations->WrapMutation(DefaultRole);
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
            const auto& instanceResource = proxyInfo->BundleControllerAnnotations->Resource;

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

IAllocatorAdapterPtr CreateRpcProxyAllocatorAdapter(
    TBundleControllerStatePtr state,
    const TDataCenterToInstanceMap& bundleProxies,
    const THashMap<std::string, THashSet<std::string>>& aliveProxies)
{
    return New<TRpcProxyAllocatorAdapter>(
        std::move(state),
        bundleProxies,
        aliveProxies);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
