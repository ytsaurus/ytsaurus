#include "internet_address_manager.h"
#include "internet_address.h"
#include "cluster.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/internet_address.h>
#include <yp/server/objects/transaction.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void TInternetAddressManager::ReconcileState(
    const TClusterPtr& cluster)
{
    ModuleIdToAddressIds_.clear();
    for (auto* address : cluster->GetInternetAddresses()) {
        if (!address->Status().has_pod_id()) {
            const auto& networkModuleId = address->Spec().network_module_id();
            ModuleIdToAddressIds_[networkModuleId].push(address->GetId());
        }
    }
}

TNullable<TString> TInternetAddressManager::TakeInternetAddress(
    const TString& networkModuleId)
{
    if (!ModuleIdToAddressIds_.has(networkModuleId)) {
        return Null;
    }

    auto& queue = ModuleIdToAddressIds_[networkModuleId];
    if (queue.empty()) {
        return Null;
    }

    auto result = queue.front();
    queue.pop();

    return result;
}

void TInternetAddressManager::AssignInternetAddressesToPod(
    const NObjects::TTransactionPtr& transaction,
    NObjects::TPod* pod,
    NObjects::TNode* node)
{
    const auto& ip6AddressRequests = pod->Spec().Other().Load().ip6_address_requests();

    for (size_t addressIdx = 0; addressIdx < ip6AddressRequests.size(); ++addressIdx) {
        const auto& ip6_address_request = ip6AddressRequests[addressIdx];
        if (!ip6_address_request.enable_internet()) {
            continue;
        }

        auto scheduledInternetAddressId = TakeInternetAddress(node->Spec().Load().network_module_id());
        if (!scheduledInternetAddressId.HasValue()) {
            THROW_ERROR_EXCEPTION("There is no spare internet address with %Qv network module id for pod %Qv on node %Qv",
                node->Spec().Load().network_module_id(),
                pod->GetId(),
                node->GetId());
        }

        auto* internetAddress = transaction->GetInternetAddress(*scheduledInternetAddressId);
        internetAddress->ValidateExists();

        auto* ip6Address = pod->Status().Other()->mutable_ip6_address_allocations(addressIdx);
        auto* statusInternetAddress = ip6Address->mutable_internet_address();

        statusInternetAddress->set_id(internetAddress->GetId());
        statusInternetAddress->set_ip4_address(internetAddress->Spec().Load().ip4_address());

        internetAddress->Status()->set_pod_id(pod->GetId());
    }
}

void TInternetAddressManager::RevokeInternetAddressesFromPod(
    const NObjects::TTransactionPtr& transaction,
    NObjects::TPod* pod)
{
    auto& podStatusOther = pod->Status().Other();
    for (auto& allocation : *podStatusOther->mutable_ip6_address_allocations()) {
        if (allocation.has_internet_address()) {
            auto* internetAddress = transaction->GetInternetAddress(allocation.internet_address().id());
            internetAddress->Status()->Clear();
            allocation.clear_internet_address();
            ModuleIdToAddressIds_[internetAddress->Spec().Load().network_module_id()].push(internetAddress->GetId());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
