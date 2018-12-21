#include "internet_address_manager.h"
#include "internet_address.h"
#include "cluster.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/internet_address.h>
#include <yp/server/objects/transaction.h>

namespace NYP::NServer::NScheduler {

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

std::optional<TString> TInternetAddressManager::TakeInternetAddress(
    const TString& networkModuleId)
{
    if (!ModuleIdToAddressIds_.contains(networkModuleId)) {
        return std::nullopt;
    }

    auto& queue = ModuleIdToAddressIds_[networkModuleId];
    if (queue.empty()) {
        return std::nullopt;
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

    for (int addressIndex = 0; addressIndex < ip6AddressRequests.size(); ++addressIndex) {
        const auto& ip6AddressRequest = ip6AddressRequests[addressIndex];
        if (!ip6AddressRequest.enable_internet()) {
            continue;
        }

        auto scheduledInternetAddressId = TakeInternetAddress(node->Spec().Load().network_module_id());
        if (!scheduledInternetAddressId) {
            THROW_ERROR_EXCEPTION("No spare internet addresses in network module %Qv for pod %Qv at node %Qv",
                node->Spec().Load().network_module_id(),
                pod->GetId(),
                node->GetId());
        }

        auto* internetAddress = transaction->GetInternetAddress(*scheduledInternetAddressId);
        internetAddress->ValidateExists();

        auto* ip6Address = pod->Status().Other()->mutable_ip6_address_allocations(addressIndex);
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

} // namespace NYP::NServer::NScheduler
