#include "internet_address_manager.h"

#include <yp/server/objects/internet_address.h>
#include <yp/server/objects/ip4_address_pool.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/transaction.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

void TInternetAddressManager::ReconcileState(TIP4AddressesPerPool freeAddresses)
{
    FreeAddresses_ = std::move(freeAddresses);
}

std::optional<NObjects::TObjectId> TInternetAddressManager::TakeInternetAddress(
    const NObjects::TObjectId& ip4AddressPoolId)
{
    if (!FreeAddresses_.contains(ip4AddressPoolId)) {
        return std::nullopt;
    }

    auto& queue = FreeAddresses_[ip4AddressPoolId];
    if (queue.empty()) {
        return std::nullopt;
    }

    auto result = queue.front();
    queue.pop();

    return result;
}

void TInternetAddressManager::AssignInternetAddressesToPod(
    const NObjects::TTransactionPtr& transaction,
    NObjects::TPod* pod)
{
    const auto& ip6AddressRequests = pod->Spec().Etc().Load().ip6_address_requests();

    for (int addressIndex = 0; addressIndex < ip6AddressRequests.size(); ++addressIndex) {
        const auto& ip6AddressRequest = ip6AddressRequests[addressIndex];
        if (!ip6AddressRequest.enable_internet() && ip6AddressRequest.ip4_address_pool_id().empty()) {
            continue;
        }

        NObjects::TObjectId ip4AddressPoolId;
        if (ip6AddressRequest.ip4_address_pool_id().empty()) {
            ip4AddressPoolId = NObjects::DefaultIP4AddressPoolId;
        } else {
            ip4AddressPoolId = ip6AddressRequest.ip4_address_pool_id();
        }

        auto scheduledInternetAddressId = TakeInternetAddress(ip4AddressPoolId);
        if (!scheduledInternetAddressId) {
            THROW_ERROR_EXCEPTION("No spare internet addresses for pod %Qv",
                pod->GetId());
        }

        auto* internetAddress = transaction->GetInternetAddress(*scheduledInternetAddressId);
        internetAddress->ValidateExists();

        auto* ip6Address = pod->Status().Etc()->mutable_ip6_address_allocations(addressIndex);
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
    auto& podStatusEtc = pod->Status().Etc();
    for (auto& allocation : *podStatusEtc->mutable_ip6_address_allocations()) {
        if (allocation.has_internet_address()) {
            auto* internetAddress = transaction->GetInternetAddress(allocation.internet_address().id());
            internetAddress->Status()->Clear();
            allocation.clear_internet_address();
            const auto& addressId = internetAddress->GetId();
            const auto& ip4AddressPool = internetAddress->GetParentId();
            FreeAddresses_[ip4AddressPool].push(addressId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNet
