#pragma once

#include <yp/server/objects/transaction.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

using TIP4AddressesPerPoolAndNetworkModule = THashMap<
    std::pair<NObjects::TObjectId, NObjects::TObjectId>,
    TQueue<NObjects::TObjectId>
>;

class TInternetAddressManager
{
public:
    void ReconcileState(TIP4AddressesPerPoolAndNetworkModule freeAddresses);

    void AssignInternetAddressesToPod(
        const NObjects::TTransactionPtr& transaction,
        const NObjects::TNode* node,
        NObjects::TPod* pod);

    void RevokeInternetAddressesFromPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    std::optional<NObjects::TObjectId> TakeInternetAddress(
        const NObjects::TObjectId& ip4AddressPoolId,
        const NObjects::TObjectId& networkModuleId);

    TIP4AddressesPerPoolAndNetworkModule FreeAddresses_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
