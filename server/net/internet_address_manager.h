#pragma once

#include <yp/server/objects/transaction.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

using TNetworkModuleId = TString;
using TIP4AddressPoolIdToFreeIP4Addresses = THashMap<std::pair<NObjects::TObjectId, TNetworkModuleId>, TQueue<NObjects::TObjectId>>;

class TInternetAddressManager
{
public:
    void ReconcileState(
        TIP4AddressPoolIdToFreeIP4Addresses moduleIdToAddressIds);

    void AssignInternetAddressesToPod(
        const NObjects::TTransactionPtr& transaction,
        const NObjects::TNode* node,
        NObjects::TPod* pod);

    void RevokeInternetAddressesFromPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    static TString GetDefaultIP4AddressPoolId();

private:
    std::optional<TString> TakeInternetAddress(
        const NObjects::TObjectId& ip4AddressPoolId,
        const TNetworkModuleId& networkModuleId);

    TIP4AddressPoolIdToFreeIP4Addresses IP4AddressPoolIdToFreeAddresses_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
