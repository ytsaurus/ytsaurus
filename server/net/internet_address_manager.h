#pragma once

#include <yp/server/objects/transaction.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

using TIP4AddressesPerPool = THashMap<
    NObjects::TObjectId,
    TQueue<NObjects::TObjectId>
>;

class TInternetAddressManager
{
public:
    void ReconcileState(TIP4AddressesPerPool freeAddresses);

    void AssignInternetAddressesToPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    void RevokeInternetAddressesFromPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    std::optional<NObjects::TObjectId> TakeInternetAddress(
        const NObjects::TObjectId& ip4AddressPoolId);

    TIP4AddressesPerPool FreeAddresses_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
