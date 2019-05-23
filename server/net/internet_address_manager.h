#pragma once

#include <yp/server/objects/transaction.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddressManager
{
public:
    void ReconcileState(
        THashMap<TString, TQueue<TString>> moduleIdToAddressIds);

    void AssignInternetAddressesToPod(
        const NObjects::TTransactionPtr& transaction,
        const NObjects::TNode* node,
        NObjects::TPod* pod);

    void RevokeInternetAddressesFromPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    std::optional<TString> TakeInternetAddress(
        const TString& networkModuleId);

    THashMap<TString, TQueue<TString>> ModuleIdToAddressIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
