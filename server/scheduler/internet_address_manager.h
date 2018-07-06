#pragma once

#include "private.h"

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddressManager
{
public:
    void ReconcileState(
        const TClusterPtr& cluster);

    void AssignInternetAddressesToPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod,
        NObjects::TNode* node);

    void RevokeInternetAddressesFromPod(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    TNullable<TString> TakeInternetAddress(
        const TString& networkModuleId);

    THashMap<TString, TQueue<TString>> ModuleIdToAddressIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
