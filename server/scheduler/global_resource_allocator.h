#pragma once

#include "private.h"

#include <util/generic/hash.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocator
{
public:
    void ReconcileState(
        const TClusterPtr& cluster);

    TErrorOr<TNode*> ComputeAllocation(TPod* pod);

private:
    bool TryAllocation(TNode* node, TPod* pod);

private:
    TClusterPtr Cluster_;
    THashMap<TString, size_t> NetworkModuleIdToFreeAddressCount_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
