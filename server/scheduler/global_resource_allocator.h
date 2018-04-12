#pragma once

#include "private.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TGlobalResourceAllocator
{
public:
    explicit TGlobalResourceAllocator(const TClusterPtr& cluster);

    TErrorOr<TNode*> ComputeAllocation(TPod* pod);

private:
    bool TryAllocation(TNode* node, TPod* pod);

private:
    class TAllocationContext;

    const TClusterPtr Cluster_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
