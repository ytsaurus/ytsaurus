#pragma once

#include "private.h"

#include <util/generic/hash.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IGlobalResourceAllocator
    : public virtual TRefCounted
{
    virtual void ReconcileState(const TClusterPtr& cluster) = 0;
    virtual TErrorOr<TNode*> ComputeAllocation(TPod* pod) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGlobalResourceAllocator)

////////////////////////////////////////////////////////////////////////////////

IGlobalResourceAllocatorPtr CreateGlobalResourceAllocator(TGlobalResourceAllocatorConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
