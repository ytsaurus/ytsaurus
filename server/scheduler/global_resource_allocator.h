#pragma once

#include "private.h"

#include <util/generic/hash.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IGlobalResourceAllocator
    : public virtual TRefCounted
{
    virtual void ReconcileState(const NCluster::TClusterPtr& cluster) = 0;
    virtual TErrorOr<NCluster::TNode*> ComputeAllocation(NCluster::TPod* pod) = 0;
};

DEFINE_REFCOUNTED_TYPE(IGlobalResourceAllocator)

////////////////////////////////////////////////////////////////////////////////

IGlobalResourceAllocatorPtr CreateGlobalResourceAllocator(
    TGlobalResourceAllocatorConfigPtr config,
    NCluster::IObjectFilterEvaluatorPtr nodeFilterEvaluator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
