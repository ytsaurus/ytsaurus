#pragma once

#include "fair_share_tree.h"
#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulerTreeHost
    : public virtual TRefCounted
{
    // May have context switches.
    virtual void OnOperationReadyInTree(TOperationId operationId, TFairShareTree* tree) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulerTreeHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
