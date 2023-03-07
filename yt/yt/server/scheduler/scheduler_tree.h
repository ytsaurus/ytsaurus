#pragma once

#include "fair_share_tree.h"
#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class TFairShareImpl>
class ISchedulerTreeHost
    : public virtual TRefCounted
{
private:
    using TTree = TFairShareTree<TFairShareImpl>;

public:
    // May have context switches.
    virtual void OnOperationReadyInTree(TOperationId operationId, TTree* tree) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
