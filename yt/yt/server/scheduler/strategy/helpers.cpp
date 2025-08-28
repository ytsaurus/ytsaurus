#include "helpers.h"

#include "operation.h"

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TOperationPoolTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(const IOperationPtr& operation, const TString& treeId)
{
    return GetOrCrash(operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree, treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
