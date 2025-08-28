#pragma once

#include "public.h"

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TOperationPoolTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(const IOperationPtr& operation, const TString& treeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
