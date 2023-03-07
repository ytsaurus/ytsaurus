#pragma once

#include "private.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

IOperationControllerPtr CreateAppropriateReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation,
    bool isJoinReduce);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
