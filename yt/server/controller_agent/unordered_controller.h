#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMapController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    NScheduler::TOperation* operation);

IOperationControllerPtr CreateUnorderedMergeController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
