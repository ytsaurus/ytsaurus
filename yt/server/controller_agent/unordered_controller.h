#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMapController(
    TControllerAgentPtr controllerAgent,
    NScheduler::TOperation* operation);

IOperationControllerPtr CreateUnorderedMergeController(
    TControllerAgentPtr controllerAgent,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
