#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateMapReduceController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
