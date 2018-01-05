#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateMapReduceController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
