#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateSortedReduceController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateJoinReduceController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
