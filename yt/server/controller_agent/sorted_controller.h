#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateSortedReduceController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateJoinReduceController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
