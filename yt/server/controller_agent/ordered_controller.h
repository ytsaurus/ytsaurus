#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateOrderedMergeController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateOrderedMapController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateEraseController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateRemoteCopyController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
