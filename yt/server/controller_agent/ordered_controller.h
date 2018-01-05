#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateOrderedMergeController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateOrderedMapController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateEraseController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

IOperationControllerPtr CreateRemoteCopyController(
    IOperationControllerHostPtr host,
    TControllerAgentPtr controllerAgent,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
