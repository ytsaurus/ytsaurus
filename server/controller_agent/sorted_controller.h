#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

IOperationControllerPtr CreateSortedReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

IOperationControllerPtr CreateJoinReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

IOperationControllerPtr CreateAppropriateReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation,
    bool isJoinReduce);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
