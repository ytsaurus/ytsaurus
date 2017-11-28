#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateSortedReduceController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateJoinReduceController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
