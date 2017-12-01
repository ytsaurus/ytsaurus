#pragma once

#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateOrderedMergeController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateOrderedMapController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateEraseController(
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateRemoteCopyController(
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
