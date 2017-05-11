#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateSortedMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateSortedReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateJoinReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
