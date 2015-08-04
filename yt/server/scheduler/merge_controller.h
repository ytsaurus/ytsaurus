#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateOrderedMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
