#pragma once

#include "public.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

IOperationControllerPtr CreateEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
