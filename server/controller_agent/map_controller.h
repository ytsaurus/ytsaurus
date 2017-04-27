#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    NScheduler::TOperation* operation);

IOperationControllerPtr CreateUnorderedMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
