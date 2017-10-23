#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMapController(
    IOperationHost* host,
    NScheduler::TOperation* operation);

IOperationControllerPtr CreateUnorderedMergeController(
    IOperationHost* host,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
