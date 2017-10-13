#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateRemoteCopyController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

