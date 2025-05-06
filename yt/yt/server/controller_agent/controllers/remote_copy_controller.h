#pragma once

#include "private.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateRemoteCopyController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
