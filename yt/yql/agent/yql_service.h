#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/server/lib/state_checker/public.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, NStateChecker::TStateCheckerPtr stateChecker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
