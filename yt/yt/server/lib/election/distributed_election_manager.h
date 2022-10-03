#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

IElectionManagerPtr CreateDistributedElectionManager(
    TDistributedElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    NRpc::IServerPtr rpcServer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
