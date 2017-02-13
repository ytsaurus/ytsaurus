#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

IElectionManagerPtr CreateDistributedElectionManager(
    TDistributedElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    NRpc::IServerPtr rpcServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
