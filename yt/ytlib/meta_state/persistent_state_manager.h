    #pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Creates the manager and also registers its RPC service at #server.
IMetaStateManagerPtr CreatePersistentStateManager(
    TPersistentStateManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr stateInvoker,
    IMetaStatePtr metaState,
    NRpc::IServerPtr rpcServer);

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
