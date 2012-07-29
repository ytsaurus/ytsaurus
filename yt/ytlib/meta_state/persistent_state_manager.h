#pragma once

#include "public.h"

#include <ytlib/rpc/server.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Creates the manager and also registers its RPC service at #server.
IMetaStateManagerPtr CreatePersistentStateManager(
    TPersistentStateManagerConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr stateInvoker,
    IMetaStatePtr metaState,
    NRpc::IServerPtr server);

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
