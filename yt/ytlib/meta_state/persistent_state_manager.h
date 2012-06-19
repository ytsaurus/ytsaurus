#pragma once

#include "public.h"

#include <ytlib/rpc/server.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Creates the manager and also registers its RPC service at #server.
IMetaStateManagerPtr CreatePersistentStateManager(
    TPersistentStateManagerConfig* config,
    IInvokerPtr controlInvoker,
    IInvokerPtr stateInvoker,
    IMetaState* metaState,
    NRpc::IServer* server);

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
