#pragma once

#include "meta_state_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

IMetaStateManager::TPtr CreatePersistentStateManager(
    const IMetaStateManager::TConfig& config,
    IInvoker* controlInvoker,
    IMetaState* metaState,
    NRpc::IRpcServer* server);

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
