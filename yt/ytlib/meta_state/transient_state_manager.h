#pragma once

#include "common.h"
#include "meta_state_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

IMetaStateManager::TPtr CreateTransientStateManager(
    const IMetaStateManager::TConfig& config,
    IInvoker* controlInvoker,
    IMetaState* metaState,
    NRpc::IServer* server);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
