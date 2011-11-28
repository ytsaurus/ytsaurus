#include "stdafx.h"
#include "meta_state_manager.h"

#include "simple_meta_state_manager.h"
#include "advanced_meta_state_manager.h"

namespace NYT {
namespace NMetaState {

IMetaStateManager::~IMetaStateManager()
{ }

IMetaStateManager::TPtr IMetaStateManager::CreateInstance(
    const TConfig& config,
    IInvoker* controlInvoker,
    IMetaState* metaState,
    NRpc::IServer* server)
{
    return New<TAdvancedMetaStateManager>(
        config, controlInvoker, metaState, server);
}

} // namespace NMetaState
} // namespace NYT
