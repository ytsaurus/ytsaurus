#pragma once

#include "common.h"
#include "registry_service_rpc.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_state_service.h"
#include "../meta_state/map.h"
#include "../rpc/server.h"

namespace NYT {
namespace NRegistry {

////////////////////////////////////////////////////////////////////////////////

class TRegistryService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TRegistryService> TPtr;
    typedef TRegistryServiceConfig TConfig;

    //! Creates an instance.
    TRegistryService(
        const TConfig& config,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        NRpc::TServer::TPtr server,
        TTransactionManager::TPtr transactionManager);

private:
    typedef TRegistryService TThis;
    typedef TRegistryServiceProxy::EErrorCode EErrorCode;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();

    RPC_SERVICE_METHOD_DECL(NProto, Get);
    RPC_SERVICE_METHOD_DECL(NProto, Set);
    RPC_SERVICE_METHOD_DECL(NProto, Remove);
    RPC_SERVICE_METHOD_DECL(NProto, Lock);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRegistry
} // namespace NYT
