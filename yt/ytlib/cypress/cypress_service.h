#pragma once

#include "common.h"
#include "cypress_service_rpc.h"
#include "cypress_manager.h"

#include "../rpc/server.h"
#include "../meta_state/meta_state_service.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TCypressService> TPtr;
    typedef TCypressServiceConfig TConfig;

    //! Creates an instance.
    TCypressService(
        const TConfig& config,
        TCypressManager::TPtr cypressManager,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server);

private:
    typedef TCypressService TThis;
    typedef TCypressServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    //! Configuration.
    TConfig Config;
    
    //! Meta-state.
    TCypressManager::TPtr CypressManager;

    //! Registers RPC methods.
    void RegisterMethods();

    RPC_SERVICE_METHOD_DECL(NProto, Get);
    RPC_SERVICE_METHOD_DECL(NProto, Set);
    RPC_SERVICE_METHOD_DECL(NProto, Remove);
    RPC_SERVICE_METHOD_DECL(NProto, Lock);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
