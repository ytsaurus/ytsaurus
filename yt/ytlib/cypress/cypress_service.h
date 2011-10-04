#pragma once

#include "common.h"
#include "cypress_service_rpc.h"
#include "cypress_state.h"

#include "../rpc/server.h"
#include "../rpc/server.h"

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
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server,
        TCypressState::TPtr state);

private:
    typedef TCypressService TThis;
    typedef TCypressServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    //! Configuration.
    TConfig Config;
    
    //! Meta-state.
    TCypressState::TPtr State;

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
