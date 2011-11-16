#pragma once

#include "common.h"
#include "cypress_service_rpc.h"
#include "cypress_manager.h"

#include "../rpc/server.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressService
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TCypressService> TPtr;

    //! Creates an instance.
    TCypressService(
        IInvoker* invoker,
        TCypressManager* cypressManager,
        NTransaction::TTransactionManager* transactionManager,
        NRpc::IServer* server);

private:
    typedef TCypressService TThis;
    typedef TCypressServiceProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    TCypressManager::TPtr CypressManager;
    NTransaction::TTransactionManager::TPtr TransactionManager;

    void ValidateTransactionId(const TTransactionId& transactionId);

    RPC_SERVICE_METHOD_DECL(NProto, Execute);
    RPC_SERVICE_METHOD_DECL(NProto, GetNodeId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
