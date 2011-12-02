#pragma once

#include "common.h"
#include "transaction_manager.h"
#include "transaction_service_rpc.h"

#include "../meta_state/meta_state_service.h"
#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////
    
class TTransactionService
    : public NMetaState::TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TTransactionService> TPtr;

    //! Creates an instance.
    TTransactionService(
        NMetaState::IMetaStateManager* metaStateManager,
        TTransactionManager* transactionManager,
        NRpc::IRpcServer* server);

private:
    typedef TTransactionService TThis;
    typedef TTransactionServiceProxy::EErrorCode EErrorCode;

    TTransactionManager::TPtr TransactionManager;

    void ValidateTransactionId(const TTransactionId& id);

    DECLARE_RPC_SERVICE_METHOD(NProto, StartTransaction);
    DECLARE_RPC_SERVICE_METHOD(NProto, CommitTransaction);
    DECLARE_RPC_SERVICE_METHOD(NProto, AbortTransaction);
    DECLARE_RPC_SERVICE_METHOD(NProto, RenewTransactionLease);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
