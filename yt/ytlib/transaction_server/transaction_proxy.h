#pragma once

#include "transaction.h"
#include "transaction_manager.h"
#include "transaction_ypath.pb.h"

#include <yt/ytlib/object_server/object_detail.h>
#include <yt/ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionProxy
    : public NObjectServer::TObjectProxyBase<TTransaction>
{
public:
    TTransactionProxy(TTransactionManager* transactionManager, const TTransactionId& id);

    virtual bool IsLogged(NRpc::IServiceContext* context) const;

private:
    typedef TObjectProxyBase<TTransaction> TBase;

    TTransactionManager::TPtr TransactionManager;

    void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NProto, Commit);
    DECLARE_RPC_SERVICE_METHOD(NProto, Abort);
    DECLARE_RPC_SERVICE_METHOD(NProto, RenewLease);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
