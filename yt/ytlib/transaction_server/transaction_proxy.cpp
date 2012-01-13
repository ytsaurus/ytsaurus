#include "stdafx.h"
#include "transaction_proxy.h"

namespace NYT {
namespace NTransactionServer {

using namespace NRpc;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TTransactionProxy::TTransactionProxy(
    TTransactionManager* transactionManager,
    const TTransactionId& id)
    : TBase(id, &transactionManager->TransactionMap)
    , TransactionManager(transactionManager)
{ }

void TTransactionProxy::DoInvoke(IServiceContext* context)
{
    Stroka verb = context->GetVerb();
    if (verb == "Commit") {
        CommitThunk(context);
    } else if (verb == "Abort") {
        AbortThunk(context);
    } else if (verb == "RenewLease") {
        RenewLeaseThunk(context);
    } else {
        TBase::DoInvoke(context);
    }
}

bool TTransactionProxy::IsLogged(IServiceContext* context) const
{
    Stroka verb = context->GetVerb();
    if (verb == "Commit" ||
        verb == "Abort")
    {
        return true;
    }
    return TBase::IsLogged(context);;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TTransactionProxy, Commit)
{
    UNUSED(request);
    UNUSED(response);

    TransactionManager->Commit(GetImplForUpdate());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTransactionProxy, Abort)
{
    UNUSED(request);
    UNUSED(response);

    TransactionManager->Abort(GetImplForUpdate());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTransactionProxy, RenewLease)
{
    UNUSED(request);
    UNUSED(response);

    TransactionManager->RenewLease(GetId());
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

