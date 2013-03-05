#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::DoExecute()
{
    TTransactionStartOptions options;
    options.Timeout = Request->Timeout;
    options.ParentId = Request->TransactionId;
    options.Ping = true;
    options.PingAncestors = Request->PingAncestorTransactions;
    if (Request->Attributes) {
        options.Attributes = ConvertToAttributes(Request->Attributes);
    }

    auto transactionManager = Context->GetTransactionManager();
    auto transaction = transactionManager->Start(options);

    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Value(transaction->GetId());

    transaction->Detach();
}

////////////////////////////////////////////////////////////////////////////////

void TRenewTransactionCommand::DoExecute()
{
    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(GetTransactionId(true)));
    req->set_renew_ancestors(Request->PingAncestorTransactions);

    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true);
    transaction->Commit();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true);
    transaction->Abort(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
