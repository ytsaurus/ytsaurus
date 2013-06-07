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
    options.MutationId = Request->MutationId;
    options.Ping = true;
    options.PingAncestors = Request->PingAncestors;

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

void TPingTransactionCommand::DoExecute()
{
    auto transactionId = GetTransactionId(false);
    // Specially for evvers@ :)
    if (transactionId == NullTransactionId)
        return;

    auto req = TTransactionYPathProxy::Ping(transactionId);
    req->set_ping_ancestors(Request->PingAncestors);

    auto rsp = ObjectProxy->Execute(req).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true, false);
    transaction->Commit(GenerateMutationId());
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true, false);
    transaction->Abort(true, GenerateMutationId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
