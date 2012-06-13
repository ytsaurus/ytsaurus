#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NCypress;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::DoExecute()
{
    auto attributes = ConvertToAttributes(Request->GetOptions());
    auto transactionManager = Context->GetTransactionManager();
    auto newTransaction = transactionManager->Start(
        ~attributes,
        Request->TransactionId);

    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());

    newTransaction->Detach();
}

////////////////////////////////////////////////////////////////////////////////

void TRenewTransactionCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(GetTransactionId(true)));
    auto rsp = proxy.Execute(req).Get();

    if (!rsp->IsOK()) {
        ReplyError(rsp->GetError());
    }
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
