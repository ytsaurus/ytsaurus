#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NCypress;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TStartTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TStartTransactionCommand::DoExecute(TStartRequestPtr request)
{
    auto attributes = IAttributeDictionary::FromMap(request->GetOptions());
    auto transactionManager = Context->GetTransactionManager();
    auto newTransaction = transactionManager->Start(
        ~attributes,
        request->TransactionId);

    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TRenewTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TRenewTransactionCommand::DoExecute(TRenewRequestPtr request)
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(request->TransactionId));
    auto response = proxy.Execute(req).Get();

    if (response->IsOK()) {
        Context->ReplySuccess();
    } else {
        Context->ReplyError(response->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TCommitTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TCommitTransactionCommand::DoExecute(TCommitRequestPtr request)
{
    auto transaction = Context->GetTransaction(request, true);
    transaction->Commit();
    Context->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TAbortTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TAbortTransactionCommand::DoExecute(TAbortTransactionRequestPtr request)
{
    auto transaction = Context->GetTransaction(request, true);
    transaction->Abort(true);
    Context->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
