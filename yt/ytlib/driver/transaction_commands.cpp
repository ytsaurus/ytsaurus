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

//TCommandDescriptor TStartTransactionCommand::GetDescriptor()
//{
//    return TCommandDescriptor(EDataType::Null, EDataType::Node);
//}

void TStartTransactionCommand::DoExecute()
{
    auto attributes = IAttributeDictionary::FromMap(Request->GetOptions());
    auto transactionManager = Context->GetTransactionManager();
    auto newTransaction = transactionManager->Start(
        ~attributes,
        Request->TransactionId);

    BuildYsonFluently(~Context->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

//TCommandDescriptor TRenewTransactionCommand::GetDescriptor()
//{
//    return TCommandDescriptor(EDataType::Null, EDataType::Null);
//}

void TRenewTransactionCommand::DoExecute()
{
    TObjectServiceProxy proxy(Context->GetMasterChannel());
    auto req = TTransactionYPathProxy::RenewLease(FromObjectId(Request->TransactionId));
    auto rsp = proxy.Execute(req).Get();

    if (rsp->IsOK()) {
        ReplySuccess();
    } else {
        ReplyError(rsp->GetError());
    }
}

////////////////////////////////////////////////////////////////////////////////

//TCommandDescriptor TCommitTransactionCommand::GetDescriptor()
//{
//    return TCommandDescriptor(EDataType::Null, EDataType::Null);
//}

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true);
    transaction->Commit();
    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

//TCommandDescriptor TAbortTransactionCommand::GetDescriptor()
//{
//    return TCommandDescriptor(EDataType::Null, EDataType::Null);
//}

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(true);
    transaction->Abort(true);
    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
