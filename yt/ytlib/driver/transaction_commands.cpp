#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TStartTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Node);
}

void TStartTransactionCommand::DoExecute(TStartRequestPtr request)
{
    auto attributes = IAttributeDictionary::FromMap(request->GetOptions());
    auto transactionManager = Host->GetTransactionManager();
    auto newTransaction = transactionManager->Start(
        ~attributes,
        request->TransactionId);

    BuildYsonFluently(~Host->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TCommitTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TCommitTransactionCommand::DoExecute(TCommitRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

TCommandDescriptor TAbortTransactionCommand::GetDescriptor()
{
    return TCommandDescriptor(EDataType::Null, EDataType::Null);
}

void TAbortTransactionCommand::DoExecute(TAbortTransactionRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Abort(true);
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
