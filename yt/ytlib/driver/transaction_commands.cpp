#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

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

void TCommitTransactionCommand::DoExecute(TCommitRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute(TAbortRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Abort(true);
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
