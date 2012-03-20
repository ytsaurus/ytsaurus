#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::DoExecute(TStartTransactionRequest* request)
{
    auto transactionManager = DriverImpl->GetTransactionManager();
    auto newTransaction = transactionManager->Start(~request->Manifest);

    BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute(TCommitTransactionRequest* request)
{
    auto transaction = DriverImpl->GetTransaction(request, true);
    transaction->Commit();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute(TAbortTransactionRequest* request)
{
    auto transaction = DriverImpl->GetTransaction(request, true);
    transaction->Commit();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
