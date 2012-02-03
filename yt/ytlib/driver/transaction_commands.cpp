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
    auto oldTransaction = DriverImpl->GetCurrentTransaction();
    if (oldTransaction) {
        oldTransaction->Abort();
        DriverImpl->SetCurrentTransaction(NULL);
    }

    auto transactionManager = DriverImpl->GetTransactionManager();
    auto newTransaction = transactionManager->Start(~request->Manifest);
    DriverImpl->SetCurrentTransaction(~newTransaction);

    BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
        .BeginMap()
            .Item("transaction_id").Scalar(newTransaction->GetId().ToString())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute(TCommitTransactionRequest* request)
{
    auto transaction = DriverImpl->GetCurrentTransaction(true);
    transaction->Commit();
    DriverImpl->SetCurrentTransaction(NULL);
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute(TAbortTransactionRequest* request)
{
    auto transaction = DriverImpl->GetCurrentTransaction(true);
    transaction->Abort();
    DriverImpl->SetCurrentTransaction(NULL);
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
