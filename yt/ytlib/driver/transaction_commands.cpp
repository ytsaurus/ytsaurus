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
    auto transactionId = request->TransactionId;
    if (transactionId == NullTransactionId ||
        transactionId == DriverImpl->GetCurrentTransactionId())
    {
        auto transaction = DriverImpl->GetCurrentTransaction(true);
        transaction->Commit();
        DriverImpl->SetCurrentTransaction(NULL);
        DriverImpl->ReplySuccess();
    } else {
        auto transactionManager = DriverImpl->GetTransactionManager();
        auto transaction = transactionManager->Attach(transactionId);
        transaction->Commit();
        DriverImpl->ReplySuccess();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute(TAbortTransactionRequest* request)
{
    auto transactionId = request->TransactionId;
    if (transactionId == NullTransactionId ||
        transactionId == DriverImpl->GetCurrentTransactionId())
    {
        auto transaction = DriverImpl->GetCurrentTransaction(true);
        transaction->Abort();
        DriverImpl->SetCurrentTransaction(NULL);
        DriverImpl->ReplySuccess();
    } else {
        auto transactionManager = DriverImpl->GetTransactionManager();
        auto transaction = transactionManager->Attach(transactionId);
        transaction->Abort();
        DriverImpl->ReplySuccess();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
