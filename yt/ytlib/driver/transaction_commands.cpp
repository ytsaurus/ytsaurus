#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void TStartCommand::DoExecute()
{
    auto oldTransaction = DriverImpl->GetCurrentTransaction();
    if (oldTransaction) {
        oldTransaction->Abort();
        DriverImpl->SetCurrentTransaction(NULL);
    }

    auto transactionManager = DriverImpl->GetTransactionManager();

    auto yson = ManifestArg->getValue();
    auto manifest = yson.empty() ? NULL : DeserializeFromYson(yson);
    auto newTransaction = transactionManager->Start(~manifest);
    DriverImpl->SetCurrentTransaction(~newTransaction);

    BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TCommitCommand::DoExecute()
{
    auto transactionId = TxArg->getValue();
    if (transactionId == NullTransactionId) {
        // TODO(panin): think about setting TxArg to required
        ythrow yexception() << "Transaction id is required argument";
    }
    auto transactionManager = DriverImpl->GetTransactionManager();
    auto transaction = transactionManager->Attach(transactionId);
    transaction->Commit();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortCommand::DoExecute()
{
    auto transactionId = TxArg->getValue();
    if (transactionId == NullTransactionId) {
        // TODO(panin): think about setting TxArg to required
        ythrow yexception() << "Transaction id is required argument";
    }
    auto transactionManager = DriverImpl->GetTransactionManager();
    auto transaction = transactionManager->Attach(transactionId);
    transaction->Abort();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
