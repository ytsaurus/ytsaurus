#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void TStartCommand::DoExecute(TStartRequestPtr request)
{
    auto transactionManager = DriverImpl->GetTransactionManager();
    auto newTransaction = transactionManager->Start(~request->Manifest);

    BuildYsonFluently(~DriverImpl->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TCommitCommand::DoExecute(TCommitRequestPtr request)
{
    auto transaction = DriverImpl->GetTransaction(request, true);
    transaction->Commit();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortCommand::DoExecute(TAbortRequestPtr request)
{
    auto transaction = DriverImpl->GetTransaction(request, true);
    transaction->Commit();
    DriverImpl->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
