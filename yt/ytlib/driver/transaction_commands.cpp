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
    auto transactionManager = Host->GetTransactionManager();
    auto newTransaction = transactionManager->Start(~request->Manifest);

    BuildYsonFluently(~Host->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TCommitCommand::DoExecute(TCommitRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortCommand::DoExecute(TAbortRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
