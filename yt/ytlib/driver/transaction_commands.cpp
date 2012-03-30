#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void TStartCommand::DoExecute(TStartRequest* request)
{
    auto transactionManager = Host->GetTransactionManager();
    auto newTransaction = transactionManager->Start(~request->Manifest);

    BuildYsonFluently(~Host->CreateOutputConsumer())
        .Scalar(newTransaction->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void TCommitCommand::DoExecute(TCommitRequest* request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortCommand::DoExecute(TAbortRequest* request)
{
    auto transaction = Host->GetTransaction(request, true);
    transaction->Commit();
    Host->ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
