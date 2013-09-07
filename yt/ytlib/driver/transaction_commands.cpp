#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/concurrency/fiber.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::DoExecute()
{
    TTransactionStartOptions options;
    options.Timeout = Request->Timeout;
    options.ParentId = Request->TransactionId;
    options.MutationId = Request->MutationId;
    options.Ping = true;
    options.AutoAbort = false;
    options.PingAncestors = Request->PingAncestors;

    if (Request->Attributes) {
        options.Attributes = ConvertToAttributes(Request->Attributes);
    }

    auto transactionManager = Context->GetTransactionManager();
    auto transactionOrError = WaitFor(transactionManager->AsyncStart(options));
    auto transaction = transactionOrError.GetValueOrThrow();
    transaction->Detach();

    ReplySuccess(BuildYsonStringFluently()
        .Value(transaction->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::DoExecute()
{
    // Specially for evvers@ :)
    if (Request->TransactionId == NullTransactionId)
        return;

    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    auto result = WaitFor(transaction->AsyncPing());
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    auto result = WaitFor(transaction->AsyncCommit(GenerateMutationId()));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    auto result =WaitFor(transaction->AsyncAbort(true, GenerateMutationId()));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
