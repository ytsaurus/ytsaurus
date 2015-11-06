#include "stdafx.h"
#include "transaction_commands.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>
#include <core/ytree/attribute_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TStartTransactionCommand::Execute(ICommandContextPtr context)
{
    Options.Ping = true;
    Options.AutoAbort = false;

    if (Attributes) {
        Options.Attributes = ConvertToAttributes(Attributes);
    }

    auto transactionManager = context->GetClient()->GetTransactionManager();
    auto transaction = WaitFor(transactionManager->Start(
        ETransactionType::Master,
        Options)).ValueOrThrow();
    transaction->Detach();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(transaction->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::Execute(ICommandContextPtr context)
{
    // Specially for evvers@ :)
    if (!Options.TransactionId)
        return;

    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());
    WaitFor(transaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());

    WaitFor(transaction->Commit(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());

    WaitFor(transaction->Abort(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
