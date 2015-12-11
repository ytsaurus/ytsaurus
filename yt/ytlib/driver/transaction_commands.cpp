#include "transaction_commands.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/attribute_helpers.h>
#include <yt/core/ytree/fluent.h>

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

    auto transactionOrError = WaitFor(context->GetClient()->StartTransaction(
        ETransactionType::Master,
        Options));

    auto transaction = transactionOrError.ValueOrThrow();
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

    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);

    WaitFor(transaction->Commit(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);

    WaitFor(transaction->Abort(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
