#include "config.h"
#include "transaction_commands.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/helpers.h>
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

    if (!Sticky && Type != ETransactionType::Master) {
        THROW_ERROR_EXCEPTION("Only master transactions could be non-sticky")
            << TErrorAttribute("requested_transaction_type", Type);
    }

    auto transaction = WaitFor(context->GetClient()->StartTransaction(Type, Options))
        .ValueOrThrow();

    if (Sticky) {
        auto timeout = Options.Timeout.Get(context->GetConfig()->TransactionManager->DefaultTransactionTimeout);
        context->PinTransaction(transaction, timeout);
    }

    transaction->Detach();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(transaction->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::Execute(ICommandContextPtr context)
{
    // Specially for evvers@ :)
    if (!Options.TransactionId) {
        return;
    }

    auto stickyTransaction = context->FindAndTouchTransaction(Options.TransactionId);
    if (stickyTransaction) {
        return;
    }

    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = context->FindAndTouchTransaction(Options.TransactionId);
    if (!transaction) {
        transaction = AttachTransaction(context, true);
    } else {
        context->UnpinTransaction(Options.TransactionId);
    }

    WaitFor(transaction->Commit(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = context->FindAndTouchTransaction(Options.TransactionId);
    if (!transaction) {
        transaction = AttachTransaction(context, true);
    } else {
        context->UnpinTransaction(Options.TransactionId);
    }

    WaitFor(transaction->Abort(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
