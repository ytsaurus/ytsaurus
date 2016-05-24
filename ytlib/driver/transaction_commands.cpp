#include "config.h"
#include "transaction_commands.h"

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/common.h>

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

    if (Sticky) {
        auto transaction = WaitFor(context->GetClient()->StartTransaction(Type, Options))
            .ValueOrThrow();
        auto timeout = Options.Timeout.Get(context->GetConfig()->TransactionManager->DefaultTransactionTimeout);

        context->PinTransaction(transaction, timeout);

        context->ProduceOutputValue(BuildYsonStringFluently()
            .Value(transaction->GetId()));
        // TODO(sandello): Return more information about transaction here.
    } else {
        if (Type != ETransactionType::Master) {
            THROW_ERROR_EXCEPTION("Only master transactions could be sticky")
                << TErrorAttribute("requested_transaction_type", Type);
        }

        auto transactionManager = context->GetClient()->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->Start(
            ETransactionType::Master,
            Options)).ValueOrThrow();
        transaction->Detach();

        context->ProduceOutputValue(BuildYsonStringFluently()
            .Value(transaction->GetId()));
    }
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

    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());
    WaitFor(transaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::Execute(ICommandContextPtr context)
{
    auto stickyTransaction = context->FindAndTouchTransaction(Options.TransactionId);
    if (stickyTransaction) {
        WaitFor(stickyTransaction->Commit())
            .ThrowOnError();
        context->UnpinTransaction(Options.TransactionId);
        return;
    }

    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());

    WaitFor(transaction->Commit(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Execute(ICommandContextPtr context)
{
    auto stickyTransaction = context->FindAndTouchTransaction(Options.TransactionId);
    if (stickyTransaction) {
        WaitFor(stickyTransaction->Abort())
            .ThrowOnError();
        context->UnpinTransaction(Options.TransactionId);
        return;
    }

    auto transaction = AttachTransaction(true, context->GetClient()->GetTransactionManager());

    WaitFor(transaction->Abort(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
