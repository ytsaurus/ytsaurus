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

void TStartTransactionCommand::DoExecute()
{
    TTransactionStartOptions options;
    SetMutatingOptions(&options);
    options.Timeout = Request_->Timeout;
    options.ParentId = Request_->TransactionId;
    options.Ping = true;
    options.AutoAbort = false;
    options.PingAncestors = Request_->PingAncestors;
    if (Request_->Attributes) {
        options.Attributes = ConvertToAttributes(Request_->Attributes);
    }
    SetPrerequisites(&options);

    auto transactionManager = Context_->GetClient()->GetTransactionManager();
    auto transaction = WaitFor(transactionManager->Start(
        ETransactionType::Master,
        options)).ValueOrThrow();
    transaction->Detach();

    Reply(BuildYsonStringFluently()
        .Value(transaction->GetId()));
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::DoExecute()
{
    // Specially for evvers@ :)
    if (Request_->TransactionId == NullTransactionId)
        return;

    auto transaction = AttachTransaction(true);
    WaitFor(transaction->Ping())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = AttachTransaction(true);
    TTransactionCommitOptions options;
    SetMutatingOptions(&options);
    SetPrerequisites(&options);
    WaitFor(transaction->Commit(options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = AttachTransaction(true);
    TTransactionAbortOptions options;
    SetMutatingOptions(&options);
    SetPrerequisites(&options);
    options.Force = Request_->Force;
    WaitFor(transaction->Abort(options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
