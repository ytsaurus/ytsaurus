#include "stdafx.h"
#include "transaction_commands.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/fluent.h>
#include <core/ytree/attribute_helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

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
    options.Timeout = Request_->Timeout;
    options.ParentId = Request_->TransactionId;
    options.MutationId = Request_->MutationId;
    options.Ping = true;
    options.AutoAbort = false;
    options.PingAncestors = Request_->PingAncestors;

    std::unique_ptr<IAttributeDictionary> attributes;
    if (Request_->Attributes) {
        options.Attributes = ConvertToAttributes(Request_->Attributes);
    }

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

    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    auto result = WaitFor(transaction->Ping());
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    TTransactionCommitOptions options;
    options.MutationId = GenerateMutationId();
    auto result = WaitFor(transaction->Commit(options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    TTransactionAbortOptions options;
    options.Force = Request_->Force;
    options.MutationId = GenerateMutationId();
    auto result = WaitFor(transaction->Abort(options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
