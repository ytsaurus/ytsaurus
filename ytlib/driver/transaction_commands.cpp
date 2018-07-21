#include "config.h"
#include "transaction_commands.h"

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/api/transaction.h>

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

TStartTransactionCommand::TStartTransactionCommand()
{
    RegisterParameter("type", Type)
        .Default(NTransactionClient::ETransactionType::Master);
    RegisterParameter("attributes", Attributes)
        .Default(nullptr);
    RegisterParameter("sticky", Options.Sticky)
        .Optional();
    RegisterParameter("timeout", Options.Timeout)
        .Optional();
    RegisterParameter("transaction_id", Options.ParentId)
        .Optional();
    RegisterParameter("ping_ancestor_transactions", Options.PingAncestors)
        .Optional();
    RegisterParameter("prerequisite_transaction_ids", Options.PrerequisiteTransactionIds)
        .Optional();
    RegisterParameter("atomicity", Options.Atomicity)
        .Optional();
    RegisterParameter("durability", Options.Durability)
        .Optional();
}

void TStartTransactionCommand::DoExecute(ICommandContextPtr context)
{
    Options.Ping = true;
    Options.AutoAbort = false;

    if (Attributes) {
        Options.Attributes = ConvertToAttributes(Attributes);
    }

    if (!Options.Sticky && Type != ETransactionType::Master) {
        THROW_ERROR_EXCEPTION("Tablet transactions must be sticky");
    }

    auto transaction = WaitFor(context->GetClient()->StartTransaction(Type, Options))
        .ValueOrThrow();

    if (!Options.Sticky) {
        transaction->Detach();
    }

    ProduceSingleOutputValue(context, "transaction_id", transaction->GetId());
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::DoExecute(ICommandContextPtr context)
{
    // Specially for evvers@ :)
    if (!Options.TransactionId) {
        return;
    }

    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Ping())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Commit(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAbortTransactionCommand::TAbortTransactionCommand()
{
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TAbortTransactionCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Abort(Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGenerateTimestampCommand::DoExecute(ICommandContextPtr context)
{
    auto timestampProvider = context->GetClient()->GetTimestampProvider();
    auto timestamp = WaitFor(timestampProvider->GenerateTimestamps())
        .ValueOrThrow();

    ProduceSingleOutputValue(context, "timestamp", timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
