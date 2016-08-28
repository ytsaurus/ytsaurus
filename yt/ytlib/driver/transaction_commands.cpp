#include "config.h"
#include "transaction_commands.h"

#include <yt/ytlib/transaction_client/timestamp_provider.h>

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

    if (!Options.Sticky && Type != ETransactionType::Master) {
        THROW_ERROR_EXCEPTION("Tablet transactions must be sticky");
    }

    auto transaction = WaitFor(context->GetClient()->StartTransaction(Type, Options))
        .ValueOrThrow();

    if (!Options.Sticky) {
        transaction->Detach();
    }

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
    //context->ProduceOutputValue(BuildYsonStringFluently()
    //    .BeginMap()
    //        .Item("commit_timestamp").Value(transaction->GetId())
    //    .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, true);
    WaitFor(transaction->Abort(Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TGenerateTimestampCommand::Execute(ICommandContextPtr context)
{
    auto timestampProvider = context->GetClient()->GetConnection()->GetTimestampProvider();
    auto timestamp = WaitFor(timestampProvider->GenerateTimestamps())
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .Value(timestamp));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
