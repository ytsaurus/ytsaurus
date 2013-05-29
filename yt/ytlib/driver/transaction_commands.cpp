#include "stdafx.h"
#include "transaction_commands.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

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

    auto this_ = MakeStrong(this);
    transactionManager->AsyncStart(options).Apply(
        BIND([this, this_] (TValueOrError<ITransactionPtr> transactionOrError) {
            if (!transactionOrError.IsOK()) {
                ReplyError(transactionOrError);
                return;
            }
            auto transaction = transactionOrError.Value();
            auto yson = BuildYsonStringFluently().Value(transaction->GetId());
            ReplySuccess(yson);
            transaction->Detach();
        })
    );
}

////////////////////////////////////////////////////////////////////////////////

void TPingTransactionCommand::DoExecute()
{
    auto req = TTransactionYPathProxy::Ping(FromObjectId(GetTransactionId(EAllowNullTransaction::No)));
    req->set_ping_ancestors(Request->PingAncestors);

    CheckAndReply(ObjectProxy->Execute(req));
}

////////////////////////////////////////////////////////////////////////////////

void TCommitTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    CheckAndReply(transaction->AsyncCommit(GenerateMutationId()));
}

////////////////////////////////////////////////////////////////////////////////

void TAbortTransactionCommand::DoExecute()
{
    auto transaction = GetTransaction(EAllowNullTransaction::No, EPingTransaction::No);
    CheckAndReply(transaction->AsyncAbort(true, GenerateMutationId()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
