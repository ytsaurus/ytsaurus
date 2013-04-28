#include "stdafx.h"
#include "command.h"

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NYTree;
using namespace NObjectClient;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TCommandBase::TCommandBase()
    : Context(nullptr)
    , Replied(false)
{ }

void TCommandBase::Prepare()
{
    ObjectProxy.Reset(new TObjectServiceProxy(Context->GetMasterChannel()));
    SchedulerProxy.Reset(new TSchedulerServiceProxy(Context->GetSchedulerChannel()));
}

void TCommandBase::ReplyError(const TError& error)
{
    YCHECK(!Replied);
    YCHECK(!error.IsOK());

    Context->GetResponse()->Error = error;
    Replied = true;
}

void TCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    Context->GetResponse()->Error = TError();
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionId TTransactionalCommand::GetTransactionId(bool required)
{
    auto transaction = GetTransaction(required, true);
    return transaction ? transaction->GetId() : NullTransactionId;
}

ITransactionPtr TTransactionalCommand::GetTransaction(bool required, bool ping)
{
    auto request = GetTransactionalRequest();
    if (required && request->TransactionId == NullTransactionId) {
        THROW_ERROR_EXCEPTION("Transaction is required");
    }

    auto transactionId = request->TransactionId;
    if (transactionId == NullTransactionId) {
        return nullptr;
    }

    TTransactionAttachOptions options(transactionId);
    options.AutoAbort = false;
    options.Ping = ping;
    options.PingAncestors = request->PingAncestors;

    auto transactionManager = Context->GetTransactionManager();
    return transactionManager->Attach(options);
}

void TTransactionalCommand::SetTransactionId(NRpc::IClientRequestPtr request, bool required)
{
    NCypressClient::SetTransactionId(request, GetTransactionId(required));
}

TTransactionalRequestPtr TTransactionalCommand::GetTransactionalRequest()
{
    if (!TransactionalRequest) {
        TransactionalRequest = dynamic_cast<TTransactionalRequest*>(~UntypedRequest);
    }
    return TransactionalRequest;
}

////////////////////////////////////////////////////////////////////////////////

TMutationId TMutatingCommand::GenerateMutationId()
{
    if (!CurrentMutationId) {
        auto request = GetMutatingRequest();
        CurrentMutationId = request->MutationId;
    }

    auto result = *CurrentMutationId;
    ++(*CurrentMutationId).Parts[0];
    return result;
}

void TMutatingCommand::GenerateMutationId(NRpc::IClientRequestPtr request)
{
    NMetaState::SetMutationId(request, GenerateMutationId());
}

TMutatingRequestPtr TMutatingCommand::GetMutatingRequest()
{
    if (!MutatingRequest) {
        MutatingRequest = dynamic_cast<TMutatingRequest*>(~UntypedRequest);
    }
    return MutatingRequest;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
