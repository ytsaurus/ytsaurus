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

TUntypedCommandBase::TUntypedCommandBase(ICommandContext* context)
    : Context(context)
    , Replied(false)
{ }

void TUntypedCommandBase::Prepare()
{
    ObjectProxy.Reset(new TObjectServiceProxy(Context->GetMasterChannel()));
    SchedulerProxy.Reset(new TSchedulerServiceProxy(Context->GetSchedulerChannel()));
}

void TUntypedCommandBase::ReplyError(const TError& error)
{
    YCHECK(!Replied);
    YCHECK(!error.IsOK());

    Context->GetResponse()->Error = error;
    Replied = true;
}

void TUntypedCommandBase::ReplySuccess(const TYsonString& yson)
{
    YCHECK(!Replied);

    auto consumer = Context->CreateOutputConsumer();
    Consume(yson, ~consumer);

    Context->GetResponse()->Error = TError();
    Replied = true;
}

////////////////////////////////////////////////////////////////////////////////

TTransactionalCommandMixin::TTransactionalCommandMixin(
    ICommandContext* context,
    TTransactionalRequestPtr request)
    : PrivateContext(context)
    , PrivateRequest(request)
{ }

TTransactionId TTransactionalCommandMixin::GetTransactionId(bool required)
{
    auto transaction = GetTransaction(required, true);
    return transaction ? transaction->GetId() : NTransactionClient::NullTransactionId;
}

ITransactionPtr TTransactionalCommandMixin::GetTransaction(bool required, bool ping)
{
    if (required && this->PrivateRequest->TransactionId == NTransactionClient::NullTransactionId) {
        THROW_ERROR_EXCEPTION("Transaction is required");
    }

    auto transactionId = this->PrivateRequest->TransactionId;
    if (transactionId == NTransactionClient::NullTransactionId) {
        return nullptr;
    }

    NTransactionClient::TTransactionAttachOptions options(transactionId);
    options.AutoAbort = false;
    options.Ping = ping;
    options.PingAncestors = this->PrivateRequest->PingAncestors;
    auto transactionManager = this->PrivateContext->GetTransactionManager();
    return transactionManager->Attach(options);
}

void TTransactionalCommandMixin::SetTransactionId(NRpc::IClientRequestPtr request, bool required)
{
    NCypressClient::SetTransactionId(request, GetTransactionId(required));
}

////////////////////////////////////////////////////////////////////////////////

TMutatingCommandMixin::TMutatingCommandMixin(
    ICommandContext* context,
    TMutatingRequestPtr request)
    : PrivateContext(context)
    , PrivateRequest(request)
    , CurrentMutationId(
        PrivateRequest->MutationId == NullMutationId
        ? NMetaState::GenerateMutationId()
        : PrivateRequest->MutationId)
{ }

TMutationId TMutatingCommandMixin::GenerateMutationId()
{
    auto result = CurrentMutationId;
    ++CurrentMutationId.Parts[0];
    return result;
}

void TMutatingCommandMixin::GenerateMutationId(NRpc::IClientRequestPtr request)
{
    NMetaState::SetMutationId(request, GenerateMutationId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
