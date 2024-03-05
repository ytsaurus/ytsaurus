#include "scheduler_service.h"

#include "private.h"
#include "scheduler.h"
#include "bootstrap.h"

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>
#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NScheduler {

using namespace NRpc;
using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TOperationService
    : public TServiceBase
{
public:
    TOperationService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
        : TServiceBase(
            bootstrap->GetControlInvoker(EControlQueue::UserRequest),
            TOperationServiceProxy::GetDescriptor(),
            SchedulerLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
        , ResponseKeeper_(responseKeeper)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompleteOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateOperationParameters));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetAllocationBriefInfo));
    }

private:
    TBootstrap* const Bootstrap_;
    const IResponseKeeperPtr ResponseKeeper_;

    DECLARE_RPC_SERVICE_METHOD(NProto, StartOperation)
    {
        auto type = CheckedEnumCast<EOperationType>(request->type());
        auto transactionId = GetTransactionId(context);
        auto mutationId = context->GetMutationId();

        context->SetRequestInfo(
            "Type: %v, TransactionId: %v",
            type,
            transactionId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        // Heavy evaluation is offloaded to RPC heavy invoker.
        auto preprocessedSpec = WaitFor(scheduler->AssignExperimentsAndParseSpec(
            type,
            context->GetAuthenticationIdentity().User,
            TYsonString(request->spec())))
            .ValueOrThrow();

        auto asyncResult = scheduler->StartOperation(
            type,
            transactionId,
            mutationId,
            context->GetAuthenticationIdentity().User,
            std::move(preprocessedSpec));

        auto operation = WaitFor(asyncResult)
            .ValueOrThrow();

        auto id = operation->GetId();
        ToProto(response->mutable_operation_id(), id);

        context->SetResponseInfo("OperationId: %v", id);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation)
    {
        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        context->SetRequestInfo(
            "OperationId: %v",
            operationIdOrAlias);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        const auto& user = context->GetAuthenticationIdentity().User;
        auto error = TError("Operation aborted by user request")
            << TErrorAttribute("user", user);
        if (request->has_abort_message()) {
            error = error << TError(request->abort_message());
        }

        auto operation = scheduler->GetOperationOrThrow(operationIdOrAlias);
        auto asyncResult = scheduler->AbortOperation(
            operation,
            error,
            user);

        context->ReplyFrom(asyncResult);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SuspendOperation)
    {
        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        bool abortRunningAllocations = request->abort_running_jobs();

        context->SetRequestInfo(
            "OperationId: %v, AbortRunningAllocations: %v",
            operationIdOrAlias,
            abortRunningAllocations);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        auto operation = scheduler->GetOperationOrThrow(operationIdOrAlias);
        auto asyncResult = scheduler->SuspendOperation(
            operation,
            context->GetAuthenticationIdentity().User,
            abortRunningAllocations);

        context->ReplyFrom(asyncResult);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ResumeOperation)
    {
        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        context->SetRequestInfo(
            "OperationId: %v",
            operationIdOrAlias);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        auto operation = scheduler->GetOperationOrThrow(operationIdOrAlias);
        auto asyncResult = scheduler->ResumeOperation(
            operation,
            context->GetAuthenticationIdentity().User);

        context->ReplyFrom(asyncResult);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CompleteOperation)
    {
        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        context->SetRequestInfo(
            "OperationId: %v",
            operationIdOrAlias);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        auto operation = scheduler->GetOperationOrThrow(operationIdOrAlias);
        auto asyncResult = scheduler->CompleteOperation(
            operation,
            TError("Operation completed by user request"),
            context->GetAuthenticationIdentity().User);

        context->ReplyFrom(asyncResult);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, UpdateOperationParameters)
    {
        auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

        context->SetRequestInfo(
            "OperationId: %v",
            operationIdOrAlias);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        INodePtr parametersNode;
        try {
            parametersNode = ConvertToNode(TYsonString(request->parameters()));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation parameters")
                << ex;
        }

        auto operation = scheduler->GetOperationOrThrow(operationIdOrAlias);
        auto asyncResult = scheduler->UpdateOperationParameters(
            operation,
            context->GetAuthenticationIdentity().User,
            parametersNode);

        context->ReplyFrom(asyncResult);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetAllocationBriefInfo)
    {
        auto allocationId = FromProto<TAllocationId>(request->allocation_id());
        auto requestedAllocationInfo = FromProto<TAllocationInfoToRequest>(request->requested_info());

        context->SetRequestInfo(
            "AllocationId: %v, RequestedInfo: {OperationId: %v, OperationAcl: %v, CADescriptor: %v, NodeDescriptor: %v}",
            allocationId,
            requestedAllocationInfo.OperationId,
            requestedAllocationInfo.OperationAcl,
            requestedAllocationInfo.ControllerAgentDescriptor,
            requestedAllocationInfo.NodeDescriptor);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        if (ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        auto briefOperationInfo = scheduler->GetAllocationBriefInfo(
            allocationId,
            requestedAllocationInfo);

        ToProto(response->mutable_allocation_brief_info(), briefOperationInfo);

        context->SetResponseInfo("AllocationId: %v", allocationId);
        context->Reply();
    }
};

IServicePtr CreateOperationService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
{
    return New<TOperationService>(bootstrap, responseKeeper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

