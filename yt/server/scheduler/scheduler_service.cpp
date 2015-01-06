#include "stdafx.h"
#include "scheduler_service.h"
#include "scheduler.h"
#include "private.h"

#include <core/rpc/service_detail.h>
#include <core/rpc/helpers.h>

#include <ytlib/scheduler/scheduler_service_proxy.h>

#include <ytlib/security_client/public.h>

#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////

class TSchedulerService
    : public TServiceBase
{
public:
    TSchedulerService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TSchedulerServiceProxy::GetServiceName(),
            SchedulerLogger,
            TSchedulerServiceProxy::GetProtocolVersion(),
            bootstrap->GetResponseKeeper())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeOperation));
    }

private:
    TBootstrap* Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NProto, StartOperation)
    {
        auto type = EOperationType(request->type());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto mutationId = GetMutationId(context->RequestHeader());

        auto maybeUser = FindAuthenticatedUser(context);
        auto user = maybeUser ? *maybeUser : RootUserName;

        IMapNodePtr spec;
        try {
            spec = ConvertToNode(TYsonString(request->spec()))->AsMap();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation spec")
                << ex;
        }

        context->SetRequestInfo("Type: %v, TransactionId: %v",
            type,
            transactionId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();
        scheduler->StartOperation(
            type,
            transactionId,
            mutationId,
            spec,
            user)
            .Subscribe(BIND([=] (const TErrorOr<TOperationPtr>& result) {
                if (!result.IsOK()) {
                    context->Reply(result);
                    return;
                }
                auto operation = result.Value();
                auto id = operation->GetId();
                ToProto(response->mutable_operation_id(), id);
                context->SetResponseInfo("OperationId: %v", id);
                context->Reply();
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation)
    {
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v", operationId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto operation = scheduler->GetOperationOrThrow(operationId);
        context->ReplyFrom(scheduler->AbortOperation(
            operation,
            TError("Operation aborted by user request")));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SuspendOperation)
    {
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v", operationId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto operation = scheduler->GetOperationOrThrow(operationId);
        auto result = scheduler->SuspendOperation(operation);
        context->ReplyFrom(result);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ResumeOperation)
    {
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v", operationId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto operation = scheduler->GetOperationOrThrow(operationId);
        auto result = scheduler->ResumeOperation(operation);
        context->ReplyFrom(result);
    }

};

IServicePtr CreateSchedulerService(TBootstrap* bootstrap)
{
    return New<TSchedulerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

