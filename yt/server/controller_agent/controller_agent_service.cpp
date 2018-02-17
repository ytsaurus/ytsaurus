#include "controller_agent_service.h"
#include "controller_agent.h"
#include "controller_agent_service_proxy.h"
#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NControllerAgent {

using namespace NRpc;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NCellScheduler;

////////////////////////////////////////////////////////////////////

class TControllerAgentService
    : public TServiceBase
{
public:
    explicit TControllerAgentService(TBootstrap* bootstrap)
        : TServiceBase(
            // TODO(babenko): better queue
            bootstrap->GetControlInvoker(EControlQueue::Default),
            TControllerAgentServiceProxy::GetDescriptor(),
            ControllerAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetOperationInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(InitializeOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PrepareOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(MaterializeOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReviveOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CompleteOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DisposeOperation));
    }

private:
    TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NProto, GetOperationInfo)
    {
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v",
            operationId);

        auto result = WaitFor(controllerAgent->BuildOperationInfo(operationId))
            .ValueOrThrow();

        response->set_progress(result.Progress.GetData());
        response->set_brief_progress(result.BriefProgress.GetData());
        response->set_running_jobs(result.RunningJobs.GetData());
        response->set_job_splitter(result.JobSplitter.GetData());
        response->set_controller_memory_usage(result.MemoryUsage);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobInfo)
    {
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("OperationId: %v, JobId: %v",
            operationId,
            jobId);

        auto info = WaitFor(controllerAgent->BuildJobInfo(operationId, jobId))
            .ValueOrThrow();
        response->set_info(info.GetData());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, RegisterOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_descriptor().operation_id());
        context->SetRequestInfo(
            "IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        controllerAgent->RegisterOperation(request->operation_descriptor());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, InitializeOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto clean = request->clean();
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v, Clean: %v",
            incarnationId,
            operationId,
            clean);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);

        TNullable<TControllerTransactions> transactions;
        if (!clean) {
            transactions.Emplace();
            transactions->AsyncId = FromProto<TTransactionId>(request->async_transaction_id());
            transactions->InputId = FromProto<TTransactionId>(request->input_transaction_id());
            transactions->OutputId = FromProto<TTransactionId>(request->output_transaction_id());
            transactions->DebugId  = FromProto<TTransactionId>(request->debug_transaction_id());
            transactions->OutputCompletionId = FromProto<TTransactionId>(request->output_completion_transaction_id());
            transactions->DebugCompletionId = FromProto<TTransactionId>(request->debug_completion_transaction_id());
        }

        auto result = WaitFor(controllerAgent->InitializeOperation(operation, transactions))
            .ValueOrThrow();

        response->set_immutable_attributes(result.Attributes.Immutable.GetData());
        response->set_mutable_attributes(result.Attributes.Mutable.GetData());
        response->set_brief_spec(result.Attributes.BriefSpec.GetData());
        response->set_unrecognized_spec(result.Attributes.UnrecognizedSpec.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PrepareOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        auto result = WaitFor(controllerAgent->PrepareOperation(operation))
            .ValueOrThrow();

        response->set_attributes(result.Attributes.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, MaterializeOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        WaitFor(controllerAgent->MaterializeOperation(operation))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, ReviveOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        auto result = WaitFor(controllerAgent->ReviveOperation(operation))
            .ValueOrThrow();

        response->set_attributes(result.Attributes.GetData());
        response->set_revived_from_snapshot(result.RevivedFromSnapshot);
        for (const auto& job : result.RevivedJobs) {
            auto* protoJob = response->add_revived_jobs();
            ToProto(protoJob->mutable_job_id(), job.JobId);
            protoJob->set_job_type(static_cast<int>(job.JobType));
            protoJob->set_start_time(ToProto<ui64>(job.StartTime));
            ToProto(protoJob->mutable_resource_limits(), job.ResourceLimits);
            protoJob->set_interruptible(job.Interruptible);
            protoJob->set_tree_id(job.TreeId);
            protoJob->set_node_id(job.NodeId);
            protoJob->set_node_address(job.NodeAddress);
        }

        context->SetResponseInfo("RevivedFromSnapshot: %v, RevivedJobCount: %v",
            result.RevivedFromSnapshot,
            result.RevivedJobs.size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CommitOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        WaitFor(controllerAgent->CommitOperation(operation))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, CompleteOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        WaitFor(controllerAgent->CompleteOperation(operation))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            LOG_DEBUG("Operation is missing; ignoring request");
            context->Reply();
            return;
        }

        WaitFor(controllerAgent->AbortOperation(operation))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, DisposeOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        auto operation = controllerAgent->GetOperationOrThrow(operationId);
        WaitFor(controllerAgent->DisposeOperation(operation))
            .ThrowOnError();

        context->Reply();
    }
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentService)

IServicePtr CreateControllerAgentService(TBootstrap* bootstrap)
{
    return New<TControllerAgentService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

