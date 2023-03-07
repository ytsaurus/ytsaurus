#include "controller_agent_service.h"
#include "controller_agent.h"
#include "bootstrap.h"
#include "private.h"
#include "operation.h"
#include "operation_controller.h"

#include <yt/core/rpc/service_detail.h>

#include <yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/ytlib/scheduler/config.h>

namespace NYT::NControllerAgent {

using namespace NRpc;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

class TControllerAgentService
    : public TServiceBase
{
public:
    explicit TControllerAgentService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TerminateOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteOperationControllerCoreDump));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnregisterOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateOperationRuntimeParameters));
    }

private:
    TBootstrap* const Bootstrap_;


    template <class F>
    void WrapAgentException(F func)
    {
        try {
            func();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(NControllerAgent::EErrorCode::AgentCallFailed, "Agent call failed")
                << ex;
        }
    }

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
        response->set_controller_state(static_cast<i32>(result.ControllerState));
        response->set_alerts(result.Alerts.GetData());

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

        WrapAgentException([&] {
            controllerAgent->RegisterOperation(request->operation_descriptor());

            context->Reply();
        });
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

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);

            std::optional<TControllerTransactionIds> transactionIds;
            if (!clean) {
                transactionIds.emplace();
                *transactionIds = FromProto<TControllerTransactionIds>(request->transaction_ids());
            }

            auto maybeResult = WaitFor(controllerAgent->InitializeOperation(operation, transactionIds))
                .ValueOrThrow();

            context->SetResponseInfo("ImmediateResult: %v", maybeResult.has_value());
            if (maybeResult) {
                ToProto(response->mutable_result(), *maybeResult);
            }

            context->Reply();
        });
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

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);
            auto maybeResult = WaitFor(controllerAgent->PrepareOperation(operation))
                .ValueOrThrow();

            context->SetResponseInfo("ImmediateResult: %v", maybeResult.has_value());
            if (maybeResult) {
                ToProto(response->mutable_result(), *maybeResult);
            }

            context->Reply();
        });
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

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);
            auto maybeResult = WaitFor(controllerAgent->MaterializeOperation(operation))
                .ValueOrThrow();

            context->SetIncrementalResponseInfo("ImmediateResult: %v", maybeResult.has_value());
            if (maybeResult) {
                ToProto(response->mutable_result(), *maybeResult);

                context->SetIncrementalResponseInfo("Suspend: %v, InitialNeededResources: %v",
                    maybeResult->Suspend,
                    FormatResources(maybeResult->InitialNeededResources));
            }

            context->Reply();
        });
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

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);
            auto maybeResult = WaitFor(controllerAgent->ReviveOperation(operation))
                .ValueOrThrow();

            context->SetIncrementalResponseInfo("ImmediateResult: %v", maybeResult.has_value());
            if (maybeResult) {
                ToProto(response->mutable_result(), *maybeResult);

                context->SetIncrementalResponseInfo("RevivedFromSnapshot: %v, RevivedJobCount: %v, RevivedBannedTreeIds: %v, NeededResources: %v",
                    maybeResult->RevivedFromSnapshot,
                    maybeResult->RevivedJobs.size(),
                    maybeResult->RevivedBannedTreeIds,
                    FormatResources(maybeResult->NeededResources));
            }

            context->Reply();
        });
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

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);
            auto maybeResult = WaitFor(controllerAgent->CommitOperation(operation))
                .ValueOrThrow();

            context->SetResponseInfo("ImmediateResult: %v", maybeResult.has_value());
            if (maybeResult) {
                ToProto(response->mutable_result(), *maybeResult);
            }

            context->Reply();
        });
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
        controllerAgent->ValidateIncarnation(incarnationId);

        WrapAgentException([&] {
            auto operation = controllerAgent->GetOperationOrThrow(operationId);
            WaitFor(controllerAgent->CompleteOperation(operation))
                .ThrowOnError();

            context->Reply();
        });
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, TerminateOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto controllerFinalState = static_cast<EControllerState>(request->controller_final_state());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v, ControllerFinalState: %v",
            incarnationId,
            operationId,
            controllerFinalState);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        WrapAgentException([&] {
            auto operation = controllerAgent->FindOperation(operationId);
            if (!operation) {
                YT_LOG_DEBUG("Operation is missing; ignoring request");
                context->Reply();
                return;
            }

            WaitFor(controllerAgent->TerminateOperation(operation, controllerFinalState))
                .ThrowOnError();

            context->Reply();
        });
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WriteOperationControllerCoreDump)
    {
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v",
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        WrapAgentException([&] {
            const auto& operation = controllerAgent->GetOperationOrThrow(operationId);
            response->set_path(operation->GetControllerOrThrow()->WriteCoreDump());

            context->Reply();
        });
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, UnregisterOperation)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        WrapAgentException([&] {
            auto result = WaitFor(controllerAgent->DisposeAndUnregisterOperation(operationId))
                .ValueOrThrow();
            ToProto(response->mutable_residual_job_metrics(), result.ResidualJobMetrics);

            context->SetResponseInfo("TreesWithResidualJobMetrics: %v",
                MakeFormattableView(result.ResidualJobMetrics, [] (auto* builder, const auto& treeTaggedJobMetrics) {
                    builder->AppendString(treeTaggedJobMetrics.TreeId);
                }));
            context->Reply();
        });
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, UpdateOperationRuntimeParameters)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        context->SetRequestInfo("IncarnationId: %v, OperationId: %v",
            incarnationId,
            operationId);

        auto update = ConvertTo<TOperationRuntimeParametersUpdatePtr>(TYsonString(request->parameters()));

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);

        WrapAgentException([&] {
            WaitFor(controllerAgent->UpdateOperationRuntimeParameters(operationId, std::move(update)))
                .ThrowOnError();

            context->Reply();
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentService)

IServicePtr CreateControllerAgentService(TBootstrap* bootstrap)
{
    return New<TControllerAgentService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

