#include "job_prober_service.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "job_tracker.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NControllerAgent {

using namespace NRpc;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NSecurityClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    explicit TJobProberService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobProberServiceProxy::GetDescriptor(),
            ControllerAgentLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(InterruptJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobShellDescriptor));
    }

private:
    TBootstrap* const Bootstrap_;

    const TControllerAgentConfigPtr& GetConfig() const
    {
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        return controllerAgent->GetConfig();
    }

    void ValidateAgentIncarnation(TIncarnationId incarnationId)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        controllerAgent->ValidateConnected();
        controllerAgent->ValidateIncarnation(incarnationId);
    }

    TOperationPtr GetOperation(TOperationId operationId)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto operation = Bootstrap_
            ->GetControllerAgent()
            ->FindOperation(operationId);

        if (!operation) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchOperation,
                "No such operation %v",
                operationId);
        }

        return operation;
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbandonJob)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());

        auto jobId = FromProto<TJobId>(request->job_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo(
            "IncarnationId: %v, OperationId: %v, JobId: %v",
            incarnationId,
            operationId,
            jobId);

        ValidateAgentIncarnation(incarnationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        auto operation = GetOperation(operationId);
        const auto& controller = operation->GetController();
        WaitFor(BIND(&IOperationController::AbandonJob, controller)
            .AsyncVia(controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue))
            .Run(jobId))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, InterruptJob)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());

        auto jobId = FromProto<TJobId>(request->job_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());

        auto timeout = FromProto<TDuration>(request->timeout());

        context->SetRequestInfo(
            "IncarnationId: %v, OperationId: %v, JobId: %v",
            incarnationId,
            operationId,
            jobId);

        ValidateAgentIncarnation(incarnationId);

        const auto& controllerAgent = Bootstrap_->GetControllerAgent();

        auto operation = GetOperation(operationId);
        const auto& controller = operation->GetController();
        WaitFor(BIND(&IOperationController::InterruptJobByUserRequest, controller)
            .AsyncVia(controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue))
            .Run(jobId, timeout))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobShellDescriptor)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        auto incarnationId = FromProto<TIncarnationId>(request->incarnation_id());

        auto jobId = FromProto<TJobId>(request->job_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto shellName = request->has_shell_name()
            ? std::make_optional(request->shell_name())
            : std::nullopt;
        context->SetRequestInfo(
            "IncarnationId: %v, OperationId: %v, JobId: %v, ShellName: %v",
            incarnationId,
            operationId,
            jobId,
            shellName);


        ValidateAgentIncarnation(incarnationId);

        auto operation = GetOperation(operationId);
        auto shells = operation->GetControllerOrThrow()->GetJobShells();

        if (!shellName && shells.empty()) {
            const auto& controllerAgent = Bootstrap_->GetControllerAgent();
            // Shell name is not provided and shells are not configured in spec,
            // using default shell.
            auto requiredPermissions = EPermissionSet(EPermission::Manage | EPermission::Read);
            controllerAgent->ValidateOperationAccess(
                context->GetAuthenticationIdentity().User,
                operationId,
                requiredPermissions);

            // Default job shell is run in root container.
            response->set_subcontainer("");
        } else {
            if (!shellName) {
                shellName = "default";
            }

            NScheduler::TJobShellPtr jobShell;
            for (const auto& shell : shells) {
                if (shell->Name == shellName) {
                    jobShell = shell;
                }
            }

            if (!jobShell) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::NoSuchJobShell,
                    "Job shell %Qv not found",
                    shellName);
            }

            auto jobShellInfo = operation->GetJobShellInfo(*shellName);

            const auto& owners = jobShellInfo->GetOwners();
            auto user = context->GetAuthenticationIdentity().User;
            ValidateJobShellAccess(
                Bootstrap_->GetClient(),
                user,
                jobShell->Name,
                owners);

            response->set_subcontainer(jobShellInfo->GetSubcontainerName());
        }

        context->SetResponseInfo(
            "Subcontainer: %v",
            response->subcontainer());
        context->Reply();
    }
};

IServicePtr CreateJobProberService(TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
