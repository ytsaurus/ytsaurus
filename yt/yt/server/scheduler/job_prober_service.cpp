#include "job_prober_service.h"

#include "private.h"
#include "scheduler.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/scheduler/job_prober_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NScheduler {

using namespace NRpc;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NSecurityClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(pogorelov)
class TJobProberService
    : public TServiceBase
{
public:
    explicit TJobProberService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(EControlQueue::UserRequest),
            TJobProberServiceProxy::GetDescriptor(),
            SchedulerLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobShellDescriptor));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& path = request->path();
        context->SetRequestInfo("JobId: %v, Path: %v",
            jobId,
            path);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        WaitFor(scheduler->DumpInputContext(jobId, path, context->GetAuthenticationIdentity().User))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobNode)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto requiredPermissions = FromProto<EPermissionSet>(request->required_permissions());
        context->SetRequestInfo("JobId: %v, RequiredPermissions: %v",
            jobId,
            requiredPermissions);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto jobNodeDescriptor = WaitFor(scheduler->GetJobNode(jobId))
            .ValueOrThrow();

        auto operationId = WaitFor(scheduler->FindOperationIdByJobId(jobId))
            .ValueOrThrow();
        if (!operationId) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchJob,
                "Job %v not found",
                jobId);
        }

        WaitFor(scheduler->ValidateOperationAccess(context->GetAuthenticationIdentity().User, operationId, requiredPermissions))
            .ThrowOnError();

        context->SetResponseInfo("NodeDescriptor: %v", jobNodeDescriptor);

        ToProto(response->mutable_node_descriptor(), jobNodeDescriptor);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbandonJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        WaitFor(scheduler->AbandonJob(jobId, context->GetAuthenticationIdentity().User))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto interruptTimeout = request->has_interrupt_timeout()
            ? std::make_optional(FromProto<TDuration>(request->interrupt_timeout()))
            : std::nullopt;
        context->SetRequestInfo("JobId: %v, InterruptTimeout: %v",
            jobId,
            interruptTimeout);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        WaitFor(scheduler->AbortJob(jobId, interruptTimeout, context->GetAuthenticationIdentity().User))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetJobShellDescriptor)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto shellName = request->has_shell_name()
            ? std::make_optional(request->shell_name())
            : std::nullopt;
        context->SetRequestInfo("JobId: %v, ShellName: %v",
            jobId,
            shellName);

        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto operationId = WaitFor(scheduler->FindOperationIdByJobId(jobId))
            .ValueOrThrow();
        if (!operationId) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchJob,
                "Job %v not found", jobId);
        }

        auto operation = scheduler->FindOperation(operationId);
        if (!operation) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchOperation,
                "Operation %v not found",
                operationId);
        }

        auto jobNodeDescriptor = WaitFor(scheduler->GetJobNode(jobId))
            .ValueOrThrow();
        ToProto(response->mutable_node_descriptor(), jobNodeDescriptor);

        auto shells = operation->Spec()->JobShells;
        if (!shellName && shells.empty()) {
            // Shell name is not provided and shells are not configured in spec,
            // using default shell.
            auto requiredPermissions = EPermissionSet(EPermission::Manage | EPermission::Read);
            WaitFor(scheduler->ValidateOperationAccess(context->GetAuthenticationIdentity().User, operationId, requiredPermissions))
                .ThrowOnError();

            // Default job shell is run in root container.
            response->set_subcontainer("");
        } else {
            if (!shellName) {
                shellName = "default";
            }

            TJobShellPtr jobShell;
            for (const auto& shell : shells) {
                if (shell->Name == shellName) {
                    jobShell = shell;
                }
            }

            auto owners = operation->GetJobShellOwners(*shellName);
            auto user = context->GetAuthenticationIdentity().User;
            WaitFor(scheduler->ValidateJobShellAccess(user, jobShell->Name, owners))
                .ThrowOnError();

            response->set_subcontainer(jobShell->Subcontainer);
        }

        context->SetResponseInfo("NodeDescriptor: %v, Subcontainer: %v",
            jobNodeDescriptor,
            response->subcontainer());
        context->Reply();
    }
};

IServicePtr CreateJobProberService(TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
