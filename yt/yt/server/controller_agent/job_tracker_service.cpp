#include "job_tracker_service.h"
#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NControllerAgent {

using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

using namespace NRpc;

////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public TServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobTrackerServiceProxy::GetDescriptor(),
            ControllerAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        if (FromProto<NScheduler::TIncarnationId>(request->controller_agent_incarnation_id()) != controllerAgent->GetIncarnationId()) {
            context->Reply(TError{EErrorCode::IncarnationMismatch, "Controller agent incarnation mismatch"});
            return;
        }

        THashMap<TOperationPtr, std::vector<NJobTrackerClient::NProto::TJobStatus*>> groupedJobStatuses;

        for (auto& job : *request->mutable_jobs()) {
            auto operationId = FromProto<TOperationId>(job.operation_id());
            auto operation = controllerAgent->FindOperation(operationId);
            if (!operation) {
                continue;
            }
            groupedJobStatuses[std::move(operation)].push_back(&job);
        }

        for (auto& [operation, protoJobStatuses] : groupedJobStatuses) {
            auto controller = operation->GetController();
            controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue)->Invoke(BIND(
                [&Logger{Logger}, controller, protoJobStatuses{std::move(protoJobStatuses)}] {
                    for (auto* protoJobStatus : protoJobStatuses) {
                        controller->OnJobRunning(std::make_unique<TRunningJobSummary>(protoJobStatus));
                    }
                }));
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////

IServicePtr CreateJobTrackerService(TBootstrap* const bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
