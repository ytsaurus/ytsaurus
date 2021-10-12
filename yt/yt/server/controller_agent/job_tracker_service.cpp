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
using NJobTrackerClient::EJobState;

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

        THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> groupedJobSummaries;
        WaitFor(BIND([&] {
            for (auto& job : *request->mutable_jobs()) {
                const auto operationId = FromProto<TOperationId>(job.operation_id());

                groupedJobSummaries[operationId].push_back(ParseJobSummary(&job, Logger));
            }
        }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()).Run()).ThrowOnError();

        controllerAgent->ValidateConnected();
        context->Reply();
        
        for (auto& [operationId, jobSummaries] : groupedJobSummaries) {
            const auto operation = controllerAgent->FindOperation(operationId);
            if (!operation) {
                continue;
            }
            const auto controller = operation->GetController();
            controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue)->Invoke(
                BIND(
                    [&Logger{Logger}, controller, jobSummaries{std::move(jobSummaries)}] () mutable {
                        for (auto& jobSummary : jobSummaries) {
                            try {
                                controller->OnJobInfoReceivedFromNode(std::move(jobSummary));
                            } catch (const std::exception& ex) {
                                YT_LOG_WARNING(
                                    ex,
                                    "Fail process job info from node (JobId: %v, JobState: %v)",
                                    jobSummary->Id,
                                    jobSummary->State);
                            }
                        }
                    }));
        }
    }
};

////////////////////////////////////////////////////////////////////

IServicePtr CreateJobTrackerService(TBootstrap* const bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
