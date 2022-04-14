#include "job_prober_service.h"
#include "bootstrap.h"
#include "controller_agent.h"
#include "private.h"

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
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TJobProberServiceProxy::GetDescriptor(),
            ControllerAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, AbandonJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("JobId: %v", jobId);
        context->SetRequestInfo("OperationId: %v", operationId);

        SwitchTo(Bootstrap_->GetControlInvoker());
        
        const auto& controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            continue;
        }
        const auto controller = operation->GetController();
        WaitFor(scheduler->AbandonJob(jobId, context->GetAuthenticationIdentity().User))
            .ThrowOnError();
        WaitFor(BIND())
        controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue)

        context->Reply();
    }
};

IServicePtr CreateJobProberService(TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
