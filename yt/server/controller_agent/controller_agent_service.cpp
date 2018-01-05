#include "controller_agent_service.h"
#include "controller_agent.h"
#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/scheduler/controller_agent_service_proxy.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NControllerAgent {

using namespace NRpc;
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
            NScheduler::TControllerAgentServiceProxy::GetDescriptor(),
            ControllerAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetOperationInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetJobInfo));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, GetOperationInfo)
    {
        auto controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operationId = FromProto<TOperationId>(request->operation_id());

        context->SetRequestInfo("OperationId: %v",
            operationId);

        controllerAgent->BuildOperationInfo(operationId, response);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, GetJobInfo)
    {
        auto controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("OperationId: %v, JobId: %v",
            operationId,
            jobId);

        response->set_info(controllerAgent->BuildJobInfo(operationId, jobId).GetData());

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

