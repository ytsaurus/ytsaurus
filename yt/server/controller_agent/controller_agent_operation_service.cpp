#include "private.h"

#include "controller_agent.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/controller_agent_operation_service_proxy.h>

#include <yt/ytlib/api/native_client.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NControllerAgent {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////

class TControllerAgentOperationService
    : public TServiceBase
{
public:
    explicit TControllerAgentOperationService(TBootstrap* bootstrap)
        : TServiceBase(
            // TODO(babenko): better queue
            bootstrap->GetControlInvoker(EControlQueue::Default),
            NScheduler::TControllerAgentOperationServiceProxy::GetDescriptor(),
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

        context->SetRequestInfo("OperationId: %v", operationId);

        controllerAgent->BuildOperationInfo(operationId, response);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, GetJobInfo)
    {
        auto controllerAgent = Bootstrap_->GetControllerAgent();
        controllerAgent->ValidateConnected();

        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("OperationId: %v, JobId: %v", operationId, jobId);

        response->set_info(controllerAgent->BuildJobInfo(operationId, jobId).GetData());

        context->Reply();
    }
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentOperationService)

IServicePtr CreateControllerAgentOperationService(TBootstrap* bootstrap)
{
    return New<TControllerAgentOperationService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

