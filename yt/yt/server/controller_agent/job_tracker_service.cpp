#include "job_tracker_service.h"

#include "bootstrap.h"
#include "controller_agent.h"
#include "job_tracker.h"

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NControllerAgent {

using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

using namespace NRpc;
using namespace NNodeTrackerClient;
using NJobTrackerClient::EJobState;

////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public TServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TJobTrackerServiceProxy::GetDescriptor(),
            ControllerAgentLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        auto incarnationId = FromProto<NScheduler::TIncarnationId>(request->controller_agent_incarnation_id());

        {
            auto nodeId = request->node_id();
            auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
            context->SetRequestInfo(
                "NodeId: %v, NodeAddress: %v, JobCount: %v, KnownIncarnationId: %v",
                nodeId,
                descriptor.GetDefaultAddress(),
                request->jobs_size(),
                incarnationId);

            Bootstrap_->GetControllerAgent()->GetJobTracker()->ProcessHeartbeat(context);
        }
    }
};

////////////////////////////////////////////////////////////////////

IServicePtr CreateJobTrackerService(TBootstrap* bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
