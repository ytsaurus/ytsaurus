#include "job_tracker_service.h"

#include "bootstrap.h"
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

// COMPAT(pogorelov): Remove when all nodes will be 23.1.
class TOldJobTrackerService
    : public TServiceBase
{
public:
    explicit TOldJobTrackerService(TBootstrap* bootstrap)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TOldJobTrackerServiceProxy::GetDescriptor(),
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

            Bootstrap_->GetJobTracker()->ProcessHeartbeat(context);
        }
    }
};

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

            Bootstrap_->GetJobTracker()->ProcessHeartbeat(context);
        }
    }
};

////////////////////////////////////////////////////////////////////

// COMPAT(pogorelov): Remove when all nodes will be 23.1.
IServicePtr CreateOldJobTrackerService(TBootstrap* bootstrap)
{
    return New<TOldJobTrackerService>(bootstrap);
}

IServicePtr CreateJobTrackerService(TBootstrap* bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
