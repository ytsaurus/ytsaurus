#include "controller_agent_tracker_service.h"
#include "controller_agent_tracker.h"
#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/controller_agent_tracker_service_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerService
    : public TServiceBase
{
public:
    explicit TControllerAgentTrackerService(TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(EControlQueue::AgentTracker),
            TControllerAgentTrackerServiceProxy::GetDescriptor(),
            SchedulerLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Handshake));
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Handshake)
    {
        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        controllerAgentTracker->ProcessAgentHandshake(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Heartbeat)
    {
        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        controllerAgentTracker->ProcessAgentHeartbeat(context);
    }
};

IServicePtr CreateControllerAgentTrackerService(TBootstrap* bootstrap)
{
    return New<TControllerAgentTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

