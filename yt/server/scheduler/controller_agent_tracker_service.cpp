#include "controller_agent_tracker_service.h"
#include "private.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/scheduler/controller_agent_tracker_service_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

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
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Heartbeat)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ProcessAgentHeartbeat(context);
    }
};

IServicePtr CreateControllerAgentTrackerService(TBootstrap* bootstrap)
{
    return New<TControllerAgentTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

