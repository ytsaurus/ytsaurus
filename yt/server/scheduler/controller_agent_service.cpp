#include "controller_agent_service.h"
#include "private.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/scheduler/controller_agent_service_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentService
    : public TServiceBase
{
public:
    explicit TControllerAgentService(TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(EControlQueue::AgentTracker),
            TControllerAgentServiceProxy::GetDescriptor(),
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
        auto scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateAcceptsHeartbeats();
        scheduler->ProcessAgentHeartbeat(context);
        context->Reply();
    }
};

IServicePtr CreateControllerAgentService(TBootstrap* bootstrap)
{
    return New<TControllerAgentService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

