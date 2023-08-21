#include "controller_agent_tracker_service.h"
#include "controller_agent_tracker.h"
#include "bootstrap.h"
#include "private.h"
#include "scheduler.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/controller_agent_tracker_service_proxy.h>

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NScheduler {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerService
    : public TServiceBase
{
public:
    TControllerAgentTrackerService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(EControlQueue::AgentTracker),
            TControllerAgentTrackerServiceProxy::GetDescriptor(),
            SchedulerLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
        , ResponseKeeper_(responseKeeper)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Handshake));
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetPooled(false));
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(ScheduleJobHeartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetPooled(false));
    }

private:
    TBootstrap* const Bootstrap_;
    IResponseKeeperPtr ResponseKeeper_;

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Handshake)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        if (controllerAgentTracker->GetConfig()->EnableResponseKeeper && ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        controllerAgentTracker->ProcessAgentHandshake(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Heartbeat)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        if (controllerAgentTracker->GetConfig()->EnableResponseKeeper && ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        controllerAgentTracker->ProcessAgentHeartbeat(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, ScheduleJobHeartbeat)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        if (controllerAgentTracker->GetConfig()->EnableResponseKeeper && ResponseKeeper_->TryReplyFrom(context)) {
            return;
        }

        controllerAgentTracker->ProcessAgentScheduleJobHeartbeat(context);
    }
};

IServicePtr CreateControllerAgentTrackerService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
{
    return New<TControllerAgentTrackerService>(bootstrap, responseKeeper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

