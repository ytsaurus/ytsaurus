#include "controller_agent_tracker_service.h"

#include "controller_agent_tracker.h"
#include "bootstrap.h"
#include "private.h"
#include "scheduler.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/controller_agent_tracker_service_proxy.h>

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTrackerService
    : public TServiceBase
{
public:
    TControllerAgentTrackerService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
        : NRpc::TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
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
            RPC_SERVICE_METHOD_DESC(ScheduleAllocationHeartbeat)
                .SetHeavy(true)
                .SetResponseCodec(NCompression::ECodec::Lz4)
                .SetPooled(false));
    }

private:
    TBootstrap* const Bootstrap_;
    IResponseKeeperPtr ResponseKeeper_;

    // Returns |true| if no further action is required.
    bool TryReplyingWithResponseKeeper(const IServiceContextPtr& context)
    {
        Bootstrap_->GetScheduler()->ValidateConnected();

        const auto& controllerAgentTracker = Bootstrap_->GetControllerAgentTracker();
        return
            controllerAgentTracker->GetConfig()->EnableResponseKeeper &&
            ResponseKeeper_->TryReplyFrom(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Handshake)
    {
        if (TryReplyingWithResponseKeeper(context)) {
            return;
        }

        Bootstrap_
            ->GetControllerAgentTracker()
            ->ProcessAgentHandshake(context);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, Heartbeat)
    {
        if (TryReplyingWithResponseKeeper(context)) {
            return;
        }

        Bootstrap_
            ->GetControllerAgentTracker()
            ->ProcessAgentHeartbeat(context);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NScheduler::NProto, ScheduleAllocationHeartbeat)
    {
        if (TryReplyingWithResponseKeeper(context)) {
            return;
        }

        Bootstrap_
            ->GetControllerAgentTracker()
            ->ProcessAgentScheduleAllocationHeartbeat(context);

        context->Reply();
    }
};

IServicePtr CreateControllerAgentTrackerService(TBootstrap* bootstrap, const IResponseKeeperPtr& responseKeeper)
{
    return New<TControllerAgentTrackerService>(bootstrap, responseKeeper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

