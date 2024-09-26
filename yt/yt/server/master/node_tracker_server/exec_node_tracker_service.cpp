#include "exec_node_tracker_service.h"

#include "private.h"
#include "exec_node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/world_initializer.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NExecNodeTrackerClient;
using namespace NHydra;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTrackerService
    : public THydraServiceBase
{
public:
    explicit TExecNodeTrackerService(TBootstrap* bootstrap)
        : THydraServiceBase(
            bootstrap->GetHydraFacade()->GetHydraManager(),
            TDispatcher::Get()->GetHeavyInvoker(),
            TExecNodeTrackerServiceProxy::GetDescriptor(),
            NodeTrackerServerLogger(),
            CreateMulticellUpstreamSynchronizer(bootstrap),
            NRpc::TServiceOptions{
                .RealmId = bootstrap->GetCellId(),
                .Authenticator =bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    TBootstrap *const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NExecNodeTrackerClient::NProto, Heartbeat)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        worldInitializer->ValidateInitialized_AnyThread();

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->ValidatePeer(EPeerKind::Leader);

        auto nodeId = FromProto<TNodeId>(request->node_id());

        // TODO(kvk1920): provide some way to get node address from
        // non-automaton thread and uncomment this.
        // const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        // auto* node = nodeTracker->GetNodeOrThrow(nodeId);
        // context->SetRequestInfo("NodeId: %v, Address: %v",
        //     nodeId,
        //     node->GetDefaultAddress());

        context->SetRequestInfo("NodeId: %v", nodeId);

        const auto& execNodeTracker = Bootstrap_->GetExecNodeTracker();
        execNodeTracker->ProcessHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateExecNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TExecNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
