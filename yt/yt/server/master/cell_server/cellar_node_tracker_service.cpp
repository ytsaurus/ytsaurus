#include "cellar_node_tracker_service.h"

#include "private.h"
#include "cellar_node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/cellar_node_tracker_service_proxy.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NRpc;
using namespace NCellarNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TCellarNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TCellarNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::CellarNodeTrackerService,
            CellServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NCellarNodeTrackerClient::NProto, Heartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v",
            nodeId,
            node->GetDefaultAddress());

        const auto& cellarNodeTracker = Bootstrap_->GetCellarNodeTracker();
        cellarNodeTracker->ProcessHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCellarNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TCellarNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
