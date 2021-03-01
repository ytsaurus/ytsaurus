#include "tablet_node_tracker_service.h"

#include "private.h"
#include "tablet_node_tracker.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/master_hydra_service.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/ytlib/tablet_node_tracker_client/tablet_node_tracker_service_proxy.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NRpc;
using namespace NTabletNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TTabletNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TTabletNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::TabletNodeTrackerService,
            CellServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NTabletNodeTrackerClient::NProto, Heartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetDefaultAddress(),
            statistics);

        const auto& tabletNodeTracker = Bootstrap_->GetTabletNodeTracker();
        tabletNodeTracker->ProcessHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTabletNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TTabletNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
