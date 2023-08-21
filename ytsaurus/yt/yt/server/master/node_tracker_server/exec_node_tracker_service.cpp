#include "exec_node_tracker_service.h"

#include "private.h"
#include "exec_node_tracker.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NExecNodeTrackerClient;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TExecNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TExecNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::ExecNodeTrackerService,
            NodeTrackerServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NExecNodeTrackerClient::NProto, Heartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto nodeId = request->node_id();
        const auto& statistics = request->statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, %v",
            nodeId,
            node->GetDefaultAddress(),
            statistics);

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
