#include "exec_node_tracker_service.h"

#include "private.h"

#include "exec_node_tracker.h"
#include "node_tracker_cache.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/world_initializer_cache.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NExecNodeTrackerClient;
using namespace NHydra;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    explicit TExecNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TExecNodeTrackerServiceProxy::GetDescriptor(),
            TMasterHydraServiceBase::TRpcHeavyDefaultInvoker{},
            NodeTrackerServerLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NExecNodeTrackerClient::NProto, Heartbeat)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodeTrackerCache = Bootstrap_->GetNodeTrackerCache();
        auto nodeAddress = nodeTrackerCache->GetNodeDefaultAddressOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v",
            nodeId,
            nodeAddress);

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
