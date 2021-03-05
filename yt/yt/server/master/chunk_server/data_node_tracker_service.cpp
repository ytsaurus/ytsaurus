#include "data_node_tracker_service.h"

#include "private.h"
#include "data_node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/data_node_tracker_service_proxy.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NDataNodeTrackerClient;
using namespace NHydra;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTrackerService
    : public TMasterHydraServiceBase
{
public:
    TDataNodeTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TDataNodeTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::DataNodeTrackerService,
            ChunkServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FullHeartbeat)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(IncrementalHeartbeat)
            .SetQueueSizeLimit(10000)
            .SetConcurrencyLimit(10000)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NDataNodeTrackerClient::NProto, FullHeartbeat)
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

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        dataNodeTracker->ProcessFullHeartbeat(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NDataNodeTrackerClient::NProto, IncrementalHeartbeat)
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

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        dataNodeTracker->ProcessIncrementalHeartbeat(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TDataNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
