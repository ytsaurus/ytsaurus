#include "data_node_tracker_service.h"

#include "private.h"
#include "data_node_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/data_node_tracker_service_proxy.h>
#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

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
        ValidateLocationDirectory(*request);
        SyncWithUpstream();

        auto nodeId = FromProto<TNodeId>(request->node_id());
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
        ValidateLocationDirectory(*request);
        SyncWithUpstream();

        auto nodeId = FromProto<TNodeId>(request->node_id());
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

    template <class TReqHeartbeat>
    void ValidateLocationDirectory(const TReqHeartbeat& request)
    {
        using namespace NDataNodeTrackerClient::NProto;
        static_assert(
            std::is_same_v<TReqHeartbeat, TReqFullHeartbeat> ||
            std::is_same_v<TReqHeartbeat, TReqIncrementalHeartbeat>,
            "TReqHeartbeat must be either TReqFullHeartbeat or TReqIncrementalHeartbeat");

        constexpr bool fullHeartbeat = std::is_same_v<TReqHeartbeat, TReqFullHeartbeat>;

        if (!FromProto<TChunkLocationDirectory>(request.location_directory()).IsValid()) {
            YT_LOG_ALERT(
                "Invalid data node %v heartbeat: location directory contains duplicates "
                "(NodeId: %v)",
                fullHeartbeat ? "full" : "incremental",
                request.node_id());
            THROW_ERROR_EXCEPTION("Location directory contains duplicates");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeTrackerService(TBootstrap* bootstrap)
{
    return New<TDataNodeTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
