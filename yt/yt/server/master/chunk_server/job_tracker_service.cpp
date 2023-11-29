#include "job_tracker_service.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/server/lib/chunk_server/helpers.h>
#include <yt/yt/server/lib/chunk_server/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChunkServer {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NChunkClient::NProto;
using namespace NChunkServer::NProto;
using namespace NCellMaster;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TJobTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::JobTrackerService,
            ChunkServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        ValidateClusterInitialized();

        ValidatePeer(EPeerKind::LeaderOrFollower);

        if (!request->reports_heartbeats_to_all_peers()) {
            YT_LOG_ALERT("Node does not report heartbeats to all peers "
                "(NodeId: %v)",
                request->node_id());
        }

        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& resourceLimits = request->resource_limits();
        const auto& resourceUsage = request->resource_usage();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, ResourceUsage: %v, SequenceNumber: %v",
            nodeId,
            node->GetDefaultAddress(),
            FormatResourceUsage(resourceUsage, resourceLimits),
            request->sequence_number());

        if (!node->ReportedDataNodeHeartbeat()) {
            SyncWithUpstream();

            // NB: Node can become dead during upstream sync.
            node = nodeTracker->GetNodeOrThrow(nodeId);
            if (!node->ReportedDataNodeHeartbeat()) {
                THROW_ERROR_EXCEPTION(
                    NNodeTrackerClient::EErrorCode::InvalidState,
                    "Cannot process a job heartbeat unless data node heartbeat has been reported");
            }
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ProcessJobHeartbeat(node, context);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* bootstrap)
{
    return New<TJobTrackerService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
