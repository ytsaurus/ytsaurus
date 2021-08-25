#include "job_tracker_service.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"
#include "config.h"

#include <yt/yt/server/master/chunk_server/new_replicator/job_tracker.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/ytlib/job_tracker_client/helpers.h>
#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChunkServer {

using namespace NHydra;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;
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
        auto heartbeatMethodDescriptor = RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true);
        if (Bootstrap_->UseNewReplicator()) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            const auto& jobTrackerInvoker = chunkManager->GetChunkInvoker(EChunkThreadQueue::JobTrackerService);
            heartbeatMethodDescriptor = heartbeatMethodDescriptor
                .SetInvoker(jobTrackerInvoker);
        }
        RegisterMethod(heartbeatMethodDescriptor);
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat)
    {
        if (Bootstrap_->UseNewReplicator()) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto jobTracker = chunkManager->GetJobTracker();
            if (!jobTracker) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }

            jobTracker->ProcessJobHeartbeat(context);
        } else {
            ValidateClusterInitialized();
            ValidatePeer(EPeerKind::Leader);

            auto nodeId = request->node_id();

            const auto& resourceLimits = request->resource_limits();
            const auto& resourceUsage = request->resource_usage();

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            auto* node = nodeTracker->GetNodeOrThrow(nodeId);

            context->SetRequestInfo("NodeId: %v, Address: %v, ResourceUsage: %v",
                nodeId,
                node->GetDefaultAddress(),
                FormatResourceUsage(resourceUsage, resourceLimits));

            if (!node->ReportedDataNodeHeartbeat()) {
                THROW_ERROR_EXCEPTION(
                    NNodeTrackerClient::EErrorCode::InvalidState,
                    "Cannot process a job heartbeat unless data node heartbeat is reported");
            }

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ProcessJobHeartbeat(node, context);
        }
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* boostrap)
{
    return New<TJobTrackerService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
