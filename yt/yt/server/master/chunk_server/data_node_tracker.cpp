#include "data_node_tracker.h"

#include "chunk_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NDataNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

using NNodeTrackerClient::NProto::TReqRegisterNode;
using NNodeTrackerClient::NProto::TRspRegisterNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTracker
    : public IDataNodeTracker
    , public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(
        TNode* node,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response),
        FullHeartbeat);
    DEFINE_SIGNAL_OVERRIDE(void(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response),
        IncrementalHeartbeat);

public:
    explicit TDataNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::DataNodeTracker)
    {
        RegisterMethod(BIND(&TDataNodeTracker::HydraIncrementalDataNodeHeartbeat, Unretained(this)));
        RegisterMethod(BIND(&TDataNodeTracker::HydraFullDataNodeHeartbeat, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TDataNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TDataNodeTracker::HydraFullDataNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), FullHeartbeatSemaphore_);
    }

    void ProcessFullHeartbeat(
        TNode* node,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto& statistics = *request->mutable_statistics();
        node->SetDataNodeStatistics(std::move(statistics), chunkManager);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Data);

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            SerializeMediumDirectory(response->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, response->mutable_medium_overrides(), chunkManager);
        }
        
        FullHeartbeat_.Fire(node, request, response);
    }

    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TDataNodeTracker::HydraIncrementalDataNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), IncrementalHeartbeatSemaphore_);
    }

    void ProcessIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response) override
    {
        YT_VERIFY(node->IsDataNode() || node->IsExecNode());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto& statistics = *request->mutable_statistics();
        node->SetDataNodeStatistics(std::move(statistics), chunkManager);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Data);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            SerializeMediumDirectory(response->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, response->mutable_medium_overrides(), chunkManager);

            node->SetDisableWriteSessionsReportedByNode(request->write_sessions_disabled());
            response->set_disable_write_sessions(node->GetDisableWriteSessions());
            node->SetDisableWriteSessionsSentToNode(node->GetDisableWriteSessions());
        }

        IncrementalHeartbeat_.Fire(node, request, response);
    }

private:
    const TAsyncSemaphorePtr FullHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);
    const TAsyncSemaphorePtr IncrementalHeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    void HydraIncrementalDataNodeHeartbeat(
        const TCtxIncrementalHeartbeatPtr& /*context*/,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (!node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process an incremental data node heartbeat until full data node heartbeat is sent");
        }

        YT_PROFILE_TIMING("/node_tracker/incremental_data_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing incremental data node heartbeat (NodeId: %v, Address: %v, State: %v, %v",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            nodeTracker->UpdateLastSeenTime(node);

            ProcessIncrementalHeartbeat(node, request, response);
        }
    }

    void HydraFullDataNodeHeartbeat(
        const TCtxFullHeartbeatPtr& /*context*/,
        TReqFullHeartbeat* request,
        TRspFullHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        if (node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Full data node heartbeat is already sent");
        }

        YT_PROFILE_TIMING("/node_tracker/full_data_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing full data node heartbeat (NodeId: %v, Address: %v, State: %v, %v",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                statistics);

            nodeTracker->UpdateLastSeenTime(node);

            ProcessFullHeartbeat(node, request, response);
        }
    }

    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        NRpc::IServiceContextPtr context,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto handler = BIND([mutation = std::move(mutation), context = std::move(context)] (TAsyncSemaphoreGuard) {
            Y_UNUSED(WaitFor(mutation->CommitAndReply(context)));
        });

        semaphore->AsyncAcquire(handler, EpochAutomatonInvoker_);
    }

    const TDynamicDataNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->DataNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        FullHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentFullHeartbeats);
        IncrementalHeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentIncrementalHeartbeats);
    }

    void ProcessRegisterNode(
        TNode* node,
        TReqRegisterNode* /*request*/,
        TRspRegisterNode* response) override
    {
        YT_VERIFY(node->IsDataNode());

        if (Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
            auto* dataNodeInfoExt = response->MutableExtension(NNodeTrackerClient::NProto::TDataNodeInfoExt::data_node_info_ext);
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            SerializeMediumDirectory(dataNodeInfoExt->mutable_medium_directory(), chunkManager);
            SerializeMediumOverrides(node, dataNodeInfoExt->mutable_medium_overrides(), chunkManager);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IDataNodeTrackerPtr CreateDataNodeTracker(TBootstrap* bootstrap)
{
    return New<TDataNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
