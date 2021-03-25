#include "cellar_node_tracker.h"

#include "config.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NCellarClient;
using namespace NCellarNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeTracker
    : public ICellarNodeTracker
    , public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL(void(
        NNodeTrackerServer::TNode* node,
        NCellarNodeTrackerClient::NProto::TReqHeartbeat* request,
        NCellarNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

public:
    explicit TCellarNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::CellarNodeTracker)
    {
        RegisterMethod(BIND(&TCellarNodeTracker::HydraCellarNodeHeartbeat, Unretained(this)));
    }

    virtual void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TCellarNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TCellarNodeTracker::HydraCellarNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    virtual void ProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response,
        bool legacyFullHeartbeat) override
    {
        YT_VERIFY(node->IsCellarNode());

        for (auto& cellarInfo : *request->mutable_cellars()) {
            auto cellarType = FromProto<ECellarType>(cellarInfo.type());
            auto& statistics = *cellarInfo.mutable_statistics();

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing cellar heartbeat (NodeId: %v, Address: %v, CellarType: %v, Statistics: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                cellarType,
                statistics);

            // TODO(savrus) Separate statistics for different cellars
            node->SetTabletNodeStatistics(std::move(statistics));
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Cellar);

        if (!legacyFullHeartbeat) {
            Heartbeat_.Fire(node, request, response);
        }
    }

private:
    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    void HydraCellarNodeHeartbeat(
        const TCtxHeartbeatPtr& context,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = request->node_id();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/cell_server/cellar_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing cellar node heartbeat (NodeId: %v, Address: %v, State: %v, ReportedCellarNodeHeartbeat: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                node->ReportedCellarNodeHeartbeat());

            nodeTracker->UpdateLastSeenTime(node);

            ProcessHeartbeat(node, request, response, /* legacyFullHeartbeat */ false);
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

    const TDynamicCellarNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->CellManager->CellarNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        HeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentHeartbeats);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellarNodeTrackerPtr CreateCellarNodeTracker(TBootstrap* bootstrap)
{
    return New<TCellarNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
