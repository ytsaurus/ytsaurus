#include "tablet_node_tracker.h"

#include "private.h"

#include <yt/server/master/cell_master/automaton.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/tablet_server/config.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTabletNodeTrackerClient::NProto;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeTracker
    : public ITabletNodeTracker
    , public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL(void(
        NNodeTrackerServer::TNode* node,
        NTabletNodeTrackerClient::NProto::TReqHeartbeat* request,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

public:
    explicit TTabletNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TabletNodeTracker)
    {
        RegisterMethod(BIND(&TTabletNodeTracker::HydraTabletNodeHeartbeat, Unretained(this)));
    }

    virtual void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TTabletNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TTabletNodeTracker::HydraTabletNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    virtual void ProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response,
        bool legacyFullHeartbeat) override
    {
        YT_VERIFY(node->IsTabletNode());

        auto& statistics = *request->mutable_statistics();
        node->SetTabletNodeStatistics(std::move(statistics));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeFlavor::Tablet);

        if (!legacyFullHeartbeat) {
            Heartbeat_.Fire(node, request, response);
        }
    }

private:
    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    void HydraTabletNodeHeartbeat(
        const TCtxHeartbeatPtr& context,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = request->node_id();
        auto& statistics = *request->mutable_statistics();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/cell_server/tablet_node_heartbeat_time") {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Processing tablet node heartbeat (NodeId: %v, Address: %v, State: %v, ReportedTabletNodeHeartbeat: %v, %v",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                node->ReportedTabletNodeHeartbeat(),
                statistics);

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

    const TDynamicTabletNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        HeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentHeartbeats);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletNodeTrackerPtr CreateTabletNodeTracker(TBootstrap* bootstrap)
{
    return New<TTabletNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
