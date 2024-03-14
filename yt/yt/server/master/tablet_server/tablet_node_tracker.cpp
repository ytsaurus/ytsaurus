#include "tablet_node_tracker.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTabletNodeTrackerClient::NProto;
using namespace NTabletServer;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletNodeTracker
    : public ITabletNodeTracker
    , public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(
        NNodeTrackerServer::TNode* node,
        NTabletNodeTrackerClient::NProto::TReqHeartbeat* request,
        NTabletNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

public:
    explicit TTabletNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TabletNodeTracker)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TTabletNodeTracker::HydraTabletNodeHeartbeat, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TTabletNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TTabletNodeTracker::HydraTabletNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    void ProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response,
        bool legacyFullHeartbeat) override
    {
        YT_VERIFY(node->IsTabletNode());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Tablet);

        if (!legacyFullHeartbeat) {
            Heartbeat_.Fire(node, request, response);
        }
    }

private:
    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    void HydraTabletNodeHeartbeat(
        const TCtxHeartbeatPtr& /*context*/,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/tablet_server/tablet_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing tablet node heartbeat (NodeId: %v, Address: %v, State: %v, ReportedTabletNodeHeartbeat: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                node->ReportedTabletNodeHeartbeat());

            nodeTracker->UpdateLastSeenTime(node);

            ProcessHeartbeat(node, request, response, /*legacyFullHeartbeat*/ false);
        }
    }

    void CommitMutationWithSemaphore(
        std::unique_ptr<TMutation> mutation,
        NRpc::IServiceContextPtr context,
        const TAsyncSemaphorePtr& semaphore)
    {
        auto timeBefore = NProfiling::GetInstant();

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto expectedMutationCommitDuration = config->CellMaster->ExpectedMutationCommitDuration;

        semaphore->AsyncAcquire().SubscribeUnique(
            BIND([=, mutation = std::move(mutation), context = std::move(context)] (TErrorOr<TAsyncSemaphoreGuard>&& guardOrError) {
                if (!guardOrError.IsOK()) {
                    context->Reply(TError("Failed to acquire semaphore") << guardOrError);
                    return;
                }

                auto requestTimeout = context->GetTimeout();
                auto timeAfter = NProfiling::GetInstant();
                if (requestTimeout && timeAfter + expectedMutationCommitDuration >= timeBefore + *requestTimeout) {
                    context->Reply(TError(NYT::EErrorCode::Timeout, "Semaphore acquisition took too long"));
                } else {
                    Y_UNUSED(WaitFor(mutation->CommitAndReply(context)));
                }
            }).Via(EpochAutomatonInvoker_));
    }

    const TDynamicTabletNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
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

} // namespace NYT::NTabletServer
