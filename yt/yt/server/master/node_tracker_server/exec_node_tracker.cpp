#include "exec_node_tracker.h"

#include "private.h"
#include "node.h"
#include "node_tracker.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NExecNodeTrackerClient::NProto;
using namespace NHydra;
using namespace NNodeTrackerClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = NodeTrackerServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeTracker
    : public IExecNodeTracker
    , public TMasterAutomatonPart
{
public:
    explicit TExecNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ExecNodeTracker)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TExecNodeTracker::HydraExecNodeHeartbeat, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TExecNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        hydraFacade->CommitMutationWithSemaphore(
            HeartbeatSemaphore_,
            context,
            BIND([=, this, this_ = MakeStrong(this)] {
                return CreateMutation(
                    Bootstrap_->GetHydraFacade()->GetHydraManager(),
                    context,
                    &TExecNodeTracker::HydraExecNodeHeartbeat,
                    this);
            }));
    }

private:
    const TAsyncSemaphorePtr HeartbeatSemaphore_ = New<TAsyncSemaphore>(0);

    void HydraExecNodeHeartbeat(
        const TCtxHeartbeatPtr& /*context*/,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());

        auto jobProxyVersion = YT_OPTIONAL_FROM_PROTO(*request, job_proxy_build_version);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/node_tracker/exec_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing exec node heartbeat (NodeId: %v, Address: %v, State: %v, JobProxyVersion: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                jobProxyVersion);

            nodeTracker->UpdateLastSeenTime(node);
            node->JobProxyVersion() = jobProxyVersion;

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            YT_VERIFY(multicellManager->IsPrimaryMaster());

            YT_VERIFY(node->IsExecNode());

            auto& statistics = *request->mutable_statistics();
            node->SetExecNodeStatistics(std::move(statistics));

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            nodeTracker->OnNodeHeartbeat(node, ENodeHeartbeatType::Exec);

            response->set_disable_scheduler_jobs(node->AreSchedulerJobsDisabled());
        }
    }

    const TDynamicNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->NodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        HeartbeatSemaphore_->SetTotal(GetDynamicConfig()->MaxConcurrentExecNodeHeartbeats);
    }
};

////////////////////////////////////////////////////////////////////////////////

IExecNodeTrackerPtr CreateExecNodeTracker(NCellMaster::TBootstrap* bootstrap)
{
    return New<TExecNodeTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
