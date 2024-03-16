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
    DEFINE_SIGNAL_OVERRIDE(void(
        NNodeTrackerServer::TNode* node,
        NCellarNodeTrackerClient::NProto::TReqHeartbeat* request,
        NCellarNodeTrackerClient::NProto::TRspHeartbeat* response),
        Heartbeat);

public:
    explicit TCellarNodeTracker(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::CellarNodeTracker)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TCellarNodeTracker::HydraCellarNodeHeartbeat, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TCellarNodeTracker::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context) override
    {
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TCellarNodeTracker::HydraCellarNodeHeartbeat,
            this);
        CommitMutationWithSemaphore(std::move(mutation), std::move(context), HeartbeatSemaphore_);
    }

    void ProcessHeartbeat(
        TNode* node,
        TReqHeartbeat* request,
        TRspHeartbeat* response,
        bool legacyFullHeartbeat) override
    {
        YT_VERIFY(node->IsCellarNode());

        auto Logger = CellServerLogger
            .WithTag("NodeId: %v", node->GetId())
            .WithTag("Address: %v", node->GetDefaultAddress());

        THashSet<ECellarType> seenCellarTypes;
        for (auto& cellarInfo : *request->mutable_cellars()) {
            auto cellarType = FromProto<ECellarType>(cellarInfo.type());
            auto& statistics = *cellarInfo.mutable_statistics();

            if (!seenCellarTypes.insert(cellarType).second) {
                YT_LOG_ALERT("Duplicate cellar type in heartbeat (CellarType: %v)",
                    cellarType);
                continue;
            }

            YT_LOG_DEBUG("Processing cellar heartbeat (CellarType: %v, Statistics: %v)",
                cellarType,
                statistics);

            node->SetCellarNodeStatistics(cellarType, std::move(statistics));
        }

        for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
            if (!seenCellarTypes.contains(cellarType)) {
                node->RemoveCellarNodeStatistics(cellarType);
            }
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
        const TCtxHeartbeatPtr& /*context*/,
        TReqHeartbeat* request,
        TRspHeartbeat* response)
    {
        auto nodeId = FromProto<TNodeId>(request->node_id());

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        node->ValidateRegistered();

        YT_PROFILE_TIMING("/cell_server/cellar_node_heartbeat_time") {
            YT_LOG_DEBUG("Processing cellar node heartbeat (NodeId: %v, Address: %v, State: %v, ReportedCellarNodeHeartbeat: %v)",
                nodeId,
                node->GetDefaultAddress(),
                node->GetLocalState(),
                node->ReportedCellarNodeHeartbeat());

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

    const TDynamicCellarNodeTrackerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->CellManager->CellarNodeTracker;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
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
