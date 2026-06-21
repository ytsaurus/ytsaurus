#include "node_tracker.h"

#include "config.h"
#include "cypress_bindings.h"
#include "private.h"

#include <yt/yt_proto/yt/client/bundle_controller/proto/bundle_controller_service.pb.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker
    : public INodeTracker
{
public:
    TNodeTracker()
        : DynamicConfig_(New<TNodeTrackerDynamicConfig>())
    { }

    void ProcessNodeHeartbeat(
        NBundleController::NProto::TReqHeartbeat* request,
        NBundleController::NProto::TRspHeartbeat* response) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto nodeAddress = FromProto<std::string>(request->node_address());

        auto& nodeState = NodeHeartbeatStates_.emplace(nodeAddress, TNodeState{}).first->second;

        YT_LOG_DEBUG("Processing node heartbeat (NodeAddress: %v, PreviousPingTime: %v, RequestConfigUpdate: %v)",
            nodeAddress,
            nodeState.LastPingTime,
            nodeState.RequestConfigUpdate);

        nodeState.LastPingTime = TInstant::Now();

        if (nodeState.RequestConfigUpdate) {
            response->set_force_update_config(true);
            response->set_expected_tag(nodeState.ExpectedTag);
            nodeState.RequestConfigUpdate = false;
        }
    }

    void OnDynamicConfigChanged(
        const TNodeTrackerDynamicConfigPtr& /*oldConfig*/,
        const TNodeTrackerDynamicConfigPtr& newConfig) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        DynamicConfig_ = newConfig;
    }

    void UpdateNodeStates(const std::map<std::string, TTabletNodeInfoPtr>& nodes) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!DynamicConfig_->Enable) {
            return;
        }

        YT_LOG_DEBUG("Updating node states through the heartbeat node tracker");

        auto now = TInstant::Now();

        int reportedOfflineNodeCount = 0;

        // Drop obsolete nodes and recompute the number of occupied offline slots:
        // a slot is freed when a previously offline node is gone or came back online.
        for (auto it = NodeHeartbeatStates_.begin(); it != NodeHeartbeatStates_.end(); ) {
            const auto& [address, heartbeatState] = *it;

            if (!nodes.contains(address)) {
                YT_LOG_DEBUG("Node dropped from node tracker state (NodeAddress: %v)",
                    address);

                NodeHeartbeatStates_.erase(it++);
                continue;
            }

            ++it;

            if (heartbeatState.LastReportedLocalState != ELocalNodeState::Offline) {
                continue;
            }

            auto nodeInfo = GetOrCrash(nodes, address);
            auto masterState = ConvertTo<NNodeTrackerClient::ENodeState>(nodeInfo->State);
            auto localState = heartbeatState.GetLocalState(now, DynamicConfig_->HeartbeatTimeout);

            if (masterState == NNodeTrackerClient::ENodeState::Online &&
                localState == ELocalNodeState::Offline)
            {
                ++reportedOfflineNodeCount;
            }
        }

        // Enrich node states with heartbeat info.
        for (const auto& [address, info] : nodes) {
            auto nodeIt = NodeHeartbeatStates_.find(address);
            auto masterState = ConvertTo<NNodeTrackerClient::ENodeState>(info->State);

            if (nodeIt == NodeHeartbeatStates_.end()) {
                NodeHeartbeatStates_.emplace(
                    address,
                    TNodeState{
                        .MasterState = masterState,
                        .LastPingTime = {},
                    });

                info->LocalState = ELocalNodeState::Unknown;
                continue;
            }

            auto& heartbeatState = nodeIt->second;

            auto localState = heartbeatState.GetLocalState(now, DynamicConfig_->HeartbeatTimeout);

            if (masterState == NNodeTrackerClient::ENodeState::Online &&
                localState == ELocalNodeState::Offline)
            {
                if (heartbeatState.LastReportedLocalState == ELocalNodeState::Offline) {
                    // Node was already reported offline (its slot is already accounted for
                    // in the first loop), keep reporting it offline.
                    info->LocalState = localState;
                } else if (reportedOfflineNodeCount < DynamicConfig_->MaxDetectedOfflineNodes) {
                    ++reportedOfflineNodeCount;
                    YT_LOG_INFO("Node considered offline by node tracker (NodeAddress: %v, ReportedOfflineNodeCount: %v)",
                        address,
                        reportedOfflineNodeCount);
                    info->LocalState = localState;
                    heartbeatState.LastReportedLocalState = localState;
                } else {
                    YT_LOG_DEBUG("Too many nodes are considered offline by node tracker, "
                        "will not add new one (NodeAddress: %v, ReportedOfflineNodeCount: %v)",
                        address,
                        reportedOfflineNodeCount);
                }
            } else {
                // Node is online (or its state is unknown); record the local state.
                // The offline slot, if any, has already been released in the first loop.
                info->LocalState = localState;
                heartbeatState.LastReportedLocalState = localState;
            }

            heartbeatState.MasterState = masterState;
        }

        ReportedOfflineNodeCountGauge_.Update(reportedOfflineNodeCount);

        YT_LOG_DEBUG("Finished node states update through the heartbeat node tracker");
    }

    void RequestConfigUpdate(const std::string& nodeAddress, std::string nodeTag) override
    {
        YT_LOG_DEBUG("Requested node config update (NodeAddress: %v, ExpectedTag: %v)",
            nodeAddress,
            nodeTag);

        NodeHeartbeatStates_[nodeAddress].RequestConfigUpdate = true;
        NodeHeartbeatStates_[nodeAddress].ExpectedTag = std::move(nodeTag);
    }

private:
    struct TNodeState
    {
        NNodeTrackerClient::ENodeState MasterState = NNodeTrackerClient::ENodeState::Unknown;
        TInstant LastPingTime;
        ELocalNodeState LastReportedLocalState = ELocalNodeState::Unknown;

        bool RequestConfigUpdate = false;
        std::string ExpectedTag;

        ELocalNodeState GetLocalState(TInstant now, TDuration timeout) const
        {
            return LastPingTime
                ? now - LastPingTime >= timeout
                    ? ELocalNodeState::Offline
                    : ELocalNodeState::Online
                : ELocalNodeState::Unknown;
        }
    };

    NProfiling::TGauge ReportedOfflineNodeCountGauge_ = BundleControllerProfiler()
        .Gauge("/node_tracker/reported_offline_node_count");

    THashMap<std::string, TNodeState> NodeHeartbeatStates_;

    TNodeTrackerDynamicConfigPtr DynamicConfig_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

INodeTrackerPtr CreateNodeTracker()
{
    return New<TNodeTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
