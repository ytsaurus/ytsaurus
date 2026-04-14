#include "node_tracker.h"

#include "config.h"
#include "cypress_bindings.h"

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

        YT_LOG_DEBUG("Processing node heartbeat (NodeAddress: %v, PreviousPingTime: %v)",
            nodeAddress,
            nodeState.LastPingTime);

        nodeState.LastPingTime = TInstant::Now();

        if (nodeState.RequestConfigUpdate) {
            response->set_force_update_config(true);
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

        YT_LOG_INFO("Updating node states through the heartbeat node tracker");

        auto now = TInstant::Now();
        auto timeout = DynamicConfig_->HeartbeatTimeout;

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

            info->LocalState = heartbeatState.LastPingTime
                ? now - heartbeatState.LastPingTime >= timeout
                    ? ELocalNodeState::Offline
                    : ELocalNodeState::Online
                    : ELocalNodeState::Unknown;

            if (masterState == NNodeTrackerClient::ENodeState::Online &&
                info->LocalState == ELocalNodeState::Offline &&
                heartbeatState.LastReportedLocalState != info->LocalState)
            {
                YT_LOG_DEBUG("Node considered offline by node tracker (NodeAddress: %v)",
                    address);
            }

            heartbeatState.LastReportedLocalState = info->LocalState;
            heartbeatState.MasterState = masterState;
        }

        YT_LOG_INFO("Finished node states update through the heartbeat node tracker");
    }

    void RequestConfigUpdate(const std::string& nodeAddress) override
    {
        NodeHeartbeatStates_[nodeAddress].RequestConfigUpdate = true;
        YT_LOG_INFO("Requested node config update (NodeAddress: %v)",
            nodeAddress);
    }

private:
    struct TNodeState
    {
        NNodeTrackerClient::ENodeState MasterState = NNodeTrackerClient::ENodeState::Unknown;
        TInstant LastPingTime;
        ELocalNodeState LastReportedLocalState;

        bool RequestConfigUpdate = false;
    };

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
