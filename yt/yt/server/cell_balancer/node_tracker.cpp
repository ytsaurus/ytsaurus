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

    void ProcessNodeHeartbeat(TReqHeartbeat* request, TRspHeartbeat* /*response*/) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto nodeAddress = FromProto<std::string>(request->node_address());

        auto nodeIt = NodeHeartbeatStates_.emplace(nodeAddress, TNodeState{}).first;

        YT_LOG_DEBUG("Processing node heartbeat (NodeAddress: %v, PreviousPingTime: %v)",
            nodeAddress,
            nodeIt->second.LastPingTime);

        nodeIt->second.LastPingTime = TInstant::Now();
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

            nodeIt->second.MasterState = masterState;
            info->LocalState = now - nodeIt->second.LastPingTime >= timeout
                ? ELocalNodeState::Offline
                : ELocalNodeState::Online;
        }

        YT_LOG_INFO("Finished node states update through the heartbeat node tracker");
    }

private:
    struct TNodeState
    {
        NNodeTrackerClient::ENodeState MasterState = NNodeTrackerClient::ENodeState::Unknown;
        TInstant LastPingTime;
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
