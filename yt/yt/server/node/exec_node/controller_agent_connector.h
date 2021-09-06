#pragma once

#include "job.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnector
    : public TRefCounted
{
public:
    TControllerAgentConnector(TControllerAgentConnectorConfigPtr config, IBootstrap* bootstrap);

    NRpc::IChannelPtr GetChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

private:
    TControllerAgentConnectorConfigPtr Config_;
    IBootstrap* const Bootstrap_;
    const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    const NConcurrency::TPeriodicExecutorPtr ScanOutdatedAgentIncarnationsExecutor_;

    struct THeartbeatInfo
    {
        TInstant LastSentHeartbeatTime_;
        TInstant LastFailedHeartbeatTime_;
        TDuration FailedHeartbeatBackoffTime_;
    };

    THashMap<TControllerAgentDescriptor, THeartbeatInfo> HeartbeatInfo_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    THashMap<NRpc::TAddressWithNetwork, NRpc::IChannelPtr> ControllerAgentChannels_;
    THashSet<TControllerAgentDescriptor> OutdatedAgentIncarnations_;

    void SendHeartbeats();
    void ScanOutdatedAgentIncarnations();
    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);
    void MarkAgentIncarnationAsOutdated(const TControllerAgentDescriptor& agentDescriptor);
    bool IsAgentIncarnationOutdated(const TControllerAgentDescriptor& agentDescriptor);
    void ResetOutdatedAgentIncarnations();
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
