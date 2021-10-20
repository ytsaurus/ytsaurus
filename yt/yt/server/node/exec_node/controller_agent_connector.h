#pragma once

#include "job.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJob;
DECLARE_REFCOUNTED_CLASS(TJob)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorPool
    : public TRefCounted
{
private:
    class TControllerAgentConnectorBase
        : public TRefCounted
    { };

public:
    using TControllerAgentConnectorLease = TIntrusivePtr<TControllerAgentConnectorBase>;

    TControllerAgentConnectorPool(TControllerAgentConnectorConfigPtr config, IBootstrap* bootstrap);

    NRpc::IChannelPtr GetOrCreateChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

    void EnqueueFinishedJob(const TJobPtr& job);

    void SendOutOfBandHeartbeatsIfNeeded();

    TControllerAgentConnectorLease CreateLeaseOnControllerAgentConnector(const TJob* job);

private:
    class TControllerAgentConnector;
    friend class TControllerAgentConnector;

    //! TControllerAgentConnector object lifetime include lifetime of map entry, so we can use raw pointers here.
    THashMap<TControllerAgentDescriptor, TControllerAgentConnector*> ControllerAgentConnectors_;

    TControllerAgentConnectorConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnControllerAgentConnectorDestroyed(
        const TControllerAgentDescriptor& controllerAgentDescriptor) noexcept;

    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
