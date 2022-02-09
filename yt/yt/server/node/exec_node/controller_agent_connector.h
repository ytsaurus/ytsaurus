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
    friend class TControllerAgentConnector;

    class TControllerAgentConnector
        : public TRefCounted
    {
    public:
        TControllerAgentConnector(
            TControllerAgentConnectorPool* controllerAgentConnectorPool,
            TControllerAgentDescriptor controllerAgentDescriptor);
        
        NRpc::IChannelPtr GetChannel() const noexcept;
        void SendOutOfBandHeartbeatIfNeeded();
        void EnqueueFinishedJob(const TJobPtr& job);

        void OnConfigUpdated();

        ~TControllerAgentConnector();

    private:
        struct THeartbeatInfo
        {
            TInstant LastSentHeartbeatTime;
            TInstant LastFailedHeartbeatTime;
            TDuration FailedHeartbeatBackoffTime;
        };
        THeartbeatInfo HeartbeatInfo_;

        TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;
        TControllerAgentDescriptor ControllerAgentDescriptor_;

        NRpc::IChannelPtr Channel_;

        const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

        NConcurrency::IReconfigurableThroughputThrottlerPtr StatisticsThrottler_;

        TDuration RunningJobInfoSendingBackoff_;
        bool SendJobResult_ = false;

        THashSet<TJobPtr> EnqueuedFinishedJobs_;
        bool ShouldSendOutOfBand_ = false;

        DECLARE_THREAD_AFFINITY_SLOT(JobThread);

        void SendHeartbeat();
        void OnAgentIncarnationOutdated() noexcept;
    };

public:
    using TControllerAgentConnectorPtr = TIntrusivePtr<TControllerAgentConnector>;

    TControllerAgentConnectorPool(TControllerAgentConnectorConfigPtr config, IBootstrap* bootstrap);

    NRpc::IChannelPtr GetOrCreateChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

    void SendOutOfBandHeartbeatsIfNeeded();

    TControllerAgentConnectorPtr GetControllerAgentConnector(const TJob* job);

    void OnDynamicConfigChanged(
        const TExecNodeDynamicConfigPtr& oldConfig,
        const TExecNodeDynamicConfigPtr& newConfig);
    
private:
    //! TControllerAgentConnector object lifetime include lifetime of map entry, so we can use raw pointers here.
    THashMap<TControllerAgentDescriptor, TControllerAgentConnector*> ControllerAgentConnectors_;

    const TControllerAgentConnectorConfigPtr StaticConfig_;
    TControllerAgentConnectorConfigPtr CurrentConfig_;

    IBootstrap* const Bootstrap_;

    TDuration TestHeartbeatDelay_{};

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnControllerAgentConnectorDestroyed(
        const TControllerAgentDescriptor& controllerAgentDescriptor) noexcept;

    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);

    void OnConfigUpdated();
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
