#pragma once

#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TAgentHeartbeatContext
    : public TRefCounted
{
    TControllerAgentDescriptor AgentDescriptor;
    NConcurrency::IThroughputThrottlerPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;
    TInstant LastTotalConfirmationTime;

    THashSet<TJobPtr> SentEnqueuedJobs;
};

DEFINE_REFCOUNTED_TYPE(TAgentHeartbeatContext)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorPool
    : public TRefCounted
{
public:
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

        using TRspHeartbeat = NRpc::TTypedClientResponse<
            NControllerAgent::NProto::TRspHeartbeat>;
        using TReqHeartbeat = NRpc::TTypedClientRequest<
            NControllerAgent::NProto::TReqHeartbeat,
            TRspHeartbeat>;
        using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
        using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

    private:
        struct THeartbeatInfo
        {
            TInstant LastSentHeartbeatTime;
            TInstant LastFailedHeartbeatTime;
            TDuration FailedHeartbeatBackoffTime;
        };
        THeartbeatInfo HeartbeatInfo_;

        const TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;
        const TControllerAgentDescriptor ControllerAgentDescriptor_;

        const NRpc::IChannelPtr Channel_;

        const NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;

        NConcurrency::IReconfigurableThroughputThrottlerPtr StatisticsThrottler_;

        TDuration RunningJobStatisticsSendingBackoff_;

        TInstant LastTotalConfirmationTime_;

        THashSet<TJobPtr> EnqueuedFinishedJobs_;
        bool ShouldSendOutOfBand_ = false;

        void SendHeartbeat();
        void OnAgentIncarnationOutdated() noexcept;

        void DoSendHeartbeat();

        void PrepareHeartbeatRequest(
            const TReqHeartbeatPtr& request,
            const TAgentHeartbeatContextPtr& context);
        void ProcessHeartbeatResponse(
            const TRspHeartbeatPtr& response,
            const TAgentHeartbeatContextPtr& context);

        void DoPrepareHeartbeatRequest(
            const TReqHeartbeatPtr& request,
            const TAgentHeartbeatContextPtr& context);
        void DoProcessHeartbeatResponse(
            const TRspHeartbeatPtr& response,
            const TAgentHeartbeatContextPtr& context);
    };

    using TControllerAgentConnectorPtr = TIntrusivePtr<TControllerAgentConnector>;

    TControllerAgentConnectorPool(TControllerAgentConnectorConfigPtr config, IBootstrap* bootstrap);

    void Start();

    NRpc::IChannelPtr GetOrCreateChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

    void SendOutOfBandHeartbeatsIfNeeded();

    TWeakPtr<TControllerAgentConnector> GetControllerAgentConnector(const TJob* job);

    void OnDynamicConfigChanged(
        const TExecNodeDynamicConfigPtr& oldConfig,
        const TExecNodeDynamicConfigPtr& newConfig);

    void OnRegisteredAgentSetReceived(THashSet<TControllerAgentDescriptor> controllerAgentDescriptors);

private:
    THashMap<TControllerAgentDescriptor, TControllerAgentConnectorPtr> ControllerAgentConnectors_;

    const TControllerAgentConnectorConfigPtr StaticConfig_;
    TControllerAgentConnectorConfigPtr CurrentConfig_;

    IBootstrap* const Bootstrap_;

    TDuration TestHeartbeatDelay_{};

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);

    TWeakPtr<TControllerAgentConnector> AddControllerAgentConnector(TControllerAgentDescriptor descriptor);

    void OnConfigUpdated();

    void OnJobFinished(const TJobPtr& job);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
