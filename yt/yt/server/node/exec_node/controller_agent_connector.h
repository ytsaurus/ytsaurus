#pragma once

#include "helpers.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NExecNode {

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

        void OnConfigUpdated(const TControllerAgentConnectorDynamicConfigPtr& newConfig);

        const TControllerAgentDescriptor& GetDescriptor() const;

        void AddUnconfirmedJobIds(std::vector<TJobId> unconfirmedJobIds);

        struct TJobStartInfo
        {
            TJobId JobId;
            NControllerAgent::NProto::TJobSpec JobSpec;
        };

        ~TControllerAgentConnector();

        using TRspHeartbeat = NRpc::TTypedClientResponse<
            NControllerAgent::NProto::TRspHeartbeat>;
        using TReqHeartbeat = NRpc::TTypedClientRequest<
            NControllerAgent::NProto::TReqHeartbeat,
            TRspHeartbeat>;
        using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
        using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

    private:
        friend class TControllerAgentConnectorPool;

        const TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;
        const TControllerAgentDescriptor ControllerAgentDescriptor_;

        const NRpc::IChannelPtr Channel_;

        const NConcurrency::TRetryingPeriodicExecutorPtr HeartbeatExecutor_;

        NConcurrency::IReconfigurableThroughputThrottlerPtr StatisticsThrottler_;

        THashSet<TJobPtr> EnqueuedFinishedJobs_;
        std::vector<TJobId> UnconfirmedJobIds_;
        bool ShouldSendOutOfBand_ = false;

        THashSet<TJobId> JobIdsToConfirm_;

        THashMap<TAllocationId, TOperationId> AllocationIdsWaitingForSpec_;

        TControllerAgentConnectorDynamicConfigPtr GetConfig() const noexcept;

        TError SendHeartbeat();
        void OnAgentIncarnationOutdated() noexcept;

        TError DoSendHeartbeat();

        void PrepareHeartbeatRequest(
            NNodeTrackerClient::TNodeId nodeId,
            const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
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

        TFuture<TJobStartInfo> SettleJob(
            TOperationId operationId,
            TAllocationId allocationId);

        void OnJobRegistered(const TJobPtr& job);

        void OnAllocationFailed(TAllocationId allocationId);
    };

    using TControllerAgentConnectorPtr = TIntrusivePtr<TControllerAgentConnector>;

    TControllerAgentConnectorPool(IBootstrap* bootstrap);

    void Start();

    void SendOutOfBandHeartbeatsIfNeeded();

    TWeakPtr<TControllerAgentConnector> GetControllerAgentConnector(const TJob* job);

    void OnDynamicConfigChanged(
        const TControllerAgentConnectorDynamicConfigPtr& oldConfig,
        const TControllerAgentConnectorDynamicConfigPtr& newConfig);

    void OnRegisteredAgentSetReceived(THashSet<TControllerAgentDescriptor> controllerAgentDescriptors);

    std::optional<TControllerAgentDescriptor> GetDescriptorByIncarnationId(NScheduler::TIncarnationId incarnationId) const;

    std::vector<NScheduler::TIncarnationId> GetRegisteredAgentIncarnationIds() const;

    TFuture<TControllerAgentConnector::TJobStartInfo> SettleJob(
        const TControllerAgentDescriptor& agentDescriptor,
        TOperationId operationId,
        TAllocationId allocationId);

    THashMap<TAllocationId, TOperationId> GetAllocationIdsWaitingForSpec() const;

private:
    THashMap<TControllerAgentDescriptor, TControllerAgentConnectorPtr> ControllerAgentConnectors_;

    TAtomicIntrusivePtr<TControllerAgentConnectorDynamicConfig> DynamicConfig_;

    IBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);

    TWeakPtr<TControllerAgentConnector> AddControllerAgentConnector(
        TControllerAgentDescriptor agentDescriptor);

    TIntrusivePtr<TControllerAgentConnector> GetControllerAgentConnector(
        const TControllerAgentDescriptor& agentDescriptor);

    NRpc::IChannelPtr GetOrCreateChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

    void OnConfigUpdated(const TControllerAgentConnectorDynamicConfigPtr& newConfig);

    void OnJobFinished(const TJobPtr& job);

    void OnJobRegistered(const TJobPtr& job);

    void OnAllocationFailed(
        TAllocationId allocationId,
        TOperationId operationId,
        const TControllerAgentDescriptor& agentDescriptor,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorPool)

////////////////////////////////////////////////////////////////////////////////

struct TAgentHeartbeatContext
    : public TRefCounted
{
    TControllerAgentConnectorPool::TControllerAgentConnectorPtr ControllerAgentConnector;
    NConcurrency::IThroughputThrottlerPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;
    TDuration JobStalenessDelay;

    THashSet<TJobPtr> JobsToForcefullySend;
    std::vector<TJobId> UnconfirmedJobIds;

    THashMap<TAllocationId, TOperationId> AllocationIdsWaitingForSpec;
};

DEFINE_REFCOUNTED_TYPE(TAgentHeartbeatContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
