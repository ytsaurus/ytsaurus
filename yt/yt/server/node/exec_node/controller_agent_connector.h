#pragma once

#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

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

        void OnConfigUpdated();

        const TControllerAgentDescriptor& GetDescriptor() const;

        void AddUnconfirmedJobIds(std::vector<TJobId> unconfirmedJobIds);

        struct TAllocationInfo
        {
            TAllocationId AllocationId;
            TOperationId OperationId;
        };

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
        std::vector<TJobId> UnconfirmedJobIds_;
        bool ShouldSendOutOfBand_ = false;

        THashSet<TJobId> JobIdsToConfirm_;

        THashMap<TAllocationId, TOperationId> AllocationIdsWaitingForSpec_;

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

        TFuture<std::vector<TErrorOr<TJobStartInfo>>>
        SettleJobs(const std::vector<TAllocationInfo>& allocationInfos);

        void OnJobRegistered(const TJobPtr& job);

        void OnJobRegistrationFailed(TAllocationId allocationId);
    };

    using TControllerAgentConnectorPtr = TIntrusivePtr<TControllerAgentConnector>;

    TControllerAgentConnectorPool(TExecNodeConfigPtr config, IBootstrap* bootstrap);

    void Start();

    void SendOutOfBandHeartbeatsIfNeeded();

    TWeakPtr<TControllerAgentConnector> GetControllerAgentConnector(const TJob* job);

    void OnDynamicConfigChanged(
        const TExecNodeDynamicConfigPtr& oldConfig,
        const TExecNodeDynamicConfigPtr& newConfig);

    void OnRegisteredAgentSetReceived(THashSet<TControllerAgentDescriptor> controllerAgentDescriptors);

    TFuture<std::vector<TErrorOr<TControllerAgentConnector::TJobStartInfo>>>
    SettleJobs(
        const TControllerAgentDescriptor& agentDescriptor,
        const std::vector<TControllerAgentConnector::TAllocationInfo>& allocationInfos);

    THashMap<TAllocationId, TOperationId> GetAllocationIdsWaitingForSpec() const;

private:
    THashMap<TControllerAgentDescriptor, TControllerAgentConnectorPtr> ControllerAgentConnectors_;

    const TControllerAgentConnectorConfigPtr StaticConfig_;
    TControllerAgentConnectorConfigPtr CurrentConfig_;

    IBootstrap* const Bootstrap_;

    TDuration TestHeartbeatDelay_{};
    TDuration SettleJobsTimeout_;

    // COMPAT(pogorelov)
    bool SendWaitingJobs_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    NRpc::IChannelPtr CreateChannel(const TControllerAgentDescriptor& agentDescriptor);

    TWeakPtr<TControllerAgentConnector> AddControllerAgentConnector(
        TControllerAgentDescriptor agentDescriptor);

    TIntrusivePtr<TControllerAgentConnector> GetControllerAgentConnector(
        const TControllerAgentDescriptor& agentDescriptor);

    NRpc::IChannelPtr GetOrCreateChannel(const TControllerAgentDescriptor& controllerAgentDescriptor);

    void OnConfigUpdated();

    void OnJobFinished(const TJobPtr& job);

    void OnJobRegistered(const TJobPtr& job);

    void OnJobRegistrationFailed(
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
    TInstant LastTotalConfirmationTime;

    THashSet<TJobPtr> JobsToForcefullySend;
    std::vector<TJobId> UnconfirmedJobIds;

    // COMPAT(pogorelov)
    bool SendWaitingJobs;

    THashMap<TAllocationId, TOperationId> AllocationIdsWaitingForSpec;
};

DEFINE_REFCOUNTED_TYPE(TAgentHeartbeatContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
