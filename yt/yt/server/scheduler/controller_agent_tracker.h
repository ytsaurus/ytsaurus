#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: Control thread (unless noted otherwise)
 */
class TControllerAgentTracker
    : public TRefCounted
{
public:
    TControllerAgentTracker(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap);
    ~TControllerAgentTracker();

    void Initialize();

    std::vector<TControllerAgentPtr> GetAgents() const;

    IOperationControllerPtr CreateController(const TOperationPtr& operation);

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& operation);
    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent);

    void UnregisterOperationFromAgent(const TOperationPtr& operation);

    TControllerAgentTrackerConfigPtr GetConfig() const;

    void UpdateConfig(TSchedulerConfigPtr config);

    const NRpc::IResponseKeeperPtr& GetResponseKeeper() const;

    IInvokerPtr GetInvoker() const;

    /*!
     *  Thread affinity: any
     */
    void HandleAgentFailure(const TControllerAgentPtr& agent, const TError& error);

    using TCtxAgentHandshake = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqHandshake,
        NScheduler::NProto::TRspHandshake>;
    using TCtxAgentHandshakePtr = TIntrusivePtr<TCtxAgentHandshake>;
    TIncarnationId ProcessAgentHandshake(const TCtxAgentHandshakePtr& context);

    using TCtxAgentHeartbeat = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqHeartbeat,
        NScheduler::NProto::TRspHeartbeat>;
    using TCtxAgentHeartbeatPtr = TIntrusivePtr<TCtxAgentHeartbeat>;
    TIncarnationId ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context);

    using TCtxAgentScheduleAllocationHeartbeat = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqScheduleAllocationHeartbeat,
        NScheduler::NProto::TRspScheduleAllocationHeartbeat>;
    using TCtxAgentScheduleAllocationHeartbeatPtr = TIntrusivePtr<TCtxAgentScheduleAllocationHeartbeat>;
    TIncarnationId ProcessAgentScheduleAllocationHeartbeat(const TCtxAgentScheduleAllocationHeartbeatPtr& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
