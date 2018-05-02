#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NScheduler {

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

    std::vector<TControllerAgentPtr> GetAgents();

    IOperationControllerPtr CreateController(const TOperationPtr& operation);

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& operation, int minAgentCount = 1);
    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent);

    TFuture<void> RegisterOperationAtAgent(const TOperationPtr& operation);
    void UnregisterOperationFromAgent(const TOperationPtr& operation);

    /*!
     *  Thread affinity: any
     */
    void HandleAgentFailure(const TControllerAgentPtr& agent, const TError& error);

    using TCtxAgentHandshake = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqHandshake,
        NScheduler::NProto::TRspHandshake>;
    using TCtxAgentHandshakePtr = TIntrusivePtr<TCtxAgentHandshake>;
    void ProcessAgentHandshake(const TCtxAgentHandshakePtr& context);

    using TCtxAgentHeartbeat = NRpc::TTypedServiceContext<
        NScheduler::NProto::TReqHeartbeat,
        NScheduler::NProto::TRspHeartbeat>;
    using TCtxAgentHeartbeatPtr = TIntrusivePtr<TCtxAgentHeartbeat>;
    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
