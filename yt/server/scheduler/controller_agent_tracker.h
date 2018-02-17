#pragma once

#include "public.h"

#include <yt/server/cell_scheduler/bootstrap.h>

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
        NCellScheduler::TBootstrap* bootstrap);
    ~TControllerAgentTracker();

    // TODO(babenko): multiagent support
    void OnAgentConnected();
    void OnAgentDisconnected();

    std::vector<TControllerAgentPtr> GetAgents();

    // TODO(babenko): eliminate
    IOperationControllerPtr CreateController(
        TControllerAgent* agent,
        TOperation* operation);

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& operation);
    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent);

    TFuture<void> RegisterOperationAtAgent(const TOperationPtr& operation);
    void UnregisterOperationFromAgent(const TOperationPtr& operation);

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
