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
    TControllerAgentPtr GetAgent();
    void OnAgentConnected();
    void OnAgentDisconnected();

    // TODO(babenko): eliminate
    IOperationControllerPtr CreateController(
        const TControllerAgentPtr& agent,
        TOperation* operation);

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
