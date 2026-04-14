#pragma once

#include "public.h"

#include <yt/yt/ytlib/bundle_controller/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerConnector
    : public TRefCounted
{
public:
    explicit TBundleControllerConnector(IBootstrap* bootstrap);

    void Initialize();

    void OnDynamicConfigChanged(
        const TBundleControllerConnectorDynamicConfigPtr& oldConfig,
        const TBundleControllerConnectorDynamicConfigPtr& newConfig);

    using TRspClientHeartbeat = NRpc::TTypedClientResponse<
        NBundleController::NProto::TRspHeartbeat>;
    using TReqClientHeartbeat = NRpc::TTypedClientRequest<
        NBundleController::NProto::TReqHeartbeat, TRspClientHeartbeat>;
    using TRspClientHeartbeatPtr = TIntrusivePtr<TRspClientHeartbeat>;
    using TReqClientHeartbeatPtr = TIntrusivePtr<TReqClientHeartbeat>;

private:
    IBootstrap* const Bootstrap_;

    TBundleControllerConnectorDynamicConfigPtr DynamicConfig_;

    const NConcurrency::TRetryingPeriodicExecutorPtr HeartbeatExecutor_;

    TError SendHeartbeat();

    void PrepareHeartbeatRequest(const TReqClientHeartbeatPtr& request);

    void ProcessHeartbeatResponse(const TRspClientHeartbeatPtr& response);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
