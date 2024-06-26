#pragma once

#include "public.h"

#include <yt/yt/ytlib/cell_master_client/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

struct IMasterHeartbeatReporterCallbacks
    : public TRefCounted
{
    virtual TFuture<void> ReportHeartbeat(NObjectClient::TCellTag cellTag) = 0;
    virtual void OnHeartbeatSucceeded(NObjectClient::TCellTag cellTag) = 0;
    virtual void OnHeartbeatFailed(NObjectClient::TCellTag cellTag) = 0;
    virtual void Reset(NObjectClient::TCellTag cellTag) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterHeartbeatReporterCallbacks)

////////////////////////////////////////////////////////////////////////////////

template <class TMasterConnector, class TNodeServiceProxy>
IMasterHeartbeatReporterCallbacksPtr CreateSingleFlavorHeartbeatCallbacks(
    TWeakPtr<TMasterConnector> owner,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode

#define MASTER_HEARTBEAT_REPORTER_CALLBACKS_H_
#include "master_heartbeat_reporter_callbacks-inl.h"
#undef MASTER_HEARTBEAT_REPORTER_CALLBACKS_H_
