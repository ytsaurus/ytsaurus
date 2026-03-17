#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/bundle_controller/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct INodeTracker
    : public TRefCounted
{
    using TRspHeartbeat = NYT::NBundleController::NProto::TRspHeartbeat;
    using TReqHeartbeat = NYT::NBundleController::NProto::TReqHeartbeat;

    virtual void OnDynamicConfigChanged(
        const TNodeTrackerDynamicConfigPtr& oldConfig,
        const TNodeTrackerDynamicConfigPtr& newConfig) = 0;

    virtual void ProcessNodeHeartbeat(TReqHeartbeat* request, TRspHeartbeat* response) = 0;

    virtual void UpdateNodeStates(const std::map<std::string, TTabletNodeInfoPtr>& nodes) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeTracker)

////////////////////////////////////////////////////////////////////////////////

INodeTrackerPtr CreateNodeTracker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
