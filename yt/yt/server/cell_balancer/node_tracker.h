#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/bundle_controller/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct INodeTracker
    : public TRefCounted
{
    virtual void OnDynamicConfigChanged(
        const TNodeTrackerDynamicConfigPtr& oldConfig,
        const TNodeTrackerDynamicConfigPtr& newConfig) = 0;

    virtual void ProcessNodeHeartbeat(
        NBundleController::NProto::TReqHeartbeat* request,
        NBundleController::NProto::TRspHeartbeat* response) = 0;

    virtual void UpdateNodeStates(const std::map<std::string, TTabletNodeInfoPtr>& nodes) = 0;

    virtual void RequestConfigUpdate(const std::string& nodeAddress) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeTracker)

////////////////////////////////////////////////////////////////////////////////

INodeTrackerPtr CreateNodeTracker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
