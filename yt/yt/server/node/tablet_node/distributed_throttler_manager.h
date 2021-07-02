#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedThrottlerManager
    : public virtual TRefCounted
{
    virtual NConcurrency::IThroughputThrottlerPtr GetOrCreateThrottler(
        const NYPath::TYPath& tablePath,
        NObjectClient::TCellTag cellTag,
        const NConcurrency::TThroughputThrottlerConfigPtr& config,
        const TString& throttlerId,
        NDistributedThrottler::EDistributedThrottlerMode mode,
        TDuration rpcTimeout,
        bool admitUnlimitedThrottler) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerManagerPtr CreateDistributedThrottlerManager(
    IBootstrap* bootstrap,
    TCellId cellId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
