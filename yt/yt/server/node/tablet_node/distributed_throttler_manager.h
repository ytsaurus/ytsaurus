#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_throttler/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedThrottlerManager
    : public virtual TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const NYPath::TYPath& tablePath,
        NObjectClient::TCellTag cellTag,
        const NConcurrency::TThroughputThrottlerConfigPtr& config,
        const NDistributedThrottler::TThrottlerId& throttlerId,
        ETabletDistributedThrottlerKind kind,
        TDuration rpcTimeout,
        bool admitUnlimitedThrottler,
        NProfiling::TProfiler profiler = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerManagerPtr CreateDistributedThrottlerManager(
    IBootstrap* bootstrap,
    NDiscoveryClient::TMemberId memberId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
