#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ITablePuller
    : public TRefCounted
{
    virtual void Enable() = 0;
    virtual void Disable() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITablePuller)

ITablePullerPtr CreateTablePuller(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    NApi::NNative::IConnectionPtr localConnection,
    ITabletSlotPtr slot,
    ITabletSnapshotStorePtr tabletSnapshotStore,
    IInvokerPtr workerInvoker,
    NConcurrency::IThroughputThrottlerPtr nodeInThrottler,
    IMemoryUsageTrackerPtr memoryTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
