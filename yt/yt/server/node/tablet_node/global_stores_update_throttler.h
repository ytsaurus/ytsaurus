#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Not thread-safe.
class TGlobalStoresUpdateThrottler final
{
public:
    TGlobalStoresUpdateThrottler(
        TGlobalStoresUpdateThrottlerConfigPtr config,
        NApi::NNative::IConnectionPtr connection,
        const NProfiling::TProfiler& profiler);

    void AddRequest(
        const std::string& bundleName,
        int storeCount,
        NObjectClient::TCellTag cellTag);

    std::vector<bool> Throttle(NTabletClient::ETabletStoresUpdateReason updateReason);

    void Reconfigure(TGlobalStoresUpdateThrottlerConfigPtr newConfig);

    bool IsEnabled() const;

private:
    TAtomicIntrusivePtr<TGlobalStoresUpdateThrottlerConfig> Config_;
    const NApi::NNative::IConnectionPtr Connection_;
    const NProfiling::TProfiler Profiler_;
    THashMap<NObjectClient::TCellTag, NProfiling::TCounter> ThrottledRequestCounters_;
    NProfiling::TCounter FailedThrottleRequestCounter_;

    //! NB: Refers to a master cell, not to a tablet cell.
    struct TCellStatus
    {
        TFuture<int> ScheduledRequestFuture;
        std::vector<int> RequestedStoreCounts;
        std::vector<int> RequestIndexes;
        std::string BundleName;
    };

    THashMap<NObjectClient::TCellTag, TCellStatus> CellStatuses_;
    std::vector<bool> Responses_;

    // COMPAT(alexelexa): drop when all masters are 26.2.
    TInstant LastNoSuchMethodError_;

    TFuture<void> InvokeMasterRequest(
        NObjectClient::TCellTag cellTag,
        TCellStatus* cellStatus,
        NTabletClient::ETabletStoresUpdateReason updateReason);

    void DoThrottle(NTabletClient::ETabletStoresUpdateReason updateReason);

    NProfiling::TCounter& GetOrCreateThrottledCounter(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TGlobalStoresUpdateThrottler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
