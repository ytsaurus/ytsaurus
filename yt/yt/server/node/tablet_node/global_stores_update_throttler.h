#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TGlobalStoresUpdateThrottler final
{
public:
    struct TRequest
    {
        std::string BundleName;
        NObjectClient::TCellTag CellTag;
        int StoreCount;
    };

    TGlobalStoresUpdateThrottler(
        TGlobalStoresUpdateThrottlerConfigPtr config,
        NApi::NNative::IConnectionPtr connection,
        const NProfiling::TProfiler& profiler);

    std::vector<bool> Throttle(
        const std::vector<TRequest>& requests,
        NTabletClient::ETabletStoresUpdateReason updateReason);

    void Reconfigure(TGlobalStoresUpdateThrottlerConfigPtr newConfig);

    bool IsEnabled() const;

private:
    TAtomicIntrusivePtr<TGlobalStoresUpdateThrottlerConfig> Config_;
    const NApi::NNative::IConnectionPtr Connection_;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CounterLock_);
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

    // COMPAT(alexelexa): drop when all masters are 26.2.
    std::atomic<TInstant> LastNoSuchMethodError_ = TInstant::Zero();

    TFuture<void> InvokeMasterRequest(
        TCellStatus* cellStatus,
        NObjectClient::TCellTag cellTag,
        NTabletClient::ETabletStoresUpdateReason updateReason);

    NProfiling::TCounter& GetOrCreateThrottledCounter(NObjectClient::TCellTag cellTag);
};

DEFINE_REFCOUNTED_TYPE(TGlobalStoresUpdateThrottler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
