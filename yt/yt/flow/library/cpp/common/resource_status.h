#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/ema.h>

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Queue size and throughput statistics of one resource on this unit, fed by
//! #IResourceManager::FeedStatus and reported with the worker status.
class TResourceStatus
{
public:
    void Update(i64 morePushedToQueue, i64 moreFetchedFromQueue, TInstant now = TInstant::Now());

    TWorkerResourceStatusPtr Collect(TInstant now = TInstant::Now());

private:
    static constexpr int TimeWindowsCount = 2;
    static constexpr std::array<TDuration, TimeWindowsCount> TimeWindowDurations = {TDuration::Seconds(30), TDuration::Minutes(10)};
    TInstant LastUpdateTime_;
    i64 QueuePushedTotal_ = 0;
    i64 QueueFetchedTotal_ = 0;
    TEmaCounter<double, TimeWindowsCount> QueuePushCount_{{TimeWindowDurations.begin(), TimeWindowDurations.end()}};
    TEmaCounter<double, TimeWindowsCount> QueueFetchCount_{{TimeWindowDurations.begin(), TimeWindowDurations.end()}};
    TMultiWindowEma<double, TimeWindowsCount, true> QueueSize_{{TimeWindowDurations}};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
