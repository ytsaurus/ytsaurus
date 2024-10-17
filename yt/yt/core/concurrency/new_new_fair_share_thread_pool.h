#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TNewNewTwoLevelFairShareThreadPoolOptions
{
    IPoolWeightProviderPtr PoolWeightProvider = nullptr;
    bool VerboseLogging = false;
    TDuration PollingPeriod = TDuration::MilliSeconds(10);
    TDuration PoolRetentionTime = TDuration::Seconds(30);
};

INewNewTwoLevelFairShareThreadPoolPtr CreateNewNewTwoLevelFairShareThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TNewNewTwoLevelFairShareThreadPoolOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
