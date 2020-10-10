#pragma once

#include "public.h"
#include "environment.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTracker
    : public TRefCounted
{
public:
    TMemoryTracker(
        TMemoryTrackerConfigPtr config,
        IUserJobEnvironmentPtr environment,
        TTmpfsManagerPtr tmpfsManager);

    void DumpMemoryUsageStatistics(
        TStatistics* statistics,
        const TString& path);

    i64 GetMemoryUsage();

private:
    const TMemoryTrackerConfigPtr Config_;
    const IUserJobEnvironmentPtr Environment_;
    const TTmpfsManagerPtr TmpfsManager_;

    std::atomic<i64> CumulativeMemoryUsageMBSec_ = 0;
    std::atomic<i64> MaxMemoryUsage_ = 0;

    TSpinLock MemoryStatisticsLock_;

    TInstant LastMemoryMeasureTime_ = TInstant::Now();

    std::optional<TMemoryStatistics> CachedMemoryStatisitcs_;

    TMemoryStatistics GetMemoryStatistics();
};

DEFINE_REFCOUNTED_TYPE(TMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
