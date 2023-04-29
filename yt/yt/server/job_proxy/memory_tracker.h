#pragma once

#include "public.h"
#include "environment.h"

#include <library/cpp/yt/threading/spin_lock.h>

#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TProcessMemoryStatistics
    : public NYTree::TYsonStruct
{
    int Pid;
    std::vector<TString> Cmdline;
    i64 Rss;
    i64 Shared;

    REGISTER_YSON_STRUCT(TProcessMemoryStatistics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessMemoryStatistics)

struct TJobMemoryStatistics
    : public TRefCounted
{
    TJobEnvironmentMemoryStatistics Total;
    std::vector<TProcessMemoryStatisticsPtr> ProcessesStatistics;
    i64 TmpfsSize = 0;
};

DEFINE_REFCOUNTED_TYPE(TJobMemoryStatistics)

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

    TJobMemoryStatisticsPtr GetMemoryStatistics();

private:
    const TMemoryTrackerConfigPtr Config_;
    const IUserJobEnvironmentPtr Environment_;
    const TTmpfsManagerPtr TmpfsManager_;

    std::atomic<i64> CumulativeMemoryUsageMBSec_ = 0;
    std::atomic<i64> MaxMemoryUsage_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MemoryStatisticsLock_);

    TInstant LastMemoryMeasureTime_ = TInstant::Now();

    TJobMemoryStatisticsPtr CachedMemoryStatistics_ = nullptr;
};

DEFINE_REFCOUNTED_TYPE(TMemoryTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
