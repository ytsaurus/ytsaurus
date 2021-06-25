#include "memory_tracker.h"
#include "tmpfs_manager.h"

#ifdef _linux_
#include <yt/yt/server/lib/containers/instance.h>
#endif

#include <yt/yt/ytlib/tools/proc.h>
#include <yt/yt/ytlib/tools/tools.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/statistics.h>

#include <util/stream/file.h>

namespace NYT::NJobProxy {

const static NLogging::TLogger Logger("MemoryTracker");

using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

TMemoryTracker::TMemoryTracker(
    TMemoryTrackerConfigPtr config,
    IUserJobEnvironmentPtr environment,
    TTmpfsManagerPtr tmpfsManager)
    : Config_(std::move(config))
    , Environment_(std::move(environment))
    , TmpfsManager_(std::move(tmpfsManager))
{
    YT_VERIFY(Environment_);
}

void TMemoryTracker::DumpMemoryUsageStatistics(TStatistics* statistics, const TString& path)
{
    statistics->AddSample(Format("%v/current_memory", path), GetMemoryStatistics());
    statistics->AddSample(Format("%v/max_memory", path), MaxMemoryUsage_);
    statistics->AddSample(Format("%v/cumulative_memory_mb_sec", path), CumulativeMemoryUsageMBSec_);
}

i64 TMemoryTracker::GetMemoryUsage()
{
    auto memoryStatistics = GetMemoryStatistics();

    i64 memoryUsage = 0;
    memoryUsage += memoryStatistics.Rss;
    if (Config_->IncludeMemoryMappedFiles) {
        memoryUsage += memoryStatistics.MappedFile;
    }
    memoryUsage += TmpfsManager_->GetTmpfsSize();
    return memoryUsage;
}

TMemoryStatistics TMemoryTracker::GetMemoryStatistics()
{
    auto guard = Guard(MemoryStatisticsLock_);

    auto now = TInstant::Now();

    if (LastMemoryMeasureTime_ + Config_->MemoryStatisticsCachePeriod >= now &&
        CachedMemoryStatisitcs_)
    {
        return *CachedMemoryStatisitcs_;
    }

#ifdef _linux_
    TMemoryStatistics memoryStatistics;
    if (auto statistics = Environment_->GetMemoryStatistics()) {
        memoryStatistics = *statistics;
    } else {
        std::vector<int> pids;

        try {
            pids = Environment_->GetJobPids();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get list of user job processes");
            return {};
        }

        if (Config_->UseSMapsMemoryTracker && TmpfsManager_->HasTmpfsVolumes()) {
            TMemoryMappingStatistics memoryMappingStatistics;
            i64 skippedBecauseOfTmpfs = 0;
            for (auto pid : pids) {
                TString smaps;
                try {
                    smaps = RunTool<TReadProcessSmapsTool>(pid);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to read process smaps (Pid: %v)", pid);
                    continue;
                }

                for (const auto& segment : ParseMemoryMappings(smaps)) {
                    YT_LOG_DEBUG("Memory segment parsed (Pid: %v, DeviceId: %v, "
                        "PrivateClean: %v, PrivateDirty: %v, SharedClean: %v, SharedDirty: %v)",
                        pid,
                        segment.DeviceId,
                        segment.Statistics.PrivateClean,
                        segment.Statistics.PrivateDirty,
                        segment.Statistics.SharedClean,
                        segment.Statistics.SharedDirty);

                    if (segment.DeviceId && TmpfsManager_->IsTmpfsDevice(*segment.DeviceId)) {
                        skippedBecauseOfTmpfs += segment.Statistics.SharedClean + segment.Statistics.SharedDirty;
                        continue;
                    }
                    memoryMappingStatistics += segment.Statistics;
                }
            }

            memoryStatistics.Rss = memoryMappingStatistics.PrivateClean + memoryMappingStatistics.PrivateDirty;
            memoryStatistics.MappedFile = memoryMappingStatistics.SharedClean + memoryMappingStatistics.SharedDirty;

            YT_LOG_DEBUG("Memory statisitcs collected (Rss: %v, Shared: %v, SkippedBecauseOfTmpfs: %v)",
                memoryStatistics.Rss,
                memoryStatistics.MappedFile,
                skippedBecauseOfTmpfs);
        } else {
            for (auto pid : pids) {
                try {
                    auto memoryUsage = GetProcessMemoryUsage(pid);

                    // RSS from /proc/pid/statm includes all pages resident to current process,
                    // including memory-mapped files and shared memory.
                    // Since we want to account shared memory separately, let's subtract it here.
                    memoryStatistics.Rss += memoryUsage.Rss - memoryUsage.Shared;
                    memoryStatistics.MappedFile += memoryUsage.Shared;

                    try {
                        memoryStatistics.MajorPageFaults = GetProcessCumulativeMajorPageFaults(pid);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Failed to get major page fault count");
                    }

                    YT_LOG_DEBUG("Memory statistics collected (Pid: %v, ProcessName: %v, Rss: %v, Shared: %v)",
                        pid,
                        GetProcessName(pid),
                        memoryStatistics.Rss,
                        memoryStatistics.MappedFile);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to collect memory statistics (Pid: %v)", pid);
                }
            }
        }

        YT_LOG_DEBUG("Current memory usage (Private: %v, Shared: %v)",
            memoryStatistics.Rss,
            memoryStatistics.MappedFile);
    }

    auto memoryUsage = memoryStatistics.Rss + memoryStatistics.MappedFile;
    MaxMemoryUsage_ = std::max<i64>(MaxMemoryUsage_, memoryUsage);

    if (now > LastMemoryMeasureTime_) {
        CumulativeMemoryUsageMBSec_ += memoryUsage * (now - LastMemoryMeasureTime_).SecondsFloat() / 1_MB;
    }
    LastMemoryMeasureTime_ = now;
    CachedMemoryStatisitcs_ = memoryStatistics;

    return memoryStatistics;

#else
    return {};
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
