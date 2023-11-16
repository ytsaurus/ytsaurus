#include "memory_tracker.h"
#include "tmpfs_manager.h"

#ifdef _linux_
#include <yt/yt/library/containers/instance.h>
#endif

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <library/cpp/yt/threading/traceless_guard.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/statistics.h>

#include <util/stream/file.h>

namespace NYT::NJobProxy {

const static NLogging::TLogger Logger("MemoryTracker");

using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

void TProcessMemoryStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("pid", &TThis::Pid)
        .Default(-1);
    registrar.Parameter("cmdline", &TThis::Cmdline)
        .Default({});
    registrar.Parameter("rss", &TThis::Rss)
        .Default(0);
    registrar.Parameter("shared", &TThis::Shared)
        .Default(0);
}

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
    statistics->AddSample(Format("%v/current_memory", path), GetMemoryStatistics()->Total);
    statistics->AddSample(Format("%v/max_memory", path), MaxMemoryUsage_);
    statistics->AddSample(Format("%v/cumulative_memory_mb_sec", path), CumulativeMemoryUsageMBSec_);
}

i64 TMemoryTracker::GetMemoryUsage()
{
    auto memoryStatistics = GetMemoryStatistics();

    i64 memoryUsage = 0;
    memoryUsage += memoryStatistics->Total.Rss;
    if (Config_->IncludeMemoryMappedFiles) {
        memoryUsage += memoryStatistics->Total.MappedFile;
    }
    memoryUsage += TmpfsManager_->GetTmpfsSize();
    return memoryUsage;
}

TJobMemoryStatisticsPtr TMemoryTracker::GetMemoryStatistics()
{
    auto guard = NThreading::TracelessGuard(MemoryStatisticsLock_);

    auto now = TInstant::Now();

    if (LastMemoryMeasureTime_ + Config_->MemoryStatisticsCachePeriod >= now &&
        CachedMemoryStatistics_)
    {
        return CachedMemoryStatistics_;
    }

#ifdef _linux_
    auto jobMemoryStatistics = New<TJobMemoryStatistics>();
    if (auto environmentMemoryStatistics = Environment_->GetMemoryStatistics();
        environmentMemoryStatistics.IsOK() && environmentMemoryStatistics.Value())
    {
        jobMemoryStatistics->Total = *environmentMemoryStatistics.Value();
    } else {
        std::vector<int> pids;

        try {
            pids = Environment_->GetJobPids();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get list of user job processes");
            return New<TJobMemoryStatistics>();
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

            jobMemoryStatistics->Total.Rss = memoryMappingStatistics.PrivateClean + memoryMappingStatistics.PrivateDirty;
            jobMemoryStatistics->Total.MappedFile = memoryMappingStatistics.SharedClean + memoryMappingStatistics.SharedDirty;

            YT_LOG_DEBUG("Job memory statistics updated (Rss: %v, Shared: %v, SkippedBecauseOfTmpfs: %v)",
                jobMemoryStatistics->Total.Rss,
                jobMemoryStatistics->Total.MappedFile,
                skippedBecauseOfTmpfs);
        } else {
            for (auto pid : pids) {
                try {
                    auto memoryUsage = GetProcessMemoryUsage(pid);
                    auto processName = GetProcessName(pid);
                    auto commandLine = GetProcessCommandLine(pid);

                    if (!commandLine.empty() && commandLine[0].EndsWith("/portod")) {
                        YT_LOG_DEBUG("Memory tracker found portod, ignoring (Pid: %v, CommandLine: %v, Rss: %v, Shared: %v)",
                            pid,
                            commandLine,
                            memoryUsage.Rss,
                            memoryUsage.Shared);
                        continue;
                    }

                    i64 majorPageFaults = 0;
                    try {
                        majorPageFaults = GetProcessCumulativeMajorPageFaults(pid);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Failed to get process major page fault count (Pid: %v)",
                            pid);
                    }

                    auto processMemoryStatistics = New<TProcessMemoryStatistics>();
                    processMemoryStatistics->Pid = pid;
                    processMemoryStatistics->Cmdline = commandLine;
                    processMemoryStatistics->Rss = memoryUsage.Rss;
                    processMemoryStatistics->Shared = memoryUsage.Shared;
                    jobMemoryStatistics->ProcessesStatistics.push_back(processMemoryStatistics);

                    YT_LOG_DEBUG(
                        "Process memory statistics collected (Pid: %v, ProcessName: %v, CommandLine: %v, Rss: %v, Shared: %v, MajorPageFaults: %v)",
                        pid,
                        processName,
                        commandLine,
                        memoryUsage.Rss,
                        memoryUsage.Shared,
                        majorPageFaults);

                    // RSS from /proc/pid/statm includes all pages resident to current process,
                    // including memory-mapped files and shared memory.
                    // Since we want to account shared memory separately, let's subtract it here.
                    jobMemoryStatistics->Total.Rss += (memoryUsage.Rss - memoryUsage.Shared);
                    jobMemoryStatistics->Total.MappedFile += memoryUsage.Shared;

                    jobMemoryStatistics->Total.MajorPageFaults += majorPageFaults;
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Failed to collect process memory statistics (Pid: %v)",
                        pid);
                }
            }
        }

        YT_LOG_DEBUG("Job memory statistics updated (Private: %v, Shared: %v)",
            jobMemoryStatistics->Total.Rss,
            jobMemoryStatistics->Total.MappedFile);
    }

    auto memoryUsage = jobMemoryStatistics->Total.Rss + jobMemoryStatistics->Total.MappedFile;
    MaxMemoryUsage_ = std::max<i64>(MaxMemoryUsage_, memoryUsage);

    jobMemoryStatistics->TmpfsSize = TmpfsManager_->GetTmpfsSize();

    if (now > LastMemoryMeasureTime_) {
        CumulativeMemoryUsageMBSec_ += memoryUsage * (now - LastMemoryMeasureTime_).SecondsFloat() / 1_MB;
    }
    LastMemoryMeasureTime_ = now;
    CachedMemoryStatistics_ = jobMemoryStatistics;

    return jobMemoryStatistics;

#else
    return New<TJobMemoryStatistics>();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
