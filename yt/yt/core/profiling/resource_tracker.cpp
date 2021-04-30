#include "resource_tracker.h"
#include "profile_manager.h"
#include "profiler.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ypath/token.h>

#include <util/folder/filelist.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#ifdef _linux_
#define RESOURCE_TRACKER_ENABLED
#endif

#ifdef RESOURCE_TRACKER_ENABLED
#include <unistd.h>
#endif

namespace NYT::NProfiling {

using namespace NYPath;
using namespace NYTree;
using namespace NProfiling;
using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(TResourceTracker)

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Profiling");
static TProfiler Profiler("/resource_tracker");

static constexpr auto procPath = "/proc/self/task";

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 GetTicksPerSecond()
{
#ifdef RESOURCE_TRACKER_ENABLED
    return sysconf(_SC_CLK_TCK);
#else
    return -1;
#endif
}

} // namespace

// Please, refer to /proc documentation to know more about available information.
// http://www.kernel.org/doc/Documentation/filesystems/proc.txt

TResourceTracker::TTimings TResourceTracker::TTimings::operator-(const TResourceTracker::TTimings& other) const
{
    return {UserJiffies - other.UserJiffies, SystemJiffies - other.SystemJiffies, CpuWaitNsec - other.CpuWaitNsec};
}

TResourceTracker::TTimings& TResourceTracker::TTimings::operator+=(const TResourceTracker::TTimings& other)
{
    UserJiffies += other.UserJiffies;
    SystemJiffies += other.SystemJiffies;
    CpuWaitNsec += other.CpuWaitNsec;
    return *this;
}

TResourceTracker::TResourceTracker()
    // CPU time is measured in jiffies; we need USER_HZ to convert them
    // to milliseconds and percentages.
    : TicksPerSecond_(GetTicksPerSecond())
    , LastUpdateTime_(TInstant::Now())
{
    Profiler.AddFuncGauge("/memory_usage/rss", MakeStrong(this), [] {
        return GetProcessMemoryUsage().Rss;
    });

    Profiler.WithSparse().AddProducer("", MakeStrong(this));
}

void TResourceTracker::CollectSensors(ISensorWriter* writer)
{
    i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - LastUpdateTime_.MicroSeconds();
    if (timeDeltaUsec <= 0) {
        return;
    }

    auto tidToInfo = ProcessThreads();
    CollectSensorsAggregatedTimings(writer, TidToInfo_, tidToInfo, timeDeltaUsec);
    CollectSensorsThreadCounts(writer, tidToInfo);
    TidToInfo_ = tidToInfo;

    LastUpdateTime_ = TInstant::Now();
}

bool TResourceTracker::ProcessThread(TString tid, TResourceTracker::TThreadInfo* info)
{
    auto threadStatPath = NFS::CombinePaths(procPath, tid);
    auto statPath = NFS::CombinePaths(threadStatPath, "stat");
    auto statusPath = NFS::CombinePaths(threadStatPath, "status");
    auto schedStatPath = NFS::CombinePaths(threadStatPath, "schedstat");

    try {
        // Parse status.
        {
            TIFStream file(statusPath);
            for (TString line; file.ReadLine(line); ) {
                auto tokens = SplitString(line, "\t");

                if (tokens.size() < 2) {
                   continue;
                }

                if (tokens[0] == "Name:") {
                    info->ThreadName = tokens[1];
                } else if (tokens[0] == "SigBlk:") {
                    // This is a heuristic way to distinguish YT thread from non-YT threads.
                    // It is used primarily for CHYT, which links against CH and Poco that
                    // have their own complicated manner of handling threads. We want to be
                    // able to visually distinguish them from our threads.
                    //
                    // Poco threads always block SIGQUIT, SIGPIPE and SIGTERM; we use the latter
                    // one presence. Feel free to change this heuristic if needed.
                    YT_VERIFY(tokens[1].size() == 16);
                    auto mask = IntFromString<ui64, 16>(tokens[1]);
                    // Note that signals are 1-based, so 14-th bit is SIGTERM (15).
                    bool sigtermBlocked = (mask >> 14) & 1ull;
                    info->IsYtThread = !sigtermBlocked;
                }
            }
        }

        // Parse schedstat.
        {
            TIFStream file(schedStatPath);
            auto tokens = SplitString(file.ReadLine(), " ");
            if (tokens.size() < 3) {
                return false;
            }

            info->Timings.CpuWaitNsec = FromString<i64>(tokens[1]);
        }

        // Parse stat.
        {
            TIFStream file(statPath);
            auto tokens = SplitString(file.ReadLine(), " ");
            if (tokens.size() < 15) {
                return false;
            }

            info->Timings.UserJiffies = FromString<i64>(tokens[13]);
            info->Timings.SystemJiffies = FromString<i64>(tokens[14]);
        }

        info->ProfilingKey = info->ThreadName;

        // Group threads by thread pool, using YT thread naming convention.
        if (auto index = info->ProfilingKey.rfind(':'); index != TString::npos) {
            bool isDigit = std::all_of(info->ProfilingKey.cbegin() + index + 1, info->ProfilingKey.cend(), [] (char c) {
                return std::isdigit(c);
            });
            if (isDigit) {
                info->ProfilingKey = info->ProfilingKey.substr(0, index);
            }
        }

        if (!info->IsYtThread) {
            info->ProfilingKey += "@";
        }
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return false;
    }

    YT_LOG_TRACE("Thread statistics (Tid: %v, ThreadName: %v, IsYtThread: %v, UserJiffies: %v, SystemJiffies: %v, CpuWaitNsec: %v)",
        tid,
        info->ThreadName,
        info->IsYtThread,
        info->Timings.UserJiffies,
        info->Timings.SystemJiffies,
        info->Timings.CpuWaitNsec);

    return true;
}

TResourceTracker::TThreadMap TResourceTracker::ProcessThreads()
{
    TDirsList dirsList;
    try {
        dirsList.Fill(procPath);
    } catch (const TSystemError&) {
        // Ignore all exceptions.
        return {};
    }

    TThreadMap tidToStats;

    for (int index = 0; index < dirsList.Size(); ++index) {
        auto tid = TString(dirsList.Next());
        TThreadInfo info;
        if (ProcessThread(tid, &info)) {
            tidToStats[tid] = info;
        } else {
            YT_LOG_TRACE("Failed to prepare thread info for thread (Tid: %v)", tid);
        }
    }

    return tidToStats;
}

void TResourceTracker::CollectSensorsAggregatedTimings(
    ISensorWriter* writer,
    const TResourceTracker::TThreadMap& oldTidToInfo,
    const TResourceTracker::TThreadMap& newTidToInfo,
    i64 timeDeltaUsec)
{
    double totalUserCpuTime = 0.0;
    double totalSystemCpuTime = 0.0;
    double totalCpuWaitTime = 0.0;

    THashMap<TString, TTimings> profilingKeyToAggregatedTimings;

    // Consider only those threads which did not change their thread names.
    // In each group of such threads with same thread name, export aggregated timings.

    for (const auto& [tid, newInfo] : newTidToInfo) {
        auto it = oldTidToInfo.find(tid);

        if (it == oldTidToInfo.end()) {
            continue;
        }

        const auto& oldInfo = it->second;

        if (oldInfo.ProfilingKey != newInfo.ProfilingKey) {
            continue;
        }

        profilingKeyToAggregatedTimings[newInfo.ProfilingKey] += newInfo.Timings - oldInfo.Timings;
    }

    for (const auto& [profilingKey, aggregatedTimings] : profilingKeyToAggregatedTimings) {
        // Multiplier 1e6 / timeDelta is for taking average over time (all values should be "per second").
        // Multiplier 100 for CPU time is for measuring CPU load in percents. It is due to historical reasons.
        double userCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.UserJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double systemCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.SystemJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double waitTime = std::max<double>(0.0, 100 * aggregatedTimings.CpuWaitNsec / 1e9 * (1e6 / timeDeltaUsec));

        totalUserCpuTime += userCpuTime;
        totalSystemCpuTime += systemCpuTime;
        totalCpuWaitTime += waitTime;

        writer->PushTag(std::pair<TString, TString>("thread", profilingKey));
        writer->AddGauge("/user_cpu", userCpuTime);
        writer->AddGauge("/system_cpu", systemCpuTime);
        writer->AddGauge("/total_cpu", userCpuTime + systemCpuTime);
        writer->AddGauge("/cpu_wait", waitTime);
        writer->PopTag();

        YT_LOG_TRACE("Thread CPU timings in percent/sec (ProfilingKey: %v, UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
            profilingKey,
            userCpuTime,
            systemCpuTime,
            waitTime);
    }

    LastUserCpu_.store(totalUserCpuTime);
    LastSystemCpu_.store(totalSystemCpuTime);
    LastCpuWait_.store(totalCpuWaitTime);

    YT_LOG_DEBUG("Total CPU timings in percent/sec (UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
        totalUserCpuTime,
        totalSystemCpuTime,
        totalCpuWaitTime);
}

void TResourceTracker::CollectSensorsThreadCounts(ISensorWriter* writer, const TThreadMap& tidToInfo) const
{
    THashMap<TString, int> profilingKeyToCount;

    for (const auto& [tid, info] : tidToInfo) {
        ++profilingKeyToCount[info.ProfilingKey];
    }

    for (const auto& [profilingKey, count] : profilingKeyToCount) {
        writer->PushTag(std::pair<TString, TString>{"thread", profilingKey});
        writer->AddGauge("/thread_count", count);
        writer->PopTag();
    }

    YT_LOG_DEBUG("Total thread count (ThreadCount: %v)",
        tidToInfo.size());
}

double TResourceTracker::GetUserCpu()
{
    return LastUserCpu_.load();
}

double TResourceTracker::GetSystemCpu()
{
    return LastSystemCpu_.load();
}

double TResourceTracker::GetCpuWait()
{
    return LastCpuWait_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
