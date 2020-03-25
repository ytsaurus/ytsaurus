#include "resource_tracker.h"
#include "profile_manager.h"
#include "profiler.h"
#include "timing.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/ypath/token.h>

#include <util/folder/filelist.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

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

static const TDuration UpdatePeriod = TDuration::Seconds(1);
static TProfiler Profiler("/resource_tracker");
static NLogging::TLogger Logger("Profiling");

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

TResourceTracker::TTimings TResourceTracker::TTimings::operator+(const TResourceTracker::TTimings& other) const
{
    return {UserJiffies + other.UserJiffies, SystemJiffies + other.SystemJiffies, CpuWaitNsec + other.CpuWaitNsec};
}

TResourceTracker::TTimings TResourceTracker::TTimings::operator-(const TResourceTracker::TTimings& other) const
{
    return {UserJiffies - other.UserJiffies, SystemJiffies - other.SystemJiffies, CpuWaitNsec - other.CpuWaitNsec};
}

TResourceTracker::TResourceTracker(IInvokerPtr invoker)
    // CPU time is measured in jiffies; we need USER_HZ to convert them
    // to milliseconds and percentages.
    : TicksPerSecond_(GetTicksPerSecond())
    , LastUpdateTime_(TInstant::Now())
{
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        invoker,
        BIND(&TResourceTracker::EnqueueUsage, Unretained(this)),
        UpdatePeriod);
}

void TResourceTracker::Start()
{
    PeriodicExecutor_->Start();
}

void TResourceTracker::EnqueueUsage()
{
    YT_LOG_DEBUG("Resource tracker enqueue started");

    EnqueueMemoryUsage();
    EnqueueCpuUsage();

    YT_LOG_DEBUG("Resource tracker enqueue finished ");
}

TResourceTracker::TThreadMap TResourceTracker::ReadThreadStats()
{
    static constexpr auto procPath = "/proc/self/task";

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
        auto threadStatPath = NFS::CombinePaths(procPath, tid);
        auto cpuStatPath = NFS::CombinePaths(threadStatPath, "stat");
        auto schedStatPath = NFS::CombinePaths(threadStatPath, "schedstat");

        std::vector<TString> fields;
        std::vector<TString> schedFields;
        try {
            TIFStream cpuStatFile(cpuStatPath);
            fields = SplitString(cpuStatFile.ReadLine(), " ");

            TIFStream schedStatFile(schedStatPath);
            schedFields = SplitString(schedStatFile.ReadLine(), " ");
            if (schedFields.size() < 3) {
                continue;
            }
        } catch (const TIoException&) {
            // Ignore all IO exceptions.
            continue;
        }

        // Get rid of parentheses in process title.
        YT_VERIFY(fields[1].size() >= 2);

        auto threadName = fields[1].substr(1, fields[1].size() - 2);
        i64 userJiffies = FromString<i64>(fields[13]); // In jiffies.
        i64 systemJiffies = FromString<i64>(fields[14]); // In jiffies.
        i64 cpuWaitNsec = FromString<i64>(schedFields[1]); // In nanoseconds.

        YT_LOG_TRACE("Thread statistics (Tid: %v, ThreadName: %v, UserJiffies: %v, SystemJiffies: %v, CpuWaitNsec: %v)",
            tid,
            threadName,
            userJiffies,
            systemJiffies,
            cpuWaitNsec);

        tidToStats[tid] = TThreadStats{threadName, TTimings{userJiffies, systemJiffies, cpuWaitNsec}};
    }

    return tidToStats;
}

void TResourceTracker::EnqueueAggregatedTimings(
    const TResourceTracker::TThreadMap& oldTidToStats,
    const TResourceTracker::TThreadMap& newTidToStats,
    i64 timeDeltaUsec)
{
    double totalUserCpuTime = 0.0;
    double totalSystemCpuTime = 0.0;
    double totalCpuWaitTime = 0.0;

    THashMap<TString, TTimings> threadNameToAggregatedTimings;

    // Consider only those threads which did not change their thread names.
    // In each group of such threads with same thread name, export aggregated timings.

    for (const auto& [tid, newStats] : newTidToStats) {
        auto it = oldTidToStats.find(tid);

        if (it == oldTidToStats.end()) {
            continue;
        }

        const auto& oldStats = it->second;

        if (oldStats.ThreadName != newStats.ThreadName) {
            continue;
        }

        threadNameToAggregatedTimings[newStats.ThreadName] = threadNameToAggregatedTimings[newStats.ThreadName] + (newStats.Timings - oldStats.Timings);
    }

    for (const auto& [threadName, aggregatedTimings] : threadNameToAggregatedTimings) {
        // Multiplier 1e6 / timeDelta is for taking average over time (all values should be "per second").
        // Multiplier 100 for cpu time is for measuring cpu load in percents. It is due to historical reasons.
        double userCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.UserJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double systemCpuTime = std::max<double>(0.0, 100. * aggregatedTimings.SystemJiffies / TicksPerSecond_ * (1e6 / timeDeltaUsec));
        double waitTime = std::max<double>(0.0, 100 * aggregatedTimings.CpuWaitNsec / 1e9 * (1e6 / timeDeltaUsec));

        totalUserCpuTime += userCpuTime;
        totalSystemCpuTime += systemCpuTime;
        totalCpuWaitTime += waitTime;

        TTagIdList tagIds;
        tagIds.push_back(TProfileManager::Get()->RegisterTag("thread", threadName));

        Profiler.Enqueue("/user_cpu", userCpuTime, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/system_cpu", systemCpuTime, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/cpu_wait", waitTime, EMetricType::Gauge, tagIds);

        YT_LOG_TRACE("Thread cpu timings in percent/sec (ThreadName: %v, UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
            threadName,
            userCpuTime,
            systemCpuTime,
            waitTime);
    }

    LastUserCpu_.store(totalUserCpuTime);
    LastSystemCpu_.store(totalSystemCpuTime);
    LastCpuWait_.store(totalCpuWaitTime);

    YT_LOG_DEBUG("Cpu timings in percent/sec (UserCpu: %v, SystemCpu: %v, CpuWait: %v)",
        totalUserCpuTime,
        totalSystemCpuTime,
        totalCpuWaitTime);
}

void TResourceTracker::EnqueueCpuUsage()
{
    i64 timeDeltaUsec = TInstant::Now().MicroSeconds() - LastUpdateTime_.MicroSeconds();
    if (timeDeltaUsec <= 0) {
        return;
    }

    auto tidToStats = ReadThreadStats();
    EnqueueAggregatedTimings(TidToStats_, tidToStats, timeDeltaUsec);
    TidToStats_ = tidToStats;

    LastUpdateTime_ = TInstant::Now();
}

void TResourceTracker::EnqueueMemoryUsage()
{
    try {
        Profiler.Enqueue("/total/memory", GetProcessMemoryUsage().Rss, EMetricType::Gauge);
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return;
    }
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
