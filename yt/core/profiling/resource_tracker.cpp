#include "resource_tracker.h"
#include "profile_manager.h"
#include "profiler.h"
#include "timing.h"

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

////////////////////////////////////////////////////////////////////////////////

static const TDuration UpdatePeriod = TDuration::Seconds(1);
static TProfiler Profiler("/resource_tracker");

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

TResourceTracker::TResourceTracker(IInvokerPtr invoker)
    // CPU time is measured in jiffies; we need USER_HZ to convert them
    // to milliseconds and percentages.
    : TicksPerSecond(GetTicksPerSecond())
    , LastUpdateTime(TInstant::Now())
{
    PeriodicExecutor = New<TPeriodicExecutor>(
        invoker,
        BIND(&TResourceTracker::EnqueueUsage, Unretained(this)),
        UpdatePeriod);
}

void TResourceTracker::Start()
{
    PeriodicExecutor->Start();
}

void TResourceTracker::EnqueueUsage()
{
    EnqueueMemoryUsage();
    EnqueueCpuUsage();
}

void TResourceTracker::EnqueueCpuUsage()
{
    i64 timeDelta = TInstant::Now().MilliSeconds() - LastUpdateTime.MilliSeconds();
    if (timeDelta <= 0)
        return;

    TString procPath("/proc/self/task");

    TDirsList dirsList;
    try {
        dirsList.Fill(procPath);
    } catch (const TSystemError&) {
        // Ignore all exceptions.
        return;
    }

    std::unordered_map<TString, std::tuple<i64, i64, i64>> threadStats;

    for (int index = 0; index < dirsList.Size(); ++index) {
        auto threadStatPath = NFS::CombinePaths(procPath, dirsList.Next());
        auto cpuStatPath = NFS::CombinePaths(threadStatPath, "stat");
        auto schedStatPath = NFS::CombinePaths(threadStatPath, "schedstat");

        std::vector<TString> fields, schedFields;
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
        i64 userJiffies = FromString<i64>(fields[13]); // In jiffies
        i64 systemJiffies = FromString<i64>(fields[14]); // In jiffies
        i64 cpuWait = FromString<i64>(schedFields[1]); // In nanoseconds

        auto it = threadStats.find(threadName);
        if (it == threadStats.end()) {
            threadStats.emplace(threadName, std::make_tuple(userJiffies, systemJiffies, cpuWait));
        } else {
            std::get<0>(it->second) += userJiffies;
            std::get<1>(it->second) += systemJiffies;
            std::get<2>(it->second) += cpuWait;
        }
    }

    for (const auto& stat : threadStats)
    {
        const auto& threadName = stat.first;
        auto [userJiffies, systemJiffies, cpuWait] = stat.second;
 
        auto it = ThreadNameToJiffies.find(threadName);
        if (it != ThreadNameToJiffies.end()) {
            auto& jiffies = it->second;
            i64 userCpuTime = std::max<i64>((userJiffies - jiffies.PreviousUser) * 1000 / TicksPerSecond, 0);
            i64 systemCpuTime = std::max<i64>((systemJiffies - jiffies.PreviousSystem) * 1000 / TicksPerSecond, 0);
            double waitTime = std::max<double>((cpuWait - jiffies.PreviousWait) / 1'000'000'000., 0.);

            TTagIdList tagIds;
            tagIds.push_back(TProfileManager::Get()->RegisterTag("thread", threadName));

            Profiler.Enqueue("/user_cpu", 100 * userCpuTime / timeDelta, EMetricType::Gauge, tagIds);
            Profiler.Enqueue("/system_cpu", 100 * systemCpuTime / timeDelta, EMetricType::Gauge, tagIds);
            Profiler.Enqueue("/cpu_wait", 100 * waitTime * 1000 / timeDelta, EMetricType::Gauge, tagIds);
        }

        {
            auto& jiffies = ThreadNameToJiffies[threadName];
            jiffies.PreviousUser = userJiffies;
            jiffies.PreviousSystem = systemJiffies;
            jiffies.PreviousWait = cpuWait;
        }
    }

    LastUpdateTime = TInstant::Now();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
