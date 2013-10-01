#include "stdafx.h"
#include "resource_tracker.h"
#include "profiler.h"
#include "timing.h"

#include <core/misc/fs.h>
#include <core/misc/proc.h>

#include <core/ypath/token.h>

#include <util/folder/filelist.h>
#include <util/stream/file.h>
#include <util/string/vector.h>

#include <core/profiling/profiling_manager.h>

#include <util/private/lfalloc/helpers.h>

#ifdef RESOURCE_TRACKER_ENABLED
    #include <unistd.h>
#endif

namespace NYT {
namespace NProfiling {

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
    if (timeDelta == 0)
        return;

    Stroka procPath("/proc/self/task");
    
    TDirsList dirsList;
    try {
        dirsList.Fill(procPath);
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return;
    }

    for (int index = 0; index < dirsList.Size(); ++index) {
        auto threadStatPath = NFS::CombinePaths(procPath, dirsList.Next());
        auto cpuStatPath = NFS::CombinePaths(threadStatPath, "stat");

        VectorStrok fields;
        try {
            TIFStream cpuStatFile(cpuStatPath);
            fields = splitStroku(cpuStatFile.ReadLine(), " ");
        } catch (const TIoException&) {
            // Ignore all IO exceptions.
            continue;
        }

        // Get rid of parentheses in process title.
        YCHECK(fields[1].size() >= 2);

        auto threadName = fields[1].substr(1, fields[1].size() - 2);
        i64 userJiffies = FromString<i64>(fields[13]); // In jiffies
        i64 systemJiffies = FromString<i64>(fields[14]); // In jiffies

        auto it = ThreadNameToJiffies.find(threadName);
        if (it != ThreadNameToJiffies.end()) {
            auto& jiffies = it->second;
            i64 userCpuTime = (userJiffies - jiffies.PreviousUser) * 1000 / TicksPerSecond;
            i64 systemCpuTime = (systemJiffies - jiffies.PreviousSystem) * 1000 / TicksPerSecond;

            TTagIdList tagIds;
            tagIds.push_back(TProfilingManager::Get()->RegisterTag("thread", threadName));

            Profiler.Enqueue("/user_cpu", 100 * userCpuTime / timeDelta, tagIds);
            Profiler.Enqueue("/system_cpu", 100 * systemCpuTime / timeDelta, tagIds);
        }

        {
            auto& jiffies = ThreadNameToJiffies[threadName];
            jiffies.PreviousUser = userJiffies;
            jiffies.PreviousSystem = systemJiffies;
        }
    }

    LastUpdateTime = TInstant::Now();
}

void TResourceTracker::EnqueueMemoryUsage()
{
    try {
        Profiler.Enqueue("/total/memory", GetProcessRss());
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return;
    }
    EnqueueLfAllocCounters();
}

void TResourceTracker::EnqueueLfAllocCounters()
{
    i64 userAllocated = GetLFAllocCounterFull(CT_USER_ALLOC);
    i64 mmaped = GetLFAllocCounterFull(CT_MMAP);
    i64 munmaped = GetLFAllocCounterFull(CT_MUNMAP);
    // Allocated for lf_allow own's needs.
    i64 systemAllocated = GetLFAllocCounterFull(CT_SYSTEM_ALLOC);
    i64 systemDeallocated = GetLFAllocCounterFull(CT_SYSTEM_FREE);
    i64 smallBlocksAllocated = GetLFAllocCounterFull(CT_SMALL_ALLOC);
    i64 smallBlocksDeallocated = GetLFAllocCounterFull(CT_SMALL_FREE);
    i64 largeBlocksAllocated = GetLFAllocCounterFull(CT_LARGE_ALLOC);
    i64 largeBlocksDeallocated = GetLFAllocCounterFull(CT_LARGE_FREE);

    Profiler.Enqueue("/lf_alloc/total/user_allocated", userAllocated);
    Profiler.Enqueue("/lf_alloc/total/mmaped", mmaped);
    Profiler.Enqueue("/lf_alloc/total/munmaped", munmaped);
    Profiler.Enqueue("/lf_alloc/total/system_allocated", systemAllocated);
    Profiler.Enqueue("/lf_alloc/total/system_deallocated", systemDeallocated);
    Profiler.Enqueue("/lf_alloc/total/small_blocks_allocated", smallBlocksAllocated);
    Profiler.Enqueue("/lf_alloc/total/small_blocks_deallocated", smallBlocksDeallocated);
    Profiler.Enqueue("/lf_alloc/total/large_blocks_allocated", largeBlocksAllocated);
    Profiler.Enqueue("/lf_alloc/total/large_blocks_deallocated", largeBlocksDeallocated);

    i64 currentMmaped = mmaped - munmaped;
    Profiler.Enqueue("/lf_alloc/current/mmaped", currentMmaped);
    i64 currentSystem = systemAllocated - systemDeallocated;
    Profiler.Enqueue("/lf_alloc/current/system", currentSystem);
    i64 currentSmallBlocks = smallBlocksAllocated - smallBlocksDeallocated;
    Profiler.Enqueue("/lf_alloc/current/small_blocks", currentSmallBlocks);
    i64 currentLargeBlocks = largeBlocksAllocated - largeBlocksDeallocated;
    Profiler.Enqueue("/lf_alloc/current/large_blocks", currentLargeBlocks);

    i64 currentUsed = currentSystem + currentLargeBlocks + currentSmallBlocks;
    Profiler.Enqueue("/lf_alloc/current/used", currentUsed);
    Profiler.Enqueue("/lf_alloc/current/locked", currentMmaped - currentUsed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
