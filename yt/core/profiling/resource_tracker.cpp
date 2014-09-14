#include "stdafx.h"
#include "resource_tracker.h"
#include "profile_manager.h"
#include "profiler.h"
#include "timing.h"

#include <core/misc/fs.h>
#include <core/misc/proc.h>
#include <core/misc/lfalloc_helpers.h>

#include <core/ypath/token.h>

#include <util/folder/filelist.h>
#include <util/stream/file.h>
#include <util/string/vector.h>

#include <core/misc/lfalloc_helpers.h>

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
            tagIds.push_back(TProfileManager::Get()->RegisterTag("thread", threadName));

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
    EnqueueLFAllocCounters();
}

void TResourceTracker::EnqueueLFAllocCounters()
{
    Profiler.Enqueue("/lf_alloc/total/user_allocated", GetLFAllocCounterFull(CT_USER_ALLOC));
    Profiler.Enqueue("/lf_alloc/total/mmapped", GetLFAllocCounterFull(CT_MMAP));
    Profiler.Enqueue("/lf_alloc/total/mmapped_count", GetLFAllocCounterFull(CT_MMAP_CNT));
    Profiler.Enqueue("/lf_alloc/total/munmapped", GetLFAllocCounterFull(CT_MUNMAP));
    Profiler.Enqueue("/lf_alloc/total/munmapped_count", GetLFAllocCounterFull(CT_MUNMAP_CNT));
    Profiler.Enqueue("/lf_alloc/total/system_allocated", GetLFAllocCounterFull(CT_SYSTEM_ALLOC));
    Profiler.Enqueue("/lf_alloc/total/system_deallocated", GetLFAllocCounterFull(CT_SYSTEM_FREE));
    Profiler.Enqueue("/lf_alloc/total/small_blocks_allocated", GetLFAllocCounterFull(CT_SMALL_ALLOC));
    Profiler.Enqueue("/lf_alloc/total/small_blocks_deallocated", GetLFAllocCounterFull(CT_SMALL_FREE));
    Profiler.Enqueue("/lf_alloc/total/large_blocks_allocated", GetLFAllocCounterFull(CT_LARGE_ALLOC));
    Profiler.Enqueue("/lf_alloc/total/large_blocks_deallocated", GetLFAllocCounterFull(CT_LARGE_FREE));

    Profiler.Enqueue("/lf_alloc/current/system", NLFAlloc::GetCurrentSystem());
    Profiler.Enqueue("/lf_alloc/current/small_blocks", NLFAlloc::GetCurrentSmallBlocks());
    Profiler.Enqueue("/lf_alloc/current/large_blocks", NLFAlloc::GetCurrentLargeBlocks());

    auto mmapped = NLFAlloc::GetCurrentMmapped();
    Profiler.Enqueue("/lf_alloc/current/mmapped", mmapped);

    auto mmappedCount = NLFAlloc::GetCurrentMmappedCount();
    Profiler.Enqueue("/lf_alloc/current/mmapped_count", mmappedCount);

    auto used = NLFAlloc::GetCurrentUsed();
    Profiler.Enqueue("/lf_alloc/current/used", used);
    Profiler.Enqueue("/lf_alloc/current/locked", mmapped - used);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
