#include "stdafx.h"
#include "resource_tracker.h"
#include "profiler.h"
#include "timing.h"

#include <ytlib/misc/fs.h>
#include <ytlib/ytree/ypath_client.h>

#include <util/folder/filelist.h>
#include <util/stream/file.h>
#include <util/string/vector.h>

namespace NYT {
namespace NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TDuration TResourceTracker::UpdateInterval = TDuration::Seconds(1);
static TProfiler Profiler("/resource_tracker");

////////////////////////////////////////////////////////////////////////////////

TResourceTracker::TResourceTracker(IInvokerPtr invoker)
    : PreviousProcTicks(0)
{
    PeriodicInvoker = New<TPeriodicInvoker>(
        invoker,
        BIND(&TResourceTracker::EnqueueUsage, Unretained(this)),
        UpdateInterval);
}

void TResourceTracker::Start()
{
    PeriodicInvoker->Start();
}

void TResourceTracker::EnqueueUsage()
{
    PeriodicInvoker->ScheduleNext();

    EnqueueMemoryUsage();
    EnqueueCpuUsage();
}

void TResourceTracker::EnqueueCpuUsage()
{
    // update proc ticks
    VectorStrok cpuFields;
    try {
        TIFStream procStat("/proc/stat");
        cpuFields = splitStroku(procStat.ReadLine(), " ");
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return;
    }

    i64 procTicks = FromString<i64>(cpuFields[1]);
    i64 totalProcTicks = procTicks - PreviousProcTicks;

    if (totalProcTicks == 0)
        return;

    Stroka path = Sprintf("/proc/self/task/");
    TDirsList dirsList;
    dirsList.Fill(path);
    i32 size = dirsList.Size();
    for (i32 i = 0; i < size; ++i) {
        Stroka threadStatPath = NFS::CombinePaths(path, dirsList.Next());
        Stroka cpuStatPath = NFS::CombinePaths(threadStatPath, "stat");
        VectorStrok cpuStatFields;
        try {
            TIFStream cpuStatFile(cpuStatPath);
            cpuStatFields = splitStroku(cpuStatFile.ReadLine(), " ");
        } catch (const TIoException&) {
            // Ignore all IO exceptions.
            continue;
        }

        // get rid of quotes
        Stroka threadName = cpuStatFields[1].substr(1, cpuStatFields[1].size() - 2);
        TYPath pathPrefix = "/" + EscapeYPathToken(threadName);

        i64 userTicks = FromString<i64>(cpuStatFields[13]); // utime
        i64 kernelTicks = FromString<i64>(cpuStatFields[14]); // stime

        auto it = PreviousUserTicks.find(threadName);
        if (it != PreviousUserTicks.end()) {
            i64 userCpuUsage = 100 * (userTicks - PreviousUserTicks[threadName]) / totalProcTicks;
            Profiler.Enqueue(pathPrefix + "/user_cpu", userCpuUsage);

            i64 kernelCpuUsage = 100 * (kernelTicks - PreviousKernelTicks[threadName]) / totalProcTicks;
            Profiler.Enqueue(pathPrefix + "/system_cpu", kernelCpuUsage);
        }
        PreviousUserTicks[threadName] = userTicks;
        PreviousKernelTicks[threadName] = kernelTicks;
    }

    PreviousProcTicks = procTicks;
}

void TResourceTracker::EnqueueMemoryUsage()
{
    VectorStrok memoryStatFields;
    try {
        TIFStream memoryStatFile("/proc/self/statm");
        memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
    } catch (const TIoException&) {
        // Ignore all IO exceptions.
        return;
    }

    i64 residentSetSize = FromString<i64>(memoryStatFields[1]);
    Profiler.Enqueue("/total/memory", residentSetSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
