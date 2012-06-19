#include "stdafx.h"
#include "resource_tracker.h"
#include "profiling_manager.h"
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

    int pid = getpid();
    Stroka path = Sprintf("/proc/%d/task/", pid);
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

        Stroka threadName = cpuStatFields[1].substr(1, cpuStatFields[1].size() - 2);
        TYPath baseProfilingPath = "/resource_usage/" + EscapeYPathToken(threadName);

        i64 userTicks = FromString<i64>(cpuStatFields[13]); // utime
        i64 kernelTicks = FromString<i64>(cpuStatFields[14]); // stime

        auto it = PreviousUserTicks.find(threadName);
        if (it != PreviousUserTicks.end()) {
            i64 userCpuUsage = 100 * (userTicks - PreviousUserTicks[threadName]) / totalProcTicks;
            TQueuedSample userSample;
            userSample.Time = GetCpuInstant();
            userSample.Path = baseProfilingPath + "/user_cpu";
            userSample.Value = userCpuUsage;

            TProfilingManager::Get()->Enqueue(userSample, false);

            i64 kernelCpuUsage = 100 * (kernelTicks - PreviousKernelTicks[threadName]) / totalProcTicks;
            TQueuedSample kernelSample;
            kernelSample.Time = GetCpuInstant();
            kernelSample.Path = baseProfilingPath + "/system_cpu";
            kernelSample.Value = kernelCpuUsage;

            TProfilingManager::Get()->Enqueue(kernelSample, false);
        }
        PreviousUserTicks[threadName] = userTicks;
        PreviousKernelTicks[threadName] = kernelTicks;

        Stroka memoryStat = NFS::CombinePaths(threadStatPath, "statm");
        VectorStrok memoryStatFields;
        try {
            TIFStream memoryStatFile(memoryStat);
            memoryStatFields = splitStroku(memoryStatFile.ReadLine(), " ");
        } catch (const TIoException&) {
            // Ignore all IO exceptions.
            continue;
        }

        i64 residentSetSize = FromString<i64>(memoryStatFields[1]);
        TQueuedSample memorySample;
        memorySample.Time = GetCpuInstant();
        memorySample.Path = baseProfilingPath + "/memory";
        memorySample.Value = residentSetSize;
        TProfilingManager::Get()->Enqueue(memorySample, false);
    }

    PreviousProcTicks = procTicks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
