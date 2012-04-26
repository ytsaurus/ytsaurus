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

TResourceTracker::TResourceTracker(IInvoker::TPtr invoker)
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
    TIFStream procStat("/proc/stat");
    auto cpuFields = splitStroku(procStat.ReadLine(), " ");
    i64 procTicks = FromString<i64>(cpuFields[1]);
    i64 totalProcTicks = procTicks - PreviousProcTicks;

    int pid = getpid();
    Stroka path = Sprintf("/proc/%d/task/", pid);
    TDirsList dirsList;
    dirsList.Fill(path);
    i32 size = dirsList.Size();
    for (i32 i = 0; i < size; ++i) {
        Stroka pathToThreadStat = NFS::CombinePaths(path, dirsList.Next());

        Stroka cpuStat = NFS::CombinePaths(pathToThreadStat, "stat");
        TIFStream cpuStatFile(cpuStat);
        auto fields = splitStroku(cpuStatFile.ReadLine(), " ");

        Stroka threadName = fields[1].substr(1, fields[1].size() - 2);
        Stroka baseProfilingPath = "/resource_usage/" + EscapeYPathToken(threadName);

        i64 userTicks = FromString<i64>(fields[13]); // utime
        i64 kernelTicks = FromString<i64>(fields[14]); // stime

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

        Stroka memoryStat = NFS::CombinePaths(pathToThreadStat, "statm");
        TIFStream memoryStatFile(memoryStat);
        auto memoryFields = splitStroku(memoryStatFile.ReadLine(), " ");

        i64 residentSetSize = FromString<i64>(memoryFields[1]);
        TQueuedSample memorySample;
        memorySample.Time = GetCpuInstant();
        memorySample.Path = baseProfilingPath + "/memory";
        memorySample.Value = residentSetSize;
        TProfilingManager::Get()->Enqueue(memorySample, false);
    }

    PreviousProcTicks = procTicks;
    PeriodicInvoker->ScheduleNext();
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NProfiling
} // namespace NYT
