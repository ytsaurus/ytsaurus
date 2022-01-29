#include "oom.h"

#include <thread>
#include <mutex>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/logging/logger.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>

#include <util/datetime/base.h>
#include <util/system/file.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {

static NYT::NLogging::TLogger Logger{"OOM"};

////////////////////////////////////////////////////////////////////////////////

void OOMWatchdog(TOOMOptions options)
{
    while (true) {
        auto memoryUsage = tcmalloc::MallocExtension::GetNumericProperty("generic.physical_memory_used");
        if (memoryUsage && options.MemoryLimit && static_cast<i64>(*memoryUsage) > *options.MemoryLimit) {
            auto profile = NYTProf::ReadHeapProfile(tcmalloc::ProfileType::kHeap);

            TFileOutput output(options.HeapDumpPath);
            NYTProf::WriteProfile(&output, profile);
            output.Finish();

            YT_LOG_FATAL("Early OOM triggered (MemoryUsage: %v, MemoryLimit: %v, HeapDump: %v)",
                *memoryUsage, *options.MemoryLimit, options.HeapDumpPath);
        }

        Sleep(TDuration::MilliSeconds(10));
    }
}

void EnableEarlyOOMWatchdog(TOOMOptions options)
{
    static std::once_flag onceFlag;

    std::call_once(onceFlag, [options] {
        std::thread(OOMWatchdog, options).detach();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
