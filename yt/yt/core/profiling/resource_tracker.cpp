#include "resource_tracker.h"
#include "profile_manager.h"
#include "profiler.h"
#include "config.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ypath/token.h>

#include <util/folder/filelist.h>

#include <util/stream/file.h>

#include <util/string/vector.h>

#if defined(_linux_) && !defined(_musl_)
#define RESOURCE_TRACKER_ENABLED

#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>

#include <sys/ioctl.h>
#include <sys/syscall.h>
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

#ifdef RESOURCE_TRACKER_ENABLED

struct TPerfEventDescription final
{
    int EventType;
    int EventConfig;
};

constexpr TPerfEventDescription SoftwareEvent(const int perfName) noexcept 
{
    return {PERF_TYPE_SOFTWARE, perfName};
}

constexpr TPerfEventDescription HardwareEvent(const int perfName) noexcept 
{
    return {PERF_TYPE_HARDWARE, perfName};
}

enum class ECacheEventType 
{
    Access,
    Miss,
};

TPerfEventDescription CacheEvent(const int perfName, const ECacheEventType eventType) noexcept 
{
    constexpr auto kEventNameShift = 0;
    constexpr auto kCacheActionTypeShift = 8;
    constexpr auto kEventTypeShift = 16;

    const int eventTypeForConfig = [&] {
        switch (eventType)
        {
            case ECacheEventType::Access:
                return PERF_COUNT_HW_CACHE_RESULT_ACCESS;
            case ECacheEventType::Miss:
                return PERF_COUNT_HW_CACHE_RESULT_MISS;
            default:
                YT_VERIFY(false);
        }
    }();

    const int eventConfig = (perfName << kEventNameShift) | 
        (PERF_COUNT_HW_CACHE_OP_READ << kCacheActionTypeShift) | 
        (eventTypeForConfig << kEventTypeShift);

    return {PERF_TYPE_HW_CACHE, eventConfig};
}

const TPerfEventDescription EventDescriptions[] = {
    HardwareEvent(PERF_COUNT_HW_CPU_CYCLES),
    HardwareEvent(PERF_COUNT_HW_INSTRUCTIONS),
    HardwareEvent(PERF_COUNT_HW_CACHE_REFERENCES),
    HardwareEvent(PERF_COUNT_HW_CACHE_MISSES),
    HardwareEvent(PERF_COUNT_HW_BRANCH_INSTRUCTIONS),
    HardwareEvent(PERF_COUNT_HW_BRANCH_MISSES),
    HardwareEvent(PERF_COUNT_HW_BUS_CYCLES),
    HardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_FRONTEND),
    HardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_BACKEND),
    HardwareEvent(PERF_COUNT_HW_REF_CPU_CYCLES),

    // `cpu-clock` is a bit broken according to this: https://stackoverflow.com/a/56967896
    SoftwareEvent(PERF_COUNT_SW_CPU_CLOCK),
    SoftwareEvent(PERF_COUNT_SW_TASK_CLOCK),
    SoftwareEvent(PERF_COUNT_SW_CONTEXT_SWITCHES),
    SoftwareEvent(PERF_COUNT_SW_CPU_MIGRATIONS),
    SoftwareEvent(PERF_COUNT_SW_ALIGNMENT_FAULTS),
    SoftwareEvent(PERF_COUNT_SW_EMULATION_FAULTS),

    CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheEventType::Access),
    CacheEvent(PERF_COUNT_HW_CACHE_DTLB, ECacheEventType::Miss),

    // Apparently it doesn't make sense to treat these values as relative:
    // https://stackoverflow.com/questions/49933319/how-to-interpret-perf-itlb-loads-itlb-load-misses
    CacheEvent(PERF_COUNT_HW_CACHE_ITLB, ECacheEventType::Access),
    CacheEvent(PERF_COUNT_HW_CACHE_ITLB, ECacheEventType::Miss),
    CacheEvent(PERF_COUNT_HW_CACHE_NODE, ECacheEventType::Access),
    CacheEvent(PERF_COUNT_HW_CACHE_NODE, ECacheEventType::Miss),
};

int OpenPerfEvent(const int tid, const int eventType, const int eventConfig) 
{
    perf_event_attr attr{};

    attr.type = eventType;
    attr.size = sizeof(attr);
    attr.config = eventConfig;
    attr.exclude_kernel = 1;
    attr.disabled = 1;

    const int fd = syscall(SYS_perf_event_open, &attr, tid, -1, -1, 0);
    if (fd == -1) {
        YT_LOG_WARNING(
            TError::FromSystem(), 
            "Fail to open perf event descriptor (FD: %v, tid: %v, EventType: %v, EventConfig: %v", 
            fd, 
            tid, 
            eventType, 
            eventConfig);
    } else {
        YT_LOG_INFO(
            "Perf event descriptor opened (FD: %v, tid: %v, EventType: %v, EventConfig: %v", 
            fd, 
            tid, 
            eventType, 
            eventConfig);
    }

    return fd;
}

ui64 FetchPerfCounter(const int fd)
{
    if (fd == -1) {
        return 0;
    }

    ui64 num{};
    YT_VERIFY(read(fd, &num, sizeof(num)) == sizeof(num) || errno == EINTR);
    return num;
}

void SetPerfEventEnableMode(const bool enable, const int FD) 
{
    if (FD <= 0) {
        return;
    }

    const auto ioctlArg = enable ? PERF_EVENT_IOC_ENABLE : PERF_EVENT_IOC_DISABLE;
    YT_VERIFY(ioctl(FD, ioctlArg, 0) != -1);
}

void OpenPerfEventIfNeeded(const TStringBuf tidString, TResourceTracker::TThreadPerfInfoMap& perfInfoMap, const int eventIndex) 
{
    auto& perfEventInfos = perfInfoMap[tidString].EventInfos;

    if (perfEventInfos[eventIndex].FD != 0) {
        return;
    }

    const auto tid = [&tidString] {
        try {
            return FromString<int>(tidString);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error parsing tidString: %v", tidString);
            throw;
        }
    }();
    perfEventInfos[eventIndex].FD = OpenPerfEvent(tid, EventDescriptions[eventIndex].EventType, EventDescriptions[eventIndex].EventConfig);
}

#endif

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
    CollectPerfMetrics(writer, TidToInfo_, tidToInfo);
    CollectSensorsThreadCounts(writer, tidToInfo);
    TidToInfo_ = tidToInfo;

    LastUpdateTime_ = TInstant::Now();
}

void TResourceTracker::FetchPerfStats(
    const TStringBuf tidString, 
    TResourceTracker::TThreadPerfInfoMap& perfInfoMap, 
    TResourceTracker::TPerfCounters& counters) 
{
    Y_UNUSED(tidString);
    Y_UNUSED(perfInfoMap);
    Y_UNUSED(counters);
#ifdef RESOURCE_TRACKER_ENABLED
    auto& perfInfo = TidToPerfInfo_[tidString];

    for (int eventIndex = 0; eventIndex < TEnumTraits<EPerfEvents>::DomainSize; ++eventIndex) {
        const auto eventCurrentlyEnabled = EventConfigSnapshot.Enabled[eventIndex];
        auto& eventInfo = perfInfo.EventInfos[eventIndex];

        if (eventCurrentlyEnabled != eventInfo.Enabled) {
            if (eventCurrentlyEnabled) {
                OpenPerfEventIfNeeded(tidString, perfInfoMap, eventIndex);
                YT_LOG_INFO("Start collecting %v metrics for thread %v", FormatEnum(EPerfEvents{eventIndex}), tidString);
            } else {
                YT_LOG_INFO("Stop collecting %v metrics for thread %v", FormatEnum(EPerfEvents{eventIndex}), tidString);
            }

            SetPerfEventEnableMode(eventCurrentlyEnabled, eventInfo.FD);
        }
        eventInfo.Enabled = eventCurrentlyEnabled;

        if (eventCurrentlyEnabled) {
            YT_LOG_TRACE("Will collect event %v", eventIndex);
            counters.Counters[eventIndex] = FetchPerfCounter(eventInfo.FD);
        } else {
            YT_LOG_TRACE("Will no collect event %v", eventIndex);
        }
    }
#endif
}

void TResourceTracker::CreateEventConfigSnapshot() noexcept
{
    for (int eventIndex = 0; eventIndex < std::ssize(EventConfigs.Enabled); ++eventIndex) {
        EventConfigSnapshot.Enabled[eventIndex] = EventConfigs.Enabled[eventIndex].load(std::memory_order_relaxed);
    }
}

void TResourceTracker::CollectPerfMetrics(
    ISensorWriter* writer,
    const TResourceTracker::TThreadMap& oldTidToInfo,
    const TResourceTracker::TThreadMap& newTidToInfo) 
{
    THashMap<TStringBuf, TPerfCounters> aggregatedPerfCounters;
    for (const auto& [tid, newInfo] : newTidToInfo) {
        const auto it = oldTidToInfo.find(tid);

        if (it == oldTidToInfo.end()) {
            continue;
        }

        const auto& oldInfo = it->second;

        if (oldInfo.ProfilingKey != newInfo.ProfilingKey) {
            continue;
        }

        aggregatedPerfCounters[newInfo.ProfilingKey] += newInfo.PerfCounters;
    }

    for (const auto& [profilingKey, perfCounters] : aggregatedPerfCounters) {
        writer->PushTag(std::pair<TString, TString>{"thread", profilingKey});

        for (int index = 0; index < std::ssize(perfCounters.Counters); ++index) {
            if (!EventConfigSnapshot.Enabled[index]) {
                continue;
            }
            writer->AddCounter("/" + FormatEnum(EPerfEvents{index}), perfCounters.Counters[index]);
        }

        writer->PopTag();
    }
}

void TResourceTracker::Configure(const TProfileManagerConfigPtr& config) 
{
    if (config) {
        SetPerfEventsConfiguration(config->EnabledPerfEvents);
    }
}

void TResourceTracker::Reconfigure(const TProfileManagerConfigPtr& config, const TProfileManagerDynamicConfigPtr& dynamicConfig)
{
    if (dynamicConfig && dynamicConfig->EnabledPerfEvents) {
        SetPerfEventsConfiguration(*dynamicConfig->EnabledPerfEvents);
    } else if (config) {
        SetPerfEventsConfiguration(config->EnabledPerfEvents);
    }
}

void TResourceTracker::SetPerfEventsConfiguration(const THashSet<EPerfEvents>& enabledEvents)
{
    for (int eventIndex = 0; eventIndex < std::ssize(EventConfigs.Enabled); ++eventIndex) {
        const bool eventEnabled = enabledEvents.contains(static_cast<EPerfEvents>(eventIndex));

        EventConfigs.Enabled[eventIndex].store(eventEnabled, std::memory_order_relaxed);
    }
}

bool TResourceTracker::ProcessThread(TString tid, TResourceTracker::TThreadInfo* info)
{
    auto threadStatPath = NFS::CombinePaths(procPath, tid);
    auto statPath = NFS::CombinePaths(threadStatPath, "stat");
    auto statusPath = NFS::CombinePaths(threadStatPath, "status");
    auto schedStatPath = NFS::CombinePaths(threadStatPath, "schedstat");

    try {
        FetchPerfStats(tid, TidToPerfInfo_, info->PerfCounters);

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

        if (!TidToInfo_.contains(tid)) {
            YT_LOG_TRACE("Thread %v named %v", tid, info->ThreadName);
        }

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
    CreateEventConfigSnapshot();

    TDirsList dirsList;
    try {
        dirsList.Fill(procPath);
    } catch (const TSystemError&) {
        // Ignore all exceptions.
        return {};
    }

    TThreadMap tidToStats;

    for (int index = 0; index < static_cast<int>(dirsList.Size()); ++index) {
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
