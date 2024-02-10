#include "memory_watchdog.h"

#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>

#include <util/system/yield.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;

static const TLogger Logger("MemoryWatchdog");

////////////////////////////////////////////////////////////////////////////////

TMemoryWatchdog::TMemoryWatchdog(
    TMemoryWatchdogConfigPtr config,
    TCallback<void()> exitCallback,
    TCallback<void()> interruptCallback)
    : Config_(std::move(config))
    , ExitCallback_(std::move(exitCallback))
    , InterruptCallback_(std::move(interruptCallback))
    , ActionQueue_(New<TActionQueue>("MemoryWatchdog"))
    , Invoker_(ActionQueue_->GetInvoker())
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TMemoryWatchdog::CheckMemoryUsage, MakeWeak(this)),
        Config_->Period))
{ }

void TMemoryWatchdog::Start()
{
    PeriodicExecutor_->Start();
}

void TMemoryWatchdog::Stop()
{
    YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());
}

void TMemoryWatchdog::CheckMemoryUsage()
{
    // Get current memory usage.
    auto usage = GetProcessMemoryUsage();
    auto rss = usage.Rss;
    auto now = TInstant::Now();

    // Update window RSS.
    WindowRssValues_.emplace_back(now, rss);
    while (!WindowRssValues_.empty() && WindowRssValues_.front().first + Config_->WindowWidth < now) {
        WindowRssValues_.pop_front();
    }

    size_t minimumWindowRss = WindowRssValues_.empty() ? 0 : WindowRssValues_.front().second;
    for (const auto& [_, usage] : WindowRssValues_) {
        if (minimumWindowRss > usage) {
            minimumWindowRss = usage;
        }
    }

    YT_LOG_DEBUG(
        "Checking memory usage "
        "(Rss: %v, MemoryLimit: %v, CodicilWatermark: %v, MinimumWindowRss: %v, WindowCodicilWatermark: %v)",
        rss,
        Config_->MemoryLimit,
        Config_->CodicilWatermark,
        minimumWindowRss,
        Config_->WindowCodicilWatermark);

    CheckRss(rss);
    CheckMinimumWindowRss(minimumWindowRss);

    // ClickHouse periodically snapshots current RSS and then tracks its
    // allocations, changing presumed value of RSS accordingly. It does
    // not work in our case as we have lots of our own allocations, so
    // we have to reconcile RSS more frequently than once per minute.
    MemoryTracker::setRSS(rss, 0);
}

void TMemoryWatchdog::CheckRss(size_t rss)
{
    if (rss + Config_->CodicilWatermark <= Config_->MemoryLimit) {
        return;
    }

    YT_LOG_ERROR(
        "Killing self because memory usage is too high (Rss: %v, MemoryLimit: %v, CodicilWatermark: %v)",
        rss,
        Config_->MemoryLimit,
        Config_->CodicilWatermark);
    WriteToStderr("*** Killing self because memory usage is too high ***\n");

    DumpRefCountedTracker();

    NYT::NLogging::TLogManager::Get()->Shutdown();
    ExitCallback_.Run();
    _exit(MemoryLimitExceededExitCode);
}

void TMemoryWatchdog::CheckMinimumWindowRss(size_t minimumWindowRss)
{
    if (minimumWindowRss + Config_->WindowCodicilWatermark <= Config_->MemoryLimit) {
        return;
    }

    YT_LOG_ERROR(
        "Interrupting self because window minimum memory usage is too high (MinimumWindowRss: %v, MemoryLimit: %v, WindowCodicilWatermark: %v)",
        minimumWindowRss,
        Config_->MemoryLimit,
        Config_->WindowCodicilWatermark);
    WriteToStderr("*** Interrupting self because window memory usage is too high ***\n");
    DumpRefCountedTracker();

    InterruptCallback_.Run();

    while (true) {
        ThreadYield();
    }
}

void TMemoryWatchdog::DumpRefCountedTracker()
{
    WriteToStderr("*** RefCountedTracker ***\n");
    WriteToStderr(TRefCountedTracker::Get()->GetDebugInfo(2 /*sortByColumn*/));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
