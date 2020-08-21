#include "memory_watchdog.h"

#include "config.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/ref_counted_tracker.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;

TLogger Logger("MemoryWatchdog");

////////////////////////////////////////////////////////////////////////////////

TMemoryWatchdog::TMemoryWatchdog(TMemoryWatchdogConfigPtr config, TCallback<void()> exitCallback, TCallback<void()> interruptCallback)
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
    PeriodicExecutor_->Stop();
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

    size_t maximumWindowRss = 0;
    for (const auto& [_, usage] : WindowRssValues_) {
        if (maximumWindowRss < usage) {
            maximumWindowRss = usage;
        }
    }

    // Check watermark and window watermark.
    YT_LOG_DEBUG(
        "Checking memory usage "
        "(Rss: %v, MemoryLimit: %v, CodicilWatermark: %v, MaximumWindowRss: %v, WindowCodicilWatermark: %v)",
        rss,
        Config_->MemoryLimit,
        Config_->CodicilWatermark,
        maximumWindowRss,
        Config_->WindowCodicilWatermark);
    if (rss + Config_->CodicilWatermark > Config_->MemoryLimit) {
        KillSelf("memory usage is too high", false);
    }
    if (maximumWindowRss + Config_->WindowCodicilWatermark > Config_->MemoryLimit) {
        KillSelf("window memory usage is too high", true);
    }

    // ClickHouse periodically snapshots current RSS and then tracks its
    // allocations, changing presumed value of RSS accordingly. It does
    // not work in our case as we have lots of our own allocations, so
    // we have to reconcile RSS more frequently than once per minute.
    total_memory_tracker.set(rss);
    CurrentMetrics::set(CurrentMetrics::MemoryTracking, rss);
}

void TMemoryWatchdog::KillSelf(TString reason, bool graceful)
{
    if (graceful) {
        YT_LOG_ERROR("Interrupting self because %v", reason);
    } else {
        YT_LOG_ERROR("Killing self because %v", reason);
    }
    WriteToStderr("*** OOM by watchdog (");
    WriteToStderr(reason);
    WriteToStderr(") ***\n");
    WriteToStderr("*** RefCountedTracker ***\n");
    WriteToStderr(TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */));
    if (graceful) {
        InterruptCallback_.Run();
        while (true);
    } else {
        NYT::NLogging::TLogManager::Get()->Shutdown();
        ExitCallback_.Run();
        _exit(MemoryLimitExceededExitCode);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
