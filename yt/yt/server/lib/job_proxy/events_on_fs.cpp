#include "events_on_fs.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <util/stream/file.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "EventsOnFs");

namespace {

void Touch(const std::string& path)
{
    TFile file(path, CreateAlways | WrOnly | Seq | CloseOnExec);
    file.Close();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<void> GetBreakpointEvent(
    const TEventsOnFsConfigPtr& config,
    NJobTrackerClient::TJobId jobId,
    EBreakpointType breakpoint)
{
    if (!config->Breakpoints.contains(breakpoint)) {
        // Breakpoint is not configured.
        return VoidFuture;
    }

    YT_LOG_DEBUG("Reached breakpoint (Breakpoint: %lv)", breakpoint);

    auto breakpointPath = NFS::CombinePaths(config->Path, Format("breakpoint_%lv_%v", breakpoint, jobId));
    auto allReleasedPath = NFS::CombinePaths(config->Path, Format("breakpoint_%lv_all_released", breakpoint));

    Touch(breakpointPath);

    return BIND([=, deadline = TInstant::Now() + config->Timeout] {
        YT_LOG_DEBUG("Waiting on breakpoint (Breakpoint: %lv)", breakpoint);

        auto isReleased = [=] {
            return !NFS::Exists(breakpointPath) || NFS::Exists(allReleasedPath);
        };

        while (!isReleased()) {
            if (TInstant::Now() > deadline) {
                THROW_ERROR_EXCEPTION("Timeout exceeded while waiting for breakpoint %Qlv to be released",
                    breakpoint);
            }
            TDelayedExecutor::WaitForDuration(config->PollPeriod);
        }

        YT_LOG_DEBUG("Breakpoint is released (Breakpoint: %lv)", breakpoint);
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
