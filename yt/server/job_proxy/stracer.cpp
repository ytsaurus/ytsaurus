#include "stracer.h"
#include "private.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/subprocess.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/finally.h>

#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static const auto TraceInterval = TDuration::Seconds(5);
static const auto TraceTimeout = TDuration::Seconds(10);
static const int StraceConcurrencyFactor = 4;

////////////////////////////////////////////////////////////////////////////////

TStracerResult TStraceTool::operator()(const std::vector<int>& pids) const
{
    SafeSetUid(0);
    return Strace(pids);;
}

REGISTER_TOOL(TStraceTool);

////////////////////////////////////////////////////////////////////////////////

TStrace DoStrace(int pid)
{
    TStrace trace;

    try {
        trace.ProcessName = GetProcessName(pid);
        trace.ProcessCommandLine = GetProcessCommandLine(pid);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to get information for process %v", pid);
    }

    TSubprocess tracer("/usr/bin/strace");
    tracer.AddArguments({
        "-tt",
        "-p",
        ToString(pid)
    });

    auto intCookie = TDelayedExecutor::Submit(BIND([&] () {
            try {
                tracer.Kill(SIGINT);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to interrupt stracer process %v", pid);
            }
        }),
        TraceInterval);

    auto killCookie = TDelayedExecutor::Submit(BIND([&] () {
            try {
                tracer.Kill(SIGKILL);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Failed to kill stracer process %v", pid);
            }
        }),
        TraceTimeout);

    TFinallyGuard cookieGuard([&] () {
        TDelayedExecutor::Cancel(intCookie);
        TDelayedExecutor::Cancel(killCookie);
    });

    auto tracerResult = tracer.Execute();
    if (!tracerResult.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to strace process %v", pid)
            << TErrorAttribute("stderr", ToString(tracerResult.Error))
            << tracerResult.Status;
    }

    trace.Trace = Stroka(tracerResult.Error.Begin(), tracerResult.Error.End());
    return trace;
}

TStracerResult Strace(const std::vector<int>& pids)
{
    TStracerResult result;

    auto pool = New<TThreadPool>(StraceConcurrencyFactor, "StracePool");

    std::map<int, TFuture<TStrace>> traceFutures;

    for (const auto& pid : pids) {
        traceFutures[pid] =
            BIND([=] () {
                return DoStrace(pid);
            })
            .AsyncVia(pool->GetInvoker())
            .Run();
    }

    for (const auto& pid : pids) {
        result.Traces[pid] = traceFutures[pid].Get().ValueOrThrow();;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NJobProxy
} // namespace NYT
