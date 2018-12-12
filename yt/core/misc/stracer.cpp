#include "stracer.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/subprocess.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/finally.h>

#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Stracer");
static const auto TraceInterval = TDuration::Seconds(5);
static const auto TraceTimeout = TDuration::Seconds(10);
static const int StraceConcurrencyFactor = 4;

////////////////////////////////////////////////////////////////////////////////

// NB: Moving these into the header will break tool registration as the linker
// will optimize it out.
TStrace::TStrace()
{
    RegisterParameter("trace", Trace);
    RegisterParameter("process_name", ProcessName);
    RegisterParameter("process_command_line", ProcessCommandLine);
}

TStracerResult::TStracerResult()
{
    RegisterParameter("traces", Traces);
}

////////////////////////////////////////////////////////////////////////////////

TStracerResultPtr TStraceTool::operator()(const std::vector<int>& pids) const
{
    SafeSetUid(0);
    return Strace(pids);;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TStracePtr DoStrace(int pid)
{
    auto trace = New<TStrace>();

    try {
        trace->ProcessName = GetProcessName(pid);
        trace->ProcessCommandLine = GetProcessCommandLine(pid);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to get information for process %v", pid);
    }

    TSubprocess tracer("/usr/bin/strace");
    tracer.AddArguments(
        {
            "-tt",
            "-p",
            ToString(pid)
        });

    auto intCookie = TDelayedExecutor::Submit(
        BIND([&] () {
            try {
                tracer.Kill(SIGINT);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to interrupt stracer process %v", pid);
            }
        }),
        TraceInterval);

    auto killCookie = TDelayedExecutor::Submit(
        BIND([&] () {
            try {
                tracer.Kill(SIGKILL);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to kill stracer process %v", pid);
            }
        }),
        TraceTimeout);

    auto cookieGuard = Finally([&] () {
        TDelayedExecutor::CancelAndClear(intCookie);
        TDelayedExecutor::CancelAndClear(killCookie);
    });

    auto tracerResult = tracer.Execute();
    if (!tracerResult.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to strace process %v", pid)
            << TErrorAttribute("stderr", ToString(tracerResult.Error))
            << tracerResult.Status;
    }

    trace->Trace = TString(tracerResult.Error.Begin(), tracerResult.Error.End());
    return trace;
}

} // namespace

TStracerResultPtr Strace(const std::vector<int>& pids)
{
    auto result = New<TStracerResult>();

    auto pool = New<TThreadPool>(StraceConcurrencyFactor, "StracePool");

    THashMap<int, TFuture<TStracePtr>> traceFutures;
    for (const auto& pid : pids) {
        traceFutures[pid] =
            BIND([=] () {
                return DoStrace(pid);
            })
            .AsyncVia(pool->GetInvoker())
            .Run();
    }

    for (const auto& pid : pids) {
        result->Traces[pid] = traceFutures[pid]
            .Get()
            .ValueOrThrow();;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
