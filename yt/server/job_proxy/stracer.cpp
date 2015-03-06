#include "stracer.h"
#include "private.h"

#include <server/exec_agent/subprocess.h>

#include <core/misc/proc.h>

#include <core/ytree/fluent.h>

#include <core/concurrency/delayed_executor.h>

namespace NYT {
namespace NJobProxy {

using namespace NExecAgent;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static const TDuration TraceInterval = TDuration::Seconds(5);
static const TDuration TraceTimeout = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TStrace& trace, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("trace").Value(trace.Trace)
            .Item("process_name").Value(trace.ProcessName)
            .Item("process_command_line").List(trace.ProcessCommandLine)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TStracerResult& stracerResult, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoFor(stracerResult.Traces, [] (TFluentMap fluent, const yhash_map<int, TStrace>::value_type& pair) {
                fluent
                    .Item(ToString(pair.first)).Value(pair.second);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TStracerResult Strace(const std::vector<int>& pids)
{
    TStracerResult result;

    for (const auto& pid : pids) {
        TStrace trace;

        try {
            trace.ProcessName = GetProcessName(pid);
            trace.ProcessCommandLine = GetProcessCommandLine(pid);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Failed to get process information for %v", pid);
        }

        TSubprocess tracer("/usr/bin/strace");
        tracer.AddArguments({
            "-tt",
            "-p",
            ToString(pid)
        });

        auto cookie = TDelayedExecutor::Submit(BIND([&] () {
                try {
                    tracer.Kill(2);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Failed to interrupt stracer %v", pid);
                }
            }),
            TraceInterval);

        auto killCookie = TDelayedExecutor::Submit(BIND([&] () {
                try {
                    tracer.Kill(9);
                } catch (const std::exception& ex) {
                    LOG_ERROR(ex, "Failed to kill stracer %v", pid);
                }
            }),
            TraceTimeout);

        TSubprocessResult tracerResult;
        try {
            tracerResult = tracer.Execute();

            TDelayedExecutor::Cancel(cookie);
            TDelayedExecutor::Cancel(killCookie);
        } catch (const std::exception&) {
            TDelayedExecutor::Cancel(cookie);
            TDelayedExecutor::Cancel(killCookie);
            throw;
        }

        if (!tracerResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to strace %v", pid)
                << TErrorAttribute("stderr", tracerResult.Error)
                << tracerResult.Status;
        }

        trace.Trace = Stroka(tracerResult.Error.Begin(), tracerResult.Error.End());
        result.Traces[pid] = trace;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NJobProxy
} // namespace NYT