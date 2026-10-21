#include "cpu_accounting.h"

#include <yt/yt/core/concurrency/context_switch.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/misc/tls.h>

#include <util/system/datetime.h>

#include <algorithm>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_THREAD_LOCAL(ui64, AccountedThreadCpuTime);

} // namespace

////////////////////////////////////////////////////////////////////////////////

void RunWithCpuAccounting(
    TFunctionRef<void()> callback,
    TFunctionRef<void(ui64)> consumeMicroseconds)
{
    struct TCheckpoint
    {
        ui64 CpuTime;
        ui64 AccountedCpuTime;
    };

    // A resumer runs underneath the resumed fiber without switching out its own handlers.
    // Deduct CPU already charged by the resumed callback on this thread.
    TCheckpoint checkpoint{ThreadCPUTime(), AccountedThreadCpuTime()};
    auto accumulate = [&checkpoint, &consumeMicroseconds] {
        ui64 now = ThreadCPUTime();
        auto& accounted = AccountedThreadCpuTime();
        ui64 delta = now - checkpoint.CpuTime;
        // Nested guards switch out from outer to inner; the outer guard may have charged
        // a prefix that started before this guard's checkpoint.
        delta -= std::min(delta, accounted - checkpoint.AccountedCpuTime);
        consumeMicroseconds(delta);
        accounted += delta;
        checkpoint = {now, accounted};
    };
    TContextSwitchGuard cpuTimeGuard(
        /*out*/ accumulate,
        /*in*/ [&checkpoint] {
            checkpoint = {ThreadCPUTime(), AccountedThreadCpuTime()};
        });
    auto finally = Finally(accumulate);

    callback();
}

TDuration RunWithCpuAccounting(TFunctionRef<void()> callback)
{
    ui64 cpuTimeMicroseconds = 0;
    RunWithCpuAccounting(callback, [&cpuTimeMicroseconds] (ui64 delta) {
        cpuTimeMicroseconds += delta;
    });
    return TDuration::MicroSeconds(cpuTimeMicroseconds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
