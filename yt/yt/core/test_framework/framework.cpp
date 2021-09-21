#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/signal_registry.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/common/env.h>

#include <util/system/fs.h>
#include <util/system/env.h>

#include <util/random/random.h>

#include <util/string/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomFileName(const char* prefix)
{
    return Format("%s-%016" PRIx64 "-%016" PRIx64,
        prefix,
        MicroSeconds(),
        RandomNumber<ui64>());
}

////////////////////////////////////////////////////////////////////////////////

void WaitForPredicate(
    std::function<bool()> predicate,
    int iterationCount,
    TDuration period)
{
    for (int iteration = 0; iteration < iterationCount; ++iteration) {
        if (predicate()) {
            return;
        }
        NConcurrency::TDelayedExecutor::WaitForDuration(period);
    }
    THROW_ERROR_EXCEPTION("Wait failed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace testing {

using namespace NYT;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void RunAndTrackFiber(TClosure closure)
{
    auto queue = New<TActionQueue>("Main");
    auto invoker = queue->GetInvoker();

    auto result = BIND([invoker, closure] () mutable {
        // NB: Make sure TActionQueue does not keep a strong reference to this fiber by forcing a yield.
        SwitchTo(invoker);

        closure.Run();
    })
    .AsyncVia(invoker)
    .Run();

    auto startedAt = TInstant::Now();
    while (!result.IsSet()) {
        if (TInstant::Now() - startedAt > TDuration::Seconds(5)) {
            GTEST_FAIL() << "Probably stuck.";
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }

    queue->Shutdown();

    SUCCEED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace testing

class TYTEnvironment
    : public ::testing::Environment
{ };

////////////////////////////////////////////////////////////////////////////////

Y_TEST_HOOK_BEFORE_RUN(GTEST_YT_SETUP)
{
#ifdef _unix_
    ::signal(SIGPIPE, SIG_IGN);
#endif
    NYT::TSignalRegistry::Get()->PushCallback(NYT::AllCrashSignals, NYT::CrashSignalHandler);
    NYT::TSignalRegistry::Get()->PushDefaultSignalHandler(NYT::AllCrashSignals);
    NYT::NYTAlloc::EnableYTLogging();
    NYT::NYTAlloc::EnableYTProfiling();
    NYT::NYTAlloc::InitializeLibunwindInterop();
    NYT::NYTAlloc::EnableStockpile();
    NYT::NYTAlloc::ConfigureFromEnv();

    auto config = NYT::NLogging::TLogManagerConfig::CreateYTServer("unittester", GetOutputPath().GetPath());
    NYT::NLogging::TLogManager::Get()->Configure(config);
    NYT::NLogging::TLogManager::Get()->EnableReopenOnSighup();

    NYT::NProfiling::TSolomonRegistry::Get()->Disable();

    ::testing::AddGlobalTestEnvironment(new TYTEnvironment());

    // TODO(ignat): support ram_drive_path when this feature would be supported in gtest machinery.
    auto testSandboxPath = GetEnv("TESTS_SANDBOX");
    if (!testSandboxPath.empty()) {
        NFs::SetCurrentWorkingDirectory(testSandboxPath);
    }
}

Y_TEST_HOOK_AFTER_RUN(GTEST_YT_TEARDOWN)
{
    NYT::Shutdown();
#ifdef _asan_enabled_
    // Wait for some time to ensure background cleanup is somewhat complete.
    Sleep(TDuration::Seconds(1));
    NYT::TRefCountedTrackerFacade::Dump();
#endif
}


////////////////////////////////////////////////////////////////////////////////

