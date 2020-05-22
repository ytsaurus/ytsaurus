#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/ytalloc/bindings.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/system/fs.h>
#include <util/system/env.h>

////////////////////////////////////////////////////////////////////////////////

class TYTEnvironment
    : public ::testing::Environment
{ };

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
    int result = 1;
    try {
#ifdef _unix_
        ::signal(SIGPIPE, SIG_IGN);
#endif
        NYT::NYTAlloc::EnableYTLogging();
        NYT::NYTAlloc::EnableYTProfiling();
        NYT::NYTAlloc::SetLibunwindBacktraceProvider();
        NYT::NYTAlloc::ConfigureFromEnv();
        NYT::NYTAlloc::EnableStockpile();
        NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();
        NYT::NLogging::TLogManager::Get()->EnableReopenOnSighup();

        ::testing::InitGoogleTest(&argc, argv);
        ::testing::InitGoogleMock(&argc, argv);
        ::testing::AddGlobalTestEnvironment(new TYTEnvironment());

        // TODO(ignat): support ram_drive_path when this feature would be supported in gtest machinery.
        auto testSandboxPath = GetEnv("TESTS_SANDBOX");
        if (!testSandboxPath.empty()) {
            NFs::SetCurrentWorkingDirectory(testSandboxPath);
        }

        result = RUN_ALL_TESTS();
    } catch (const std::exception& ex) {
        fprintf(stderr, "Unhandled exception: %s\n", ex.what());
    }
    NYT::Shutdown();
#ifdef _asan_enabled_
    // Wait for some time to ensure background cleanup is somewhat complete.
    Sleep(TDuration::Seconds(1));
    NYT::TRefCountedTrackerFacade::Dump();
#endif
    return result;
}
