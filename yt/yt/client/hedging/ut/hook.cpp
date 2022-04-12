#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <library/cpp/testing/hook/hook.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

Y_TEST_HOOK_BEFORE_RUN(TEST_YT_SETUP)
{
    NYT::NYTAlloc::ConfigureFromEnv();
    NYT::NYTAlloc::EnableStockpile();
    NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();
}

Y_TEST_HOOK_AFTER_RUN(TEST_YT_TEARDOWN)
{
    NYT::Shutdown();
#ifdef _asan_enabled_
    // Wait for some time to ensure background cleanup is somewhat complete.
    Sleep(TDuration::Seconds(1));
    NYT::TRefCountedTrackerFacade::Dump();
#endif
}
