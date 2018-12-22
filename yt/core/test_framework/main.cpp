#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/alloc/alloc.h>

////////////////////////////////////////////////////////////////////////////////

class TYTEnvironment
    : public ::testing::Environment
{
public:
    virtual void SetUp() override
    {
        NYT::NYTAlloc::EnableLogging();
        NYT::NYTAlloc::EnableProfiling();
        NYT::NYTAlloc::EnableStockpile();
        NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();
    }

    virtual void TearDown() override
    {
        NYT::Shutdown();
#ifdef _asan_enabled_
        // Wait for some time to ensure background cleanup is somewhat complete.
        Sleep(TDuration::Seconds(1));
        NYT::TRefCountedTrackerFacade::Dump();
#endif
    }
};

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new TYTEnvironment());

    return RUN_ALL_TESTS();
}
