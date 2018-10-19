#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/alloc/alloc.h>

class TYTEnvironment
    : public ::testing::Environment
{
public:
    virtual void SetUp() override
    {
        NYT::NYTAlloc::EnableLogging();
        NYT::NYTAlloc::EnableProfiling();
        NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();
    }

    virtual void TearDown() override
    {
        NYT::Shutdown();
    }
};

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

