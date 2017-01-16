#include "framework.h"

#include <yt/ytlib/shutdown.h>
#include <yt/core/logging/log_manager.h>

// XXX(sandello): This is a dirty hack. :(
#include <yt/server/hydra/private.h>

class TYTEnvironment
    : public ::testing::Environment
{
public:
    virtual void SetUp() override
    {
        NYT::NLogging::TLogManager::Get()->ConfigureFromEnv();
    }

    virtual void TearDown() override
    {
        // TODO(sandello): Replace me with a better shutdown mechanism.
        NYT::NHydra::ShutdownHydraIOInvoker();
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

