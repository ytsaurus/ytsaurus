#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

class TYTEnvironment
    : public ::testing::Environment
{
public:
    virtual void SetUp() override
    {
        NYT::ConfigureLogging(
            getenv("YT_LOG_LEVEL"),
            getenv("YT_LOG_EXCLUDE_CATEGORIES"),
            getenv("YT_LOG_INCLUDE_CATEGORIES"));
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

