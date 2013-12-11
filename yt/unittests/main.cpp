#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

#include <server/hydra/file_changelog.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <util/string/printf.h>
#include <util/string/escape.h>

int main(int argc, char **argv)
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    testing::InitGoogleTest(&argc, argv);
    testing::InitGoogleMock(&argc, argv);

    int result = RUN_ALL_TESTS();

    NYT::NHydra::ShutdownChangelogs();
    NYT::Shutdown();

    return result;
}

