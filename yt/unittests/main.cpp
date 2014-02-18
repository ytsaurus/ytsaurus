#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv)
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    testing::InitGoogleTest(&argc, argv);
    int rv = RUN_ALL_TESTS();
    NYT::Shutdown();

    return rv;
}

////////////////////////////////////////////////////////////////////////////////
