#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

#include <core/ytree/convert.h>
#include <core/ytree/convert.h>

#include <core/logging/log_manager.h>

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

    NYT::NLog::TLogManager::Get()->Configure(NYT::NYTree::ConvertToNode(NYT::NYTree::TYsonString(
        "{"
        "    rules = ["
        "        {"
        "            min_level = debug;"
        "            writers = [file];"
        "            categories = [\"*\"];"
        "        };"
        "        {"
        "            min_level = error;"
        "            writers = [std_err];"
        "            categories = [\"*\"];"
        "        }"
        "    ];"
        "    writers = {"
        "        std_err = {"
        "            type = std_err;"
        "            pattern = \"$(datetime) $(level) $(category) $(message)\";"
        "        };"
        "        file = {"
        "            type = file;"
        "            pattern = \"$(datetime) $(level) $(category) $(message)\";"
        "            file_name = \"unittester.log\";"
        "        };"
        "    };"
        "}"
    )));

    int result = RUN_ALL_TESTS();

    NYT::NHydra::ShutdownChangelogs();
    NYT::Shutdown();

    return result;
}

