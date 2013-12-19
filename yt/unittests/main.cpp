#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

#include <core/ytree/convert.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/fluent.h>

#include <core/logging/log_manager.h>

#include <server/hydra/file_changelog.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <util/string/printf.h>
#include <util/string/escape.h>
#include <util/string/vector.h>

class TYTEnvironment
    : public ::testing::Environment
{
public:
    virtual void SetUp() override
    {
#ifdef _unix_
        const char* loggingLevelFromEnv = getenv("YT_LOG_LEVEL");
        Stroka loggingLevel;
        if (loggingLevelFromEnv) {
            loggingLevel = loggingLevelFromEnv;
            loggingLevel.to_lower(0, Stroka::npos);
        } else {
            loggingLevel = "fatal";
        }

        const char* loggingCategoriesFromEnv = getenv("YT_LOG_CATEGORIES");
        VectorStrok loggingCategories;
        if (loggingCategoriesFromEnv) {
            SplitStroku(&loggingCategories, loggingLevelFromEnv, ",");
        } else {
            loggingCategories.push_back("*");
        }

        auto builder = CreateBuilderFromFactory(NYT::NYTree::GetEphemeralNodeFactory());
        NYT::NYTree::BuildYsonFluently(builder.get())
            .BeginMap()
                .Item("rules")
                .BeginList()
                    .Item()
                    .BeginMap()
                        .Item("min_level").Value(loggingLevel)
                        .Item("writers").BeginList().Item().Value("stderr").EndList()
                        .Item("categories").List(loggingCategories)
                    .EndMap()
                .EndList()
                .Item("writers")
                .BeginMap()
                    .Item("stderr")
                    .BeginMap()
                        .Item("type").Value("stderr")
                        .Item("pattern").Value("$(datetime) $(level) $(category) $(message)")
                    .EndMap()
                .EndMap()
            .EndMap();
        NYT::NLog::TLogManager::Get()->Configure(builder->EndTree());
#endif
    }

    virtual void TearDown() override
    {
        NYT::NHydra::ShutdownChangelogs();
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

    int result = RUN_ALL_TESTS();

    return result;
}

