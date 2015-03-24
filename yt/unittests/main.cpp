#include "stdafx.h"
#include "framework.h"

#include <ytlib/shutdown.h>

#include <core/ytree/convert.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/fluent.h>

#include <core/logging/log_manager.h>

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
        if (!getenv("YT_LOG_LEVEL") && !getenv("YT_LOG_EXCLUDE_CATEGORIES") && !getenv("YT_LOG_INCLUDE_CATEGORIES")) {
            return;
        }

        const char* logLevelFromEnv = getenv("YT_LOG_LEVEL");
        Stroka logLevel;
        if (logLevelFromEnv) {
            logLevel = logLevelFromEnv;
            logLevel.to_lower(0, Stroka::npos);
        } else {
            logLevel = "fatal";
        }

        const char* logExcludeCategoriesFromEnv = getenv("YT_LOG_EXCLUDE_CATEGORIES");
        VectorStrok logExcludeCategories;
        if (logExcludeCategoriesFromEnv) {
            SplitStroku(&logExcludeCategories, logExcludeCategoriesFromEnv, ",");
        } else {
            logExcludeCategories.push_back("*");
        }

        const char* logIncludeCategoriesFromEnv = getenv("YT_LOG_INCLUDE_CATEGORIES");
        VectorStrok logIncludeCategories;
        if (logIncludeCategoriesFromEnv) {
            SplitStroku(&logIncludeCategories, logIncludeCategoriesFromEnv, ",");
        }

        Cout << logIncludeCategories.size() << Endl;

        auto builder = CreateBuilderFromFactory(NYT::NYTree::GetEphemeralNodeFactory());
        NYT::NYTree::BuildYsonFluently(builder.get())
            .BeginMap()
                .Item("rules")
                .BeginList()
                    .Item()
                    .BeginMap()
                        .Item("min_level").Value(logLevel)
                        .Item("writers").BeginList().Item().Value("stderr").EndList()
                        .Item("exclude_categories").List(logExcludeCategories)
                        .DoIf(!logIncludeCategories.empty(), [&] (NYT::NYTree::TFluentMap fluent) {
                            fluent.Item("include_categories").List(logIncludeCategories);
                        })
                    .EndMap()
                .EndList()
                .Item("writers")
                .BeginMap()
                    .Item("stderr")
                    .BeginMap()
                        .Item("type").Value("stderr")
                    .EndMap()
                .EndMap()
            .EndMap();
        NYT::NLogging::TLogManager::Get()->Configure(builder->EndTree());
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

