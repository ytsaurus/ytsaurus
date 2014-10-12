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
        if (!getenv("YT_LOG_LEVEL") && !getenv("YT_LOG_CATEGORIES")) {
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

        const char* logCategoriesFromEnv = getenv("YT_LOG_CATEGORIES");
        VectorStrok logCategories;
        if (logCategoriesFromEnv) {
            SplitStroku(&logCategories, logLevelFromEnv, ",");
        } else {
            logCategories.push_back("*");
        }

        auto builder = CreateBuilderFromFactory(NYT::NYTree::GetEphemeralNodeFactory());
        NYT::NYTree::BuildYsonFluently(builder.get())
            .BeginMap()
                .Item("rules")
                .BeginList()
                    .Item()
                    .BeginMap()
                        .Item("min_level").Value(logLevel)
                        .Item("writers").BeginList().Item().Value("stderr").EndList()
                        .Item("categories").List(logCategories)
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
    }

    virtual void TearDown() override
    {
        NYT::Shutdown();
    }

};

int main(int argc, char **argv)
{
    NYT::TGuid a;
    NYT::TGuid b(a);

    printf("%d", a == b);
    printf("%d", a != b);
    printf("%d", a < b);

#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new TYTEnvironment());

    return RUN_ALL_TESTS();
}

