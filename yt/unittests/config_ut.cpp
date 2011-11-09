#include "../ytlib/misc/new_config.h"
#include "../ytlib/ytree/tree_builder.h"
#include "../ytlib/ytree/ephemeral.h"
#include "../ytlib/ytree/fluent.h"

#include <contrib/testing/framework.h>


namespace NYT {
namespace NConfig {

using namespace NYTree;

struct TTestSubconfig
    : public TConfigBase
{
    i32 I1;
    bool B1;

    TTestSubconfig()
    {
        Register("i1", &I1).Default(100);
        Register("b1", &B1).Default(false);
    }
};

struct TTestConfig
    : public TConfigBase
{
    Stroka S1;
    TTestSubconfig Subconfig;

    TTestConfig()
    {
        Register("s1", &S1);
        Register("sub", &Subconfig);
    }
};

TEST(TConfigTest, Complete)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("s1").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("i1").Scalar(99)
                .Item("b1").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ(config.S1, "TestString");
    EXPECT_EQ(config.Subconfig.I1, 99);
    EXPECT_EQ(config.Subconfig.B1, true);
}

TEST(TConfigTest, MissingParameter)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("s1").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("b1").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ(config.S1, "TestString");
    EXPECT_EQ(config.Subconfig.I1, 100);
    EXPECT_EQ(config.Subconfig.B1, true);
}

TEST(TConfigTest, MissingSubconfig)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("s1").Scalar("TestString")
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ(config.S1, "TestString");
    EXPECT_EQ(config.Subconfig.I1, 100);
    EXPECT_EQ(config.Subconfig.B1, false);
}

TEST(TConfigTest, MissingRequiredParameter)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("sub").BeginMap()
                .Item("i1").Scalar(99)
                .Item("b1").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    EXPECT_THROW(config.Validate(), yexception);
}

} // namespace NConfig
} // namespace NYT