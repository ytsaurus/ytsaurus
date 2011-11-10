#include "../ytlib/misc/new_config.h"
#include "../ytlib/ytree/tree_builder.h"
#include "../ytlib/ytree/ephemeral.h"
#include "../ytlib/ytree/fluent.h"
#include "../ytlib/ytree/yson_writer.h"
#include "../ytlib/ytree/tree_visitor.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace NConfig {

using namespace NYTree;

DECLARE_ENUM(ETestEnum,
    (Value0)
    (Value1)
    (Value2)
);

struct TTestSubconfig
    : public TConfigBase
{
    i32 I1;
    bool B1;
    yvector<Stroka> LS1;
    ETestEnum E1;

    TTestSubconfig()
    {
        Register("i1", I1).Default(100).InRange(95, 105);
        Register("b1", B1).Default(false);
        Register("ls1", LS1).Default();
        Register("e1", E1).Default(ETestEnum::Value1);
    }
};

struct TTestConfig
    : public TConfigBase
{
    Stroka S1;
    TTestSubconfig Subconfig;

    TTestConfig()
    {
        Register("s1", S1).NonEmpty();
        Register("sub", Subconfig);
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
                .Item("e1").Scalar("Value2")
                .Item("ls1").BeginList()
                    .Item().Scalar("ListItem0")
                    .Item().Scalar("ListItem1")
                    .Item().Scalar("ListItem2")
                .EndList()
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ(config.S1, "TestString");
    EXPECT_EQ(config.Subconfig.I1, 99);
    EXPECT_EQ(config.Subconfig.B1, true);
    EXPECT_EQ(config.Subconfig.LS1.ysize(), 3);
    EXPECT_EQ(config.Subconfig.LS1[0], "ListItem0");
    EXPECT_EQ(config.Subconfig.LS1[1], "ListItem1");
    EXPECT_EQ(config.Subconfig.LS1[2], "ListItem2");
    EXPECT_EQ(config.Subconfig.E1, ETestEnum::Value2);
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
    EXPECT_EQ(config.Subconfig.LS1.ysize(), 0);
    EXPECT_EQ(config.Subconfig.E1, ETestEnum::Value1);
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
    EXPECT_EQ(config.Subconfig.LS1.ysize(), 0);
    EXPECT_EQ(config.Subconfig.E1, ETestEnum::Value1);
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
    EXPECT_THROW(config.Load(~configNode->AsMap()), yexception);
}

TEST(TConfigTest, Validate)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("s1").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("i1").Scalar(110) // out of range
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