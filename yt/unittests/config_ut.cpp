#include "stdafx.h"

#include "../ytlib/misc/config.h"
#include "../ytlib/ytree/tree_builder.h"
#include "../ytlib/ytree/ephemeral.h"
#include "../ytlib/ytree/fluent.h"
#include "../ytlib/ytree/yson_writer.h"
#include "../ytlib/ytree/tree_visitor.h"

#include <contrib/testing/framework.h>

namespace NYT {

using namespace NYTree;

DECLARE_ENUM(ETestEnum,
    (Value0)
    (Value1)
    (Value2)
);

struct TTestSubconfig
    : public TConfigBase
{
    int MyInt;
    bool MyBool;
    yvector<Stroka> MyStringList;
    ETestEnum MyEnum;

    TTestSubconfig()
    {
        Register("my_int", MyInt).Default(100).InRange(95, 105);
        Register("my_bool", MyBool).Default(false);
        Register("my_string_list", MyStringList).Default();
        Register("my_enum", MyEnum).Default(ETestEnum::Value1);
    }
};

struct TTestConfig
    : public TConfigBase
{
    Stroka MyString;
    TTestSubconfig Subconfig;

    TTestConfig()
    {
        Register("my_string", MyString).NonEmpty();
        Register("sub", Subconfig);
    }
};

TEST(TConfigTest, Complete)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("my_string").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Scalar(99)
                .Item("my_bool").Scalar(true)
                .Item("my_enum").Scalar("Value2")
                .Item("my_string_list").BeginList()
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

    EXPECT_EQ("TestString", config.MyString);
    EXPECT_EQ(99, config.Subconfig.MyInt);
    EXPECT_IS_TRUE(config.Subconfig.MyBool);
    EXPECT_EQ(3, config.Subconfig.MyStringList.ysize());
    EXPECT_EQ("ListItem0", config.Subconfig.MyStringList[0]);
    EXPECT_EQ("ListItem1", config.Subconfig.MyStringList[1]);
    EXPECT_EQ("ListItem2", config.Subconfig.MyStringList[2]);
    EXPECT_EQ(ETestEnum::Value2, config.Subconfig.MyEnum);
}

TEST(TConfigTest, MissingParameter)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("my_string").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("my_bool").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ("TestString", config.MyString);
    EXPECT_EQ(100, config.Subconfig.MyInt);
    EXPECT_IS_TRUE(config.Subconfig.MyBool);
    EXPECT_EQ(0, config.Subconfig.MyStringList.ysize());
    EXPECT_EQ(ETestEnum::Value1, config.Subconfig.MyEnum);
}

TEST(TConfigTest, MissingSubconfig)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("my_string").Scalar("TestString")
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    config.Validate();

    EXPECT_EQ("TestString", config.MyString);
    EXPECT_EQ(100, config.Subconfig.MyInt);
    EXPECT_IS_FALSE(config.Subconfig.MyBool);
    EXPECT_EQ(0, config.Subconfig.MyStringList.ysize());
    EXPECT_EQ(ETestEnum::Value1, config.Subconfig.MyEnum);
}

TEST(TConfigTest, MissingRequiredParameter)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("sub").BeginMap()
                .Item("my_int").Scalar(99)
                .Item("my_bool").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    EXPECT_THROW(config.Load(~configNode->AsMap()), yexception);
}

TEST(TConfigTest, IncorrectNodeType)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("my_string").Scalar(1) // incorrect type
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    EXPECT_THROW(config.Load(~configNode->AsMap()), yexception);
}

TEST(TConfigTest, ArithmeticOverflow)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(~builder)
        .BeginMap()
            .Item("my_string").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Scalar(Max<i64>())
                .Item("my_bool").Scalar(true)
                .Item("my_enum").Scalar("Value2")
                .Item("my_string_list").BeginList()
                    .Item().Scalar("ListItem0")
                    .Item().Scalar("ListItem1")
                    .Item().Scalar("ListItem2")
                .EndList()
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
            .Item("my_string").Scalar("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Scalar(110) // out of range
                .Item("my_bool").Scalar(true)
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    TTestConfig config;
    config.Load(~configNode->AsMap());
    EXPECT_THROW(config.Validate(), yexception);
}

} // namespace NYT
