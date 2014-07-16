#include "stdafx.h"
#include "framework.h"

#include <core/ytree/yson_serializable.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/fluent.h>
#include <core/ytree/tree_visitor.h>
#include <core/ytree/ypath_client.h>

#include <core/yson/writer.h>

namespace NYT {
namespace NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETestEnum,
    (Value0)
    (Value1)
    (Value2)
);

////////////////////////////////////////////////////////////////////////////////

struct TTestSubconfig
    : public TYsonSerializable
{
    int MyInt;
    bool MyBool;
    std::vector<Stroka> MyStringList;
    ETestEnum MyEnum;

    TTestSubconfig()
    {
        RegisterParameter("my_int", MyInt).Default(100).InRange(95, 205);
        RegisterParameter("my_bool", MyBool).Default(false);
        RegisterParameter("my_string_list", MyStringList).Default();
        RegisterParameter("my_enum", MyEnum).Default(ETestEnum::Value1);
    }
};

typedef TIntrusivePtr<TTestSubconfig> TTestSubconfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TTestConfig
    : public TYsonSerializable
{
public:
    Stroka MyString;
    TTestSubconfigPtr Subconfig;
    std::vector<TTestSubconfigPtr> SubconfigList;
    yhash_map<Stroka, TTestSubconfigPtr> SubconfigMap;

    TTestConfig()
    {
        RegisterParameter("my_string", MyString).NonEmpty();
        RegisterParameter("sub", Subconfig).DefaultNew();
        RegisterParameter("sub_list", SubconfigList).Default();
        RegisterParameter("sub_map", SubconfigMap).Default();

        RegisterInitializer([&] () {
            MyString = "x";
            Subconfig->MyInt = 200;
        });
    }
};

typedef TIntrusivePtr<TTestConfig> TTestConfigPtr;

////////////////////////////////////////////////////////////////////////////////

void TestCompleteSubconfig(TTestSubconfig* subconfig)
{
    EXPECT_EQ(99, subconfig->MyInt);
    EXPECT_TRUE(subconfig->MyBool);
    EXPECT_EQ(3, subconfig->MyStringList.size());
    EXPECT_EQ("ListItem0", subconfig->MyStringList[0]);
    EXPECT_EQ("ListItem1", subconfig->MyStringList[1]);
    EXPECT_EQ("ListItem2", subconfig->MyStringList[2]);
    EXPECT_EQ(ETestEnum::Value2, subconfig->MyEnum);
}

TEST(TYsonSerializableTest, Complete)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(99)
                .Item("my_bool").Value(true)
                .Item("my_enum").Value("value2")
                .Item("my_string_list").BeginList()
                    .Item().Value("ListItem0")
                    .Item().Value("ListItem1")
                    .Item().Value("ListItem2")
                .EndList()
            .EndMap()
            .Item("sub_list").BeginList()
                .Item().BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
            .EndList()
            .Item("sub_map").BeginMap()
                .Item("sub1").BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
                .Item("sub2").BeginMap()
                    .Item("my_int").Value(99)
                    .Item("my_bool").Value(true)
                    .Item("my_enum").Value("value2")
                    .Item("my_string_list").BeginList()
                        .Item().Value("ListItem0")
                        .Item().Value("ListItem1")
                        .Item().Value("ListItem2")
                    .EndList()
                .EndMap()
            .EndMap()
        .EndMap();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    TestCompleteSubconfig(config->Subconfig.Get());
    EXPECT_EQ(2, config->SubconfigList.size());
    TestCompleteSubconfig(config->SubconfigList[0].Get());
    TestCompleteSubconfig(config->SubconfigList[1].Get());
    EXPECT_EQ(2, config->SubconfigMap.size());
    auto it1 = config->SubconfigMap.find("sub1");
    EXPECT_FALSE(it1 == config->SubconfigMap.end());
    TestCompleteSubconfig(it1->second.Get());
    auto it2 = config->SubconfigMap.find("sub2");
    EXPECT_FALSE(it2 == config->SubconfigMap.end());
    TestCompleteSubconfig(it2->second.Get());
}

TEST(TYsonSerializableTest, MissingParameter)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_bool").Value(true)
            .EndMap()
        .EndMap();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    EXPECT_EQ(200, config->Subconfig->MyInt);
    EXPECT_TRUE(config->Subconfig->MyBool);
    EXPECT_EQ(0, config->Subconfig->MyStringList.size());
    EXPECT_EQ(ETestEnum::Value1, config->Subconfig->MyEnum);
    EXPECT_EQ(0, config->SubconfigList.size());
    EXPECT_EQ(0, config->SubconfigMap.size());
}

TEST(TYsonSerializableTest, MissingSubconfig)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
        .EndMap();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap());

    EXPECT_EQ("TestString", config->MyString);
    EXPECT_EQ(200, config->Subconfig->MyInt);
    EXPECT_FALSE(config->Subconfig->MyBool);
    EXPECT_EQ(0, config->Subconfig->MyStringList.size());
    EXPECT_EQ(ETestEnum::Value1, config->Subconfig->MyEnum);
    EXPECT_EQ(0, config->SubconfigList.size());
    EXPECT_EQ(0, config->SubconfigMap.ysize());
}

TEST(TYsonSerializableTest, Options)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
        .EndMap();

    auto config = New<TTestConfig>();
    config->SetKeepOptions(true);
    config->Load(configNode->AsMap());

    auto optionsNode = config->GetOptions();
    EXPECT_EQ(1, optionsNode->GetChildCount());
    for (const auto& pair : optionsNode->GetChildren()) {
        const auto& name = pair.first;
        auto child = pair.second;
        EXPECT_EQ("option", name);
        EXPECT_EQ(1, child->AsInt64()->GetValue());
    }
}

TEST(TYsonSerializableTest, MissingRequiredParameter)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("sub").BeginMap()
                .Item("my_int").Value(99)
                .Item("my_bool").Value(true)
            .EndMap()
        .EndMap();

    auto config = New<TTestConfig>();
    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
}

TEST(TYsonSerializableTest, IncorrectNodeType)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value(1) // incorrect type
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
}

TEST(TYsonSerializableTest, ArithmeticOverflow)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(Max<i64>())
                .Item("my_bool").Value(true)
                .Item("my_enum").Value("Value2")
                .Item("my_string_list").BeginList()
                    .Item().Value("ListItem0")
                    .Item().Value("ListItem1")
                    .Item().Value("ListItem2")
                .EndList()
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
}

TEST(TYsonSerializableTest, Validate)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("") // empty!
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    config->Load(configNode, false);
    EXPECT_THROW(config->Validate(), std::exception);
}

TEST(TYsonSerializableTest, ValidateSubconfig)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub").BeginMap()
                .Item("my_int").Value(210) // out of range
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap(), false);
    EXPECT_THROW(config->Validate(), std::exception);
}

TEST(TYsonSerializableTest, ValidateSubconfigList)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub_list").BeginList()
                .Item().BeginMap()
                    .Item("my_int").Value(210) // out of range
                .EndMap()
            .EndList()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap(), false);
    EXPECT_THROW(config->Validate(), std::exception);
}

TEST(TYsonSerializableTest, ValidateSubconfigMap)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("sub_map").BeginMap()
                .Item("sub").BeginMap()
                    .Item("my_int").Value(210) // out of range
                .EndMap()
            .EndMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap(), false);
    EXPECT_THROW(config->Validate(), std::exception);
}

TEST(TYsonSerializableTest, Save)
{
    auto config = New<TTestConfig>();

    // add non-default fields;
    config->MyString = "hello!";
    config->SubconfigList.push_back(New<TTestSubconfig>());
    config->SubconfigMap["item"] = New<TTestSubconfig>();

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    Stroka subconfigYson =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=200;"
        "\"my_string_list\"=[]}";

    Stroka subconfigYsonOrigin =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=100;"
        "\"my_string_list\"=[]}";

    Stroka expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"sub\"=" + subconfigYson + ";";
    expectedYson += "\"sub_list\"=[" + subconfigYsonOrigin + "];";
    expectedYson += "\"sub_map\"={\"item\"=" + subconfigYsonOrigin + "}}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.Data()))));
}

TEST(TYsonSerializableTest, TestConfigUpdate)
{
    auto config = New<TTestConfig>();
    {
        auto newConfig = UpdateYsonSerializable(config, 0);
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
    }

    {
        auto newConfig = UpdateYsonSerializable(config, ConvertToNode(TYsonString("{\"sub\"={\"my_int\"=150}}")));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 150);
    }

    {
        auto newConfig = UpdateYsonSerializable(config, ConvertToNode(TYsonString("{\"sub\"={\"my_int_\"=150}}")));
        EXPECT_EQ(newConfig->Subconfig->MyInt, 200);
    }
}

TEST(TYsonSerializableTest, NoDefaultNewAliasing)
{
    auto config1 = New<TTestConfig>();
    auto config2 = New<TTestConfig>();
    EXPECT_NE(config1->Subconfig, config2->Subconfig);
}

TEST(TYsonSerializableTest, Reconfigure)
{
    auto config = New<TTestConfig>();
    auto subconfig = config->Subconfig;

    EXPECT_EQ("x", config->MyString);
    EXPECT_EQ(200, subconfig->MyInt);

    auto configNode1 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("y")
        .EndMap();
    ReconfigureYsonSerializable(config, configNode1);

    EXPECT_EQ("y", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(200, subconfig->MyInt);

    auto configNode2 = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("z")
            .Item("sub").BeginMap()
                .Item("my_int").Value(95)
            .EndMap()
        .EndMap();
    ReconfigureYsonSerializable(config, configNode2);

    EXPECT_EQ("z", config->MyString);
    EXPECT_EQ(subconfig, config->Subconfig);
    EXPECT_EQ(95, subconfig->MyInt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYTree
} // namespace NYT
