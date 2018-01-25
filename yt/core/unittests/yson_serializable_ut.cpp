#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/writer.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/tree_visitor.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/yson_serializable.h>

#include <array>

namespace NYT {
namespace NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestEnum,
    (Value0)
    (Value1)
    (Value2)
);

////////////////////////////////////////////////////////////////////////////////

struct TTestSubconfig
    : public TYsonSerializable
{
    int MyInt;
    unsigned int MyUint;
    bool MyBool;
    std::vector<TString> MyStringList;
    ETestEnum MyEnum;

    TTestSubconfig()
    {
        RegisterParameter("my_int", MyInt).Default(100).InRange(95, 205);
        RegisterParameter("my_uint", MyUint).Default(50).InRange(31, 117);
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
    TString MyString;
    TTestSubconfigPtr Subconfig;
    std::vector<TTestSubconfigPtr> SubconfigList;
    std::unordered_map<TString, TTestSubconfigPtr> SubconfigMap;
    TNullable<i64> NullableInt;

    TTestConfig()
    {
        SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

        RegisterParameter("my_string", MyString).NonEmpty();
        RegisterParameter("sub", Subconfig).DefaultNew();
        RegisterParameter("sub_list", SubconfigList).Default();
        RegisterParameter("sub_map", SubconfigMap).Default();
        RegisterParameter("nullable_int", NullableInt).Default(Null);

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
    EXPECT_EQ(101, subconfig->MyUint);
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
                .Item("my_uint").Value(101)
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
                    .Item("my_uint").Value(101)
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
                    .Item("my_uint").Value(101)
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
                    .Item("my_uint").Value(101)
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
                    .Item("my_uint").Value(101)
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
    EXPECT_EQ(0, config->SubconfigMap.size());
}

TEST(TYsonSerializableTest, UnrecognizedSimple)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
        .EndMap();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap());

    auto unrecognizedNode = config->GetUnrecognized();
    auto unrecognizedRecursivelyNode = config->GetUnrecognizedRecursively();
    EXPECT_TRUE(AreNodesEqual(unrecognizedNode, unrecognizedRecursivelyNode));
    EXPECT_EQ(1, unrecognizedNode->GetChildCount());
    for (const auto& pair : unrecognizedNode->GetChildren()) {
        const auto& name = pair.first;
        auto child = pair.second;
        EXPECT_EQ("option", name);
        EXPECT_EQ(1, child->AsInt64()->GetValue());
    }

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
    auto deserializedConfig = ConvertTo<TTestConfigPtr>(output);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(config), ConvertToNode(deserializedConfig)));
}

TEST(TYsonSerializableTest, UnrecognizedRecursive)
{
    auto configNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("my_string").Value("TestString")
            .Item("option").Value(1)
            .Item("sub").BeginMap()
                .Item("sub_option").Value(42)
            .EndMap()
        .EndMap();

    auto config = New<TTestConfig>();
    config->Load(configNode->AsMap());

    auto unrecognizedRecursivelyNode = config->GetUnrecognizedRecursively();
    EXPECT_EQ(2, unrecognizedRecursivelyNode->GetChildCount());
    for (const auto& pair : unrecognizedRecursivelyNode->GetChildren()) {
        const auto& name = pair.first;
        auto child = pair.second;
        if (name == "option") {
            EXPECT_EQ(1, child->AsInt64()->GetValue());
        } else {
            EXPECT_EQ("sub", name);
            EXPECT_EQ(42, child->AsMap()->GetChild("sub_option")->AsInt64()->GetValue());
        }
    }

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);
    auto deserializedConfig = ConvertTo<TTestConfigPtr>(output);
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(config), ConvertToNode(deserializedConfig)));
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
    config->NullableInt = 42;

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    TString subconfigYson =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=200;"
        "\"my_uint\"=50u;"
        "\"my_string_list\"=[]}";

    TString subconfigYsonOrigin =
        "{\"my_bool\"=%false;"
        "\"my_enum\"=\"value1\";"
        "\"my_int\"=100;"
        "\"my_uint\"=50u;"
        "\"my_string_list\"=[]}";

    TString expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"sub\"=" + subconfigYson + ";";
    expectedYson += "\"sub_list\"=[" + subconfigYsonOrigin + "];";
    expectedYson += "\"sub_map\"={\"item\"=" + subconfigYsonOrigin + "};";
    expectedYson += "\"nullable_int\"=42}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.GetData()))));
}

TEST(TYsonSerializableTest, TestConfigUpdate)
{
    auto config = New<TTestConfig>();
    {
        auto newConfig = UpdateYsonSerializable(config, nullptr);
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

class TTestConfigLite
    : public TYsonSerializableLite
{
public:
    TString MyString;
    TNullable<i64> NullableInt;

    TTestConfigLite()
    {
        RegisterParameter("my_string", MyString).NonEmpty();
        RegisterParameter("nullable_int", NullableInt).Default(Null);
    }
};

TEST(TYsonSerializableTest, SaveLite)
{
    TTestConfigLite config;

    config.MyString = "hello!";
    config.NullableInt = 42;

    auto output = ConvertToYsonString(config, NYson::EYsonFormat::Text);

    TString expectedYson;
    expectedYson += "{\"my_string\"=\"hello!\";";
    expectedYson += "\"nullable_int\"=42}";

    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(TYsonString(expectedYson)),
        ConvertToNode(TYsonString(output.GetData()))));
}

////////////////////////////////////////////////////////////////////////////////

class TTestConfigWithAliases
    : public TYsonSerializable
{
public:
    TString Value;

    TTestConfigWithAliases()
    {
        RegisterParameter("key", Value)
            .Alias("alias1")
            .Alias("alias2");
    }
};

TEST(TYsonSerializableTest, Aliases1)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST(TYsonSerializableTest, Aliases2)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST(TYsonSerializableTest, Aliases3)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value")
            .Item("alias2").Value("value")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();
    config->Load(configNode->AsMap(), false);

    EXPECT_EQ("value", config->Value);
}

TEST(TYsonSerializableTest, Aliases4)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
            .Item("alias1").Value("value1")
            .Item("alias2").Value("value2")
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();

    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
}

TEST(TYsonSerializableTest, Aliases5)
{
    auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
    builder->BeginTree();
    BuildYsonFluently(builder.get())
        .BeginMap()
        .EndMap();
    auto configNode = builder->EndTree();

    auto config = New<TTestConfigWithAliases>();

    EXPECT_THROW(config->Load(configNode->AsMap()), std::exception);
}

TEST(TYsonSerializableTest, ParameterTuplesAndContainers)
{
    class TTestClass
        : public NYTree::TYsonSerializableLite
    {
    public:
        std::vector<TString> Vector;
        std::array<TString, 3> Array;
        std::pair<size_t, TString> Pair;
        std::set<TString> Set;
        std::map<TString, int> Map;
        std::multiset<int> MultiSet;
        std::unordered_set<TString> UnorderedSet;
        std::unordered_map<TString, int> UnorderedMap;
        std::unordered_multiset<size_t> UnorderedMultiSet;

        TTestClass()
        {
            RegisterParameter("vector", Vector)
                .Default();
            RegisterParameter("array", Array)
                .Default();
            RegisterParameter("pair", Pair)
                .Default();
            RegisterParameter("set", Set)
                .Default();
            RegisterParameter("map", Map)
                .Default();
            RegisterParameter("multiset", MultiSet)
                .Default();
            RegisterParameter("unordered_set", UnorderedSet)
                .Default();
            RegisterParameter("unordered_map", UnorderedMap)
                .Default();
            RegisterParameter("unordered_multiset", UnorderedMultiSet)
                .Default();
        }
    };

    TTestClass original, deserialized;

    original.Vector = { "fceswf", "sadfcesa" };
    original.Array = {{ "UYTUY", ":LL:a", "78678678" }};
    original.Pair = { 7U, "UYTUY" };
    original.Set = { "  q!", "12343e", "svvr", "0001" };
    original.Map = { {"!", 4398}, {"zzz", 0} };
    original.MultiSet = { 33, 33, 22, 22, 11 };
    original.UnorderedSet = { "41", "52", "001", "set" };
    original.UnorderedMap = { {"12345", 8}, {"XXX", 9}, {"XYZ", 42} };
    original.UnorderedMultiSet = { 1U, 2U, 1U, 0U, 0U };

    Deserialize(deserialized, ConvertToNode(ConvertToYsonStringStable(original)));

    EXPECT_EQ(original.Vector, deserialized.Vector);
    EXPECT_EQ(original.Array, deserialized.Array);
    EXPECT_EQ(original.Pair, deserialized.Pair);
    EXPECT_EQ(original.Set, deserialized.Set);
    EXPECT_EQ(original.Map, deserialized.Map);
    EXPECT_EQ(original.MultiSet, deserialized.MultiSet);
    EXPECT_EQ(original.UnorderedSet, deserialized.UnorderedSet);
    EXPECT_EQ(original.UnorderedMap, deserialized.UnorderedMap);
    EXPECT_EQ(original.UnorderedMultiSet, deserialized.UnorderedMultiSet);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSerializableTest, EnumAsKeyToYHash)
{
    THashMap<ETestEnum, TString> deserialized, original = {
        {ETestEnum::Value0, "abc"}
    };

    TString serialized = "{\"value0\"=\"abc\";}";
    ASSERT_EQ(serialized, ConvertToYsonString(original, EYsonFormat::Text).GetData());

    Deserialize(deserialized, ConvertToNode(TYsonString(serialized, EYsonType::Node)));

    ASSERT_EQ(original, deserialized);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYTree
} // namespace NYT
