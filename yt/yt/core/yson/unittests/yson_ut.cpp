#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>
#include <yt/core/yson/stream.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/misc/serialize.h>

namespace NYT::NYson {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

using TYsonStringTypes = ::testing::Types<TYsonString, TYsonStringBuf>;

template <typename T>
class TYsonTypedTest
    : public ::testing::Test
{ };

TYPED_TEST_SUITE(TYsonTypedTest, TYsonStringTypes);

TYPED_TEST(TYsonTypedTest, GetYPath)
{
    TString yson = "{key=value; submap={ other_key=other_value; }}";
    auto node = NYT::NYTree::ConvertToNode(TypeParam(yson));

    EXPECT_EQ("/submap/other_key", node->AsMap()->GetChildOrThrow("submap")->AsMap()->GetChildOrThrow("other_key")->GetPath());
}

TYPED_TEST(TYsonTypedTest, SetNodeByYPath)
{
    auto node = NYT::NYTree::ConvertToNode(TypeParam(TStringBuf("{}")));
    ForceYPath(node, "/submap/other_key");

    auto submap = node->AsMap()->GetChildOrThrow("submap")->AsMap();
    EXPECT_EQ(0, submap->GetChildCount());

    auto value = NYT::NYTree::ConvertToNode(TypeParam(TStringBuf("4")));

    SetNodeByYPath(node, "/submap/other_key", value);
    submap = node->AsMap()->GetChildOrThrow("submap")->AsMap();
    EXPECT_EQ(4, ConvertTo<int>(submap->GetChildOrThrow("other_key")));
}

TYPED_TEST(TYsonTypedTest, RemoveNodeByYPathMap)
{
    auto node = NYT::NYTree::ConvertToNode(TypeParam(TStringBuf("{x={y={z=1}}}")));
    EXPECT_EQ(true, RemoveNodeByYPath(node, "/x/y/z"));

    auto submap = node->AsMap()->GetChildOrThrow("x")->AsMap()->GetChildOrThrow("y")->AsMap();
    EXPECT_EQ(0, submap->GetChildCount());
}

TYPED_TEST(TYsonTypedTest, RemoveNodeByYPathList)
{
    auto node = NYT::NYTree::ConvertToNode(TypeParam(TStringBuf("{x={y=[1]}}")));
    EXPECT_EQ(true, RemoveNodeByYPath(node, "/x/y/0"));

    auto sublist = node->AsMap()->GetChildOrThrow("x")->AsMap()->GetChildOrThrow("y")->AsList();
    EXPECT_EQ(0, sublist->GetChildCount());
}

TYPED_TEST(TYsonTypedTest, RemoveNodeByYPathInvalid)
{
    auto node = NYT::NYTree::ConvertToNode(TypeParam(TStringBuf("{map={a=1};list=[1]}")));
    auto nodeCopy = CloneNode(node);
    EXPECT_EQ(false, RemoveNodeByYPath(node, "/map/b"));
    EXPECT_THROW(RemoveNodeByYPath(node, "/map/a/1"), std::exception);
    EXPECT_EQ(false, RemoveNodeByYPath(node, "/map/1"));
    EXPECT_EQ(false, RemoveNodeByYPath(node, "/list/1"));
    EXPECT_THROW(RemoveNodeByYPath(node, "/list/a"), std::exception);
    EXPECT_THROW(RemoveNodeByYPath(node, "/list/0/a"), std::exception);
    EXPECT_TRUE(AreNodesEqual(node, nodeCopy));
}

TYPED_TEST(TYsonTypedTest, ConvertToNode)
{
    TString yson = "{key=value; other_key=10}";
    auto node = NYT::NYTree::ConvertToNode(TypeParam(yson));

    ASSERT_NO_THROW(node->AsMap());
    ASSERT_THROW(node->AsList(), std::exception);
    EXPECT_EQ("key", node->AsMap()->GetKeys().front());

    EXPECT_EQ("{\"key\"=\"value\";\"other_key\"=10;}",
        ConvertToYsonString(node, EYsonFormat::Text).AsStringBuf());

    NYT::NYTree::INodePtr child;

    child = node->AsMap()->FindChild("key");
    for (auto format : TEnumTraits<EYsonFormat>::GetDomainValues()) {
        EXPECT_EQ("value", ConvertTo<TString>(child));
        EXPECT_EQ("value", ConvertTo<TString>(ConvertToYsonString(child, format)));
    }
    EXPECT_EQ(ConvertTo<TString>(ConvertToYsonString(child)), "value");

    child = node->AsMap()->FindChild("other_key");
    for (auto format : TEnumTraits<EYsonFormat>::GetDomainValues()) {
        EXPECT_EQ(10, ConvertTo<i32>(child));
        EXPECT_EQ(10, ConvertTo<i32>(ConvertToYsonString(child, format)));
    }
}

TYPED_TEST(TYsonTypedTest, ListFragment)
{
    TString yson = "{a=b};{c=d}";
    NYT::NYTree::INodePtr node;

    node = NYT::NYTree::ConvertToNode(TypeParam(yson, EYsonType::ListFragment));
    ASSERT_NO_THROW(node->AsList());
    EXPECT_EQ("[{\"a\"=\"b\";};{\"c\"=\"d\";};]",
        ConvertToYsonString(node, EYsonFormat::Text).AsStringBuf());
}

TYPED_TEST(TYsonTypedTest, ConvertFromStream)
{
    TString yson = "{key=value}";
    TStringInput ysonStream(yson);

    auto node = ConvertToNode(&ysonStream);
    ASSERT_NO_THROW(node->AsMap());
    EXPECT_EQ("{\"key\"=\"value\";}",
        ConvertToYsonString(node, EYsonFormat::Text).AsStringBuf());
}

TYPED_TEST(TYsonTypedTest, ConvertToProducerNode)
{
    // Make consumer
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Text);

    // Make producer
    auto ysonProducer = ConvertToProducer(TypeParam(TStringBuf("{key=value}")));

    // Apply producer to consumer
    ysonProducer.Run(&writer);

    EXPECT_EQ("{\"key\"=\"value\";}", output.Str());
}

TYPED_TEST(TYsonTypedTest, ConvertToProducerListFragment)
{
    {
        auto producer = ConvertToProducer(TypeParam(TStringBuf("{a=b}; {c=d}"), EYsonType::ListFragment));
        EXPECT_EQ("{\"a\"=\"b\";};\n{\"c\"=\"d\";};\n",
            ConvertToYsonString(producer, EYsonFormat::Text).AsStringBuf());
    }

    {
        auto producer = ConvertToProducer(TypeParam(TStringBuf("{key=value}")));
        EXPECT_EQ("{\"key\"=\"value\";}",
            ConvertToYsonString(producer, EYsonFormat::Text).AsStringBuf());
    }
}

TYPED_TEST(TYsonTypedTest, ConvertToForPodTypes)
{
    {
        auto node = ConvertToNode(42);
        EXPECT_EQ(42, ConvertTo<i32>(node));
        EXPECT_EQ(42, ConvertTo<i64>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("42", ysonStr.AsStringBuf());
        EXPECT_EQ(42, ConvertTo<i32>(ysonStr));
        EXPECT_EQ(42, ConvertTo<i64>(ysonStr));
        EXPECT_EQ(42.0, ConvertTo<double>(ysonStr));
    }

    {
        auto node = ConvertToNode(0.1);
        EXPECT_EQ(0.1, ConvertTo<double>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("0.1", ysonStr.AsStringBuf());
        EXPECT_EQ(0.1, ConvertTo<double>(ysonStr));
    }

    {
        std::vector<i32> numbers;
        numbers.push_back(1);
        numbers.push_back(2);
        auto node = ConvertToNode(numbers);
        auto converted = ConvertTo< std::vector<i32> >(node);
        EXPECT_EQ(numbers, converted);
        auto yson = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ(EYsonType::Node, yson.GetType());
        EXPECT_EQ("[1;2;]", yson.AsStringBuf());
    }

    {
        bool boolean = true;
        auto node = ConvertToNode(boolean);
        auto converted = ConvertTo<bool>(node);
        EXPECT_EQ(boolean, converted);
        auto yson = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ(EYsonType::Node, yson.GetType());
        EXPECT_EQ("%true", yson.AsStringBuf());
    }

    EXPECT_EQ(ConvertTo<bool>("false"), false);
    EXPECT_EQ(ConvertTo<bool>(TypeParam(TStringBuf("%false"))), false);

    EXPECT_EQ(ConvertTo<bool>("true"), true);
    EXPECT_EQ(ConvertTo<bool>(TypeParam(TStringBuf("%true"))), true);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonTest, UpdateNodes)
{
    auto base = BuildYsonNodeFluently()
        .BeginMap()
            .Item("key_a")
            .Value(0)

            .Item("key_b")
            .BeginAttributes()
                .Item("attr")
                .Value("some_attr")
            .EndAttributes()
            .Value(3.0)

            .Item("key_c")
            .BeginMap()
                .Item("ignat")
                .Value(70.0)
            .EndMap()
        .EndMap();

    auto patch = BuildYsonNodeFluently()
        .BeginMap()
            .Item("key_a")
            .Value(100)

            .Item("key_b")
            .Value(0.0)

            .Item("key_c")
            .BeginMap()
                .Item("max")
                .Value(75.0)
            .EndMap()

            .Item("key_d")
            .BeginMap()
                .Item("x")
                .Value("y")
            .EndMap()
        .EndMap();

    auto res = PatchNode(base, patch);

    EXPECT_EQ(
        "100",
        ConvertToYsonString(res->AsMap()->FindChild("key_a"), EYsonFormat::Text).AsStringBuf());
    EXPECT_EQ(
        "<\"attr\"=\"some_attr\";>0.",
        ConvertToYsonString(res->AsMap()->FindChild("key_b"), EYsonFormat::Text).AsStringBuf());
    EXPECT_EQ(
        "70.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("ignat"), EYsonFormat::Text).AsStringBuf());
    EXPECT_EQ(
        "75.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("max"), EYsonFormat::Text).AsStringBuf());
    EXPECT_EQ(
        "{\"x\"=\"y\";}",
        ConvertToYsonString(res->AsMap()->FindChild("key_d"), EYsonFormat::Text).AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonTest, TYsonStringTypesConversion)
{
    auto ysonString = TYsonString(TStringBuf(("{x=y;z=1}")));
    auto AsStringBuf = [] (const TYsonStringBuf& ysonStringBuf) {
        return ysonStringBuf.AsStringBuf();
    };
    auto getType = [] (const TYsonStringBuf& ysonStringBuf) {
        return ysonStringBuf.GetType();
    };

    // We expect these functions to cast arguments implicitly.
    EXPECT_EQ(AsStringBuf(ysonString), ysonString.AsStringBuf());
    EXPECT_EQ(getType(ysonString), ysonString.GetType());
}

TEST(TYsonTest, TYsonStringTypesHashing)
{
    auto ysonString = TYsonString(TStringBuf("{x=y;z=1}"));
    auto ysonStringBuf = TYsonStringBuf(TStringBuf("{x=y;z=1}"));
    EXPECT_EQ(THash<TYsonString>()(ysonString), THash<TYsonStringBuf>()(ysonStringBuf));
}

TEST(TYsonTest, TYsonStringTypesComparisons)
{
    auto ysonString = TYsonString(TStringBuf("{x=y;z=1}"));
    auto ysonStringBuf = TYsonStringBuf(TStringBuf("{x=y;z=1}"));
    EXPECT_EQ(ysonString, ysonString);
    EXPECT_EQ(ysonStringBuf, ysonStringBuf);
    EXPECT_EQ(ysonString, ysonStringBuf);
    EXPECT_EQ(ysonStringBuf, ysonString);

    auto otherYsonString = TYsonString(TStringBuf("{x=z;y=1}"));
    auto otherYsonStringBuf = TYsonStringBuf(TStringBuf("{x=z;y=1}"));
    EXPECT_NE(ysonString, otherYsonStringBuf);
    EXPECT_NE(ysonStringBuf, otherYsonStringBuf);
    EXPECT_NE(ysonString, otherYsonStringBuf);
    EXPECT_NE(ysonStringBuf, otherYsonString);
}

TEST(TYsonTest, TYsonStringFromStringBuf)
{
    auto stringBuf = TStringBuf("test");
    auto ysonString = TYsonString(stringBuf);
    EXPECT_EQ(stringBuf, ysonString.AsStringBuf());
    EXPECT_EQ(stringBuf, ysonString.ToString());
}

////////////////////////////////////////////////////////////////////////////////

class TYsonStringSerializationTest
    : public ::testing::TestWithParam<TYsonString>
{ };

TEST_P(TYsonStringSerializationTest, Do)
{
    auto str = GetParam();
    TString buffer;
    TStringOutput output(buffer);
    TStreamSaveContext saveContext(&output);
    Save(saveContext, str);
    TStringInput input(buffer);
    TStreamLoadContext loadContext(&input);
    EXPECT_EQ(str, Load<TYsonString>(loadContext));
}

INSTANTIATE_TEST_SUITE_P(
    Do,
    TYsonStringSerializationTest,
    ::testing::ValuesIn({
        TYsonString(), // null
        TYsonString(TStringBuf("test")),
        TYsonString(TStringBuf("1;2;3"), EYsonType::ListFragment),
        TYsonString(TStringBuf("a=1;b=2;c=3"), EYsonType::MapFragment),
    }));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
