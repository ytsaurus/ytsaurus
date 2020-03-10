#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>
#include <yt/core/yson/stream.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT::NYson {
namespace {

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

    EXPECT_EQ("/submap/other_key", node->AsMap()->GetChild("submap")->AsMap()->GetChild("other_key")->GetPath());
}

TYPED_TEST(TYsonTypedTest, SetNodeByYPath)
{
    auto node = NYT::NYTree::ConvertToNode(TypeParam("{}"));
    ForceYPath(node, "/submap/other_key");

    auto submap = node->AsMap()->GetChild("submap")->AsMap();
    EXPECT_EQ(0, submap->GetChildCount());

    auto value = NYT::NYTree::ConvertToNode(TypeParam("4"));

    SetNodeByYPath(node, "/submap/other_key", value);
    submap = node->AsMap()->GetChild("submap")->AsMap();
    EXPECT_EQ(4, ConvertTo<int>(submap->GetChild("other_key")));
}

TYPED_TEST(TYsonTypedTest, ConvertToNode)
{
    TString yson = "{key=value; other_key=10}";
    auto node = NYT::NYTree::ConvertToNode(TypeParam(yson));

    ASSERT_NO_THROW(node->AsMap());
    ASSERT_THROW(node->AsList(), std::exception);
    EXPECT_EQ("key", node->AsMap()->GetKeys().front());

    EXPECT_EQ("{\"key\"=\"value\";\"other_key\"=10;}",
              ConvertToYsonString(node, EYsonFormat::Text).GetData());

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
              ConvertToYsonString(node, EYsonFormat::Text).GetData());
}

TYPED_TEST(TYsonTypedTest, ConvertFromStream)
{
    TString yson = "{key=value}";
    TStringInput ysonStream(yson);

    auto node = ConvertToNode(&ysonStream);
    ASSERT_NO_THROW(node->AsMap());
    EXPECT_EQ("{\"key\"=\"value\";}",
              ConvertToYsonString(node, EYsonFormat::Text).GetData());
}

TYPED_TEST(TYsonTypedTest, ConvertToProducerNode)
{
    // Make consumer
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Text);

    // Make producer
    auto ysonProducer = ConvertToProducer(TypeParam("{key=value}"));

    // Apply producer to consumer
    ysonProducer.Run(&writer);

    EXPECT_EQ("{\"key\"=\"value\";}", output.Str());
}

TYPED_TEST(TYsonTypedTest, ConvertToProducerListFragment)
{
    {
        auto producer = ConvertToProducer(TypeParam("{a=b}; {c=d}", EYsonType::ListFragment));
        EXPECT_EQ("{\"a\"=\"b\";};\n{\"c\"=\"d\";};\n",
            ConvertToYsonString(producer, EYsonFormat::Text).GetData());
    }

    {
        auto producer = ConvertToProducer(TypeParam("{key=value}"));
        EXPECT_EQ("{\"key\"=\"value\";}",
            ConvertToYsonString(producer, EYsonFormat::Text).GetData());
    }
}

TYPED_TEST(TYsonTypedTest, ConvertToForPodTypes)
{
    {
        auto node = ConvertToNode(42);
        EXPECT_EQ(42, ConvertTo<i32>(node));
        EXPECT_EQ(42, ConvertTo<i64>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("42", ysonStr.GetData());
        EXPECT_EQ(42, ConvertTo<i32>(ysonStr));
        EXPECT_EQ(42, ConvertTo<i64>(ysonStr));
        EXPECT_EQ(42.0, ConvertTo<double>(ysonStr));
    }

    {
        auto node = ConvertToNode(0.1);
        EXPECT_EQ(0.1, ConvertTo<double>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("0.1", ysonStr.GetData());
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
        EXPECT_EQ("[1;2;]", yson.GetData());
    }

    {
        bool boolean = true;
        auto node = ConvertToNode(boolean);
        auto converted = ConvertTo<bool>(node);
        EXPECT_EQ(boolean, converted);
        auto yson = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ(EYsonType::Node, yson.GetType());
        EXPECT_EQ("%true", yson.GetData());
    }

    EXPECT_EQ(ConvertTo<bool>("false"), false);
    EXPECT_EQ(ConvertTo<bool>(TypeParam("%false")), false);

    EXPECT_EQ(ConvertTo<bool>("true"), true);
    EXPECT_EQ(ConvertTo<bool>(TypeParam("%true")), true);
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
        ConvertToYsonString(res->AsMap()->FindChild("key_a"), EYsonFormat::Text).GetData());
    EXPECT_EQ(
        "<\"attr\"=\"some_attr\";>0.",
        ConvertToYsonString(res->AsMap()->FindChild("key_b"), EYsonFormat::Text).GetData());
    EXPECT_EQ(
        "70.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("ignat"), EYsonFormat::Text).GetData());
    EXPECT_EQ(
        "75.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("max"), EYsonFormat::Text).GetData());
    EXPECT_EQ(
        "{\"x\"=\"y\";}",
        ConvertToYsonString(res->AsMap()->FindChild("key_d"), EYsonFormat::Text).GetData());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonTest, TYsonStringTypesConversion)
{
    auto ysonString = TYsonString("{x=y;z=1}");
    auto getData = [] (const TYsonStringBuf& ysonStringBuf) {
        return ysonStringBuf.GetData();
    };
    auto getType = [] (const TYsonStringBuf& ysonStringBuf) {
        return ysonStringBuf.GetType();
    };

    // We expect these functions to cast arguments implicitly.
    EXPECT_EQ(getData(ysonString), ysonString.GetData());
    EXPECT_EQ(getType(ysonString), ysonString.GetType());
}

TEST(TYsonTest, TYsonStringTypesHashing)
{
    auto ysonString = TYsonString("{x=y;z=1}");
    auto ysonStringBuf = TYsonStringBuf("{x=y;z=1}");
    EXPECT_EQ(THash<TYsonString>()(ysonString), THash<TYsonStringBuf>()(ysonStringBuf));
}

TEST(TYsonTest, TYsonStringTypesComparisons)
{
    auto ysonString = TYsonString("{x=y;z=1}");
    auto ysonStringBuf = TYsonStringBuf("{x=y;z=1}");
    EXPECT_EQ(ysonString, ysonString);
    EXPECT_EQ(ysonStringBuf, ysonStringBuf);
    EXPECT_EQ(ysonString, ysonStringBuf);
    EXPECT_EQ(ysonStringBuf, ysonString);

    auto otherYsonString = TYsonString("{x=z;y=1}");
    auto otherYsonStringBuf = TYsonStringBuf("{x=z;y=1}");
    EXPECT_NE(ysonString, otherYsonStringBuf);
    EXPECT_NE(ysonStringBuf, otherYsonStringBuf);
    EXPECT_NE(ysonString, otherYsonStringBuf);
    EXPECT_NE(ysonStringBuf, otherYsonString);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYson
