#include "stdafx.h"
#include "framework.h"

#include <core/ytree/yson_string.h>
#include <core/ytree/yson_stream.h>
#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>
#include <core/ytree/ypath_client.h>

namespace NYT {
namespace NYson {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonTest, ConvertToNode)
{
    Stroka yson = "{key=value; other_key=10}";
    auto node = NYT::NYTree::ConvertToNode(TYsonString(yson));

    ASSERT_NO_THROW(node->AsMap());
    ASSERT_THROW(node->AsList(), std::exception);
    EXPECT_EQ("key", node->AsMap()->GetKeys().front());

    EXPECT_EQ("{\"key\"=\"value\";\"other_key\"=10}",
              ConvertToYsonString(node, EYsonFormat::Text).Data());

    NYT::NYTree::INodePtr child;

    child = node->AsMap()->FindChild("key");
    for (auto format : EYsonFormat::GetDomainValues()) {
        EXPECT_EQ("value", ConvertTo<Stroka>(child));
        EXPECT_EQ("value", ConvertTo<Stroka>(ConvertToYsonString(child, format)));
    }
    EXPECT_EQ(ConvertTo<Stroka>(ConvertToYsonString(child)), "value");

    child = node->AsMap()->FindChild("other_key");
    for (auto format : EYsonFormat::GetDomainValues()) {
        EXPECT_EQ(10, ConvertTo<i32>(child));
        EXPECT_EQ(10, ConvertTo<i32>(ConvertToYsonString(child, format)));
    }
}

TEST(TYsonTest, ListFragment)
{
    Stroka yson = "{a=b};{c=d}";
    NYT::NYTree::INodePtr node;

    node = NYT::NYTree::ConvertToNode(TYsonString(yson, EYsonType::ListFragment));
    ASSERT_NO_THROW(node->AsList());
    EXPECT_EQ("[{\"a\"=\"b\"};{\"c\"=\"d\"}]",
              ConvertToYsonString(node, EYsonFormat::Text).Data());
}

TEST(TYsonTest, ConvertFromStream)
{
    Stroka yson = "{key=value}";
    TStringInput ysonStream(yson);

    auto node = ConvertToNode(&ysonStream);
    ASSERT_NO_THROW(node->AsMap());
    EXPECT_EQ("{\"key\"=\"value\"}",
              ConvertToYsonString(node, EYsonFormat::Text).Data());
}

TEST(TYsonTest, ConvertToProducerNode)
{
    // Make consumer
    TStringStream output;
    TYsonWriter writer(&output, EYsonFormat::Text);

    // Make producer
    auto ysonProducer = ConvertToProducer(TYsonString("{key=value}"));

    // Apply producer to consumer
    ysonProducer.Run(&writer);

    EXPECT_EQ("{\"key\"=\"value\"}", output.Str());
}

TEST(TYsonTest, ConvertToProducerListFragment)
{
    {
        auto producer = ConvertToProducer(TYsonString("{a=b}; {c=d}", EYsonType::ListFragment));
        EXPECT_EQ("{\"a\"=\"b\"};\n{\"c\"=\"d\"};\n",
            ConvertToYsonString(producer, EYsonFormat::Text).Data());
    }

    {
        auto producer = ConvertToProducer(TYsonString("{key=value}"));
        EXPECT_EQ("{\"key\"=\"value\"}",
            ConvertToYsonString(producer, EYsonFormat::Text).Data());
    }
}

TEST(TYsonTest, ConvertToForPodTypes)
{
    {
        auto node = ConvertToNode(42);
        EXPECT_EQ(42, ConvertTo<i32>(node));
        EXPECT_EQ(42, ConvertTo<i64>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("42", ysonStr.Data());
        EXPECT_EQ(42, ConvertTo<i32>(ysonStr));
        EXPECT_EQ(42, ConvertTo<i64>(ysonStr));
        EXPECT_EQ(42.0, ConvertTo<double>(ysonStr));
    }

    {
        auto node = ConvertToNode(0.1);
        EXPECT_EQ(0.1, ConvertTo<double>(node));

        auto ysonStr = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("0.1", ysonStr.Data());
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
        EXPECT_EQ("[1;2]", yson.Data());
    }

    {
        bool boolean = true;
        auto node = ConvertToNode(boolean);
        auto converted = ConvertTo<bool>(node);
        EXPECT_EQ(boolean, converted);
        auto yson = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ(EYsonType::Node, yson.GetType());
        EXPECT_EQ("%true", yson.Data());
    }

    EXPECT_EQ(ConvertTo<bool>("false"), false);
    EXPECT_EQ(ConvertTo<bool>(TYsonString("%false")), false);

    EXPECT_EQ(ConvertTo<bool>("true"), true);
    EXPECT_EQ(ConvertTo<bool>(TYsonString("%true")), true);
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

    auto res = UpdateNode(base, patch);

    EXPECT_EQ(
        "100",
        ConvertToYsonString(res->AsMap()->FindChild("key_a"), EYsonFormat::Text).Data());
    EXPECT_EQ(
        "<\"attr\"=\"some_attr\">0.",
        ConvertToYsonString(res->AsMap()->FindChild("key_b"), EYsonFormat::Text).Data());
    EXPECT_EQ(
        "70.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("ignat"), EYsonFormat::Text).Data());
    EXPECT_EQ(
        "75.",
        ConvertToYsonString(res->AsMap()->FindChild("key_c")->AsMap()->FindChild("max"), EYsonFormat::Text).Data());
    EXPECT_EQ(
        "{\"x\"=\"y\"}",
        ConvertToYsonString(res->AsMap()->FindChild("key_d"), EYsonFormat::Text).Data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYson
} // namespace NYT
