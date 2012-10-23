#include "stdafx.h"

#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/yson_stream.h>
#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/ypath_client.h>

#include <contrib/testing/framework.h>

////////////////////////////////////////////////////////////////////////////////

using NYT::NYTree::ConvertTo;
using NYT::NYTree::ConvertToNode;
using NYT::NYTree::ConvertToYsonString;
using NYT::NYTree::ConvertToProducer;
using NYT::NYTree::TYsonString;
using NYT::NYTree::TYsonInput;
using NYT::NYTree::TYsonWriter;
using NYT::NYTree::EYsonType;
using NYT::NYTree::EYsonFormat;
using NYT::NYTree::BuildYsonNodeFluently;

class TYsonTest: public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYsonTest, ConvertToNode)
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
    FOREACH (auto format, EYsonFormat::GetDomainValues()) {
        EXPECT_EQ("value", ConvertTo<Stroka>(child));
        EXPECT_EQ("value", ConvertTo<Stroka>(ConvertToYsonString(child, format)));
    }
    EXPECT_EQ(ConvertTo<Stroka>(ConvertToYsonString(child)), "value");

    child = node->AsMap()->FindChild("other_key");
    FOREACH (auto format, EYsonFormat::GetDomainValues()) {
        EXPECT_EQ(10, ConvertTo<i32>(child));
        EXPECT_EQ(10, ConvertTo<i32>(ConvertToYsonString(child, format)));
    }
}

TEST_F(TYsonTest, ListFragment)
{
    Stroka yson = "{a=b};{c=d}";
    NYT::NYTree::INodePtr node;

    node = NYT::NYTree::ConvertToNode(TYsonString(yson, EYsonType::ListFragment));
    ASSERT_NO_THROW(node->AsList());
    EXPECT_EQ("[{\"a\"=\"b\"};{\"c\"=\"d\"}]",
              ConvertToYsonString(node, EYsonFormat::Text).Data());
}

TEST_F(TYsonTest, ConvertFromStream)
{
    Stroka yson = "{key=value}";
    TStringInput ysonStream(yson);

    auto node = ConvertToNode(&ysonStream);
    ASSERT_NO_THROW(node->AsMap());
    EXPECT_EQ("{\"key\"=\"value\"}",
              ConvertToYsonString(node, EYsonFormat::Text).Data());
}

TEST_F(TYsonTest, ConvertToProducerNode)
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

TEST_F(TYsonTest, ConvertToProducerListFragment)
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

TEST_F(TYsonTest, ConvertToForPODTypes)
{
    {
        auto node = ConvertToNode(42);
        EXPECT_EQ(42, ConvertTo<i32>(node));
        EXPECT_EQ("42", ConvertToYsonString(node, EYsonFormat::Text).Data());
    }

    {
        std::vector<i32> numbers;
        numbers.push_back(1);
        numbers.push_back(2);
        auto node = ConvertToNode(numbers);
        auto converted = ConvertTo< std::vector<i32> >(node);
        EXPECT_EQ(numbers, converted);
        auto yson = ConvertToYsonString(node, EYsonFormat::Text);
        EXPECT_EQ("[1;2]", yson.Data());
        EXPECT_EQ(EYsonType::Node, yson.GetType());
        EXPECT_EQ("[1;2]", yson.Data());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYsonTest, UpdateNodes)
{
    auto base = BuildYsonNodeFluently()
        .BeginMap()
            .Item("key_a")
            .Scalar(0)

            .Item("key_b")
            .BeginAttributes()
                .Item("attr")
                .Scalar("some_attr")
            .EndAttributes()
            .Scalar(3.0)

            .Item("key_c")
            .BeginMap()
                .Item("ignat")
                .Scalar(70.0)
            .EndMap()
        .EndMap();

    auto patch = BuildYsonNodeFluently()
        .BeginMap()
            .Item("key_a")
            .Scalar(100)

            .Item("key_b")
            .Scalar(0.0)

            .Item("key_c")
            .BeginMap()
                .Item("max")
                .Scalar(75.0)
            .EndMap()

            .Item("key_d")
            .BeginMap()
                .Item("x")
                .Scalar("y")
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
