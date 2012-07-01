#include "stdafx.h"

#include <ytlib/ytree/attributes.h>
#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/yson_string.h>

#include <contrib/testing/framework.h>

////////////////////////////////////////////////////////////////////////////////

using NYT::Null;
using NYT::NYTree::IAttributeDictionary;
using NYT::NYTree::CreateEphemeralAttributes;

class TAttributesTest: public ::testing::Test
{
protected:
    virtual void SetUp()
    { }

    virtual void TearDown()
    { }
};


namespace {

bool IsEqual (
    const IAttributeDictionary& lhs,
    const IAttributeDictionary& rhs)
{
    if (lhs.List() != rhs.List()) {
        return false;
    }
    FOREACH (const auto& key, lhs.List()) {
        auto value = rhs.FindYson(key);
        if (!value) {
            return false;
        }
    }
    return true;
}

} //anonymous namespace

TEST_F(TAttributesTest, CheckAccessors)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<Stroka>("name", "Petr");
    attributes->Set<int>("age", 30);
    attributes->Set<double>("weight", 70.5);

    Stroka keys[] = {"name", "age", "weight"};
    EXPECT_EQ(yhash_set<Stroka>(keys, keys + 3), attributes->List());

    EXPECT_EQ("Petr", attributes->Get<Stroka>("name"));
    EXPECT_THROW(attributes->Get<int>("name"), yexception);

    EXPECT_EQ(30, attributes->Find<int>("age"));
    EXPECT_EQ(30, attributes->Get<int>("age"));
    EXPECT_THROW(attributes->Get<char>("age"), yexception);

    EXPECT_EQ(70.5, attributes->Get<double>("weight"));
    EXPECT_THROW(attributes->Get<Stroka>("weight"), yexception);

    EXPECT_FALSE(attributes->Find<int>("unknown_key"));
    EXPECT_EQ(42, attributes->Get<int>("unknown_key", 42));
    EXPECT_THROW(attributes->Get<double>("unknown_key"), yexception);
}


TEST_F(TAttributesTest, MergeFromTest)
{
    auto attributesX = CreateEphemeralAttributes();
    attributesX->Set<Stroka>("name", "Petr");
    attributesX->Set<int>("age", 30);

    auto attributesY = CreateEphemeralAttributes();
    attributesY->Set<Stroka>("name", "Oleg");

    attributesX->MergeFrom(*attributesY);
    EXPECT_EQ("Oleg", attributesX->Get<Stroka>("name"));
    EXPECT_EQ(30, attributesX->Get<int>("age"));

    NYT::NYTree::INodePtr node = ConvertToNode(NYT::NYTree::TYsonString("{age=20}"));
    attributesX->MergeFrom(node->AsMap());
    EXPECT_EQ("Oleg", attributesX->Get<Stroka>("name"));
    EXPECT_EQ(20, attributesX->Get<int>("age"));
}

TEST_F(TAttributesTest, SerializeToNode)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<Stroka>("name", "Petr");
    attributes->Set<int>("age", 30);

    auto node = ConvertToNode(*attributes);
    auto convertedAttributes = ConvertToAttributes(node);
    EXPECT_TRUE(IsEqual(*attributes, *convertedAttributes));
}

TEST_F(TAttributesTest, SerializeToProto)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set<Stroka>("name", "Petr");
    attributes->Set<int>("age", 30);

    NYT::NYTree::NProto::TAttributes protoAttributes;
    NYT::NYTree::ToProto(&protoAttributes, *attributes);
    auto convertedAttributes = NYT::NYTree::FromProto(protoAttributes);
    EXPECT_TRUE(IsEqual(*attributes, *convertedAttributes));
}
