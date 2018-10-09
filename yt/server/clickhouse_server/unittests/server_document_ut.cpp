#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/server/document.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NClickHouse {

using NYson::TYsonString;
using NYTree::ConvertToNode;
using NClickHouse::CreateDocument;
using NInterop::TDocumentKeys;

////////////////////////////////////////////////////////////////////////////////

TEST(TDocumentTest, NestedNodesAccess)
{
    TString yson = "{a={b={c={d=1}}}}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    EXPECT_TRUE(document->Has({"a", "b", "c"}));
    EXPECT_FALSE(document->Has({"a", "b", "d"}));
    EXPECT_THROW(document->GetValue({"a", "b", "d"}), NYT::TErrorException);
}

TEST(TDocumentTest, SupportedValueTypes)
{
    TString yson = "{a=-1; b=2u; c=%true; d=0.1; e=\"test\"}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    auto a = document->GetValue({"a"});
    EXPECT_TRUE(a.IsInt());
    EXPECT_EQ(-1, a.Int);

    auto b = document->GetValue({"b"});
    EXPECT_TRUE(b.IsUInt());
    EXPECT_EQ(2, b.UInt);

    auto c = document->GetValue({"c"});
    EXPECT_TRUE(c.IsBoolean());
    EXPECT_EQ(true, c.Boolean);

    auto d = document->GetValue({"d"});
    EXPECT_TRUE(d.IsFloat());
    EXPECT_TRUE(fabs(d.Float - 0.1) < 1e-5);

    auto e = document->GetValue({"e"});
    EXPECT_TRUE(e.IsString());
    EXPECT_EQ("test", e.AsStringBuf());
}

TEST(TDocumentTest, UnsupportedValueTypes)
{
    TString yson =  "{a={b=1}}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    EXPECT_TRUE(document->Has({"a"}));
    EXPECT_TRUE(document->GetSubDocument({"a"})->IsComposite());
    EXPECT_THROW(document->GetValue({"a"}), NYT::TErrorException);
}

TEST(TDocumentTest, ListKeys)
{
    TString yson = "{a={b={c={}; x=2}}}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    EXPECT_EQ(TDocumentKeys({"b"}), document->ListKeys({"a"}));
    EXPECT_EQ(TDocumentKeys({"c", "x"}), document->ListKeys({"a", "b"}));
    EXPECT_EQ(TDocumentKeys({}), document->ListKeys({"a", "b", "c"}));
}

TEST(TDocumentTest, CompositeNodes)
{
    TString yson = "{a={b={c={}; x=2}; y=4}}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    EXPECT_TRUE(document->GetSubDocument({"a", "b"})->IsComposite());
    EXPECT_TRUE(document->GetSubDocument({"a", "b", "c"})->IsComposite());
    EXPECT_FALSE(document->GetSubDocument({"a", "y"})->IsComposite());
    EXPECT_FALSE(document->GetSubDocument({"a", "b", "x"})->IsComposite());
}

TEST(TDocumentTest, Serialize)
{
    TString yson =  "{\"a\"={\"b\"=123;};}";
    auto node = ConvertToNode(TYsonString(yson));
    auto document = CreateDocument(std::move(node));

    EXPECT_EQ(yson, document->Serialize());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouse
} // NYT
