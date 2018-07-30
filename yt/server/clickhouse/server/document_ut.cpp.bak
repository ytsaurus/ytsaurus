#include "document.h"

#include <library/unittest/registar.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NClickHouse {

using NYT::NYson::TYsonString;
using NYT::NYTree::ConvertToNode;
using NYT::NClickHouse::CreateDocument;
using NInterop::TDocumentKeys;

// to expand macros from library/unittest
using ::TStringBuilder;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDocumentTests)
{
    Y_UNIT_TEST(NestedNodesAccess)
    {
        TString yson = "{a={b={c={d=1}}}}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        UNIT_ASSERT(document->Has({"a", "b", "c"}));
        UNIT_ASSERT(!document->Has({"a", "b", "d"}));
        UNIT_ASSERT_EXCEPTION(document->GetValue({"a", "b", "d"}), NYT::TErrorException);
    }

    Y_UNIT_TEST(SupportedValueTypes)
    {
        TString yson = "{a=-1; b=2u; c=%true; d=0.1; e=\"test\"}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        auto a = document->GetValue({"a"});
        UNIT_ASSERT(a.IsInt());
        UNIT_ASSERT_EQUAL(a.Int, -1);

        auto b = document->GetValue({"b"});
        UNIT_ASSERT(b.IsUInt());
        UNIT_ASSERT_EQUAL(b.UInt, 2);

        auto c = document->GetValue({"c"});
        UNIT_ASSERT(c.IsBoolean());
        UNIT_ASSERT_EQUAL(c.Boolean, true);

        auto d = document->GetValue({"d"});
        UNIT_ASSERT(d.IsFloat());
        UNIT_ASSERT(fabs(d.Float - 0.1) < 1e-5);

        auto e = document->GetValue({"e"});
        UNIT_ASSERT(e.IsString());
        UNIT_ASSERT_EQUAL(e.AsStringBuf(), "test");
    }

    Y_UNIT_TEST(UnsupportedValueTypes)
    {
        TString yson =  "{a={b=1}}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        UNIT_ASSERT(document->Has({"a"}));
        UNIT_ASSERT(document->GetSubDocument({"a"})->IsComposite());
        UNIT_ASSERT_EXCEPTION(document->GetValue({"a"}), NYT::TErrorException);
    }

    Y_UNIT_TEST(ListKeys)
    {
        TString yson = "{a={b={c={}; x=2}}}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        UNIT_ASSERT_EQUAL(document->ListKeys({"a"}), TDocumentKeys({"b"}));
        UNIT_ASSERT_EQUAL(document->ListKeys({"a", "b"}), TDocumentKeys({"c", "x"}));
        UNIT_ASSERT_EQUAL(document->ListKeys({"a", "b", "c"}), TDocumentKeys({}));
    }

    Y_UNIT_TEST(CompositeNodes)
    {
        TString yson = "{a={b={c={}; x=2}; y=4}}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        UNIT_ASSERT(document->GetSubDocument({"a", "b"})->IsComposite());
        UNIT_ASSERT(document->GetSubDocument({"a", "b", "c"})->IsComposite());
        UNIT_ASSERT(!document->GetSubDocument({"a", "y"})->IsComposite());
        UNIT_ASSERT(!document->GetSubDocument({"a", "b", "x"})->IsComposite());
    }

    Y_UNIT_TEST(Serialize)
    {
        TString yson =  "{\"a\"={\"b\"=123;};}";
        auto node = ConvertToNode(TYsonString(yson));
        auto document = CreateDocument(std::move(node));

        UNIT_ASSERT_EQUAL(document->Serialize(), yson);
    }
}

} // namespace NClickHouse
} // NYT
