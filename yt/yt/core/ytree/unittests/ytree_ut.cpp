#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void SyncYPathMultisetAttributes(
    const IYPathServicePtr& service,
    const TYPath& path,
    const std::vector<std::pair<TString, TYsonString>>& requests)
{
    auto multisetAttributesRequest = TYPathProxy::MultisetAttributes(path);
    for (const auto& request : requests) {
        auto req = multisetAttributesRequest->add_subrequests();
        req->set_attribute(request.first);
        req->set_value(request.second.ToString());
    }
    ExecuteVerb(service, multisetAttributesRequest)
        .Get()
        .ThrowOnError();
}

TEST(TYTreeTest, TestMultisetAttributes)
{
    std::vector<std::pair<TString, TYsonString>> attributes1 = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"k2", TYsonString(TStringBuf("v2"))},
        {"k3", TYsonString(TStringBuf("v3"))}
    };

    std::vector<std::pair<TString, TYsonString>> attributes2 = {
        {"k2", TYsonString(TStringBuf("v4"))},
        {"k3", TYsonString(TStringBuf("v5"))},
        {"k4", TYsonString(TStringBuf("v6"))}
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    SyncYPathMultisetAttributes(node, "/@", attributes1);

    EXPECT_EQ(node->Attributes().ListKeys().size(), 3);
    for (const auto& [attribute, value] : attributes1) {
        EXPECT_EQ(node->Attributes().GetYson(attribute).ToString(), value.ToString());
    }

    SyncYPathMultisetAttributes(node, "/@", attributes2);
    EXPECT_EQ(node->Attributes().ListKeys().size(), 4);
    for (const auto& [attribute, value] : attributes2) {
        EXPECT_EQ(node->Attributes().GetYson(attribute).ToString(), value.ToString());
    }
    EXPECT_EQ(node->Attributes().GetYson("k1").ToString(), "v1");

}

TEST(TYTreeTest, TestMultisetInvalidAttributes)
{
    std::vector<std::pair<TString, TYsonString>> validAttributes = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"k2", TYsonString(TStringBuf("v2"))},
        {"k3", TYsonString(TStringBuf("v3"))}
    };
    std::vector<std::pair<TString, TYsonString>> invalidAttributes = {
        {"k1", TYsonString(TStringBuf("v1"))},
        {"",   TYsonString(TStringBuf("v2"))}, // Empty attributes are not allowed.
        {"k3", TYsonString(TStringBuf("v3"))}
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    EXPECT_THROW(SyncYPathMultisetAttributes(node, "", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/@/", validAttributes), std::exception);
    EXPECT_THROW(SyncYPathMultisetAttributes(node, "/@", invalidAttributes), std::exception);
}

TEST(TYTreeTest, TestMultisetAttributesByPath)
{
    std::vector<std::pair<TString, TYsonString>> attributes1 = {
        {"a", TYsonString(TStringBuf("{}"))},
    };
    std::vector<std::pair<TString, TYsonString>> attributes2 = {
        {"a/b", TYsonString(TStringBuf("v1"))},
        {"a/c", TYsonString(TStringBuf("v2"))},
    };
    std::vector<std::pair<TString, TYsonString>> attributes3 = {
        {"b", TYsonString(TStringBuf("v3"))},
        {"c", TYsonString(TStringBuf("v4"))},
    };

    auto node = ConvertToNode(TYsonString(TStringBuf("{}")));

    SyncYPathMultisetAttributes(node, "/@", attributes1);
    SyncYPathMultisetAttributes(node, "/@a", attributes3);

    auto attribute = ConvertToNode(node->Attributes().GetYson("a"))->AsMap();
    EXPECT_EQ(attribute->GetKeys(), std::vector<TString>({"b", "c"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree

