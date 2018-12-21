#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void SyncYPathMultiset(
    const IYPathServicePtr& service,
    const TYPath& path,
    const std::vector<std::pair<TString, TYsonString>>& requests)
{
    auto multisetRequest = TYPathProxy::Multiset(path);
    for (const auto& request : requests) {
        auto req = multisetRequest->add_subrequests();
        req->set_key(request.first);
        req->set_value(request.second.GetData());
    }
    ExecuteVerb(service, multisetRequest)
        .Get()
        .ThrowOnError();
}

TEST(TYTreeTest, Multiset)
{
    std::vector<std::pair<TString, TYsonString>> children = {
        {"k1", TYsonString("v1")},
        {"k2", TYsonString("v2")},
        {"k3", TYsonString("v3")}
    };

    auto node = ConvertToNode(TYsonString("{}"));
    SyncYPathMultiset(node, "", children);
    SyncYPathMultiset(node, "/@", children);

    for (const auto& child : children) {
        EXPECT_EQ(
            node->AsMap()->GetChild(child.first)->AsString()->GetValue(),
            child.second.GetData());
    }

    const auto& attributes = node->Attributes();
    for (const auto& child : children) {
        EXPECT_EQ(attributes.GetYson(child.first).GetData(), child.second.GetData());
    }

    auto mutableAttributes = node->MutableAttributes();
    mutableAttributes->SetYson("map", BuildYsonStringFluently()
        .BeginMap()
            .Item("k").Value("v")
            .Item("k1").Value("other")
        .EndMap());

    SyncYPathMultiset(node, "/@map", children);

    auto attributeNodeMap = ConvertToNode(attributes.GetYson("map"))->AsMap();
    EXPECT_EQ(attributeNodeMap->GetChild("k")->AsString()->GetValue(), "v");
    for (const auto& child : children) {
        EXPECT_EQ(attributeNodeMap->GetChild(child.first)->AsString()->GetValue(), child.second.GetData());
    }

    auto listNode = ConvertToNode(TYsonString("[]"))->AsList();
    EXPECT_THROW(SyncYPathMultiset(listNode, "", children), std::exception);

    std::vector<std::pair<TString, TYsonString>> listChildren = {
        {"end", TYsonString("v1")},
        {"after:0", TYsonString("v2")},
        {"before:1", TYsonString("v3")}
    };

    EXPECT_THROW(SyncYPathMultiset(listNode, "", listChildren), std::exception);
    EXPECT_EQ(listNode->GetChildCount(), 0);

    auto emptyNode = ConvertToNode(TYsonString("{}"));
    std::vector<std::pair<TString, TYsonString>> childrenWithInvalidKey = {
        {"a", TYsonString("b")},
        {"", TYsonString("d")}
    };
    // Currently map-nodes can have empty keys.
    SyncYPathMultiset(emptyNode, "", childrenWithInvalidKey);
    EXPECT_THROW(SyncYPathMultiset(emptyNode, "/@", childrenWithInvalidKey), std::exception);

    const auto& emptyNodeAttributes = emptyNode->Attributes();
    EXPECT_EQ(emptyNodeAttributes.GetYson("a").GetData(), "b");
    EXPECT_FALSE(emptyNodeAttributes.Contains(""));

    for (const auto& child : childrenWithInvalidKey) {
        EXPECT_EQ(
            emptyNode->AsMap()->GetChild(child.first)->AsString()->GetValue(),
            child.second.GetData());
    }
}

TEST(TYTreeTest, TestBizzareKeys)
{
    // NOTE(asaitgalin): Keys are not escaped and not tokenized.
    std::vector<std::pair<TString, TYsonString>> children = {
        {"k1/k2/k3", TYsonString("v1")},
        {"/@a", TYsonString("v2")}
    };

    auto node = ConvertToNode(TYsonString("{}"));
    SyncYPathMultiset(node, "", children);
    for (const auto& child : children) {
        EXPECT_EQ(
            node->AsMap()->GetChild(child.first)->AsString()->GetValue(),
            child.second.GetData());
    }

    SyncYPathMultiset(node, "/@", children);
    const auto& attributes = node->Attributes();
    for (const auto& child : children) {
        EXPECT_EQ(attributes.GetYson(child.first).GetData(), child.second.GetData());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree

