#include "document.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/core/yson/public.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT {
namespace NClickHouse {

using NYT::NYTree::INode;
using NYT::NYTree::INodePtr;
using NYT::NYTree::ENodeType;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const NInterop::TDocumentPath& path)
{
    return JoinSeq("/", path);
}

////////////////////////////////////////////////////////////////////////////////

bool IsMapNode(const INode& node)
{
    return node.GetType() == ENodeType::Map;
}

bool IsCompositeNode(const INode& node)
{
    const auto type = node.GetType();
    return (type == ENodeType::Map) || (type == ENodeType::List);
}

////////////////////////////////////////////////////////////////////////////////

TString SerializeData(const INodePtr& node)
{
    const auto yson = NYT::NYTree::ConvertToYsonString(node, NYT::NYson::EYsonFormat::Text);
    return yson.GetData();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// TODO: support list nodes

class TDocument
    : public NInterop::IDocument
{
private:
    INodePtr Root;

public:
    TDocument(INodePtr root)
        : Root(std::move(root))
    {
    }

    bool Has(const NInterop::TDocumentPath& path) const override;
    NInterop::IDocumentPtr GetSubDocument(const NInterop::TDocumentPath& path) const override;

    NInterop::TValue AsValue() const override;
    NInterop::TDocumentKeys ListKeys() const override;

    bool IsComposite() const;
    TString Serialize() const override;

private:
    INodePtr TryAccess(const NInterop::TDocumentPath& path) const;
    INodePtr Access(const NInterop::TDocumentPath& path) const;

    static NInterop::TValue ToValue(const INodePtr& node);
};

////////////////////////////////////////////////////////////////////////////////

INodePtr TDocument::TryAccess(const NInterop::TDocumentPath& path) const
{
    auto currentNode = Root;

    for (const auto& key : path) {
        if (!IsMapNode(*currentNode)) {
            return nullptr;
        }
        auto mapNode = currentNode->AsMap();
        auto childNode = mapNode->FindChild(key);
        if (!childNode) {
            return nullptr;
        }
        currentNode = childNode;
    }

    return currentNode;
}

INodePtr TDocument::Access(const NInterop::TDocumentPath& path) const
{
    auto node = TryAccess(path);
    if (!node) {
        THROW_ERROR_EXCEPTION("Requested node not found in document")
            << TErrorAttribute("path", JoinPath(path));
    }
    return node;
}

bool TDocument::Has(const NInterop::TDocumentPath& path) const
{
    auto node = TryAccess(path);
    return static_cast<bool>(node);
}

NInterop::IDocumentPtr TDocument::GetSubDocument(const NInterop::TDocumentPath& path) const
{
    auto node = Access(path);
    return CreateDocument(std::move(node));
}

NInterop::TValue TDocument::ToValue(const INodePtr& node)
{
    NInterop::TValue result;

    switch (node->GetType()) {
        case ENodeType::Int64:
            result.SetInt(node->AsInt64()->GetValue());
            return result;

        case ENodeType::Uint64:
            result.SetUInt(node->AsUint64()->GetValue());
            return result;

        case ENodeType::Boolean:
            result.SetBoolean(node->AsBoolean()->GetValue());
            return result;

        case ENodeType::Double:
            result.SetFloat(node->AsDouble()->GetValue());
            return result;

        case ENodeType::String:
            {
                auto& string = node->AsString()->GetValue();
                result.SetString(~string, +string);
                return result;
            }

        default:
            THROW_ERROR_EXCEPTION(
                "Cannot convert document node to value: node type %Qlv not supported", node->GetType())
                << TErrorAttribute("path", node->GetPath());
    }

    Y_UNREACHABLE();
}

NInterop::TValue TDocument::AsValue() const
{
    return ToValue(Root);
}

NInterop::TDocumentKeys TDocument::ListKeys() const
{
    auto mapNode = Root->AsMap();
    return mapNode->GetKeys();
}

bool TDocument::IsComposite() const
{
    return IsCompositeNode(*Root);
}

TString TDocument::Serialize() const
{
    return SerializeData(Root);
}

////////////////////////////////////////////////////////////////////////////////

NInterop::IDocumentPtr CreateDocument(INodePtr documentNode)
{
    return std::make_shared<TDocument>(std::move(documentNode));
}

} // namespace NClickHouse
} // namespace NYT
