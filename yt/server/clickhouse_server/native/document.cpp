#include "document.h"

#include <yt/core/yson/public.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <util/string/join.h>
#include <util/string/split.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using NYTree::INode;
using NYTree::INodePtr;
using NYTree::ENodeType;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TDocumentPath& path)
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
    const auto yson = NYTree::ConvertToYsonString(node, NYson::EYsonFormat::Text);
    return yson.GetData();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

// TODO: support list nodes

class TDocument
    : public IDocument
{
private:
    INodePtr Root;

public:
    TDocument(INodePtr root)
        : Root(std::move(root))
    {
    }

    bool Has(const TDocumentPath& path) const override;
    IDocumentPtr GetSubDocument(const TDocumentPath& path) const override;

    TValue AsValue() const override;
    TDocumentKeys ListKeys() const override;

    bool IsComposite() const;
    TString Serialize() const override;

private:
    INodePtr TryAccess(const TDocumentPath& path) const;
    INodePtr Access(const TDocumentPath& path) const;

    static TValue ToValue(const INodePtr& node);
};

////////////////////////////////////////////////////////////////////////////////

INodePtr TDocument::TryAccess(const TDocumentPath& path) const
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

INodePtr TDocument::Access(const TDocumentPath& path) const
{
    auto node = TryAccess(path);
    if (!node) {
        THROW_ERROR_EXCEPTION("Requested node not found in document")
            << TErrorAttribute("path", JoinPath(path));
    }
    return node;
}

bool TDocument::Has(const TDocumentPath& path) const
{
    auto node = TryAccess(path);
    return static_cast<bool>(node);
}

IDocumentPtr TDocument::GetSubDocument(const TDocumentPath& path) const
{
    auto node = Access(path);
    return CreateDocument(std::move(node));
}

TValue TDocument::ToValue(const INodePtr& node)
{
    TValue result;

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
                result.SetString(string.data(), string.size());
                return result;
            }

        default:
            THROW_ERROR_EXCEPTION(
                "Cannot convert document node to value: node type %Qlv not supported", node->GetType())
                << TErrorAttribute("path", node->GetPath());
    }

    Y_UNREACHABLE();
}

TValue TDocument::AsValue() const
{
    return ToValue(Root);
}

TDocumentKeys TDocument::ListKeys() const
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

IDocumentPtr CreateDocument(INodePtr documentNode)
{
    return std::make_shared<TDocument>(std::move(documentNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
