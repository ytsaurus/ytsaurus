#include "node.h"
#include "convert.h"
#include "node_detail.h"
#include "tree_visitor.h"

#include <yt/core/misc/cast.h>

#include <yt/core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {


////////////////////////////////////////////////////////////////////////////////

const TString& TScalarTypeTraits<TString>::GetValue(const IConstNodePtr& node)
{
    return node->AsString()->GetValue();
}

void TScalarTypeTraits<TString>::SetValue(const INodePtr& node, const TString& value)
{
    node->AsString()->SetValue(value);
}

////////////////////////////////////////////////////////////////////////////////

i64 TScalarTypeTraits<i64>::GetValue(const IConstNodePtr& node)
{
    switch (node->GetType()) {
        case ENodeType::Int64:
            return node->AsInt64()->GetValue();
        case ENodeType::Uint64:
            return CheckedIntegralCast<i64>(node->AsUint64()->GetValue());
        default:
            ThrowInvalidNodeType(node, ENodeType::Int64, node->GetType());
            Y_UNREACHABLE();
    }
}

void TScalarTypeTraits<i64>::SetValue(const INodePtr& node, i64 value)
{
    switch (node->GetType()) {
        case ENodeType::Int64:
            node->AsInt64()->SetValue(value);
            break;
        case ENodeType::Uint64:
            node->AsUint64()->SetValue(CheckedIntegralCast<ui64>(value));
            break;
        case ENodeType::Double:
            node->AsDouble()->SetValue(static_cast<double>(value));
            break;
        default:
            ThrowInvalidNodeType(node, ENodeType::Int64, node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

ui64 TScalarTypeTraits<ui64>::GetValue(const IConstNodePtr& node)
{
    switch (node->GetType()) {
        case ENodeType::Uint64:
            return node->AsUint64()->GetValue();
        case ENodeType::Int64:
            return CheckedIntegralCast<ui64>(node->AsInt64()->GetValue());
        default:
            ThrowInvalidNodeType(node, ENodeType::Uint64, node->GetType());
            Y_UNREACHABLE();
    }
}

void TScalarTypeTraits<ui64>::SetValue(const INodePtr& node, ui64 value)
{
    switch (node->GetType()) {
        case ENodeType::Uint64:
            node->AsUint64()->SetValue(value);
            break;
        case ENodeType::Int64:
            node->AsInt64()->SetValue(CheckedIntegralCast<i64>(value));
            break;
        case ENodeType::Double:
            node->AsDouble()->SetValue(static_cast<double>(value));
            break;
        default:
            ThrowInvalidNodeType(node, ENodeType::Uint64, node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

double TScalarTypeTraits<double>::GetValue(const IConstNodePtr& node)
{
    switch (node->GetType()) {
        case ENodeType::Double:
            return node->AsDouble()->GetValue();
        case ENodeType::Int64:
            return static_cast<double>(node->AsInt64()->GetValue());
        case ENodeType::Uint64:
            return static_cast<double>(node->AsUint64()->GetValue());
        default:
            ThrowInvalidNodeType(node, ENodeType::Double, node->GetType());
            Y_UNREACHABLE();
    }
}

void TScalarTypeTraits<double>::SetValue(const INodePtr& node, double value)
{
    node->AsDouble()->SetValue(value);
}

////////////////////////////////////////////////////////////////////////////////

bool TScalarTypeTraits<bool>::GetValue(const IConstNodePtr& node)
{
    return node->AsBoolean()->GetValue();
}

void TScalarTypeTraits<bool>::SetValue(const INodePtr& node, bool value)
{
    node->AsBoolean()->SetValue(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

INodePtr IMapNode::GetChild(const TString& key) const
{
    auto child = FindChild(key);
    if (!child) {
        ThrowNoSuchChildKey(this, key);
    }
    return child;
}

TString IMapNode::GetChildKeyOrThrow(const IConstNodePtr& child)
{
    auto maybeKey = FindChildKey(child);
    if (!maybeKey) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *maybeKey;
}

////////////////////////////////////////////////////////////////////////////////

INodePtr IListNode::GetChild(int index) const
{
    auto child = FindChild(index);
    if (!child) {
        ThrowNoSuchChildIndex(this, index);
    }
    return child;
}

int IListNode::GetChildIndexOrThrow(const IConstNodePtr& child)
{
    auto maybeIndex = FindChildIndex(child);
    if (!maybeIndex) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *maybeIndex;
}

int IListNode::AdjustChildIndex(int index) const
{
    int adjustedIndex = index >= 0 ? index : index + GetChildCount();
    if (adjustedIndex < 0 || adjustedIndex >= GetChildCount()) {
        ThrowNoSuchChildIndex(this, index);
    }
    return adjustedIndex;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(INode& value, IYsonConsumer* consumer)
{
    VisitTree(&value, consumer, true /*stable*/, std::nullopt /*attributeKeys*/);
}

void Deserialize(INodePtr& value, const INodePtr& node)
{
    value = node;
}

#define DESERIALIZE_TYPED(type) \
    void Deserialize(I##type##NodePtr& value, const INodePtr& node) \
    { \
        value = node->As##type(); \
    }

DESERIALIZE_TYPED(String)
DESERIALIZE_TYPED(Int64)
DESERIALIZE_TYPED(Uint64)
DESERIALIZE_TYPED(Double)
DESERIALIZE_TYPED(Boolean)
DESERIALIZE_TYPED(Map)
DESERIALIZE_TYPED(List)
DESERIALIZE_TYPED(Entity)

#undef DESERIALIZE_TYPED

TYsonString ConvertToYsonStringStable(const INodePtr& node)
{
    TStringStream stream;
    TBufferedBinaryYsonWriter writer(&stream);
    VisitTree(
        node,
        &writer,
        true, // truth matters :)
        std::nullopt);
    writer.Flush();
    return TYsonString(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
