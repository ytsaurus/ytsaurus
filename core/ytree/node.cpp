#include "node.h"
#include "convert.h"
#include "node_detail.h"
#include "tree_visitor.h"

#include <yt/core/misc/cast.h>

#include <yt/core/yson/writer.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TString GetSetStringRepresentation(const THashSet<T>& set)
{
    TStringBuilder result;
    result.AppendString("{");
    bool first = true;
    for (const auto& element : set) {
        if (!first) {
            result.AppendString(", ");
        }
        result.AppendFormat("%Qlv", element);
        first = false;
    }
    result.AppendString("}");
    return result.Flush();
}

////////////////////////////////////////////////////////////////////////////////

THashSet<ENodeType> TScalarTypeTraits<TString>::GetValueSupportedTypes = {
    ENodeType::String,
};

THashSet<ENodeType> TScalarTypeTraits<TString>::SetValueSupportedTypes = {
    ENodeType::String,
};

TString TScalarTypeTraits<TString>::GetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<TString>::GetValueSupportedTypes);

TString TScalarTypeTraits<TString>::SetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<TString>::SetValueSupportedTypes);

const TString& TScalarTypeTraits<TString>::GetValue(const IConstNodePtr& node)
{
    ValidateNodeType(node, GetValueSupportedTypes, GetValueSupportedTypesStringRepresentation);
    return node->AsString()->GetValue();
}

void TScalarTypeTraits<TString>::SetValue(const INodePtr& node, const TString& value)
{
    ValidateNodeType(node, SetValueSupportedTypes, SetValueSupportedTypesStringRepresentation);
    node->AsString()->SetValue(value);
}

////////////////////////////////////////////////////////////////////////////////

THashSet<ENodeType> TScalarTypeTraits<i64>::GetValueSupportedTypes = {
    ENodeType::Int64,
    ENodeType::Uint64,
};

THashSet<ENodeType> TScalarTypeTraits<i64>::SetValueSupportedTypes = {
    ENodeType::Int64,
    ENodeType::Uint64,
    ENodeType::Double,
};

TString TScalarTypeTraits<i64>::GetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<i64>::GetValueSupportedTypes);

TString TScalarTypeTraits<i64>::SetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<i64>::SetValueSupportedTypes);

i64 TScalarTypeTraits<i64>::GetValue(const IConstNodePtr& node)
{
    ValidateNodeType(node, GetValueSupportedTypes, GetValueSupportedTypesStringRepresentation);
    switch (node->GetType()) {
        case ENodeType::Int64:
            return node->AsInt64()->GetValue();
        case ENodeType::Uint64:
            return CheckedIntegralCast<i64>(node->AsUint64()->GetValue());
        default:
            YT_ABORT();
    }
}

void TScalarTypeTraits<i64>::SetValue(const INodePtr& node, i64 value)
{
    ValidateNodeType(node, SetValueSupportedTypes, SetValueSupportedTypesStringRepresentation);
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
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

THashSet<ENodeType> TScalarTypeTraits<ui64>::GetValueSupportedTypes = {
    ENodeType::Int64,
    ENodeType::Uint64,
};

THashSet<ENodeType> TScalarTypeTraits<ui64>::SetValueSupportedTypes = {
    ENodeType::Int64,
    ENodeType::Uint64,
    ENodeType::Double,
};

TString TScalarTypeTraits<ui64>::GetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<ui64>::GetValueSupportedTypes);

TString TScalarTypeTraits<ui64>::SetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<ui64>::SetValueSupportedTypes);

ui64 TScalarTypeTraits<ui64>::GetValue(const IConstNodePtr& node)
{
    ValidateNodeType(node, GetValueSupportedTypes, GetValueSupportedTypesStringRepresentation);
    switch (node->GetType()) {
        case ENodeType::Uint64:
            return node->AsUint64()->GetValue();
        case ENodeType::Int64:
            return CheckedIntegralCast<ui64>(node->AsInt64()->GetValue());
        default:
            YT_ABORT();
    }
}

void TScalarTypeTraits<ui64>::SetValue(const INodePtr& node, ui64 value)
{
    ValidateNodeType(node, SetValueSupportedTypes, SetValueSupportedTypesStringRepresentation);
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
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

THashSet<ENodeType> TScalarTypeTraits<double>::GetValueSupportedTypes = {
    ENodeType::Double,
    ENodeType::Int64,
    ENodeType::Uint64,
};

THashSet<ENodeType> TScalarTypeTraits<double>::SetValueSupportedTypes = {
    ENodeType::Double,
};

TString TScalarTypeTraits<double>::GetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<double>::GetValueSupportedTypes);

TString TScalarTypeTraits<double>::SetValueSupportedTypesStringRepresentation =
    GetSetStringRepresentation(TScalarTypeTraits<double>::SetValueSupportedTypes);

double TScalarTypeTraits<double>::GetValue(const IConstNodePtr& node)
{
    ValidateNodeType(node, GetValueSupportedTypes, GetValueSupportedTypesStringRepresentation);
    switch (node->GetType()) {
        case ENodeType::Double:
            return node->AsDouble()->GetValue();
        case ENodeType::Int64:
            return static_cast<double>(node->AsInt64()->GetValue());
        case ENodeType::Uint64:
            return static_cast<double>(node->AsUint64()->GetValue());
        default:
            YT_ABORT();
    }
}

void TScalarTypeTraits<double>::SetValue(const INodePtr& node, double value)
{
    ValidateNodeType(node, SetValueSupportedTypes, SetValueSupportedTypesStringRepresentation);
    node->AsDouble()->SetValue(value);
}

////////////////////////////////////////////////////////////////////////////////

THashSet<ENodeType> TScalarTypeTraits<bool>::GetValueSupportedTypes = {
    ENodeType::Boolean,
};

THashSet<ENodeType> TScalarTypeTraits<bool>::SetValueSupportedTypes = {
    ENodeType::Boolean,
};

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
    auto optionalKey = FindChildKey(child);
    if (!optionalKey) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *optionalKey;
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
    auto optionalIndex = FindChildIndex(child);
    if (!optionalIndex) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
    return *optionalIndex;
}

int IListNode::AdjustChildIndexOrThrow(int index) const
{
    auto adjustedIndex = TryAdjustChildIndex(index, GetChildCount());
    if (!adjustedIndex) {
        ThrowNoSuchChildIndex(this, index);
    }
    return *adjustedIndex;
}

std::optional<int> TryAdjustChildIndex(int index, int childCount)
{
    int adjustedIndex = index >= 0 ? index : index + childCount;
    if (adjustedIndex < 0 || adjustedIndex >= childCount) {
        return std::nullopt;
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

} // namespace NYT::NYTree
