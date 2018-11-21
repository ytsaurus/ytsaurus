#include "node.h"
#include "convert.h"
#include "node_detail.h"
#include "tree_visitor.h"

#include <yt/core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const ENodeType TScalarTypeTraits<TString>::NodeType = ENodeType::String;
const ENodeType TScalarTypeTraits<i64>::NodeType = ENodeType::Int64;
const ENodeType TScalarTypeTraits<ui64>::NodeType = ENodeType::Uint64;
const ENodeType TScalarTypeTraits<double>::NodeType = ENodeType::Double;
const ENodeType TScalarTypeTraits<bool>::NodeType = ENodeType::Boolean;

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
    VisitTree(&value, consumer, true /*stable*/, Null /*attributeKeys*/);
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
        Null);
    writer.Flush();
    return TYsonString(stream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
