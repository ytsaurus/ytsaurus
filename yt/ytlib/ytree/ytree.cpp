#include "ytree.h"
#include "convert.h"
#include "node_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const ENodeType::EDomain TScalarTypeTraits<Stroka>::NodeType = ENodeType::String;
const ENodeType::EDomain TScalarTypeTraits<i64>::NodeType = ENodeType::Integer;
const ENodeType::EDomain TScalarTypeTraits<double>::NodeType = ENodeType::Double;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TYPath INode::GetPath() const
{
    return GetResolver()->GetPath(const_cast<INode*>(this));
}

INodePtr IMapNode::GetChild(const TStringBuf& key) const
{
    auto child = FindChild(key);
    if (!child) {
        ThrowNoSuchChildKey(this, Stroka(key));
    }
    return child;
}

INodePtr IListNode::GetChild(int index) const
{
    auto child = FindChild(index);
    if (!child) {
        ThrowNoSuchChildIndex(this, index);
    }
    return child;
}

int IListNode::AdjustAndValidateChildIndex(int index) const
{
    int adjustedIndex = index >= 0 ? index : index + GetChildCount();
    if (adjustedIndex < 0 || adjustedIndex > GetChildCount()) {
        ThrowNoSuchChildIndex(this, index);
    }
    return adjustedIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
