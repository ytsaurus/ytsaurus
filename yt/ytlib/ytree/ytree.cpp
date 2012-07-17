#include "ytree.h"

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
