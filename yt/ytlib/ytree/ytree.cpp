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

} // namespace NYTree
} // namespace NYT
