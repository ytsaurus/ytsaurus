#include "stdafx.h"
#include "cypress_ypath_proxy.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYPath FromObjectId(const TObjectId& id)
{
    return Stroka('#') + id.ToString().Quote();
}

TYPath WithTransaction(const TYPath& path, const TTransactionId& id)
{
    return
        id == NullTransactionId
        ? path
        : Stroka('!') + id.ToString().Quote() + path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

