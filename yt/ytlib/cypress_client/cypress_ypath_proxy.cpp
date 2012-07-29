#include "stdafx.h"
#include "cypress_ypath_proxy.h"

namespace NYT {
namespace NCypressClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYPath FromObjectId(const TObjectId& id)
{
    return Stroka('#') + EscapeYPathToken(id.ToString());
}

TYPath WithTransaction(const TYPath& path, const TTransactionId& id)
{
    return
        id == NullTransactionId
        ? path
        : Stroka('!') + EscapeYPathToken(id.ToString()) + path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT

