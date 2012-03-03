#include "stdafx.h"
#include "cypress_ypath_proxy.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TYPath ObjectIdMarker = "#";
const TYPath TransactionIdMarker = "!";

////////////////////////////////////////////////////////////////////////////////

TYPath FromObjectId(const TObjectId& id)
{
    return ObjectIdMarker + id.ToString();
}

TYPath WithTransaction(const TYPath& path, const TTransactionId& id)
{
    return
        id == NullTransactionId
        ? path
        : TransactionIdMarker + id.ToString() + path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

