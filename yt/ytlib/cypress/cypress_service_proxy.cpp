#include "stdafx.h"
#include "cypress_service_proxy.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TYPath ObjectIdMarker = "#";
const TYPath TransactionIdMarker = "!";

////////////////////////////////////////////////////////////////////////////////

TYPath TCypressServiceProxy::GetObjectPath(const TObjectId& id)
{
    return ObjectIdMarker + "(" + id.ToString() + ")";
}

TYPath TCypressServiceProxy::GetTransactionPath(const TTransactionId& id)
{
    return
        id == NullTransactionId
        ? ""
        : TransactionIdMarker + "(" + id.ToString() + ")";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
