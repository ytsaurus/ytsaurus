#include "stdafx.h"
#include "cypress_service_proxy.h"

namespace NYT {
namespace NCypress {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TYPath ObjectIdMarker = "#";
const TYPath TransactionIdMarker = "!";
const TYPath SystemPath = "#0-0-0-0";

////////////////////////////////////////////////////////////////////////////////

TYPath FromObjectId(const TObjectId& id)
{
    return ObjectIdMarker + id.ToString();
}

TYPath WithTransaction(const TYPath& path, const TTransactionId& id)
{
    return TransactionIdMarker + id.ToString() + path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
