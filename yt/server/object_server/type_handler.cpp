#include "stdafx.h"
#include "type_handler.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TTypeCreationOptions::TTypeCreationOptions(
    EObjectTransactionMode transactionMode,
    EObjectAccountMode accountMode)
    : TransactionMode(transactionMode)
    , AccountMode(accountMode)
{ }

////////////////////////////////////////////////////////////////////////////////

TObjectBase* IObjectTypeHandler::GetObject(const TObjectId& id)
{
    auto* object = FindObject(id);
    YCHECK(object);
    return object;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

