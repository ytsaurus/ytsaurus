#include "stdafx.h"
#include "type_handler.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TTypeCreationOptions::TTypeCreationOptions()
    : StagingSupported(false)
{ }

TTypeCreationOptions::TTypeCreationOptions(
    EObjectTransactionMode transactionMode,
    EObjectAccountMode accountMode,
    bool supportsStaging)
    : TransactionMode(transactionMode)
    , AccountMode(accountMode)
    , StagingSupported(supportsStaging)
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

