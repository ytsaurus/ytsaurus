#include "helpers.h"

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/error.h>

namespace NYT::NTransactionClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool IsMasterTransactionId(TTransactionId id)
{
    auto type = TypeFromId(id);
    // NB: Externalized transactions are for internal use only.
    return type == NObjectClient::EObjectType::Transaction ||
           type == NObjectClient::EObjectType::NestedTransaction;
}

void ValidateTabletTransactionId(TTransactionId id)
{
    auto type = TypeFromId(id);
    if (type != EObjectType::Transaction &&
        type != EObjectType::AtomicTabletTransaction &&
        type != EObjectType::NonAtomicTabletTransaction)
    {
        THROW_ERROR_EXCEPTION("%v is not a valid tablet transaction id",
            id);
    }
}

void ValidateMasterTransactionId(TTransactionId id)
{
    auto type = TypeFromId(id);
    if (type != EObjectType::Transaction &&
        type != EObjectType::NestedTransaction)
    {
        THROW_ERROR_EXCEPTION("%v is not a valid master transaction id",
            id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
