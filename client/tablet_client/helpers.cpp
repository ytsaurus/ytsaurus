#include "helpers.h"

#include <yt/client/object_client/public.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NTabletClient {

using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

void ValidateTabletTransaction(const TTransactionId& transactionId)
{
    if (TypeFromId(transactionId) == EObjectType::NestedTransaction) {
        THROW_ERROR_EXCEPTION("Nested master transactions cannot be used for updating dynamic tables");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
