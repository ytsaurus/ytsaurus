#include "helpers.h"

#include <yt/client/object_client/helpers.h>

namespace NYT::NTransactionClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool IsMasterTransactionId(TTransactionId id)
{
    auto type = NObjectClient::TypeFromId(id);
    return type == NObjectClient::EObjectType::Transaction ||
           type == NObjectClient::EObjectType::NestedTransaction;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
