#include "stdafx.h"
#include "transaction.h"

#include <core/rpc/client.h>

#include <ytlib/cypress_client/rpc_helpers.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(IClientRequestPtr request, ITransactionPtr transaction)
{
    NCypressClient::SetTransactionId(
        request,
        transaction ? transaction->GetId() : NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
