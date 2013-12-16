#include "stdafx.h"
#include "rpc_helpers.h"
#include "transaction_manager.h"

#include <core/rpc/client.h>

#include <ytlib/cypress_client/rpc_helpers.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(IClientRequestPtr request, TTransactionPtr transaction)
{
    NCypressClient::SetTransactionId(
        request,
        transaction ? transaction->GetId() : NullTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

