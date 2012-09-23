#include "stdafx.h"
#include "transaction.h"

#include <ytlib/rpc/client.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

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
