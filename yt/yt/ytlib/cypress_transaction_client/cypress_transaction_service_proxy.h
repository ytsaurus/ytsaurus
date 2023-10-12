#pragma once

#include "public.h"

#include <yt/yt/core/rpc/client.h>

namespace NYT::NCypressTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TCypressTransactionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TCypressTransactionServiceProxy, CypressTransactionService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, CommitTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, PingTransaction);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressTransactionClient
