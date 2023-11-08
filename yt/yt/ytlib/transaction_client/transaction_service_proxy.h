#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_client/proto/transaction_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTransactionServiceProxy, TransactionService,
        .SetAcceptsBaggage(false));

    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
    DEFINE_RPC_PROXY_METHOD(NProto, RegisterTransactionActions);
    DEFINE_RPC_PROXY_METHOD(NProto, ReplicateTransactions);
    DEFINE_RPC_PROXY_METHOD(NProto, IssueLeases);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
