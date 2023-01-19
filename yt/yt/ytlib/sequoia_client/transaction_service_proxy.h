#pragma once

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransactionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TSequoiaTransactionServiceProxy, SequoiaTransactionService);

    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
