#pragma once

#include "public.h"

#include <yt/ytlib/transaction_client/transaction_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTransactionServiceProxy, RPC_PROXY_DESC(TransctionService));

    DEFINE_RPC_PROXY_METHOD(NProto, StartTransaction);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
